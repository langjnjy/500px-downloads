package db

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// SeenDB 用于跟踪已处理的 URL（去重键的 sha1 为 object_id），并记录 image_url、status、resolution。
type SeenDB struct {
	db   *sql.DB
	mu   sync.Mutex
	path string
}

func migrateSeen(db *sql.DB) error {
	rows, err := db.Query(`PRAGMA table_info(seen)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	have := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return err
		}
		have[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	adds := []struct {
		name string
		ddl  string
	}{
		{"image_url", `ALTER TABLE seen ADD COLUMN image_url TEXT`},
		{"status", `ALTER TABLE seen ADD COLUMN status TEXT`},
		{"resolution", `ALTER TABLE seen ADD COLUMN resolution TEXT`},
	}
	for _, a := range adds {
		if have[a.name] {
			continue
		}
		if _, err := db.Exec(a.ddl); err != nil {
			return fmt.Errorf("migrate seen add %s: %w", a.name, err)
		}
	}
	return nil
}

// NewSeenDB 创建并初始化 SeenDB（兼容仅有 object_id 的旧库，自动补列）。
func NewSeenDB(dbPath string) (*SeenDB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=30000", dbPath))
	if err != nil {
		return nil, fmt.Errorf("打开 seen 数据库失败: %w", err)
	}

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(5 * time.Minute)

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS seen (object_id TEXT PRIMARY KEY)`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("创建 seen 表失败: %w", err)
	}
	if err := migrateSeen(db); err != nil {
		db.Close()
		return nil, err
	}

	return &SeenDB{db: db, path: dbPath}, nil
}

func objectIDFromKey(key string) string {
	sum := sha1.Sum([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Upsert 写入或更新一条 seen（重试成功后 status 可覆盖为 ok）。
func (s *SeenDB) Upsert(dedupeKey, imageURL, status, resolution string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := objectIDFromKey(dedupeKey)
	_, err := s.db.Exec(`
INSERT INTO seen(object_id, image_url, status, resolution) VALUES(?,?,?,?)
ON CONFLICT(object_id) DO UPDATE SET
  image_url=excluded.image_url,
  status=excluded.status,
  resolution=excluded.resolution
`, oid, imageURL, status, resolution)
	if err != nil {
		return fmt.Errorf("seen upsert: %w", err)
	}
	return nil
}

// IsOK 当且仅当 status 为 ok（忽略大小写与空白）。
func (s *SeenDB) IsOK(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := objectIDFromKey(key)
	var st sql.NullString
	err := s.db.QueryRow(`SELECT status FROM seen WHERE object_id=?`, oid).Scan(&st)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.Valid {
		return false, nil
	}
	return strings.EqualFold(strings.TrimSpace(st.String), "ok"), nil
}

// Claim 与 Python SeenDB.claim 一致：对 key 做 sha1_hex 后插入（仅当不存在时成功）。
// 用于 metadata writer 内去重；与 Upsert 可共用同一表。
func (s *SeenDB) Claim(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := objectIDFromKey(key)

	res, err := s.db.Exec(`INSERT OR IGNORE INTO seen(object_id) VALUES (?)`, oid)
	if err != nil {
		return false, fmt.Errorf("seen_db 声明失败: %w", err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("获取 rows affected 失败: %w", err)
	}
	return rowsAffected == 1, nil
}

// Release 释放 key 对应的 object_id。
func (s *SeenDB) Release(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := objectIDFromKey(key)

	_, err := s.db.Exec(`DELETE FROM seen WHERE object_id=?`, oid)
	if err != nil {
		return fmt.Errorf("seen_db 释放失败: %w", err)
	}
	return nil
}

// Contains 判断是否存在该行（任意 status）。
func (s *SeenDB) Contains(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := objectIDFromKey(key)
	var one int
	err := s.db.QueryRow(`SELECT 1 FROM seen WHERE object_id=? LIMIT 1`, oid).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Close 关闭数据库连接
func (s *SeenDB) Close() error {
	return s.db.Close()
}
