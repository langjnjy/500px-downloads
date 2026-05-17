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

// StatusPendingUpscale 表示 metadata_large 批中已跳过、待 metadata_small 批再走下载+CV2 的小图（元数据已判低于阈值）。
const StatusPendingUpscale = "pending_upscale"

// StatusPendingLarge 表示 metadata_small 批中已跳过、待 metadata_large 批再直下的大图（元数据已判满足阈值）。
const StatusPendingLarge = "pending_large"

// SeenDB 用于跟踪已处理的 URL（去重键的 sha1 为 object_id），并记录 image_url、status、route、detail。
// route 仅 large_direct / cv2_upscale；成功与失败由 status 区分；detail 存失败原因、pending 时的 WxH 等辅助信息。
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
		{"route", `ALTER TABLE seen ADD COLUMN route TEXT`},
		{"detail", `ALTER TABLE seen ADD COLUMN detail TEXT`},
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
	if err := migrateSeenLegacyResolution(db); err != nil {
		db.Close()
		return nil, err
	}

	return &SeenDB{db: db, path: dbPath}, nil
}

func seenTableHasColumn(db *sql.DB, col string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(seen)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(strings.TrimSpace(name), col) {
			return true, nil
		}
	}
	return false, rows.Err()
}

// migrateSeenLegacyResolution 将旧列 resolution 拆成 route + detail，并尽量 DROP resolution。
func migrateSeenLegacyResolution(db *sql.DB) error {
	hasRes, err := seenTableHasColumn(db, "resolution")
	if err != nil {
		return err
	}
	if !hasRes {
		return nil
	}
	stmts := []string{
		`UPDATE seen SET route = lower(trim(resolution)), detail = '' WHERE lower(trim(coalesce(status,''))) = 'ok' AND (route IS NULL OR trim(coalesce(route,'')) = '') AND lower(trim(resolution)) IN ('large_direct', 'cv2_upscale')`,
		`UPDATE seen SET route = 'large_direct', detail = CASE WHEN instr(resolution, '|') > 0 THEN substr(resolution, instr(resolution, '|') + 1) ELSE '' END WHERE lower(trim(coalesce(status,''))) = 'failed' AND (route IS NULL OR trim(coalesce(route,'')) = '') AND lower(trim(resolution)) LIKE 'large_direct%'`,
		`UPDATE seen SET route = 'cv2_upscale', detail = CASE WHEN instr(resolution, '|') > 0 THEN substr(resolution, instr(resolution, '|') + 1) ELSE '' END WHERE lower(trim(coalesce(status,''))) = 'failed' AND (route IS NULL OR trim(coalesce(route,'')) = '') AND lower(trim(resolution)) LIKE 'cv2_upscale%'`,
		`UPDATE seen SET route = 'cv2_upscale', detail = trim(substr(resolution, 13)) WHERE lower(trim(coalesce(status,''))) = 'pending_upscale' AND (route IS NULL OR trim(coalesce(route,'')) = '') AND lower(trim(resolution)) LIKE 'pending_cv2:%'`,
		`UPDATE seen SET route = 'large_direct', detail = trim(substr(resolution, 15)) WHERE lower(trim(coalesce(status,''))) = 'pending_large' AND (route IS NULL OR trim(coalesce(route,'')) = '') AND lower(trim(resolution)) LIKE 'pending_large:%'`,
		`UPDATE seen SET route = 'cv2_upscale', detail = trim(resolution) WHERE lower(trim(coalesce(status,''))) = 'pending_upscale' AND (route IS NULL OR trim(coalesce(route,'')) = '') AND trim(coalesce(resolution,'')) != '' AND lower(trim(resolution)) NOT LIKE 'pending_cv2:%'`,
		`UPDATE seen SET route = 'large_direct', detail = trim(resolution) WHERE lower(trim(coalesce(status,''))) = 'pending_large' AND (route IS NULL OR trim(coalesce(route,'')) = '') AND trim(coalesce(resolution,'')) != '' AND lower(trim(resolution)) NOT LIKE 'pending_large:%'`,
		`UPDATE seen SET route = 'cv2_upscale', detail = trim(coalesce(resolution,'')) WHERE (route IS NULL OR trim(coalesce(route,'')) = '') AND trim(coalesce(resolution,'')) != '' AND lower(trim(coalesce(status,''))) IN ('failed', 'pending_upscale', 'pending_large')`,
	}
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("migrate seen legacy resolution: %w", err)
		}
	}
	if _, err := db.Exec(`ALTER TABLE seen DROP COLUMN resolution`); err != nil {
		// SQLite < 3.35 不支持 DROP COLUMN：保留旧列，应用只写 route/detail
	}
	return nil
}

func objectIDFromKey(key string) string {
	sum := sha1.Sum([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Upsert 写入或更新一条 seen（重试成功后 status 可覆盖为 ok）。
func (s *SeenDB) Upsert(dedupeKey, imageURL, status, route, detail string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := objectIDFromKey(dedupeKey)
	_, err := s.db.Exec(`
INSERT INTO seen(object_id, image_url, status, route, detail) VALUES(?,?,?,?,?)
ON CONFLICT(object_id) DO UPDATE SET
  image_url=excluded.image_url,
  status=excluded.status,
  route=excluded.route,
  detail=excluded.detail
`, oid, imageURL, status, route, detail)
	if err != nil {
		return fmt.Errorf("seen upsert: %w", err)
	}
	return nil
}

// FailedRow seen 表中按 image_url 列出的一行（failed / pending_* 复用）。
type FailedRow struct {
	ImageURL string
	Route    string
	Detail   string
}

// ListFailed 返回所有 status 为 failed 且 image_url 非空的记录。
func (s *SeenDB) ListFailed() ([]FailedRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`
SELECT image_url, route, detail FROM seen
WHERE lower(trim(coalesce(status,'')))='failed'
  AND trim(coalesce(image_url,''))!=''
ORDER BY rowid`)
	if err != nil {
		return nil, fmt.Errorf("list failed: %w", err)
	}
	defer rows.Close()
	var out []FailedRow
	for rows.Next() {
		var url, route, det sql.NullString
		if err := rows.Scan(&url, &route, &det); err != nil {
			return nil, fmt.Errorf("scan failed row: %w", err)
		}
		out = append(out, FailedRow{
			ImageURL: strings.TrimSpace(url.String),
			Route:    strings.TrimSpace(route.String),
			Detail:   strings.TrimSpace(det.String),
		})
	}
	return out, rows.Err()
}

// CountFailed 统计 status=failed 的行数。
func (s *SeenDB) CountFailed() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int
	err := s.db.QueryRow(`
SELECT COUNT(*) FROM seen
WHERE lower(trim(coalesce(status,'')))='failed'
  AND trim(coalesce(image_url,''))!=''`).Scan(&n)
	return n, err
}

// IsFailed 当且仅当 status 为 failed。
func (s *SeenDB) IsFailed(key string) (bool, error) {
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
	return strings.EqualFold(strings.TrimSpace(st.String), "failed"), nil
}

// IsPendingUpscale 当且仅当 status 为 pending_upscale（与 StatusPendingUpscale 忽略大小写比较）。
func (s *SeenDB) IsPendingUpscale(key string) (bool, error) {
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
	return strings.EqualFold(strings.TrimSpace(st.String), StatusPendingUpscale), nil
}

// ListPendingUpscale 返回 status=pending_upscale 且 image_url 非空的记录（供 metadata_small 批启动时排空）。
func (s *SeenDB) ListPendingUpscale() ([]FailedRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`
SELECT image_url, route, detail FROM seen
WHERE lower(trim(coalesce(status,'')))=lower(?)
  AND trim(coalesce(image_url,''))!=''
ORDER BY rowid`, StatusPendingUpscale)
	if err != nil {
		return nil, fmt.Errorf("list pending_upscale: %w", err)
	}
	defer rows.Close()
	var out []FailedRow
	for rows.Next() {
		var url, route, det sql.NullString
		if err := rows.Scan(&url, &route, &det); err != nil {
			return nil, fmt.Errorf("scan pending_upscale row: %w", err)
		}
		out = append(out, FailedRow{
			ImageURL: strings.TrimSpace(url.String),
			Route:    strings.TrimSpace(route.String),
			Detail:   strings.TrimSpace(det.String),
		})
	}
	return out, rows.Err()
}

// IsPendingLarge 当且仅当 status 为 pending_large。
func (s *SeenDB) IsPendingLarge(key string) (bool, error) {
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
	return strings.EqualFold(strings.TrimSpace(st.String), StatusPendingLarge), nil
}

// ListPendingLarge 返回 status=pending_large 且 image_url 非空的记录（供 metadata_large 批启动时排空）。
func (s *SeenDB) ListPendingLarge() ([]FailedRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.Query(`
SELECT image_url, route, detail FROM seen
WHERE lower(trim(coalesce(status,'')))=lower(?)
  AND trim(coalesce(image_url,''))!=''
ORDER BY rowid`, StatusPendingLarge)
	if err != nil {
		return nil, fmt.Errorf("list pending_large: %w", err)
	}
	defer rows.Close()
	var out []FailedRow
	for rows.Next() {
		var url, route, det sql.NullString
		if err := rows.Scan(&url, &route, &det); err != nil {
			return nil, fmt.Errorf("scan pending_large row: %w", err)
		}
		out = append(out, FailedRow{
			ImageURL: strings.TrimSpace(url.String),
			Route:    strings.TrimSpace(route.String),
			Detail:   strings.TrimSpace(det.String),
		})
	}
	return out, rows.Err()
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
