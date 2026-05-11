package db

import (
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// SeenDB 用于跟踪已处理的 object_id
type SeenDB struct {
	db   *sql.DB
	mu   sync.Mutex
	path string
}

// NewSeenDB 创建并初始化 SeenDB
func NewSeenDB(dbPath string) (*SeenDB, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_journal_mode=WAL&_synchronous=NORMAL&_busy_timeout=30000", dbPath))
	if err != nil {
		return nil, fmt.Errorf("打开 seen 数据库失败: %w", err)
	}

	// 设置连接池（优化：根据高并发场景增加连接数）
	db.SetMaxOpenConns(100) // 最大打开连接数（从 25 增加到 100，支持高并发）
	db.SetMaxIdleConns(20)  // 最大空闲连接数（从 5 增加到 20）
	db.SetConnMaxLifetime(5 * time.Minute)

	_, err = db.Exec("CREATE TABLE IF NOT EXISTS seen (object_id TEXT PRIMARY KEY)")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("创建 seen 表失败: %w", err)
	}

	return &SeenDB{db: db, path: dbPath}, nil
}

func objectIDFromKey(key string) string {
	sum := sha1.Sum([]byte(key))
	return hex.EncodeToString(sum[:])
}

// Claim 与 Python SeenDB.claim 一致：对 key 做 sha1_hex 后插入。
func (s *SeenDB) Claim(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := objectIDFromKey(key)

	res, err := s.db.Exec("INSERT OR IGNORE INTO seen(object_id) VALUES (?)", oid)
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

	_, err := s.db.Exec("DELETE FROM seen WHERE object_id=?", oid)
	if err != nil {
		return fmt.Errorf("seen_db 释放失败: %w", err)
	}
	return nil
}

// Contains 判断 key（经 sha1_hex）是否已存在。
func (s *SeenDB) Contains(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := objectIDFromKey(key)
	var one int
	err := s.db.QueryRow("SELECT 1 FROM seen WHERE object_id=? LIMIT 1", oid).Scan(&one)
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
