package metadata

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// PersistentBloom 与 scripts/download.py 的 PersistentBloom 一致：位图 mmap(MAP_SHARED) + 周期性 msync。
type PersistentBloom struct {
	mu        sync.Mutex
	f         *os.File
	data      []byte // mmap 视图
	bits      int64
	hashes    int
	flushSec  int
	lastFlush time.Time
}

// OpenBloom 打开或创建 bloom 文件并截断为 (bits+7)/8 字节，再 mmap。
func OpenBloom(path string, bits int64, hashes int, flushSec int) (*PersistentBloom, error) {
	if bits <= 0 {
		return nil, fmt.Errorf("metadata bloom: bits 必须 > 0")
	}
	if hashes <= 0 {
		hashes = 7
	}
	size := int64((bits + 7) / 8)
	if size > int64(^uint(0)>>1) {
		return nil, fmt.Errorf("metadata bloom: 文件过大")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	if st, err := f.Stat(); err != nil {
		f.Close()
		return nil, err
	} else if st.Size() != size {
		if err := f.Truncate(size); err != nil {
			f.Close()
			return nil, err
		}
	}
	n := int(size)
	data, err := unix.Mmap(int(f.Fd()), 0, n, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("metadata bloom mmap: %w", err)
	}
	return &PersistentBloom{
		f:        f,
		data:     data,
		bits:     bits,
		hashes:   hashes,
		flushSec: flushSec,
	}, nil
}

func (b *PersistentBloom) indices(key string) []uint64 {
	h := sha1.Sum([]byte(key))
	a := binary.BigEndian.Uint64(h[0:8])
	c := binary.BigEndian.Uint64(h[8:16])
	if c == 0 {
		c = 0x9E3779B97F4A7C15
	}
	m := uint64(b.bits)
	out := make([]uint64, b.hashes)
	for i := 0; i < b.hashes; i++ {
		out[i] = (a + uint64(i)*c) % m
	}
	return out
}

// MightContain 与 Python might_contain 一致。
func (b *PersistentBloom) MightContain(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, idx := range b.indices(key) {
		byteI := idx >> 3
		bit := byte(1 << (idx & 7))
		if (b.data[byteI] & bit) == 0 {
			return false
		}
	}
	return true
}

// Add 与 Python add 一致。
func (b *PersistentBloom) Add(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, idx := range b.indices(key) {
		byteI := idx >> 3
		bit := byte(1 << (idx & 7))
		b.data[byteI] |= bit
	}
	b.maybeFlushLocked()
}

func (b *PersistentBloom) maybeFlushLocked() {
	if b.flushSec <= 0 || len(b.data) == 0 {
		return
	}
	now := time.Now()
	if now.Sub(b.lastFlush) < time.Duration(b.flushSec)*time.Second {
		return
	}
	_ = unix.Msync(b.data, unix.MS_SYNC)
	b.lastFlush = now
}

// Close 关闭 bloom（msync + munmap + 关闭文件）。
func (b *PersistentBloom) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.data) > 0 {
		_ = unix.Msync(b.data, unix.MS_SYNC)
		_ = unix.Munmap(b.data)
		b.data = nil
	}
	if b.f == nil {
		return nil
	}
	err := b.f.Close()
	b.f = nil
	return err
}
