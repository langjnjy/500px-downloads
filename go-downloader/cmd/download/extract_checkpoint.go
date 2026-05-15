package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// extractCheckpoint 按行序号有序推进断点：仅当连续前缀行全部处理完成后才持久化 offset。
type extractCheckpoint struct {
	mu              sync.Mutex
	path            string
	readFile        string
	interval        int
	nextLineIndex   int64
	pending         map[int64]int64
	linesSinceSave  int
	lastSavedOffset int64
}

func newExtractCheckpoint(path, readFile string, interval int, startLineIndex int64) *extractCheckpoint {
	return &extractCheckpoint{
		path:          path,
		readFile:      readFile,
		interval:      interval,
		nextLineIndex: startLineIndex,
		pending:       make(map[int64]int64),
	}
}

func (c *extractCheckpoint) complete(lineIndex, byteOffset int64) {
	if c == nil || c.interval <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending[lineIndex] = byteOffset
	for {
		off, ok := c.pending[c.nextLineIndex]
		if !ok {
			return
		}
		delete(c.pending, c.nextLineIndex)
		c.nextLineIndex++
		c.linesSinceSave++
		c.lastSavedOffset = off
		if c.linesSinceSave >= c.interval {
			_ = saveExtractCheckpoint(c.path, c.readFile, off, c.nextLineIndex)
			c.linesSinceSave = 0
		}
	}
}

func (c *extractCheckpoint) flushFinal() {
	if c == nil || c.interval <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastSavedOffset > 0 || c.nextLineIndex > 0 {
		_ = saveExtractCheckpoint(c.path, c.readFile, c.lastSavedOffset, c.nextLineIndex)
	}
}

func loadExtractCheckpoint(checkpointPath string) (filePath string, offset, nextLineIndex int64, ok bool) {
	f, err := os.Open(checkpointPath)
	if err != nil {
		return "", 0, 0, false
	}
	defer f.Close()
	r := bufio.NewReader(f)
	pathLine, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", 0, 0, false
	}
	pathLine = strings.TrimSpace(pathLine)
	if pathLine == "" {
		return "", 0, 0, false
	}
	offsetLine, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", 0, 0, false
	}
	offsetLine = strings.TrimSpace(offsetLine)
	off, err := strconv.ParseInt(offsetLine, 10, 64)
	if err != nil || off < 0 {
		return "", 0, 0, false
	}
	indexLine, err := r.ReadString('\n')
	nextLineIndex = 0
	if err == nil || err == io.EOF {
		indexLine = strings.TrimSpace(indexLine)
		if indexLine != "" {
			if idx, e := strconv.ParseInt(indexLine, 10, 64); e == nil && idx >= 0 {
				nextLineIndex = idx
			}
		}
	}
	if _, err := os.Stat(pathLine); err != nil {
		return "", 0, 0, false
	}
	return pathLine, off, nextLineIndex, true
}

func saveExtractCheckpoint(checkpointPath, filePath string, offset, nextLineIndex int64) error {
	dir := filepath.Dir(checkpointPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmpPath := checkpointPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%s\n%d\n%d\n", filePath, offset, nextLineIndex)
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, checkpointPath)
}
