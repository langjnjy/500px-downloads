package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const teeChanBuf = 512

// teeLogToFile 将 stdout/stderr 同时写入终端和 logPath。写入文件时每行前加 [2006-01-02 15:04:05]。
func teeLogToFile(logPath string) (cleanup func()) {
	if logPath == "" {
		return func() {}
	}
	dir := filepath.Dir(logPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return func() {}
	}
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return func() {}
	}
	var fMu sync.Mutex

	writeLineToFile := func(line []byte) {
		if len(line) == 0 {
			return
		}
		ts := time.Now().Format("2006-01-02 15:04:05")
		fMu.Lock()
		if _, err := fmt.Fprintf(f, "[%s] %s\n", ts, bytes.TrimRight(line, "\r\n")); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: 写 tee 日志失败: %v\n", err)
		}
		fMu.Unlock()
	}

	tee := func(src **os.File) (closeW func(), wait func()) {
		r, w, err := os.Pipe()
		if err != nil {
			return func() {}, func() {}
		}
		orig := *src
		*src = w
		ch := make(chan []byte, teeChanBuf)
		var wg sync.WaitGroup
		wg.Add(2)
		var lineBuf []byte
		go func() {
			defer wg.Done()
			buf := make([]byte, 32*1024)
			for {
				n, err := r.Read(buf)
				if n > 0 {
					lineBuf = append(lineBuf, buf[:n]...)
					for {
						idx := bytes.IndexByte(lineBuf, '\n')
						if idx < 0 {
							break
						}
						line := lineBuf[:idx]
						lineBuf = lineBuf[idx+1:]
						writeLineToFile(line)
					}
					cp := make([]byte, n)
					copy(cp, buf[:n])
					select {
					case ch <- cp:
					default:
					}
				}
				if err != nil {
					if len(lineBuf) > 0 {
						writeLineToFile(lineBuf)
					}
					close(ch)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			for b := range ch {
				if orig != nil {
					if _, err := orig.Write(b); err != nil {
						fmt.Fprintf(os.Stderr, "ERROR: tee 写 stdout/stderr 失败: %v\n", err)
					}
				}
			}
		}()
		return func() { w.Close() }, wg.Wait
	}

	closeStdout, waitStdout := tee(&os.Stdout)
	closeStderr, waitStderr := tee(&os.Stderr)
	return func() {
		closeStdout()
		closeStderr()
		waitStdout()
		waitStderr()
		if err := f.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: 关闭 tee 日志文件失败: %v\n", err)
		}
	}
}
