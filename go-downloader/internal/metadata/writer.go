package metadata

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/unsplash_downloads/go-downloader/internal/config"
	"github.com/unsplash_downloads/go-downloader/internal/db"
)

// Record JSONL 记录
type Record struct {
	ImageURL   string `json:"image_url"`
	Resolution string `json:"resolution"`
	Timestamp  string `json:"timestamp"`
	ImageKey   string `json:"image_key"`
	// LocalPath 本地下载路径；去重跳过写 metadata 时删除（与 Python writer_loop 一致），不落 JSON。
	LocalPath string `json:"-"`
}

// Writer 元数据写入器
type Writer struct {
	cfg           *config.Config
	metadataRoot  string
	currentFile   *os.File
	currentWriter *bufio.Writer
	currentCount  int
	currentIndex  int
	mu            sync.Mutex
	recordChan    chan Record
	doneChan      chan struct{}
	flushEvery    int
	flushInterval time.Duration
	bloom         *PersistentBloom
	seenDB        *db.SeenDB
	// 按天单文件 metadata.jsonl（NewWriterForFile）：目录按 UTC 日归档，seen 仍全局
	category       string
	currentDateUTC string
	dailyPath      string
	fixedDailyFile bool
	onDayClosed    func(dateUTC, dailyPath string)
}

// NewWriter 创建新的元数据写入器（category 与 discover.yaml 一致）
func NewWriter(cfg *config.Config) (*Writer, error) {
	metadataRoot := filepath.Join(cfg.MetadataDir, cfg.Category)
	if err := os.MkdirAll(metadataRoot, 0755); err != nil {
		return nil, fmt.Errorf("创建元数据目录失败: %w", err)
	}

	writer := &Writer{
		cfg:           cfg,
		metadataRoot:  metadataRoot,
		recordChan:    make(chan Record, 10000), // 缓冲 10000 条记录
		doneChan:      make(chan struct{}),
		flushEvery:    maxInt(1, cfg.MetadataFlushEvery),
		flushInterval: time.Duration(maxInt(1, cfg.MetadataFlushIntervalSec)) * time.Second,
	}

	// 尝试恢复上次的 shard
	existing, err := filepath.Glob(filepath.Join(metadataRoot, "metadata_*.jsonl"))
	if err == nil && len(existing) > 0 {
		sort.Strings(existing)
		lastPath := existing[len(existing)-1]
		lines, err := countLines(lastPath)
		if err == nil && lines < cfg.MetadataLinesPerFile {
			writer.currentIndex = parseIndexFromFileName(lastPath)
			writer.currentCount = lines
			writer.currentFile, writer.currentWriter, err = openMetadataFile(lastPath)
			if err != nil {
				return nil, fmt.Errorf("恢复元数据文件失败: %w", err)
			}
		}
	}

	if writer.currentFile == nil {
		writer.currentIndex = 1
		writer.currentCount = 0
		newPath := filepath.Join(metadataRoot, fmt.Sprintf("metadata_%06d.jsonl", writer.currentIndex))
		writer.currentFile, writer.currentWriter, err = openMetadataFile(newPath)
		if err != nil {
			return nil, fmt.Errorf("创建元数据文件失败: %w", err)
		}
	}

	go writer.run()
	return writer, nil
}

// NewWriterForCategory 多 category 时为指定 category 创建元数据写入器（只写该 category 目录）
func NewWriterForCategory(metadataDir, categoryPlural string, linesPerFile int) (*Writer, error) {
	metadataRoot := filepath.Join(metadataDir, categoryPlural)
	if err := os.MkdirAll(metadataRoot, 0755); err != nil {
		return nil, fmt.Errorf("创建元数据目录失败: %w", err)
	}
	writer := &Writer{
		metadataRoot:  metadataRoot,
		recordChan:    make(chan Record, 10000),
		doneChan:      make(chan struct{}),
		flushEvery:    maxInt(1, 1000),
		flushInterval: 2 * time.Second,
	}
	writer.cfg = &config.Config{MetadataLinesPerFile: linesPerFile}

	existing, err := filepath.Glob(filepath.Join(metadataRoot, "metadata_*.jsonl"))
	if err == nil && len(existing) > 0 {
		sort.Strings(existing)
		lastPath := existing[len(existing)-1]
		lines, err := countLines(lastPath)
		if err == nil && lines < linesPerFile {
			writer.currentIndex = parseIndexFromFileName(lastPath)
			writer.currentCount = lines
			writer.currentFile, writer.currentWriter, err = openMetadataFile(lastPath)
			if err != nil {
				return nil, fmt.Errorf("恢复元数据文件失败: %w", err)
			}
		}
	}
	if writer.currentFile == nil {
		writer.currentIndex = 1
		writer.currentCount = 0
		newPath := filepath.Join(metadataRoot, fmt.Sprintf("metadata_%06d.jsonl", writer.currentIndex))
		var err error
		writer.currentFile, writer.currentWriter, err = openMetadataFile(newPath)
		if err != nil {
			return nil, fmt.Errorf("创建元数据文件失败: %w", err)
		}
	}
	go writer.run()
	return writer, nil
}

// NewWriterForFile 只写 daily：{project}/.../metadata/{UTC-date}/metadata.jsonl，每目录单文件。
// UTC 跨日时关闭旧文件并打开新日期路径；seen_db / Bloom 仍为每 category 全局去重。
// onDayClosed 在旧日文件关闭并已 flush 后调用（可用于将该日文件上传到 S3），可为 nil。
func NewWriterForFile(cfg *config.Config, category string, seenDB *db.SeenDB, onDayClosed func(dateUTC, dailyPath string)) (*Writer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("NewWriterForFile 需要非 nil cfg")
	}
	dateUTC := time.Now().UTC().Format("2006-01-02")
	dailyPath := cfg.ExpandPathWithDate(cfg.MetadataDailyPathTemplate, category, dateUTC)
	metadataRoot := filepath.Dir(dailyPath)
	if err := os.MkdirAll(metadataRoot, 0755); err != nil {
		return nil, fmt.Errorf("创建元数据目录失败: %w", err)
	}
	flushEvery := maxInt(1, cfg.MetadataFlushEvery)
	flushInterval := time.Duration(maxInt(1, cfg.MetadataFlushIntervalSec)) * time.Second
	writer := &Writer{
		metadataRoot:   metadataRoot,
		recordChan:     make(chan Record, 10000),
		doneChan:       make(chan struct{}),
		cfg:            cfg,
		flushEvery:     flushEvery,
		flushInterval:  flushInterval,
		seenDB:         seenDB,
		category:       category,
		currentDateUTC: dateUTC,
		dailyPath:      dailyPath,
		fixedDailyFile: true,
		onDayClosed:    onDayClosed,
	}
	if cfg.MetadataBloomEnable && strings.TrimSpace(cfg.MetadataBloomPathTemplate) != "" {
		// 模板通常不含 {date}，全局 Bloom；若含 {date} 则固定用启动日路径避免按日换 Bloom 文件
		bloomPath := cfg.ExpandPathWithDate(cfg.MetadataBloomPathTemplate, category, dateUTC)
		bloom, err := OpenBloom(bloomPath, cfg.MetadataBloomBits, cfg.MetadataBloomHashes, cfg.MetadataBloomFlushSec)
		if err != nil {
			return nil, fmt.Errorf("打开 metadata bloom: %w", err)
		}
		writer.bloom = bloom
	}
	f, bw, err := openMetadataFile(dailyPath)
	if err != nil {
		if writer.bloom != nil {
			_ = writer.bloom.Close()
		}
		return nil, fmt.Errorf("打开元数据文件失败: %w", err)
	}
	writer.currentFile = f
	writer.currentWriter = bw
	writer.currentIndex = 1
	writer.currentCount = 0
	go writer.run()
	return writer, nil
}

// WriteRecord 写入一条记录
func (w *Writer) WriteRecord(record Record) {
	w.recordChan <- record
}

// flushAndSync 刷新并同步到磁盘（需在持锁时调用）
func (w *Writer) flushAndSync() error {
	if w.currentWriter == nil || w.currentFile == nil {
		return nil
	}
	if err := w.currentWriter.Flush(); err != nil {
		return err
	}
	return w.currentFile.Sync()
}

// Close 关闭写入器
func (w *Writer) Close() {
	close(w.recordChan)
	<-w.doneChan // 等待所有记录写入完成
	w.mu.Lock()
	// 进程退出前上传「当前仍打开的」daily 文件（日期/路径与 writer 一致，而非仅 time.Now()）
	if w.fixedDailyFile && w.onDayClosed != nil && w.currentDateUTC != "" && strings.TrimSpace(w.dailyPath) != "" {
		if err := w.flushAndSync(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: 关闭前元数据 Flush/Sync 失败: %v\n", err)
		}
		date := w.currentDateUTC
		path := w.dailyPath
		cb := w.onDayClosed
		w.mu.Unlock()
		cb(date, path)
		w.mu.Lock()
	}
	if w.currentWriter != nil {
		if err := w.flushAndSync(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: 关闭时元数据 Flush/Sync 失败: %v\n", err)
		}
		_ = w.currentFile.Close()
		w.currentFile = nil
		w.currentWriter = nil
	}
	if w.bloom != nil {
		if err := w.bloom.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: 关闭 bloom 失败: %v\n", err)
		}
		w.bloom = nil
	}
	w.mu.Unlock()
}

func (w *Writer) run() {
	defer close(w.doneChan)
	lastFlush := time.Now()
	for record := range w.recordChan {
		w.mu.Lock()
		// 按 UTC 日轮换 daily 文件（与「本条写入日」一致）
		if w.fixedDailyFile && w.category != "" {
			today := time.Now().UTC().Format("2006-01-02")
			if w.currentDateUTC != "" && today != w.currentDateUTC {
				oldDate := w.currentDateUTC
				oldPath := w.dailyPath
				if err := w.flushAndSync(); err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: UTC 换日前元数据 Flush/Sync 失败: %v\n", err)
				}
				if w.currentFile != nil {
					_ = w.currentFile.Close()
					w.currentFile = nil
					w.currentWriter = nil
				}
				cb := w.onDayClosed
				w.mu.Unlock()
				if cb != nil {
					cb(oldDate, oldPath)
				}
				w.mu.Lock()
				w.currentDateUTC = today
				w.dailyPath = w.cfg.ExpandPathWithDate(w.cfg.MetadataDailyPathTemplate, w.category, today)
				if err := os.MkdirAll(filepath.Dir(w.dailyPath), 0755); err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: 创建新日 metadata 目录失败: %v\n", err)
					w.mu.Unlock()
					continue
				}
				nf, nw, err := openMetadataFile(w.dailyPath)
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: 打开新日 metadata 文件失败: %v\n", err)
					w.mu.Unlock()
					continue
				}
				w.currentFile, w.currentWriter = nf, nw
				w.currentCount = 0
			}
		}
		key := SeenDedupeKey(w.cfg, record.ImageURL)
		if key == "" {
			if record.LocalPath != "" {
				_ = os.Remove(record.LocalPath)
			}
			w.mu.Unlock()
			continue
		}
		if w.bloom != nil {
			if !w.bloom.MightContain(key) {
				w.bloom.Add(key)
			}
		}
		writeIt := true
		if w.seenDB != nil {
			claimed, err := w.seenDB.Claim(key)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: metadata seen_db claim 失败: %v\n", err)
				writeIt = false
			} else if !claimed {
				writeIt = false
			}
		}
		if !writeIt {
			if record.LocalPath != "" {
				_ = os.Remove(record.LocalPath)
			}
			w.mu.Unlock()
			continue
		}
		if !w.fixedDailyFile && w.currentCount >= w.cfg.MetadataLinesPerFile {
			if err := w.flushAndSync(); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: 刷新元数据文件失败: %v\n", err)
			}
			newIndex := w.currentIndex + 1
			newPath := filepath.Join(w.metadataRoot, fmt.Sprintf("metadata_%06d.jsonl", newIndex))
			newFile, newWriter, err := openMetadataFile(newPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: 切换元数据文件失败 %s: %v，继续写入当前文件\n", newPath, err)
			} else {
				w.currentFile.Close()
				w.currentFile, w.currentWriter = newFile, newWriter
				w.currentIndex = newIndex
				w.currentCount = 0
			}
		}

		// crawl_hash：image_key = <prefix>/<basename>（与 extract 一致）。其它 fixed daily：prefix/UTC 日/basename。
		if w.fixedDailyFile && w.cfg != nil && strings.TrimSpace(record.LocalPath) != "" {
			prefix := strings.Trim(strings.TrimSpace(w.cfg.ImageKeyPrefix), "/")
			base := filepath.Base(record.LocalPath)
			if base == "" || base == "." {
				// 保持 WriteRecord 传入的 ImageKey
			} else if strings.EqualFold(strings.TrimSpace(w.cfg.ImageKeyStyle), "crawl_hash") {
				if prefix != "" {
					record.ImageKey = prefix + "/" + base
				}
			} else if prefix != "" && w.currentDateUTC != "" {
				record.ImageKey = fmt.Sprintf("%s/%s/%s", prefix, w.currentDateUTC, base)
			}
		}

		data, err := json.Marshal(record)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: 序列化元数据失败: %v\n", err)
			w.mu.Unlock()
			continue
		}
		if _, err := w.currentWriter.Write(data); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: 写元数据失败(Write): %v\n", err)
			w.mu.Unlock()
			continue
		}
		if _, err := w.currentWriter.WriteString("\n"); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: 写元数据失败(WriteString): %v\n", err)
			w.mu.Unlock()
			continue
		}
		w.currentCount++
		if w.currentCount%w.flushEvery == 0 || time.Since(lastFlush) >= w.flushInterval {
			if err := w.flushAndSync(); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: 元数据 Flush/Sync 失败: %v\n", err)
			}
			lastFlush = time.Now()
		}
		w.mu.Unlock()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentWriter != nil {
		if err := w.flushAndSync(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: run 结束前元数据 Flush/Sync 失败: %v\n", err)
		}
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func openMetadataFile(path string) (*os.File, *bufio.Writer, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, err
	}
	writer := bufio.NewWriterSize(file, 64*1024) // 64KB 缓冲
	return file, writer, nil
}

func countLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		count++
	}
	return count, scanner.Err()
}

func parseIndexFromFileName(filePath string) int {
	base := filepath.Base(filePath)
	parts := strings.Split(base, "_")
	if len(parts) == 2 {
		idxStr := strings.TrimSuffix(parts[1], ".jsonl")
		var idx int
		if _, err := fmt.Sscanf(idxStr, "%d", &idx); err == nil {
			return idx
		}
	}
	return 1
}
