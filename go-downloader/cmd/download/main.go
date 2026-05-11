package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/unsplash_downloads/go-downloader/internal/config"
	"github.com/unsplash_downloads/go-downloader/internal/db"
	"github.com/unsplash_downloads/go-downloader/internal/downloader"
	"github.com/unsplash_downloads/go-downloader/internal/metadata"
)

var deltaDateRe = regexp.MustCompile(`_urls_delta_(\d{2}|\d{4})-(\d{2})-(\d{2})(?:_shard)?$`)

var (
	s3ClientMu    sync.Mutex
	s3ClientCache = map[string]*s3.S3{}
)

// sessionLog 写入 output/logs/download/ 下的带时间戳日志
type sessionLog struct {
	f *os.File
}

func (s *sessionLog) log(msg string) {
	if s == nil || s.f == nil {
		return
	}
	ts := time.Now().Format("2006-01-02 15:04:05")
	if _, err := fmt.Fprintf(s.f, "[%s] %s\n", ts, msg); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: 写 session 日志失败: %v\n", err)
	}
	if err := s.f.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Sync session 日志失败: %v\n", err)
	}
}

func openSessionLog(projectRoot string) (*sessionLog, error) {
	logDir := filepath.Join(projectRoot, "output", "logs", "download")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("download_%s.log", time.Now().Format("2006-01-02_150405"))
	f, err := os.Create(filepath.Join(logDir, name))
	if err != nil {
		return nil, err
	}
	return &sessionLog{f: f}, nil
}

var (
	configPath = flag.String("config", "", "配置文件路径；未指定时优先选用 go-downloader/config/download-http.yaml（在仓库根目录运行时），其次为同目录下 config/download-http.yaml、再为仓库根 config/download.yaml（兼容旧布局）")
)

func main() {
	flag.Parse()

	// 确定配置文件路径
	configFile := *configPath
	if configFile == "" {
		exeDir := filepath.Dir(os.Args[0])
		// go-downloader 默认使用本模块下的 download-http.yaml（与仓库根 config/download.yaml 解耦）
		possiblePaths := []string{
			"go-downloader/config/download-http.yaml", // 当前目录为仓库根
			"config/download-http.yaml",               // 当前目录为 go-downloader
			filepath.Join(exeDir, "../config/download-http.yaml"), // 二进制在 go-downloader/cmd/download 等
			filepath.Join(exeDir, "config/download-http.yaml"),    // 二进制放在 go-downloader/ 下
			// 兼容：仅存在旧配置时仍可读仓库根 config/download.yaml
			"config/download.yaml",
			"go-downloader/config/download.yaml",
			filepath.Join(exeDir, "../config/download.yaml"),
		}
		for _, path := range possiblePaths {
			if _, err := os.Stat(path); err == nil {
				configFile = path
				break
			}
		}
		if configFile == "" {
			configFile = "go-downloader/config/download-http.yaml"
		}
	}

	// 加载配置
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}
	if abs, err := filepath.Abs(configFile); err == nil {
		fmt.Fprintf(os.Stderr, "使用配置: %s\n", abs)
	}
	downloader.SetSkipSuffixes(cfg.SkipSuffixes)
	slog, err := openSessionLog(cfg.ProjectRoot)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建日志文件失败: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if slog != nil && slog.f != nil {
			slog.f.Close()
		}
	}()

	// 单 category 模式：include/exclude 仍生效（与多 category 一致）
	if cfg.ExtractDir == "" && !cfg.CategorySelected(cfg.Category) {
		fmt.Fprintf(os.Stderr, "category %s 与 include_categories/exclude_categories 冲突\n", cfg.Category)
		os.Exit(1)
	}

	if cfg.ExtractDir != "" {
		runMultiCategory(cfg, slog)
		return
	}
	if len(cfg.IncludeCategories) > 1 {
		runMultiCategory(cfg, slog)
		return
	}

	// 单 category：retry_failed 模式从 failed_urls 读；正常模式仅消费 download/urls 下的 shards
	inputFile := cfg.Input
	if cfg.RetryFailed {
		inputFile = cfg.ExpandPath(cfg.FailedURLsPathTemplate, cfg.Category)
	}
	inputStat, inputErr := os.Stat(inputFile)
	inputExists := inputErr == nil && !inputStat.IsDir()
	if cfg.RetryFailed && !inputExists {
		fmt.Fprintf(os.Stderr, "输入文件不存在: %s\n", inputFile)
		os.Exit(2)
	}

	// 创建输出目录
	category := cfg.Category
	outputDir := cfg.OutputDir
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "创建输出目录失败: %v\n", err)
		os.Exit(2)
	}

	// 清理孤儿 .part 文件
	cleanupOrphanPartFiles(outputDir, 3600, slog)

	// 初始化 seen DB
	var seenDB *db.SeenDB
	if cfg.SeenDB.Enable {
		seenDBPath := cfg.SeenDB.Path
		if seenDBPath == "" {
			seenDBPath = filepath.Join(cfg.MetadataDir, category, "seen.db")
		}
		if err := os.MkdirAll(filepath.Dir(seenDBPath), 0755); err != nil {
			fmt.Fprintf(os.Stderr, "创建 seen DB 目录失败: %v\n", err)
			os.Exit(2)
		}
		var err error
		seenDB, err = db.NewSeenDB(seenDBPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "初始化 seen DB 失败: %v\n", err)
			os.Exit(2)
		}
		defer seenDB.Close()
	}

	var metaSeen *db.SeenDB
	if cfg.SeenDB.Enable {
		metaSeen = seenDB
	}

	// 创建下载器（seen 去重在 metadata writer，与 Python 一致）
	dl := downloader.NewDownloader(cfg)

	if err := maybePullDeltasFromS3(cfg, category, slog); err != nil {
		slog.log(fmt.Sprintf("source_s3 pull failed category=%s err=%v", category, err))
	}
	stopPullScheduler := startSourceS3PullScheduler(cfg, []string{category}, slog)
	defer stopPullScheduler()

	// 创建元数据写入器（按 UTC 日单文件 metadata.jsonl；跨日轮换；seen 全局）
	onDayClosed := func(dateUTC, dailyPath string) {
		if err := syncMetadataForCategory(cfg, category, dateUTC, dailyPath, slog); err != nil && slog != nil {
			slog.log(fmt.Sprintf("metadata sync failed category=%s err=%v", category, err))
		}
	}
	metaWriter, err := metadata.NewWriterForFile(cfg, category, metaSeen, onDayClosed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建元数据写入器失败: %v\n", err)
		os.Exit(2)
	}
	stopMetaSync := startMetadataSyncLoop(cfg, []string{category}, slog)
	defer stopMetaSync()
	// 退出时由 metaWriter.Close() 内对当前 UTC 日文件触发 onDayClosed 上传，避免仅用 time.Now() 与打开文件日不一致
	defer metaWriter.Close()

	// 创建失败 URL 写入器
	failedFile := cfg.ExpandPath(cfg.FailedURLsPathTemplate, category)
	if err := os.MkdirAll(filepath.Dir(failedFile), 0755); err != nil {
		fmt.Fprintf(os.Stderr, "创建失败文件目录失败: %v\n", err)
		os.Exit(2)
	}
	appendFailed := !cfg.RetryFailed
	failedWriter, err := os.OpenFile(failedFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开失败文件失败: %v\n", err)
		os.Exit(2)
	}
	defer failedWriter.Close()
	failedBuf := bufio.NewWriter(failedWriter)

	// 失败 URL 由单 goroutine 写入，避免 bufio.Writer 并发写导致 panic
	failedChan := make(chan string, 10000)
	var failedWg sync.WaitGroup
	failedWg.Add(1)
	go func() {
		defer failedWg.Done()
		for url := range failedChan {
			if _, err := failedBuf.WriteString(url + "\n"); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: 写失败 URL 失败: %v\n", err)
			}
		}
		if err := failedBuf.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Flush 失败 URL 文件失败: %v\n", err)
		}
	}()

	// 工作队列
	urlChan := make(chan string, 2000)
	var wg sync.WaitGroup

	// 启动 URL 生产者：优先消费 delta shards（含多轮 idle 重扫）
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(urlChan)
		checkpointPath := cfg.ExpandPath(cfg.CheckpointPathTemplate, category)
		if !cfg.RetryFailed {
			if ok := consumeShardsWithRescan(cfg, category, checkpointPath, urlChan, slog); ok {
				return
			}
			slog.log(fmt.Sprintf("no local shards found category=%s dir=%s", category, cfg.ExpandPath(cfg.URLsDeltaDirTemplate, category)))
			return
		}
		if !inputExists {
			fmt.Fprintf(os.Stderr, "输入文件不存在且未发现可消费的 delta shard: %s\n", inputFile)
			return
		}
		// retry_failed 模式：消费 failed_urls.txt
		readFile := inputFile
		var startOffset int64
		if cfg.CheckpointInterval > 0 {
			if path, off, ok := loadCheckpoint(checkpointPath); ok {
				readFile = path
				startOffset = off
			}
		}
		file, err := os.Open(readFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "打开 URL 文件失败: %v\n", err)
			return
		}
		defer file.Close()
		if startOffset > 0 {
			_, _ = file.Seek(startOffset, io.SeekStart)
		}
		reader := bufio.NewReaderSize(file, 512*1024)
		offset := startOffset
		linesSinceCheckpoint := 0
		for {
			line, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				break
			}
			offset += int64(len(line))
			url := parseURLLine(line)
			if url != "" {
				urlChan <- url
				linesSinceCheckpoint++
				if cfg.CheckpointInterval > 0 && linesSinceCheckpoint >= cfg.CheckpointInterval {
					_ = saveCheckpoint(checkpointPath, readFile, offset)
					linesSinceCheckpoint = 0
				}
			}
			if err == io.EOF {
				break
			}
		}
	}()

	// 启动下载 workers
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for url := range urlChan {
				result := dl.Download(url, outputDir)
				if result.Success {
					imageKey := fmt.Sprintf("%s/%s/%s", cfg.ImageKeyPrefix, category, result.FileName)
					metaWriter.WriteRecord(metadata.Record{
						ImageURL:   url,
						Resolution: result.Resolution,
						Timestamp:  result.Timestamp,
						ImageKey:   imageKey,
						LocalPath:  filepath.Join(outputDir, result.FileName),
					})
				} else if !result.SkippedLowRes && appendFailed {
					failedChan <- url
				}
			}
		}()
	}

	// 启动进度报告
	progressStart := time.Now()
	progressTicker := time.NewTicker(time.Duration(cfg.ProgressInterval) * time.Second)
	defer progressTicker.Stop()
	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		var prevDownloaded int64
		for {
			select {
			case <-progressTicker.C:
				success, failed, skipped := dl.GetStats()
				deltaDownloaded := success - prevDownloaded
				prevDownloaded = success
				elapsed := formatElapsedCompact(time.Since(progressStart))
				_ = skipped
				slog.log(fmt.Sprintf("progress elapsed=%s downloaded=%d failed=%d", elapsed, deltaDownloaded, failed))
			case <-stopProgress:
				return
			}
		}
	}()

	// 等待所有下载任务完成
	wg.Wait()
	close(failedChan)
	failedWg.Wait()

	progressTicker.Stop()
	close(stopProgress)
	<-progressDone

	// 最终统计（美化 + 写 log）
	success, failed, skipped := dl.GetStats()
	totalElapsed := time.Since(progressStart).Round(time.Second)
	slog.log(fmt.Sprintf("done elapsed=%s downloaded=%d failed=%d skipped=%d", totalElapsed, success, failed, skipped))
	fmt.Println()
	fmt.Println("  " + strings.Repeat("─", 52))
	fmt.Printf("  本轮结束  elapsed=%s  downloaded=%d  failed=%d  skipped=%d\n", totalElapsed, success, failed, skipped)
	fmt.Println("  " + strings.Repeat("─", 52))
	if cfg.RetryFailed && seenDB != nil {
		if err := pruneFailedBySeenDB(failedFile, seenDB); err != nil {
			slog.log(fmt.Sprintf("prune failed_urls by seen_db error: %v", err))
		}
	}
}

// writeFilteredFile 写入过滤后的文件
func writeFilteredFile(src, dst string) (total, kept int, err error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return 0, 0, err
	}

	srcFile, err := os.Open(src)
	if err != nil {
		return 0, 0, err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return 0, 0, err
	}
	defer dstFile.Close()

	scanner := bufio.NewScanner(srcFile)
	writer := bufio.NewWriter(dstFile)
	defer func() {
		if err := writer.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: Flush 过滤 URL 文件失败: %v\n", err)
		}
	}()

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		total++
		if !downloader.IsValidURL(line) {
			continue
		}
		if downloader.IsSkipURL(line) {
			continue
		}
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return total, kept, fmt.Errorf("写过滤 URL 文件: %w", err)
		}
		kept++
	}

	return total, kept, scanner.Err()
}

// cleanupOrphanPartFiles 清理孤儿 .part 文件
func cleanupOrphanPartFiles(downloadDir string, maxAgeSeconds int, slog *sessionLog) {
	cutoff := time.Now().Add(-time.Duration(maxAgeSeconds) * time.Second)
	matches, _ := filepath.Glob(filepath.Join(downloadDir, "*.part"))
	removed := 0
	for _, match := range matches {
		if info, err := os.Stat(match); err == nil {
			if info.ModTime().Before(cutoff) {
				os.Remove(match)
				removed++
			}
		}
	}
	if removed > 0 && slog != nil {
		slog.log(fmt.Sprintf("cleanup removed %d orphan .part file(s)", removed))
	}
}

// sha1Hex 计算 SHA1 哈希
func sha1Hex(s string) string {
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

// loadCheckpoint 读取断点：第一行=文件路径，第二行=字节 offset。用于大文件重启续传。
func loadCheckpoint(checkpointPath string) (filePath string, offset int64, ok bool) {
	f, err := os.Open(checkpointPath)
	if err != nil {
		return "", 0, false
	}
	defer f.Close()
	r := bufio.NewReader(f)
	pathLine, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", 0, false
	}
	pathLine = strings.TrimSpace(pathLine)
	if pathLine == "" {
		return "", 0, false
	}
	offsetLine, err := r.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", 0, false
	}
	offsetLine = strings.TrimSpace(offsetLine)
	off, err := strconv.ParseInt(offsetLine, 10, 64)
	if err != nil || off < 0 {
		return "", 0, false
	}
	if _, err := os.Stat(pathLine); err != nil {
		return "", 0, false
	}
	return pathLine, off, true
}

// saveCheckpoint 原子写入断点（先写 .tmp 再 rename，并 Sync）。
func saveCheckpoint(checkpointPath, filePath string, offset int64) error {
	dir := filepath.Dir(checkpointPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmpPath := checkpointPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%s\n%d\n", filePath, offset)
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

// category 统一使用 discover.yaml 的键名，路径与 image_key 均为 {category}，不再做复数转换

// multiCategoryJob 多 category 任务
type multiCategoryJob struct {
	URL      string
	Category string // 与 discover.yaml 的 category 一致
}

// multiCategoryFailed 失败写入项
type multiCategoryFailed struct {
	URL      string
	Category string
}

// multiCategoryState 每个 category 的状态
type multiCategoryState struct {
	outputDir  string
	seenDB     *db.SeenDB
	metaWriter *metadata.Writer
	failedFile *os.File
	failedBuf  *bufio.Writer
	failedMu   sync.Mutex
}

func runMultiCategory(cfg *config.Config, slog *sessionLog) {
	var categories []string

	extractDir := strings.TrimSpace(cfg.ExtractDir)
	if extractDir != "" {
		matches, err := filepath.Glob(filepath.Join(extractDir, "*_urls.txt"))
		if err != nil {
			fmt.Fprintf(os.Stderr, "glob extract 目录失败: %v\n", err)
			os.Exit(2)
		}
		if len(matches) == 0 {
			fmt.Fprintf(os.Stderr, "未找到 *_urls.txt: %s\n", filepath.Join(extractDir, "*_urls.txt"))
			os.Exit(2)
		}
		for _, p := range matches {
			base := filepath.Base(p)
			cat := strings.TrimSuffix(base, "_urls.txt")
			if cat == "" || !cfg.CategorySelected(cat) {
				continue
			}
			categories = append(categories, cat)
		}
	} else {
		for _, cat := range cfg.IncludeCategories {
			cat = strings.TrimSpace(cat)
			if cat == "" || !cfg.CategorySelected(cat) {
				continue
			}
			categories = append(categories, cat)
		}
	}
	if len(categories) == 0 {
		fmt.Fprintf(os.Stderr, "include_categories/exclude_categories 过滤后没有可处理的分类（请检查配置）\n")
		os.Exit(2)
	}
	if err := initialPullAllCategories(cfg, categories, slog); err != nil {
		fmt.Fprintf(os.Stderr, "启动阶段 source_s3 拉取失败: %v\n", err)
		os.Exit(2)
	}

	metadataDir := filepath.Join(cfg.ProjectRoot, "output", "metadata")
	linesPerFile := cfg.MetadataLinesPerFile
	if linesPerFile == 0 {
		linesPerFile = 1000000
	}
	failedFileName := cfg.FailedFileName
	if failedFileName == "" {
		failedFileName = "failed_urls.txt"
	}

	states := make(map[string]*multiCategoryState)
	for _, category := range categories {
		outputDir := cfg.OutputDir
		if strings.TrimSpace(cfg.MediaDirTemplate) != "" {
			outputDir = cfg.ExpandPath(cfg.MediaDirTemplate, category)
		}
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "创建输出目录失败 %s: %v\n", outputDir, err)
			os.Exit(2)
		}
		cleanupOrphanPartFiles(outputDir, 3600, slog)

		var seenDB *db.SeenDB
		if cfg.SeenDB.Enable {
			seenDBPath := cfg.ExpandPath(cfg.SeenDB.Path, category)
			if strings.TrimSpace(seenDBPath) == "" {
				seenDBPath = filepath.Join(metadataDir, category, "seen.db")
			}
			if err := os.MkdirAll(filepath.Dir(seenDBPath), 0755); err != nil {
				fmt.Fprintf(os.Stderr, "创建 seen DB 目录失败: %v\n", err)
				os.Exit(2)
			}
			var err error
			seenDB, err = db.NewSeenDB(seenDBPath)
			if err != nil {
				fmt.Fprintf(os.Stderr, "初始化 seen DB 失败 %s: %v\n", category, err)
				os.Exit(2)
			}
			defer seenDB.Close()
		}

		_ = linesPerFile
		cat := category
		var wseen *db.SeenDB
		if cfg.SeenDB.Enable {
			wseen = seenDB
		}
		onDayClosed := func(dateUTC, dailyPath string) {
			if err := syncMetadataForCategory(cfg, cat, dateUTC, dailyPath, slog); err != nil && slog != nil {
				slog.log(fmt.Sprintf("metadata sync failed category=%s err=%v", cat, err))
			}
		}
		metaWriter, err := metadata.NewWriterForFile(cfg, cat, wseen, onDayClosed)
		if err != nil {
			fmt.Fprintf(os.Stderr, "创建元数据写入器失败 %s: %v\n", category, err)
			os.Exit(2)
		}
		defer metaWriter.Close()

		failedPath := cfg.ExpandPath(cfg.FailedURLsPathTemplate, category)
		if strings.TrimSpace(failedPath) == "" {
			failedPath = filepath.Join(metadataDir, category, failedFileName)
		}
		if err := os.MkdirAll(filepath.Dir(failedPath), 0755); err != nil {
			fmt.Fprintf(os.Stderr, "创建失败文件目录失败: %v\n", err)
			os.Exit(2)
		}
		failedFile, err := os.OpenFile(failedPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "打开失败文件失败 %s: %v\n", failedPath, err)
			os.Exit(2)
		}
		defer failedFile.Close()

		states[category] = &multiCategoryState{
			outputDir:  outputDir,
			seenDB:     seenDB,
			metaWriter: metaWriter,
			failedFile: failedFile,
			failedBuf:  bufio.NewWriter(failedFile),
		}
	}
	stopMetaSync := startMetadataSyncLoop(cfg, categories, slog)
	defer stopMetaSync()
	stopPullScheduler := startSourceS3PullScheduler(cfg, categories, slog)
	defer stopPullScheduler()

	jobChan := make(chan multiCategoryJob, cfg.Workers*4)
	var producerWg sync.WaitGroup
	for _, category := range categories {
		checkpointPath := cfg.ExpandPath(cfg.CheckpointPathTemplate, category)
		producerWg.Add(1)
		go func(cat, ckPath string) {
			defer producerWg.Done()
			if !cfg.RetryFailed {
				if consumeShardsWithRescan(cfg, cat, ckPath, wrapCategoryJobChan(jobChan, cat), slog) {
					return
				}
				slog.log(fmt.Sprintf("no local shards found category=%s dir=%s", cat, cfg.ExpandPath(cfg.URLsDeltaDirTemplate, cat)))
				return
			}
			useFile := cfg.ExpandPath(cfg.FailedURLsPathTemplate, cat)
			if _, err := os.Stat(useFile); err != nil {
				slog.log(fmt.Sprintf("skip category=%s retry source not found: %s", cat, useFile))
				return
			}
			readFile := useFile
			var startOffset int64
			if cfg.CheckpointInterval > 0 {
				if path, off, ok := loadCheckpoint(ckPath); ok {
					readFile = path
					startOffset = off
				}
			}
			f, err := os.Open(readFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "打开 URL 文件失败 %s: %v\n", readFile, err)
				return
			}
			defer f.Close()
			if startOffset > 0 {
				if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
					fmt.Fprintf(os.Stderr, "Seek 断点失败 [%s]: %v\n", cat, err)
					return
				}
			}
			reader := bufio.NewReaderSize(f, 512*1024)
			offset := startOffset
			linesSinceCheckpoint := 0
			for {
				line, err := reader.ReadString('\n')
				if err != nil && err != io.EOF {
					break
				}
				offset += int64(len(line))
				url := parseURLLine(line)
				if url != "" {
					jobChan <- multiCategoryJob{URL: url, Category: cat}
					linesSinceCheckpoint++
					if cfg.CheckpointInterval > 0 && linesSinceCheckpoint >= cfg.CheckpointInterval {
						if e := saveCheckpoint(ckPath, readFile, offset); e == nil {
							linesSinceCheckpoint = 0
						}
					}
				}
				if err == io.EOF {
					break
				}
			}
			if cfg.CheckpointInterval > 0 && offset > startOffset {
				saveCheckpoint(ckPath, readFile, offset)
			}
		}(category, checkpointPath)
	}
	go func() {
		producerWg.Wait()
		close(jobChan)
	}()

	failedChan := make(chan multiCategoryFailed, 10000)
	var failedWg sync.WaitGroup
	failedWg.Add(1)
	go func() {
		defer failedWg.Done()
		for item := range failedChan {
			st := states[item.Category]
			if st != nil {
				st.failedMu.Lock()
				if _, err := st.failedBuf.WriteString(item.URL + "\n"); err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: 写失败 URL 失败 [%s]: %v\n", item.Category, err)
				}
				st.failedMu.Unlock()
			}
		}
		for cat, st := range states {
			st.failedMu.Lock()
			if err := st.failedBuf.Flush(); err != nil {
				fmt.Fprintf(os.Stderr, "ERROR: Flush 失败 URL 文件失败 [%s]: %v\n", cat, err)
			}
			st.failedMu.Unlock()
		}
	}()

	dl := downloader.NewDownloader(cfg)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobChan {
				st := states[j.Category]
				if st == nil {
					continue
				}
				result := dl.Download(j.URL, st.outputDir)
				if result.Success {
					imageKey := fmt.Sprintf("%s/%s/%s", cfg.ImageKeyPrefix, j.Category, result.FileName)
					st.metaWriter.WriteRecord(metadata.Record{
						ImageURL:   j.URL,
						Resolution: result.Resolution,
						Timestamp:  result.Timestamp,
						ImageKey:   imageKey,
						LocalPath:  filepath.Join(st.outputDir, result.FileName),
					})
				} else if !result.SkippedLowRes && !cfg.RetryFailed {
					failedChan <- multiCategoryFailed{URL: j.URL, Category: j.Category}
				}
			}
		}()
	}

	progressStart := time.Now()
	progressTicker := time.NewTicker(time.Duration(cfg.ProgressInterval) * time.Second)
	defer progressTicker.Stop()
	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		var prevDownloaded int64
		for {
			select {
			case <-progressTicker.C:
				success, failed, skipped := dl.GetStats()
				deltaDownloaded := success - prevDownloaded
				prevDownloaded = success
				elapsed := formatElapsedCompact(time.Since(progressStart))
				_ = skipped
				slog.log(fmt.Sprintf("progress elapsed=%s downloaded=%d failed=%d", elapsed, deltaDownloaded, failed))
			case <-stopProgress:
				return
			}
		}
	}()

	wg.Wait()
	close(failedChan)
	failedWg.Wait()
	progressTicker.Stop()
	close(stopProgress)
	<-progressDone

	success, failed, skipped := dl.GetStats()
	elapsed := time.Since(progressStart).Round(time.Second)
	slog.log(fmt.Sprintf("done elapsed=%s downloaded=%d failed=%d skipped=%d categories=%v", elapsed, success, failed, skipped, categories))
	fmt.Println()
	fmt.Println("  " + strings.Repeat("─", 52))
	fmt.Printf("  本轮结束  elapsed=%s  downloaded=%d  failed=%d  skipped=%d  categories=%v\n", elapsed, success, failed, skipped, categories)
	fmt.Println("  " + strings.Repeat("─", 52))
	if cfg.RetryFailed {
		for cat, st := range states {
			if st.seenDB == nil {
				continue
			}
			f := cfg.ExpandPath(cfg.FailedURLsPathTemplate, cat)
			if err := pruneFailedBySeenDB(f, st.seenDB); err != nil {
				slog.log(fmt.Sprintf("prune retry failed_urls category=%s err=%v", cat, err))
			}
		}
	}
}

func parseURLLine(line string) string {
	s := strings.TrimSpace(line)
	if s == "" {
		return ""
	}
	if strings.Contains(s, "\t") {
		parts := strings.SplitN(s, "\t", 2)
		return strings.TrimSpace(parts[0])
	}
	return s
}

func deltaShardFiles(deltaDir string) []string {
	entries, err := os.ReadDir(deltaDir)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		if strings.Contains(name, "_urls_delta_") && strings.Contains(name, "_shard") {
			out = append(out, filepath.Join(deltaDir, name))
		}
	}
	sort.Strings(out)
	return out
}

func consumeShardsWithRescan(cfg *config.Config, category, checkpointPath string, urlChan chan<- string, slog *sessionLog) bool {
	deltaDir := cfg.ExpandPath(cfg.URLsDeltaDirTemplate, category)
	if st, err := os.Stat(deltaDir); err != nil || !st.IsDir() {
		return false
	}
	processed := map[string]bool{}
	idleRounds := 0
	totalEmitted := 0
	idlePoll := time.Duration(maxInt(1, cfg.LoopIdlePollSeconds)) * time.Second
	maxIdleRounds := 3
	if cfg.Loop {
		maxIdleRounds = -1 // 常驻模式：一直等新 shard
	}
	for maxIdleRounds < 0 || idleRounds < maxIdleRounds {
		files := deltaShardFiles(deltaDir)
		if cfg.Loop && len(processed) > 0 {
			alive := make(map[string]struct{}, len(files))
			for _, f := range files {
				alive[f] = struct{}{}
			}
			for k := range processed {
				if _, ok := alive[k]; !ok {
					delete(processed, k)
				}
			}
		}
		pending := make([]string, 0, len(files))
		for _, f := range files {
			if !processed[f] {
				pending = append(pending, f)
			}
		}
		if len(pending) == 0 {
			idleRounds++
			time.Sleep(idlePoll)
			continue
		}
		idleRounds = 0
		for _, shard := range pending {
			n, err := consumeOneFile(shard, checkpointPath, cfg.CheckpointInterval, urlChan)
			if err != nil {
				if slog != nil {
					slog.log(fmt.Sprintf("consume shard failed category=%s shard=%s err=%v", category, shard, err))
				}
				continue
			}
			totalEmitted += n
			if slog != nil {
				slog.log(fmt.Sprintf("consume shard done category=%s shard=%s emitted=%d", category, filepath.Base(shard), n))
			}
			processed[shard] = true
		}
	}
	if slog != nil {
		slog.log(fmt.Sprintf("consume shards summary category=%s emitted_total=%d processed_shards=%d", category, totalEmitted, len(processed)))
	}
	return true
}

func consumeOneFile(readFile, checkpointPath string, checkpointInterval int, urlChan chan<- string) (int, error) {
	var startOffset int64
	if checkpointInterval > 0 {
		if path, off, ok := loadCheckpoint(checkpointPath); ok && path == readFile {
			startOffset = off
		}
	}
	file, err := os.Open(readFile)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	if startOffset > 0 {
		if _, err := file.Seek(startOffset, io.SeekStart); err != nil {
			return 0, err
		}
	}
	reader := bufio.NewReaderSize(file, 512*1024)
	offset := startOffset
	linesSinceCheckpoint := 0
	emitted := 0
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return emitted, err
		}
		offset += int64(len(line))
		url := parseURLLine(line)
		if url != "" {
			urlChan <- url
			emitted++
			linesSinceCheckpoint++
			if checkpointInterval > 0 && linesSinceCheckpoint >= checkpointInterval {
				if e := saveCheckpoint(checkpointPath, readFile, offset); e == nil {
					linesSinceCheckpoint = 0
				}
			}
		}
		if err == io.EOF {
			break
		}
	}
	if checkpointInterval > 0 && offset > startOffset {
		_ = saveCheckpoint(checkpointPath, readFile, offset)
	}
	return emitted, nil
}

func wrapCategoryJobChan(jobChan chan<- multiCategoryJob, category string) chan<- string {
	ch := make(chan string, 1024)
	go func() {
		for u := range ch {
			jobChan <- multiCategoryJob{URL: u, Category: category}
		}
	}()
	return ch
}

func maybePullDeltasFromS3(cfg *config.Config, category string, slog *sessionLog) error {
	s3 := cfg.SourceS3
	if !s3.Enabled || strings.TrimSpace(s3.Bucket) == "" {
		return nil
	}
	deltaDir := cfg.ExpandPath(cfg.URLsDeltaDirTemplate, category)
	if err := os.MkdirAll(deltaDir, 0755); err != nil {
		return err
	}
	prefix := strings.Trim(strings.TrimSpace(s3.KeyPrefix), "/")
	if prefix == "" {
		prefix = "pipeline-queue/pinterest"
	}
	remotePrefix := fmt.Sprintf("%s/%s/extract/urls/delta/", prefix, category)
	client, err := getSourceS3Client(s3)
	if err != nil {
		return fmt.Errorf("初始化 source_s3 SDK 失败: %w", err)
	}
	keys, err := listDeltaKeysFromS3(client, s3, remotePrefix)
	if err != nil {
		return err
	}
	sort.Slice(keys, func(i, j int) bool {
		ai, ad, ab := deltaSortKey(filepath.Base(keys[i]))
		aj, jd, jb := deltaSortKey(filepath.Base(keys[j]))
		if ai != aj {
			return ai
		}
		if ad != jd {
			return ad < jd
		}
		return ab < jb
	})
	attempts := s3.PullRetry
	if attempts <= 0 {
		attempts = 1
	}
	for _, key := range keys {
		base := filepath.Base(key)
		localPath := filepath.Join(deltaDir, base)
		if _, err := os.Stat(localPath); err == nil {
			continue
		}
		uri := fmt.Sprintf("s3://%s/%s", s3.Bucket, key)
		ok := false
		lastErr := ""
		for i := 0; i < attempts; i++ {
			tmpPath := localPath + ".tmp"
			_ = os.Remove(tmpPath)
			if err := downloadObjectToFile(client, s3.Bucket, key, tmpPath); err == nil {
				if s3.VerifyHead {
					remoteLen, err := headObjectContentLength(client, s3.Bucket, key)
					if err != nil {
						lastErr = err.Error()
						_ = os.Remove(tmpPath)
					} else if fi, err := os.Stat(tmpPath); err != nil {
						lastErr = err.Error()
						_ = os.Remove(tmpPath)
					} else if fi.Size() != remoteLen {
						lastErr = fmt.Sprintf("size mismatch local=%d remote=%d", fi.Size(), remoteLen)
						_ = os.Remove(tmpPath)
					} else if err := os.Rename(tmpPath, localPath); err == nil {
						ok = true
						break
					} else {
						lastErr = err.Error()
						_ = os.Remove(tmpPath)
					}
				} else if err := os.Rename(tmpPath, localPath); err == nil {
					ok = true
					break
				} else {
					lastErr = err.Error()
					_ = os.Remove(tmpPath)
				}
			} else {
				lastErr = err.Error()
				_ = os.Remove(tmpPath)
			}
			if i < attempts-1 {
				sleep := time.Duration(1<<i) * time.Second
				if sleep > 15*time.Second {
					sleep = 15 * time.Second
				}
				time.Sleep(sleep)
			}
		}
		if !ok && slog != nil {
			slog.log(fmt.Sprintf("pull shard failed finally uri=%s err=%s", uri, lastErr))
		}
	}
	return nil
}

func startSourceS3PullScheduler(cfg *config.Config, categories []string, slog *sessionLog) func() {
	s3 := cfg.SourceS3
	if !s3.Enabled || !s3.PullSchedulerEnable || strings.TrimSpace(s3.Bucket) == "" || len(categories) == 0 {
		return func() {}
	}
	cats := make([]string, 0, len(categories))
	for _, c := range categories {
		c = strings.TrimSpace(c)
		if c != "" {
			cats = append(cats, c)
		}
	}
	if len(cats) == 0 {
		return func() {}
	}
	interval := s3.PullSchedulerIntervalSec
	if interval <= 0 {
		interval = 120
	}
	hh, mm := parseHHMMUTC(s3.PullTimeUTC)
	stop := make(chan struct{})
	done := make(chan struct{})
	lastDaily := ""
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()
		runSweep := func(nowUTC time.Time, reason string, force bool) {
			date := nowUTC.Format("2006-01-02")
			if !force && (nowUTC.Hour() < hh || (nowUTC.Hour() == hh && nowUTC.Minute() < mm) || lastDaily == date) {
				return
			}
			pulled := 0
			withData := 0
			for _, cat := range cats {
				if err := maybePullDeltasFromS3(cfg, cat, slog); err != nil {
					if slog != nil {
						slog.log(fmt.Sprintf("source_s3 scheduler pull failed category=%s err=%v", cat, err))
					}
					continue
				}
				pulled++
				deltaDir := cfg.ExpandPath(cfg.URLsDeltaDirTemplate, cat)
				if len(deltaShardFiles(deltaDir)) > 0 {
					withData++
				}
			}
			lastDaily = date
			if slog != nil {
				slog.log(fmt.Sprintf("source_s3 scheduler sweep done reason=%s utc>=%02d:%02d categories_ok=%d categories_with_shards=%d", reason, hh, mm, pulled, withData))
			}
		}
		// 启动即执行一次 sweep，避免首轮主流程先退出导致空跑。
		runSweep(time.Now().UTC(), "startup", true)
		for {
			nowUTC := time.Now().UTC()
			runSweep(nowUTC, "scheduled", false)
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func initialPullAllCategories(cfg *config.Config, categories []string, slog *sessionLog) error {
	okCount := 0
	withData := 0
	var firstErr error
	for _, cat := range categories {
		if err := maybePullDeltasFromS3(cfg, cat, slog); err != nil {
			if slog != nil {
				slog.log(fmt.Sprintf("source_s3 startup pull failed category=%s err=%v", cat, err))
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		okCount++
		deltaDir := cfg.ExpandPath(cfg.URLsDeltaDirTemplate, cat)
		cnt := len(deltaShardFiles(deltaDir))
		if cnt > 0 {
			withData++
		}
		if slog != nil {
			slog.log(fmt.Sprintf("source_s3 startup pull category=%s local_shards=%d", cat, cnt))
		}
	}
	if slog != nil {
		slog.log(fmt.Sprintf("source_s3 startup pull done categories_ok=%d total=%d categories_with_shards=%d", okCount, len(categories), withData))
	}
	if firstErr != nil {
		return firstErr
	}
	return nil
}

// durationUntilNextMetadataSyncUTC 返回距离下一次「当天 UTC 的 hh:mm」的时长（若已过则指向次日）。
func durationUntilNextMetadataSyncUTC(hh, mm int) time.Duration {
	now := time.Now().UTC()
	target := time.Date(now.Year(), now.Month(), now.Day(), hh, mm, 0, 0, time.UTC)
	if !now.Before(target) {
		target = target.Add(24 * time.Hour)
	}
	return target.Sub(now)
}

func startMetadataSyncLoop(cfg *config.Config, categories []string, slog *sessionLog) func() {
	if !cfg.MetadataSync.Enabled || len(categories) == 0 {
		return func() {}
	}
	ms := cfg.MetadataSync
	hh, mm := parseHHMMUTC(ms.TimeUTC)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		runAll := func(reason string) {
			date := time.Now().UTC().Format("2006-01-02")
			for _, cat := range categories {
				cat = strings.TrimSpace(cat)
				if cat == "" {
					continue
				}
				daily := cfg.ExpandPathWithDate(cfg.MetadataDailyPathTemplate, cat, date)
				if err := syncMetadataForCategory(cfg, cat, date, daily, slog); err != nil && slog != nil {
					slog.log(fmt.Sprintf("metadata sync failed category=%s reason=%s err=%v", cat, reason, err))
				}
			}
		}
		// 进程启动（重启）后立即上传一次
		runAll("startup")
		var ticker *time.Ticker
		var tickerCh <-chan time.Time
		if ms.IntervalSec > 0 {
			interval := time.Duration(maxInt(60, ms.IntervalSec)) * time.Second
			ticker = time.NewTicker(interval)
			defer ticker.Stop()
			tickerCh = ticker.C
		}
		nextDaily := time.NewTimer(durationUntilNextMetadataSyncUTC(hh, mm))
		defer nextDaily.Stop()
		for {
			select {
			case <-stop:
				return
			case <-nextDaily.C:
				runAll("daily_utc")
				nextDaily.Reset(durationUntilNextMetadataSyncUTC(hh, mm))
			case <-tickerCh:
				runAll("interval")
			}
		}
	}()
	return func() {
		close(stop)
		<-done
	}
}

func syncMetadataForCategory(cfg *config.Config, category, dateUTC, dailyPath string, slog *sessionLog) error {
	ms := cfg.MetadataSync
	if !ms.Enabled {
		return nil
	}
	if strings.TrimSpace(cfg.SourceS3.Bucket) == "" {
		return fmt.Errorf("metadata_sync 启用但 source_s3.bucket 为空")
	}
	client, err := getSourceS3Client(cfg.SourceS3)
	if err != nil {
		return fmt.Errorf("创建 metadata sync SDK client 失败: %w", err)
	}
	keyPrefix := strings.Trim(strings.TrimSpace(ms.KeyPrefix), "/")
	if keyPrefix == "" {
		keyPrefix = "metadata/photo-download"
	}
	dailyKey := fmt.Sprintf("%s/%s/%s/metadata.jsonl", keyPrefix, dateUTC, category)
	if err := putFileToS3WithRetry(client, cfg.SourceS3.Bucket, dailyKey, dailyPath, maxInt(1, ms.Retry)); err != nil {
		return fmt.Errorf("上传 daily metadata 失败: %w", err)
	}
	if slog != nil {
		slog.log(fmt.Sprintf("metadata sync uploaded daily category=%s key=%s", category, dailyKey))
	}
	return nil
}

func putFileToS3WithRetry(client *s3.S3, bucket, key, localPath string, attempts int) error {
	if attempts <= 0 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		err := putFileToS3(client, bucket, key, localPath)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetriableS3Err(err) || i == attempts-1 {
			break
		}
		sleep := time.Duration(1<<i) * time.Second
		if sleep > 20*time.Second {
			sleep = 20 * time.Second
		}
		time.Sleep(sleep)
	}
	return lastErr
}

func putFileToS3(client *s3.S3, bucket, key, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, err = client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        f,
		ContentType: aws.String("application/x-ndjson"),
	})
	return err
}

func parseHHMMUTC(raw string) (int, int) {
	s := strings.TrimSpace(strings.ToLower(raw))
	if s == "" {
		return 0, 1
	}
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return 0, 1
	}
	hh, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	mm, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil || hh < 0 || hh > 23 || mm < 0 || mm > 59 {
		return 0, 1
	}
	return hh, mm
}

func listDeltaKeysFromS3(client *s3.S3, s3cfg config.SourceS3Config, remotePrefix string) ([]string, error) {
	keys := make([]string, 0, 128)
	attempts := maxInt(1, s3cfg.PullRetry)
	for i := 0; i < attempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		err := client.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(s3cfg.Bucket),
			Prefix:  aws.String(remotePrefix),
			MaxKeys: aws.Int64(1000),
		}, func(page *s3.ListObjectsV2Output, _ bool) bool {
			for _, item := range page.Contents {
				if item == nil || item.Key == nil {
					continue
				}
				key := strings.TrimSpace(aws.StringValue(item.Key))
				base := filepath.Base(key)
				if key == "" || !strings.Contains(base, "_urls_delta_") {
					continue
				}
				keys = append(keys, key)
			}
			return true
		})
		cancel()
		if err == nil {
			return keys, nil
		}
		if !isRetriableS3Err(err) || i == attempts-1 {
			return nil, fmt.Errorf("list-objects-v2 SDK 失败: %w", err)
		}
		sleep := time.Duration(1<<i) * time.Second
		if sleep > 15*time.Second {
			sleep = 15 * time.Second
		}
		time.Sleep(sleep)
	}
	return keys, nil
}

func downloadObjectToFile(client *s3.S3, bucket, key, dst string) error {
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	resp, err := client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if _, err := io.Copy(out, resp.Body); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	return nil
}

func headObjectContentLength(client *s3.S3, bucket, key string) (int64, error) {
	attempts := 2
	var lastErr error
	for i := 0; i < attempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		out, err := client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		cancel()
		if err == nil {
			return aws.Int64Value(out.ContentLength), nil
		}
		lastErr = err
		if !isRetriableS3Err(err) {
			break
		}
		time.Sleep(time.Duration(1+i) * time.Second)
	}
	return 0, lastErr
}

func newSourceS3Client(s3cfg config.SourceS3Config) (*s3.S3, error) {
	region := strings.TrimSpace(s3cfg.RegionName)
	if region == "" {
		region = "us-east-1"
	}
	httpClient := &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          64,
			MaxIdleConnsPerHost:   64,
			ResponseHeaderTimeout: 45 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
			IdleConnTimeout:       90 * time.Second,
		},
	}
	awsCfg := aws.Config{
		Region:      aws.String(region),
		HTTPClient:  httpClient,
		Credentials: credentials.NewSharedCredentials("", s3cfg.Profile),
		MaxRetries:  aws.Int(maxInt(2, s3cfg.PullRetry)),
	}
	if ep := strings.TrimSpace(s3cfg.EndpointURL); ep != "" {
		awsCfg.Endpoint = aws.String(ep)
		awsCfg.S3ForcePathStyle = aws.Bool(true)
	}
	sess, err := session.NewSessionWithOptions(session.Options{
		Profile: s3cfg.Profile,
		Config:  awsCfg,
	})
	if err != nil {
		return nil, err
	}
	return s3.New(sess), nil
}

func getSourceS3Client(s3cfg config.SourceS3Config) (*s3.S3, error) {
	key := strings.Join([]string{
		strings.TrimSpace(s3cfg.Profile),
		strings.TrimSpace(s3cfg.RegionName),
		strings.TrimSpace(s3cfg.EndpointURL),
	}, "|")
	s3ClientMu.Lock()
	if c, ok := s3ClientCache[key]; ok {
		s3ClientMu.Unlock()
		return c, nil
	}
	s3ClientMu.Unlock()
	c, err := newSourceS3Client(s3cfg)
	if err != nil {
		return nil, err
	}
	s3ClientMu.Lock()
	if existing, ok := s3ClientCache[key]; ok {
		s3ClientMu.Unlock()
		return existing, nil
	}
	s3ClientCache[key] = c
	s3ClientMu.Unlock()
	return c, nil
}

func isRetriableS3Err(err error) bool {
	if err == nil {
		return false
	}
	var nerr net.Error
	if errors.As(err, &nerr) && (nerr.Timeout() || nerr.Temporary()) {
		return true
	}
	var reqErr awserr.RequestFailure
	if errors.As(err, &reqErr) {
		code := reqErr.StatusCode()
		return code == 429 || (code >= 500 && code <= 599)
	}
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		switch awsErr.Code() {
		case request.CanceledErrorCode, "RequestTimeout", "Throttling", "SlowDown":
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "timeout") || strings.Contains(msg, "connection reset") || strings.Contains(msg, "no such host")
}

func deltaSortKey(name string) (bool, string, string) {
	base := strings.TrimSuffix(filepath.Base(name), filepath.Ext(name))
	m := deltaDateRe.FindStringSubmatch(base)
	if len(m) != 4 {
		return true, "9999-99-99", base
	}
	year := m[1]
	if len(year) == 2 {
		year = "20" + year
	}
	return false, fmt.Sprintf("%s-%s-%s", year, m[2], m[3]), base
}

func pruneFailedBySeenDB(path string, seen *db.SeenDB) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := path + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(out)
	sc := bufio.NewScanner(in)
	kept := map[string]struct{}{}
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		key := metadata.NormalizeImageURLKey(line)
		if key == "" {
			continue
		}
		if _, ok := kept[key]; ok {
			continue
		}
		has, err := seen.Contains(key)
		if err != nil {
			continue
		}
		if has {
			continue
		}
		kept[key] = struct{}{}
		if _, err := w.WriteString(line + "\n"); err != nil {
			out.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	if err := sc.Err(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := w.Flush(); err != nil {
		out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func formatElapsedCompact(d time.Duration) string {
	secs := int64(d.Round(time.Second) / time.Second)
	if secs <= 0 {
		return "0s"
	}
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	out := ""
	if h > 0 {
		out += fmt.Sprintf("%dh", h)
	}
	if m > 0 {
		out += fmt.Sprintf("%dm", m)
	}
	if s > 0 {
		out += fmt.Sprintf("%ds", s)
	}
	if out == "" {
		return "0s"
	}
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
