package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/unsplash_downloads/go-downloader/internal/config"
	"github.com/unsplash_downloads/go-downloader/internal/db"
	"github.com/unsplash_downloads/go-downloader/internal/downloader"
	"github.com/unsplash_downloads/go-downloader/internal/metadata"
)

var deltaDateRe = regexp.MustCompile(`_urls_delta_(\d{2}|\d{4})-(\d{2})-(\d{2})(?:_shard)?$`)

// recordImageKey 与 metadata writer / extract 的 image_key 规则一致。
func recordImageKey(cfg *config.Config, categoryPlural, fileName string) string {
	pfx := strings.Trim(strings.TrimSpace(cfg.ImageKeyPrefix), "/")
	if downloader.IsCrawlHashStyle(cfg) {
		if pfx != "" {
			return pfx + "/" + fileName
		}
		return fileName
	}
	if pfx != "" {
		return fmt.Sprintf("%s/%s/%s", pfx, categoryPlural, fileName)
	}
	return fmt.Sprintf("%s/%s", categoryPlural, fileName)
}

// sessionLog 写入 output/logs/download/ 下的带时间戳日志。
// 每条 log 不 fsync，减少高并发进度日志时的磁盘压力；进程正常退出时在 close 里统一 Sync。
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
}

func (s *sessionLog) close() {
	if s == nil || s.f == nil {
		return
	}
	ts := time.Now().Format("2006-01-02 15:04:05")
	_, _ = fmt.Fprintf(s.f, "[%s] === download session end ===\n", ts)
	if err := s.f.Sync(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Sync session 日志失败: %v\n", err)
	}
	_ = s.f.Close()
	s.f = nil
}

func openSessionLog(projectRoot, configPath string) (*sessionLog, error) {
	logDir := filepath.Join(projectRoot, "output", "logs", "download")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("download_%s.log", time.Now().Format("2006-01-02_150405"))
	logPath := filepath.Join(logDir, name)
	f, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	absLog, _ := filepath.Abs(logPath)
	fmt.Fprintf(os.Stderr, "下载会话日志: %s\n", absLog)
	s := &sessionLog{f: f}
	ts := time.Now().Format("2006-01-02 15:04:05")
	cfgLine := configPath
	if cfgAbs, err := filepath.Abs(configPath); err == nil {
		cfgLine = cfgAbs
	}
	_, _ = fmt.Fprintf(f, "[%s] === download session start config=%s ===\n", ts, cfgLine)
	return s, nil
}

var (
	configPath = flag.String("config", "", "配置文件路径；未指定时优先 go-downloader/config/download-500px.yaml（若存在），否则 download-http.yaml 等同路径")
)

func main() {
	flag.Parse()

	// 确定配置文件路径
	configFile := *configPath
	if configFile == "" {
		exeDir := filepath.Dir(os.Args[0])
		// 本仓库优先 download-500px.yaml；否则与历史一致使用 download-http.yaml
		possiblePaths := []string{
			"go-downloader/config/download-500px.yaml",
			"config/download-500px.yaml",
			filepath.Join(exeDir, "../config/download-500px.yaml"),
			filepath.Join(exeDir, "config/download-500px.yaml"),
			"go-downloader/config/download-http.yaml",
			"config/download-http.yaml",
			filepath.Join(exeDir, "../config/download-http.yaml"),
			filepath.Join(exeDir, "config/download-http.yaml"),
		}
		for _, path := range possiblePaths {
			if _, err := os.Stat(path); err == nil {
				configFile = path
				break
			}
		}
		if configFile == "" {
			configFile = "go-downloader/config/download-500px.yaml"
		}
	}

	// 加载配置
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}
	configAbs := configFile
	if abs, err := filepath.Abs(configFile); err == nil {
		configAbs = abs
		fmt.Fprintf(os.Stderr, "使用配置: %s\n", abs)
	}
	downloader.SetSkipSuffixes(cfg.SkipSuffixes)
	slog, err := openSessionLog(cfg.ProjectRoot, configAbs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建日志文件失败: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if slog != nil {
			slog.close()
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

	// 创建输出目录
	category := cfg.Category
	outputDir := cfg.OutputDir
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "创建输出目录失败: %v\n", err)
		os.Exit(2)
	}

	// 清理上次中断遗留的 *.part / *.up.tmp（勿同目录并行跑两个 download）
	cleanupStaleMediaTemps(outputDir, slog)

	useExtractMeta := cfg.IsExtractMetadataInput()
	var upscaleDir string
	if useExtractMeta {
		upscaleDir = cfg.MediaUpscaleDir
		if strings.TrimSpace(upscaleDir) == "" {
			upscaleDir = filepath.Join(cfg.ProjectRoot, "output", "media_upscale")
		}
		if err := os.MkdirAll(upscaleDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "创建 media_upscale 目录失败: %v\n", err)
			os.Exit(2)
		}
		cleanupStaleMediaTemps(upscaleDir, slog)
		logMediaUpscaleResumeHint(upscaleDir, slog)
	}

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
	if cfg.SeenDB.Enable && !useExtractMeta {
		metaSeen = seenDB
	}

	// 创建下载器（seen 去重在 metadata writer，与 Python 一致）
	dl := downloader.NewDownloader(cfg)

	var metaWriter *metadata.Writer
	if !useExtractMeta {
		metaWriter, err = metadata.NewWriterForFile(cfg, category, metaSeen, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "创建元数据写入器失败: %v\n", err)
			os.Exit(2)
		}
		defer metaWriter.Close()
	}

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
	var wg sync.WaitGroup
	var extractCP *extractCheckpoint

	if useExtractMeta && !cfg.RetryFailed {
		if len(cfg.ExtractMetadataInputs) == 0 {
			fmt.Fprintf(os.Stderr,
				"未找到 extract metadata 输入：请配置 extract_metadata_path_template，或在 output/metadata 下放置 extract_metadata_<n>.jsonl\n")
			os.Exit(2)
		}
		for _, p := range cfg.ExtractMetadataInputs {
			if inputStat, inputErr := os.Stat(p); inputErr != nil || inputStat.IsDir() {
				fmt.Fprintf(os.Stderr, "extract metadata 输入文件不存在: %s\n", p)
				os.Exit(2)
			}
		}
		if len(cfg.ExtractMetadataInputs) > 1 {
			fmt.Fprintf(os.Stderr, "extract metadata 输入（按 n 升序）共 %d 个:\n", len(cfg.ExtractMetadataInputs))
			for _, p := range cfg.ExtractMetadataInputs {
				fmt.Fprintf(os.Stderr, "  %s\n", p)
			}
		}
	}

	if useExtractMeta {
		jobChan := make(chan downloadJob, 2000)
		checkpointPath := cfg.ExpandPath(cfg.CheckpointPathTemplate, category)
		if !cfg.RetryFailed {
			startFile := cfg.ExtractMetadataInputs[0]
			var startLineIndex int64
			if cfg.CheckpointInterval > 0 {
				if path, _, idx, ok := loadExtractCheckpoint(checkpointPath); ok {
					for _, f := range cfg.ExtractMetadataInputs {
						if f == path {
							startFile = path
							startLineIndex = idx
							break
						}
					}
				}
			}
			cp := newExtractCheckpoint(checkpointPath, startFile, cfg.CheckpointInterval, startLineIndex)
			extractCP = cp
		}
		sess := &downloadSession{
			cfg:          cfg,
			category:     category,
			outputDir:    outputDir,
			upscaleDir:   upscaleDir,
			dl:           dl,
			seenDB:       seenDB,
			checkpoint:   extractCP,
			upscaleSem:   make(chan struct{}, maxInt(1, cfg.UpscaleWorkers)),
			python:       cfg.UpscalePython,
			script:       cfg.UpscaleScript,
			appendFailed: appendFailed,
			failedChan:   failedChan,
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(jobChan)
			if cfg.RetryFailed {
				if seenDB != nil && cfg.SeenDB.Enable {
					n, err := emitFailedJobsFromSeenDB(seenDB, func(j downloadJob) {
						jobChan <- j
					})
					if err != nil {
						slog.log(fmt.Sprintf("retry from seen_db error: %v", err))
						fmt.Fprintf(os.Stderr, "从 seen.db 读取 failed 记录失败: %v\n", err)
						return
					}
					if n > 0 {
						slog.log(fmt.Sprintf("retry from seen_db emitted=%d", n))
						return
					}
				}
				if !inputExists {
					fmt.Fprintf(os.Stderr, "retry_failed: seen.db 中无 failed 记录，且失败列表文件不存在: %s\n", inputFile)
					return
				}
				consumeFailedURLsAsJobs(inputFile, checkpointPath, cfg.CheckpointInterval, jobChan)
				return
			}
			n, err := consumeExtractMetadataFiles(cfg.ExtractMetadataInputs, checkpointPath, cfg.CheckpointInterval, extractCP, sess, func(j downloadJob) {
				jobChan <- j
			})
			if err != nil {
				slog.log(fmt.Sprintf("extract metadata error err=%v", err))
				fmt.Fprintf(os.Stderr, "读取 extract metadata 失败: %v\n", err)
			} else {
				slog.log(fmt.Sprintf("extract metadata done files=%d emitted=%d", len(cfg.ExtractMetadataInputs), n))
			}
		}()

		for i := 0; i < cfg.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for job := range jobChan {
					sess.ProcessJob(job)
				}
			}()
		}
	} else {
		urlChan := make(chan string, 2000)

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
				if st, err := os.Stat(cfg.Input); err == nil && !st.IsDir() {
					n, err := consumeOneFile(cfg.Input, checkpointPath, cfg.CheckpointInterval, urlChan)
					if err != nil {
						slog.log(fmt.Sprintf("flat urls file error path=%s err=%v", cfg.Input, err))
					} else {
						slog.log(fmt.Sprintf("flat urls file done path=%s emitted=%d", cfg.Input, n))
					}
					return
				}
				slog.log(fmt.Sprintf("no local shards and no urls file category=%s dir=%s input=%s", category, cfg.ExpandPath(cfg.URLsDeltaDirTemplate, category), cfg.Input))
				return
			}
			if cfg.RetryFailed {
				if seenDB != nil && cfg.SeenDB.Enable {
					n, err := emitFailedURLsFromSeenDB(seenDB, func(u string) {
						urlChan <- u
					})
					if err != nil {
						slog.log(fmt.Sprintf("retry from seen_db error: %v", err))
						fmt.Fprintf(os.Stderr, "从 seen.db 读取 failed 记录失败: %v\n", err)
						return
					}
					if n > 0 {
						slog.log(fmt.Sprintf("retry from seen_db emitted=%d", n))
						return
					}
				}
				if !inputExists {
					fmt.Fprintf(os.Stderr, "retry_failed: seen.db 中无 failed 记录，且失败列表文件不存在: %s\n", inputFile)
					return
				}
				consumeFailedURLsAsStrings(inputFile, checkpointPath, cfg.CheckpointInterval, urlChan)
				return
			}
		}()

		for i := 0; i < cfg.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for url := range urlChan {
					if cfg.SeenDB.Enable && seenDB != nil && downloader.IsCrawlHashStyle(cfg) {
						k := metadata.SeenDedupeKey(cfg, url)
						if k != "" {
							if ok, err := seenDB.IsOK(k); err == nil && ok {
								continue
							}
							if !cfg.RetryFailed {
								if failed, err := seenDB.IsFailed(k); err == nil && failed {
									continue
								}
							}
						}
					}
					result := dl.Download(url, outputDir)
					if result.Success {
						imageKey := recordImageKey(cfg, category, result.FileName)
						metaWriter.WriteRecord(metadata.Record{
							ImageURL:   url,
							Resolution: result.Resolution,
							Timestamp:  result.Timestamp,
							ImageKey:   imageKey,
							LocalPath:  filepath.Join(outputDir, result.FileName),
						})
					} else if appendFailed && !result.Success && !result.SkippedLowRes && !result.SkipFailedList {
						failedChan <- url
					}
				}
			}()
		}
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
	if extractCP != nil {
		extractCP.flushFinal()
	}
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
		if err := pruneFailedBySeenDB(cfg, failedFile, seenDB); err != nil {
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

// cleanupStaleMediaTemps 启动时删除上一次中断/崩溃留下的临时文件：
//   - *.part：HTTP 下载写入中的临时文件（成功后会 rename 为成品，残留即未完成）
//   - *.up.tmp：extract 管线放大输出时的临时文件
//
// 注意：若在同一目录并行运行两个 download 进程，可能删掉对方正在写的临时文件；单进程常态下重启即可安全清理。
func cleanupStaleMediaTemps(dir string, slog *sessionLog) {
	if strings.TrimSpace(dir) == "" {
		return
	}
	patterns := []string{"*.part", "*.up.tmp"}
	removed := 0
	sample := make([]string, 0, 5)
	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			continue
		}
		for _, match := range matches {
			if err := os.Remove(match); err != nil {
				continue
			}
			removed++
			if len(sample) < 5 {
				sample = append(sample, filepath.Base(match))
			}
		}
	}
	if removed == 0 || slog == nil {
		return
	}
	msg := fmt.Sprintf("cleanup temp dir=%s removed=%d (*.part *.up.tmp)", dir, removed)
	if len(sample) > 0 {
		msg += fmt.Sprintf(" e.g. %v", sample)
		if removed > len(sample) {
			msg += " ..."
		}
	}
	slog.log(msg)
}

// logMediaUpscaleResumeHint 启动时提示 media_upscale 内可能尚未放大的成品文件数量；续跑 extract 时会复用这些文件（Download 判「已存在」后直接进入 cubic）。
func logMediaUpscaleResumeHint(upscaleDir string, slog *sessionLog) {
	if slog == nil || strings.TrimSpace(upscaleDir) == "" {
		return
	}
	entries, err := os.ReadDir(upscaleDir)
	if err != nil {
		return
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		low := strings.ToLower(e.Name())
		if strings.HasSuffix(low, ".part") || strings.HasSuffix(low, ".up.tmp") {
			continue
		}
		n++
	}
	if n > 0 {
		slog.log(fmt.Sprintf("media_upscale: %d 个非临时文件；续跑时 JSONL 未记 ok 的行会复用本地图并继续放大到 output/media", n))
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
		cleanupStaleMediaTemps(outputDir, slog)

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
		metaWriter, err := metadata.NewWriterForFile(cfg, cat, wseen, nil)
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
				flatPath := cfg.ExpandPath(cfg.URLsPathTemplate, cat)
				if st, err := os.Stat(flatPath); err == nil && !st.IsDir() {
					n, err := consumeOneFile(flatPath, ckPath, cfg.CheckpointInterval, wrapCategoryJobChan(jobChan, cat))
					if err != nil {
						slog.log(fmt.Sprintf("flat urls file error category=%s path=%s err=%v", cat, flatPath, err))
					} else {
						slog.log(fmt.Sprintf("flat urls file done category=%s path=%s emitted=%d", cat, flatPath, n))
					}
					return
				}
				slog.log(fmt.Sprintf("no local shards and no urls file category=%s dir=%s flat=%s", cat, cfg.ExpandPath(cfg.URLsDeltaDirTemplate, cat), flatPath))
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
				if cfg.SeenDB.Enable && st.seenDB != nil && downloader.IsCrawlHashStyle(cfg) {
					k := metadata.SeenDedupeKey(cfg, j.URL)
					if k != "" {
						if ok, err := st.seenDB.IsOK(k); err == nil && ok {
							bn := downloader.BaseNameForURL(cfg, j.URL)
							if _, err := os.Stat(filepath.Join(st.outputDir, bn)); err == nil {
								continue
							}
						}
					}
				}
				result := dl.Download(j.URL, st.outputDir)
				if result.Success {
					imageKey := recordImageKey(cfg, j.Category, result.FileName)
					st.metaWriter.WriteRecord(metadata.Record{
						ImageURL:   j.URL,
						Resolution: result.Resolution,
						Timestamp:  result.Timestamp,
						ImageKey:   imageKey,
						LocalPath:  filepath.Join(st.outputDir, result.FileName),
					})
				} else if !result.SkippedLowRes && !result.SkipFailedList && !cfg.RetryFailed {
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
			if err := pruneFailedBySeenDB(cfg, f, st.seenDB); err != nil {
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

func pruneFailedBySeenDB(cfg *config.Config, path string, seen *db.SeenDB) error {
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
		key := metadata.SeenDedupeKey(cfg, line)
		if key == "" {
			continue
		}
		if _, ok := kept[key]; ok {
			continue
		}
		has, err := seen.IsOK(key)
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
