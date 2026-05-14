package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/500px-downloads/go-uploader/internal/config"
	"github.com/500px-downloads/go-uploader/internal/uploader"
	"golang.org/x/sync/errgroup"
)

var (
	configPath = flag.String("config", "", "配置文件路径（默认: go-uploader/config/upload-500px.yaml）")
	dryRun     = flag.Bool("dry-run", false, "只打印将上传的对象，不实际上传")
	category   = flag.String("category", "", "只处理单个 category（覆盖配置）")
	loop       = flag.Bool("loop", false, "持续轮询模式（覆盖配置）")
	untilEmpty = flag.Bool("until-empty", false, "多轮扫描上传，本轮扫到 0 个文件则退出（覆盖配置）")
	noUntilEmpty = flag.Bool("no-until-empty", false, "关闭 until_empty，只执行一轮扫描上传（覆盖配置）")
	once       = flag.Bool("once", false, "等同 -no-until-empty -loop=false，单次上传后退出")
)

func main() {
	flag.Parse()

	// 确定配置文件路径
	configFile := *configPath
	if configFile == "" {
		possiblePaths := []string{
			"go-uploader/config/upload-500px.yaml",
			"config/upload-500px.yaml",
			filepath.Join(filepath.Dir(os.Args[0]), "../config/upload-500px.yaml"),
		}
		for _, path := range possiblePaths {
			if _, err := os.Stat(path); err == nil {
				configFile = path
				break
			}
		}
		if configFile == "" {
			configFile = "go-uploader/config/upload-500px.yaml"
		}
	}

	// 加载配置
	cfg, err := config.LoadConfig(configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "加载配置失败: %v\n", err)
		os.Exit(1)
	}

	logDir := filepath.Join(cfg.ProjectRoot, "output", "logs", "upload")
	ts := time.Now().Format("20060102_150405")
	logPath := filepath.Join(logDir, fmt.Sprintf("upload_%s.log", ts))
	defer teeLogToFile(logPath)()

	// 命令行参数覆盖配置
	if *dryRun {
		cfg.Upload.DryRun = true
	}
	if *category != "" {
		cfg.Upload.Category = *category
		cfg.Upload.Categories = nil
		cfg.Upload.DiscoverCategories = false
	}
	if *loop {
		cfg.Upload.Loop = true
	}
	if *once {
		cfg.Upload.Loop = false
		cfg.Upload.UntilEmpty = false
	}
	if *noUntilEmpty {
		cfg.Upload.UntilEmpty = false
	}
	if *untilEmpty {
		cfg.Upload.UntilEmpty = true
	}
	// dry-run 不会删除本地文件，until_empty 会一直扫到相同文件导致死循环
	if cfg.Upload.DryRun && cfg.Upload.UntilEmpty {
		cfg.Upload.UntilEmpty = false
	}
	// dry-run 不删本地，loop 会反复扫同一批文件导致空转
	if cfg.Upload.DryRun && cfg.Upload.Loop {
		cfg.Upload.Loop = false
	}

	categories, ok := cfg.UploadCategories()
	if !ok || len(categories) == 0 {
		fmt.Fprintf(os.Stderr, "未指定 category：请配置 upload.category、upload.categories 或 upload.discover_categories: true\n")
		os.Exit(1)
	}

	// 创建上传器
	up, err := uploader.NewUploader(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建上传器失败: %v\n", err)
		os.Exit(1)
	}

	modeStr := "一次性上传 (once)"
	if cfg.Upload.Loop {
		modeStr = "持续轮询 (loop)"
	} else if cfg.Upload.UntilEmpty {
		modeStr = "多轮扫描上传，直到本轮 0 个文件退出 (until-empty)"
	}
	fmt.Println("  " + strings.Repeat("─", 52))
	fmt.Println("  上传器 · 启动")
	fmt.Println("  " + strings.Repeat("─", 52))
	fmt.Printf("  模式: %s\n", modeStr)
	if len(categories) == 1 {
		fmt.Printf("  分类: %s\n", categories[0])
	} else {
		fmt.Printf("  多分类: %d 个（共享 worker 池）\n", len(categories))
	}
	if cfg.Upload.ScanRoot != "" {
		fmt.Printf("  扫描目录: %s\n", cfg.Upload.ScanRoot)
		fmt.Printf("  S3: s3://%s/%s/<相对路径>\n", cfg.Wasabi.Bucket, cfg.Wasabi.KeyPrefix)
	}
	fmt.Printf("  并发数: %d  队列: %d  连接池: %d\n", cfg.Wasabi.UploadWorkers, cfg.Wasabi.MaxQueueSize, cfg.Wasabi.MaxPoolConnections)
	if cfg.Upload.Loop {
		fmt.Printf("  轮询间隔: %d秒 (正常) / %d秒 (空闲)\n", cfg.Upload.PollSeconds, cfg.Upload.IdlePollSeconds)
	}
	fmt.Printf("  统计间隔: %d秒\n", cfg.Upload.ReportSeconds)
	fmt.Println("  " + strings.Repeat("─", 52))
	fmt.Println()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	if len(categories) == 1 {
		if cfg.Upload.Loop {
			if err := runLoop(ctx, cfg, up, categories[0]); err != nil {
				fmt.Fprintf(os.Stderr, "运行失败: %v\n", err)
				os.Exit(1)
			}
		} else if cfg.Upload.UntilEmpty {
			if err := runUntilEmpty(ctx, cfg, up, categories[0]); err != nil {
				fmt.Fprintf(os.Stderr, "运行失败: %v\n", err)
				os.Exit(1)
			}
		} else {
			if err := runOnce(ctx, cfg, up, categories[0]); err != nil {
				fmt.Fprintf(os.Stderr, "运行失败: %v\n", err)
				os.Exit(1)
			}
		}
	} else {
		if cfg.Upload.Loop {
			if err := runLoopMulti(ctx, cfg, up, categories); err != nil {
				fmt.Fprintf(os.Stderr, "运行失败: %v\n", err)
				os.Exit(1)
			}
		} else if cfg.Upload.UntilEmpty {
			if err := runUntilEmptyMulti(ctx, cfg, up, categories); err != nil {
				fmt.Fprintf(os.Stderr, "运行失败: %v\n", err)
				os.Exit(1)
			}
		} else {
			if err := runOnceMulti(ctx, cfg, up, categories); err != nil {
				fmt.Fprintf(os.Stderr, "运行失败: %v\n", err)
				os.Exit(1)
			}
		}
	}

	// 打印最终统计
	fmt.Println()
	fmt.Println("  " + strings.Repeat("─", 52))
	up.PrintStats()
	fmt.Println("  " + strings.Repeat("─", 52))
}

// runOnce 运行一次上传（单 category）
func runOnce(ctx context.Context, cfg *config.Config, up *uploader.Uploader, category string) error {
	uploadDateUTC := time.Now().UTC().Format("2006-01-02")
	downloadsPath := mediaDir(cfg, category)
	fmt.Printf("扫描目录: %s\n", downloadsPath)
	if cfg.Upload.ScanRoot == "" {
		fmt.Printf("S3 路径 UTC 日期: %s\n", uploadDateUTC)
	}
	records, err := scanCategoryDir(cfg, category, downloadsPath, uploadDateUTC)
	if err != nil {
		return err
	}
	fmt.Printf("扫描到文件数: %d\n", len(records))
	return uploadFiles(ctx, cfg, up, records, false)
}

// scanAllCategories 扫描多 category 目录，返回合并的 Record 列表
func scanAllCategories(cfg *config.Config, categories []string, uploadDateUTC string) ([]uploader.Record, error) {
	var all []uploader.Record
	for _, cat := range categories {
		path := mediaDir(cfg, cat)
		list, err := scanCategoryDir(cfg, cat, path, uploadDateUTC)
		if err != nil {
			return nil, err
		}
		all = append(all, list...)
	}
	return all, nil
}

// runUntilEmptyMulti 多轮扫描上传（多 category），本轮扫到 0 个文件则退出
func runUntilEmptyMulti(ctx context.Context, cfg *config.Config, up *uploader.Uploader, categories []string) error {
	// 顶层 report ticker：整个 until_empty 期间每 report_seconds 打一次统计，否则单批 <10min 时从未输出
	reportDone := make(chan struct{})
	defer close(reportDone)
	go func() {
		ticker := time.NewTicker(cfg.ReportInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reportDone:
				return
			case <-ticker.C:
				up.PrintStats()
			}
		}
	}()

	round := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		round++
		uploadDateUTC := time.Now().UTC().Format("2006-01-02")
		records, err := scanAllCategories(cfg, categories, uploadDateUTC)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			fmt.Printf("第 %d 轮扫描: 0 个文件，退出\n", round)
			return nil
		}
		fmt.Printf("第 %d 轮扫描: utc_date=%s %d 个目录共 %d 个文件，开始上传\n", round, uploadDateUTC, len(categories), len(records))
		if err := uploadFiles(ctx, cfg, up, records, true); err != nil {
			return err
		}
	}
}

// scanCategoryDir 扫描单个 category 目录，返回 Record 列表（不检查 ctx）。
// S3 key 为 key_prefix/{utc_date}/{category}/{filename}，其中 utc_date 由调用方传入（每次扫描取当时 UTC 日历日）。
func scanCategoryDir(cfg *config.Config, category, downloadsPath, uploadDateUTC string) ([]uploader.Record, error) {
	var list []uploader.Record
	err := filepath.Walk(downloadsPath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		if !shouldUploadLocalFile(path, info) {
			return nil
		}
		list = append(list, makeUploadRecord(cfg, category, path, uploadDateUTC))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("扫描目录失败: %w", err)
	}
	return list, nil
}

// runUntilEmpty 多轮扫描上传（单 category），本轮扫到 0 个文件则退出
func runUntilEmpty(ctx context.Context, cfg *config.Config, up *uploader.Uploader, category string) error {
	reportDone := make(chan struct{})
	defer close(reportDone)
	go func() {
		ticker := time.NewTicker(cfg.ReportInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-reportDone:
				return
			case <-ticker.C:
				up.PrintStats()
			}
		}
	}()

	downloadsPath := filepath.Join(cfg.OutputDir, cfg.DownloadsDir, category)
	downloadsPath = mediaDir(cfg, category)
	round := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		round++
		uploadDateUTC := time.Now().UTC().Format("2006-01-02")
		records, err := scanCategoryDir(cfg, category, downloadsPath, uploadDateUTC)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			fmt.Printf("第 %d 轮扫描: 0 个文件，退出\n", round)
			return nil
		}
		fmt.Printf("第 %d 轮扫描: utc_date=%s %s 共 %d 个文件，开始上传\n", round, uploadDateUTC, downloadsPath, len(records))
		if err := uploadFiles(ctx, cfg, up, records, true); err != nil {
			return err
		}
	}
}

// runLoop 持续轮询模式（单 category）
func runLoop(ctx context.Context, cfg *config.Config, up *uploader.Uploader, category string) error {
	normalPollInterval := cfg.PollInterval()
	idlePollInterval := cfg.IdlePollInterval()
	currentPollInterval := normalPollInterval

	pollTicker := time.NewTicker(currentPollInterval)
	defer pollTicker.Stop()

	reportTicker := time.NewTicker(cfg.ReportInterval())
	defer reportTicker.Stop()

	// 工作队列；doneChan 由 workers 发送，必须在 Walk 期间被消费否则会死锁（Walk 阻塞在 recordChan 满时 workers 会堵在 doneChan）
	recordChan := make(chan uploader.Record, cfg.Wasabi.MaxQueueSize)
	doneChan := make(chan struct{}, cfg.Wasabi.MaxQueueSize)
	defer close(recordChan)

	workers := cfg.Wasabi.UploadWorkers
	var wg sync.WaitGroup
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var batchDone atomic.Int64
	go func() {
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-doneChan:
				batchDone.Add(1)
			}
		}
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case record, ok := <-recordChan:
					if !ok {
						return
					}
					_ = up.UploadRecord(workerCtx, record, record.LocalPath)
					select {
					case doneChan <- struct{}{}:
					case <-workerCtx.Done():
					}
				}
			}
		}()
	}

	scanOnce := func(scanCtx context.Context) bool {
		up.BeginRewriteBatch()
		defer func() {
			if err := up.FinishRewriteBatch(); err != nil {
				fmt.Printf("[metadata-rewrite] err=%v\n", err)
			}
		}()

		batchDone.Store(0)
		uploadDateUTC := time.Now().UTC().Format("2006-01-02")
		downloadsPath := mediaDir(cfg, category)

		fileCount := 0
		err := filepath.Walk(downloadsPath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if info.IsDir() {
				return nil
			}
			if !shouldUploadLocalFile(path, info) {
				return nil
			}

			select {
			case recordChan <- makeUploadRecord(cfg, category, path, uploadDateUTC):
				fileCount++
			case <-scanCtx.Done():
				return filepath.SkipAll
			}
			return nil
		})

		if err != nil && err != filepath.SkipAll {
			fmt.Printf("[poll] %s scan_error=%v\n", time.Now().Format("15:04:05"), err)
			return false
		}

		// 等本批全部上传完再进入下一轮扫描（drain 在 Walk 期间已消费 doneChan，此处仅轮询计数）
		for batchDone.Load() < int64(fileCount) {
			select {
			case <-scanCtx.Done():
				return false
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}

		if fileCount == 0 {
			if currentPollInterval != idlePollInterval {
				currentPollInterval = idlePollInterval
				pollTicker.Reset(currentPollInterval)
			}
		} else {
			if currentPollInterval != normalPollInterval {
				currentPollInterval = normalPollInterval
				pollTicker.Reset(currentPollInterval)
			}
		}
		fmt.Printf("[poll] %s utc_date=%s category=%s scan_files=%d next_poll=%v\n", time.Now().Format("15:04:05"), uploadDateUTC, category, fileCount, currentPollInterval)
		return true
	}

	// 扫描：启动立即扫一轮；之后每轮扫一批 -> 等本批全部上传完 -> 再等 poll 间隔 -> 下一轮扫描
	scanGroup, scanCtx := errgroup.WithContext(ctx)
	scanGroup.Go(func() error {
		if !scanOnce(scanCtx) {
			return nil
		}
		for {
			select {
			case <-scanCtx.Done():
				return nil
			case <-pollTicker.C:
				if !scanOnce(scanCtx) {
					return nil
				}
			}
		}
	})

	// 报告 goroutine
	reportGroup, reportCtx := errgroup.WithContext(ctx)
	reportGroup.Go(func() error {
		for {
			select {
			case <-reportCtx.Done():
				return nil
			case <-reportTicker.C:
				up.PrintStats()
			}
		}
	})

	// 等待
	scanGroup.Wait()
	workerCancel()
	wg.Wait()
	reportGroup.Wait()

	return nil
}

// runOnceMulti 多 category 一次上传，共享 worker 池
func runOnceMulti(ctx context.Context, cfg *config.Config, up *uploader.Uploader, categories []string) error {
	uploadDateUTC := time.Now().UTC().Format("2006-01-02")
	if cfg.Upload.ScanRoot == "" {
		fmt.Printf("S3 路径 UTC 日期: %s（多分类同一批次）\n", uploadDateUTC)
	}
	recordChan := make(chan uploader.Record, cfg.Wasabi.MaxQueueSize)
	var producerWg sync.WaitGroup

	for _, cat := range categories {
		downloadsPath := mediaDir(cfg, cat)
		producerWg.Add(1)
		go func(cat string, path string) {
			defer producerWg.Done()
			filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
				if err != nil || info == nil || info.IsDir() {
					return nil
				}
				if !shouldUploadLocalFile(p, info) {
					return nil
				}
				select {
				case <-ctx.Done():
					return filepath.SkipAll
				case recordChan <- makeUploadRecord(cfg, cat, p, uploadDateUTC):
				}
				return nil
			})
		}(cat, downloadsPath)
	}

	go func() {
		producerWg.Wait()
		close(recordChan)
	}()

	fmt.Printf("多分类扫描: %d 个目录（共享 worker 池）\n", len(categories))
	return uploadFilesFromChan(ctx, cfg, up, recordChan)
}

// uploadFilesFromChan 从 channel 消费并上传（多 category 共享 worker 池）
func uploadFilesFromChan(ctx context.Context, cfg *config.Config, up *uploader.Uploader, recordChan <-chan uploader.Record) error {
	up.BeginRewriteBatch()
	defer func() {
		if err := up.FinishRewriteBatch(); err != nil {
			fmt.Fprintf(os.Stderr, "metadata image_key 重写失败: %v\n", err)
		}
	}()

	workers := cfg.Wasabi.UploadWorkers
	reportTicker := time.NewTicker(cfg.ReportInterval())
	defer reportTicker.Stop()
	uploadDone := make(chan struct{})
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-uploadDone:
				return
			case <-reportTicker.C:
				up.PrintStats()
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case record, ok := <-recordChan:
					if !ok {
						return
					}
					_ = up.UploadRecord(ctx, record, record.LocalPath)
				}
			}
		}()
	}
	wg.Wait()
	close(uploadDone)
	reportTicker.Stop()
	<-reportDone
	return nil
}

// runLoopMulti 多 category 持续轮询，共享 worker 池
func runLoopMulti(ctx context.Context, cfg *config.Config, up *uploader.Uploader, categories []string) error {
	normalPoll := cfg.PollInterval()
	idlePoll := cfg.IdlePollInterval()
	currentPoll := normalPoll
	pollTicker := time.NewTicker(currentPoll)
	defer pollTicker.Stop()
	reportTicker := time.NewTicker(cfg.ReportInterval())
	defer reportTicker.Stop()

	recordChan := make(chan uploader.Record, cfg.Wasabi.MaxQueueSize)
	doneChan := make(chan struct{}, cfg.Wasabi.MaxQueueSize)

	var wg sync.WaitGroup
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var batchDone atomic.Int64
	go func() {
		for {
			select {
			case <-workerCtx.Done():
				return
			case <-doneChan:
				batchDone.Add(1)
			}
		}
	}()

	for i := 0; i < cfg.Wasabi.UploadWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case record, ok := <-recordChan:
					if !ok {
						return
					}
					_ = up.UploadRecord(workerCtx, record, record.LocalPath)
					select {
					case doneChan <- struct{}{}:
					case <-workerCtx.Done():
					}
				}
			}
		}()
	}

	scanOnce := func() bool {
		up.BeginRewriteBatch()
		defer func() {
			if err := up.FinishRewriteBatch(); err != nil {
				fmt.Printf("[metadata-rewrite] err=%v\n", err)
			}
		}()

		batchDone.Store(0)
		uploadDateUTC := time.Now().UTC().Format("2006-01-02")
		fileCount := 0
		for _, cat := range categories {
			path := mediaDir(cfg, cat)
			filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
				if err != nil || info == nil || info.IsDir() {
					return nil
				}
				if !shouldUploadLocalFile(p, info) {
					return nil
				}
				select {
				case recordChan <- makeUploadRecord(cfg, cat, p, uploadDateUTC):
					fileCount++
				case <-ctx.Done():
					return filepath.SkipAll
				}
				return nil
			})
		}
		// 等本批全部上传完再下一轮（drain 在 Walk 期间已消费 doneChan）
		for batchDone.Load() < int64(fileCount) {
			select {
			case <-ctx.Done():
				return false
			default:
				time.Sleep(50 * time.Millisecond)
			}
		}
		if fileCount == 0 {
			if currentPoll != idlePoll {
				currentPoll = idlePoll
				pollTicker.Reset(currentPoll)
			}
		} else {
			if currentPoll != normalPoll {
				currentPoll = normalPoll
				pollTicker.Reset(normalPoll)
			}
		}
		fmt.Printf("[poll] %s utc_date=%s categories=%d scan_files=%d next_poll=%v\n", time.Now().Format("15:04:05"), uploadDateUTC, len(categories), fileCount, currentPoll)
		return true
	}

	// 扫描一批 -> 等本批全部上传完 -> 再等 poll 间隔 -> 下一轮扫描，避免同一文件重复入队
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		if !scanOnce() {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-pollTicker.C:
				if !scanOnce() {
					return
				}
			}
		}
	}()

	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-reportTicker.C:
				up.PrintStats()
			}
		}
	}()

	<-scanDone
	workerCancel()
	wg.Wait()
	reportTicker.Stop()
	<-reportDone
	return nil
}

// uploadFiles 并发上传记录。skipReport=true 时由上层（until_empty）负责定时统计，避免重复且保证每 10min 有输出。
func uploadFiles(ctx context.Context, cfg *config.Config, up *uploader.Uploader, records []uploader.Record, skipReport bool) error {
	up.BeginRewriteBatch()
	defer func() {
		if err := up.FinishRewriteBatch(); err != nil {
			fmt.Fprintf(os.Stderr, "metadata image_key 重写失败: %v\n", err)
		}
	}()

	workers := cfg.Wasabi.UploadWorkers
	if workers > len(records) {
		workers = len(records)
	}

	recordChan := make(chan uploader.Record, len(records))
	for _, record := range records {
		recordChan <- record
	}
	close(recordChan)

	uploadDone := make(chan struct{})
	var reportDone chan struct{}
	if !skipReport {
		reportTicker := time.NewTicker(cfg.ReportInterval())
		defer reportTicker.Stop()
		reportDone = make(chan struct{})
		go func() {
			defer close(reportDone)
			for {
				select {
				case <-ctx.Done():
					return
				case <-uploadDone:
					return
				case <-reportTicker.C:
					up.PrintStats()
				}
			}
		}()
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case record, ok := <-recordChan:
					if !ok {
						return
					}
					_ = up.UploadRecord(ctx, record, record.LocalPath)
				}
			}
		}()
	}

	wg.Wait()
	close(uploadDone)
	if reportDone != nil {
		<-reportDone
	}
	return nil
}

func mediaDir(cfg *config.Config, category string) string {
	if cfg.Upload.ScanRoot != "" {
		return filepath.Clean(cfg.Upload.ScanRoot)
	}
	// 与 download.py / go-downloader 对齐：output/pipeline/{category}/download/media
	return filepath.Join(cfg.OutputDir, cfg.PipelineRootDir, category, "download", "media")
}

// shouldUploadLocalFile 跳过隐藏文件与未完成下载（如 .part）
func shouldUploadLocalFile(path string, info os.FileInfo) bool {
	if info == nil || info.IsDir() {
		return false
	}
	name := filepath.Base(path)
	if strings.HasPrefix(name, ".") {
		return false
	}
	lower := strings.ToLower(name)
	if strings.HasSuffix(lower, ".part") {
		return false
	}
	return true
}

func makeUploadRecord(cfg *config.Config, category, localPath, uploadDateUTC string) uploader.Record {
	fileName := filepath.Base(localPath)
	var s3Key string
	if cfg.Upload.ScanRoot != "" {
		rel, err := filepath.Rel(cfg.Upload.ScanRoot, localPath)
		if err != nil || strings.HasPrefix(rel, "..") {
			rel = fileName
		}
		s3Key = filepath.ToSlash(filepath.Join(cfg.Wasabi.KeyPrefix, rel))
	} else {
		s3Key = filepath.ToSlash(filepath.Join(cfg.Wasabi.KeyPrefix, uploadDateUTC, category, fileName))
	}
	return uploader.Record{
		ImageKey:  s3Key,
		Category:  category,
		LocalPath: localPath,
	}
}

// category 已统一使用 discover.yaml 的键名，不再做复数转换；本地目录与 S3 路径均为 {category}
