package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unsplash_downloads/go-downloader/internal/config"
	"github.com/unsplash_downloads/go-downloader/internal/db"
	"github.com/unsplash_downloads/go-downloader/internal/downloader"
	"github.com/unsplash_downloads/go-downloader/internal/imgmeta"
	"github.com/unsplash_downloads/go-downloader/internal/metadata"
	"github.com/unsplash_downloads/go-downloader/internal/upscale"
)

type downloadJob struct {
	URL            string
	ImageKey       string
	PhotoID        string
	HasMetaSize    bool
	MetaW, MetaH   int
	MetaResolution string
	LineIndex      int64
	ByteOffset     int64
	// SkipCheckpoint 为 true 时不更新 extract JSONL 断点（如从 seen 排空 pending_upscale / pending_large 的任务）。
	SkipCheckpoint bool
}

func parseWXH(s string) (w, h int, err error) {
	s = strings.TrimSpace(strings.ToLower(s))
	parts := strings.Split(s, "x")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("bad resolution")
	}
	w, err = strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, err
	}
	h, err = strconv.Atoi(strings.TrimSpace(parts[1]))
	return w, h, err
}

func parseExtractMetadataLine(line string) (downloadJob, bool) {
	line = strings.TrimSpace(line)
	if line == "" {
		return downloadJob{}, false
	}
	var row struct {
		ImageURL   string      `json:"image_url"`
		Resolution string      `json:"resolution"`
		ImageKey   string      `json:"image_key"`
		ID         interface{} `json:"id"`
	}
	if err := json.Unmarshal([]byte(line), &row); err != nil {
		return downloadJob{}, false
	}
	url := strings.TrimSpace(row.ImageURL)
	if url == "" {
		return downloadJob{}, false
	}
	j := downloadJob{
		URL:            url,
		ImageKey:       strings.TrimSpace(row.ImageKey),
		PhotoID:        formatPhotoID(row.ID),
		MetaResolution: strings.TrimSpace(row.Resolution),
	}
	if w, h, err := parseWXH(j.MetaResolution); err == nil && w > 0 && h > 0 {
		j.HasMetaSize = true
		j.MetaW, j.MetaH = w, h
	}
	return j, true
}

func formatPhotoID(v interface{}) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case float64:
		return strconv.FormatInt(int64(x), 10)
	case json.Number:
		return strings.TrimSpace(x.String())
	default:
		return ""
	}
}

func downloadJobFromURL(url, resolution string, lineIndex int64) downloadJob {
	j := downloadJob{
		URL:            url,
		PhotoID:        downloader.PhotoIDFrom500px(url, ""),
		MetaResolution: resolution,
		LineIndex:      lineIndex,
		ByteOffset:     lineIndex + 1,
	}
	if w, h, err := parseWXH(resolution); err == nil && w > 0 && h > 0 {
		j.HasMetaSize = true
		j.MetaW, j.MetaH = w, h
	}
	return j
}

func emitPendingUpscaleFromSeenDB(seenDB *db.SeenDB, emit func(downloadJob)) (int, error) {
	rows, err := seenDB.ListPendingUpscale()
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		j := downloadJobFromURL(row.ImageURL, strings.TrimSpace(row.Detail), -1)
		j.SkipCheckpoint = true
		emit(j)
	}
	return len(rows), nil
}

func emitPendingLargeFromSeenDB(seenDB *db.SeenDB, emit func(downloadJob)) (int, error) {
	rows, err := seenDB.ListPendingLarge()
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		j := downloadJobFromURL(row.ImageURL, strings.TrimSpace(row.Detail), -1)
		j.SkipCheckpoint = true
		emit(j)
	}
	return len(rows), nil
}

// emitPendingDrainByPolicy 按批策略排空 seen.db 中的 pending 队列；full 时两者都排空。
func emitPendingDrainByPolicy(policy string, seenDB *db.SeenDB, emit func(downloadJob)) (upscaleN, largeN int, err error) {
	pol := strings.ToLower(strings.TrimSpace(policy))
	if pol == "metadata_small" || pol == "full" {
		upscaleN, err = emitPendingUpscaleFromSeenDB(seenDB, emit)
		if err != nil {
			return upscaleN, largeN, err
		}
	}
	if pol == "metadata_large" || pol == "full" {
		largeN, err = emitPendingLargeFromSeenDB(seenDB, emit)
	}
	return upscaleN, largeN, err
}

func emitFailedJobsFromSeenDB(seenDB *db.SeenDB, extractPolicy string, emit func(downloadJob)) (emitted int, totalFailed int, err error) {
	rows, err := seenDB.ListFailed()
	if err != nil {
		return 0, 0, err
	}
	totalFailed = len(rows)
	for i, row := range rows {
		if !db.FailedRowMatchesRetryPolicy(extractPolicy, row.Route) {
			continue
		}
		emit(downloadJobFromSeenFailedRow(row.ImageURL, row.Route, row.Detail, int64(i)))
		emitted++
	}
	return emitted, totalFailed, nil
}

// downloadJobFromSeenFailedRow 从 seen 失败行的 detail（及兼容旧库的 route 拼串）恢复 job，解析 | 分段中的 WxH。
func downloadJobFromSeenFailedRow(url, route, detail string, lineIndex int64) downloadJob {
	for _, part := range strings.Split(detail, "|") {
		p := strings.TrimSpace(part)
		if w, h, err := parseWXH(p); err == nil && w > 0 && h > 0 {
			return downloadJobFromURL(url, p, lineIndex)
		}
	}
	for _, part := range strings.Split(route, "|") {
		p := strings.TrimSpace(part)
		if w, h, err := parseWXH(p); err == nil && w > 0 && h > 0 {
			return downloadJobFromURL(url, p, lineIndex)
		}
	}
	return downloadJobFromURL(url, "", lineIndex)
}

func emitFailedURLsFromSeenDB(seenDB *db.SeenDB, emit func(string)) (int, error) {
	rows, err := seenDB.ListFailed()
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		emit(row.ImageURL)
	}
	return len(rows), nil
}

func consumeExtractMetadataFile(readFile, checkpointPath string, checkpointInterval int, emit func(downloadJob)) (int, error) {
	var startOffset, startLineIndex int64
	if checkpointInterval > 0 {
		if path, off, idx, ok := loadExtractCheckpoint(checkpointPath); ok && path == readFile {
			startOffset = off
			startLineIndex = idx
		} else if path, off, ok := loadCheckpoint(checkpointPath); ok && path == readFile {
			startOffset = off
		}
	}
	return consumeExtractMetadataFileFrom(readFile, startOffset, startLineIndex, emit)
}

func consumeExtractMetadataFileFrom(readFile string, startOffset, startLineIndex int64, emit func(downloadJob)) (int, error) {
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
	lineIndex := startLineIndex
	emitted := 0
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return emitted, err
		}
		offset += int64(len(line))
		if job, ok := parseExtractMetadataLine(line); ok {
			job.LineIndex = lineIndex
			job.ByteOffset = offset
			emit(job)
			emitted++
			lineIndex++
		}
		if err == io.EOF {
			break
		}
	}
	return emitted, nil
}

func consumeExtractMetadataFiles(files []string, checkpointPath string, checkpointInterval int, cp *extractCheckpoint, sess *downloadSession, emit func(downloadJob)) (int, error) {
	if len(files) == 0 {
		return 0, fmt.Errorf("无 extract metadata 输入文件")
	}
	startIdx := 0
	var startOffset, startLineIndex int64
	if checkpointInterval > 0 {
		if path, off, idx, ok := loadExtractCheckpoint(checkpointPath); ok {
			for i, f := range files {
				if f == path {
					startIdx = i
					startOffset = off
					startLineIndex = idx
					break
				}
			}
		}
	}
	total := 0
	for i := startIdx; i < len(files); i++ {
		readFile := files[i]
		off := int64(0)
		lineIdx := int64(0)
		if i == startIdx {
			off = startOffset
			lineIdx = startLineIndex
		}
		if cp != nil {
			cp.resetForFile(readFile, lineIdx)
		}
		n, err := consumeExtractMetadataFileFrom(readFile, off, lineIdx, emit)
		total += n
		if err != nil {
			return total, fmt.Errorf("%s: %w", readFile, err)
		}
		if sess != nil {
			sess.waitInflight()
		}
		if cp != nil {
			cp.flushFinal()
		}
	}
	return total, nil
}

func consumeFailedURLsAsStrings(readFile, checkpointPath string, checkpointInterval int, urlChan chan<- string) {
	consumeFailedURLs(readFile, checkpointPath, checkpointInterval, func(line string) {
		url := parseURLLine(line)
		if url != "" {
			urlChan <- url
		}
	})
}

func consumeFailedURLsAsJobs(readFile, checkpointPath string, checkpointInterval int, jobChan chan<- downloadJob) {
	consumeFailedURLs(readFile, checkpointPath, checkpointInterval, func(line string) {
		url := parseURLLine(line)
		if url != "" {
			jobChan <- downloadJob{URL: url}
		}
	})
}

func consumeFailedURLs(readFile, checkpointPath string, checkpointInterval int, emit func(string)) {
	var startOffset int64
	if checkpointInterval > 0 {
		if path, off, ok := loadCheckpoint(checkpointPath); ok {
			readFile = path
			startOffset = off
		}
	}
	file, err := os.Open(readFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开失败 URL 文件失败: %v\n", err)
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
		emit(line)
		linesSinceCheckpoint++
		if checkpointInterval > 0 && linesSinceCheckpoint >= checkpointInterval {
			_ = saveCheckpoint(checkpointPath, readFile, offset)
			linesSinceCheckpoint = 0
		}
		if err == io.EOF {
			break
		}
	}
	if checkpointInterval > 0 && offset > startOffset {
		_ = saveCheckpoint(checkpointPath, readFile, offset)
	}
}

type downloadSession struct {
	cfg          *config.Config
	category     string
	outputDir    string
	upscaleDir   string
	dl           *downloader.Downloader
	seenDB       *db.SeenDB
	checkpoint   *extractCheckpoint
	// extractInflight 仅在多 JSONL 顺序扫描时非 nil：每入队一任务 +1，ProcessJob 退出时 -1，用于切换下一文件前排空队列。
	extractInflight *atomic.Int64
	inflight        sync.Map
	upscaleSem      chan struct{}
	python          string
	script          string
	appendFailed    bool
	failedChan      chan<- string
}

func (s *downloadSession) markFailed(dedupeKey, imageURL, route, detail string) {
	if s.seenDB != nil && s.cfg.SeenDB.Enable {
		_ = s.seenDB.Upsert(dedupeKey, imageURL, "failed", route, detail)
	}
	if s.appendFailed {
		s.failedChan <- imageURL
	}
}

func (s *downloadSession) markOK(dedupeKey, imageURL, route string) {
	if s.seenDB != nil && s.cfg.SeenDB.Enable {
		_ = s.seenDB.Upsert(dedupeKey, imageURL, "ok", route, "")
	}
}

func (s *downloadSession) resolutionNote(job downloadJob, r *downloader.DownloadResult) string {
	if r != nil && strings.TrimSpace(r.Resolution) != "" && r.Resolution != "0x0" {
		return r.Resolution
	}
	if strings.TrimSpace(job.MetaResolution) != "" {
		return job.MetaResolution
	}
	return "unknown"
}

func truncateSeenDetail(s string, max int) string {
	s = strings.TrimSpace(strings.ReplaceAll(s, "\n", " "))
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max]
}

func formatLargeDirectDetail(job downloadJob, note string) string {
	meta := strings.TrimSpace(job.MetaResolution)
	if meta == "" {
		meta = "-"
	}
	return meta + "|" + truncateSeenDetail(note, 240)
}

func formatCV2Detail(note string) string {
	return truncateSeenDetail(note, 320)
}

func (s *downloadSession) tierFromDims(w, h int) bool {
	return imgmeta.MeetsMinShortMinLong(w, h, s.cfg.ResolutionMinShort, s.cfg.ResolutionMinLong)
}

type downloadOutcome int

const (
	downloadOutcomeOK downloadOutcome = iota
	downloadOutcomeExists
	downloadOutcomeFail
)

// classifyDownload 判断 HTTP 下载结果；Exists 表示目标目录已有同名文件（disk_glob_fallback）。
func (s *downloadSession) classifyDownload(r *downloader.DownloadResult, dir string) downloadOutcome {
	if r == nil {
		return downloadOutcomeFail
	}
	if r.FileName != "" && r.SkipFailedList {
		if _, err := os.Stat(filepath.Join(dir, r.FileName)); err == nil {
			return downloadOutcomeExists
		}
	}
	if r.SkippedLowRes || !r.Success {
		return downloadOutcomeFail
	}
	return downloadOutcomeOK
}

func (s *downloadSession) mediaFileName(job downloadJob, r *downloader.DownloadResult) string {
	if r != nil && strings.TrimSpace(r.FileName) != "" {
		return r.FileName
	}
	return downloader.FileNameFromImageKey(s.cfg, job.ImageKey, job.URL)
}

func (s *downloadSession) downloadExtract(job downloadJob, dir string) *downloader.DownloadResult {
	return s.dl.DownloadExtract(downloader.DownloadExtractOpts{
		IdentityURL:     job.URL,
		InitialFetchURL: job.URL,
		FileName:        downloader.FileNameFromImageKey(s.cfg, job.ImageKey, job.URL),
		PhotoID:         downloader.PhotoIDFrom500px(job.URL, job.PhotoID),
	}, dir)
}

func (s *downloadSession) shouldSkip(dedupe string) bool {
	if s.seenDB != nil && s.cfg.SeenDB.Enable {
		// ok：已下载并上传 S3 后本地文件会删除，仅以 seen.db 为准，不重复下载
		if ok, err := s.seenDB.IsOK(dedupe); err == nil && ok {
			return true
		}
		// metadata_large：已记入 pending_upscale 的小图行不重扫（待 metadata_small 排空）
		if s.cfg.IsExtractMetadataLargeBatch() && !s.cfg.RetryFailed {
			if pending, err := s.seenDB.IsPendingUpscale(dedupe); err == nil && pending {
				return true
			}
		}
		// metadata_small：已记入 pending_large 的大图行不重扫（待 metadata_large 排空）
		if s.cfg.IsExtractMetadataSmallBatch() && !s.cfg.RetryFailed {
			if pending, err := s.seenDB.IsPendingLarge(dedupe); err == nil && pending {
				return true
			}
		}
		// 正常模式：failed 留待 retry_failed 时重试
		if !s.cfg.RetryFailed {
			if failed, err := s.seenDB.IsFailed(dedupe); err == nil && failed {
				return true
			}
		}
	}
	if _, loaded := s.inflight.LoadOrStore(dedupe, struct{}{}); loaded {
		return true
	}
	return false
}

func (s *downloadSession) waitInflight() {
	for {
		busy := false
		s.inflight.Range(func(_, _ interface{}) bool {
			busy = true
			return false
		})
		if !busy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *downloadSession) finishJob(job downloadJob, dedupe string) {
	s.inflight.Delete(dedupe)
	if s.checkpoint != nil && !job.SkipCheckpoint {
		s.checkpoint.complete(job.LineIndex, job.ByteOffset)
	}
}

func (s *downloadSession) ProcessJob(job downloadJob) {
	if s.extractInflight != nil {
		defer s.extractInflight.Add(-1)
	}
	dedupe := metadata.SeenDedupeKey(s.cfg, job.URL)

	if s.shouldSkip(dedupe) {
		if s.checkpoint != nil {
			s.checkpoint.complete(job.LineIndex, job.ByteOffset)
		}
		return
	}
	defer s.finishJob(job, dedupe)

	if downloader.IsSkipURL(job.URL) {
		s.markFailed(dedupe, job.URL, "", "skip_url")
		return
	}

	lg := s.cfg.IsExtractMetadataLargeBatch()
	sm := s.cfg.IsExtractMetadataSmallBatch()
	retry := s.cfg.RetryFailed
	// full 批（及 retry_failed）当场处理，不写 pending_*；两阶段批按 lg/sm 推迟到 pending。

	// 元数据已满足阈值 → 大图批/full/retry 直下 media；小图批推迟到 pending_large
	if job.HasMetaSize && s.tierFromDims(job.MetaW, job.MetaH) {
		if sm && !retry {
			if s.seenDB != nil && s.cfg.SeenDB.Enable {
				_ = s.seenDB.Upsert(dedupe, job.URL, db.StatusPendingLarge, db.SeenResolutionLargeDirect, strings.TrimSpace(job.MetaResolution))
			} else {
				fmt.Fprintf(os.Stderr, "警告: metadata_small 需启用 metadata_seen_db，否则无法记录待大图 URL: %s\n", job.URL)
			}
			return
		}
		r := s.downloadExtract(job, s.outputDir)
		switch s.classifyDownload(r, s.outputDir) {
		case downloadOutcomeExists, downloadOutcomeOK:
			s.markOK(dedupe, job.URL, db.SeenResolutionLargeDirect)
		default:
			s.markFailed(dedupe, job.URL, db.SeenResolutionLargeDirect, formatLargeDirectDetail(job, s.resolutionNote(job, r)))
		}
		return
	}

	if err := os.MkdirAll(s.upscaleDir, 0755); err != nil {
		s.markFailed(dedupe, job.URL, db.SeenResolutionCV2Upscale, formatCV2Detail("mkdir_upscale:"+err.Error()))
		return
	}

	// 元数据低于阈值 → 大图批推迟到 pending_upscale；小图批走下载+CV2
	if job.HasMetaSize && !s.tierFromDims(job.MetaW, job.MetaH) {
		if lg && !retry {
			if s.seenDB != nil && s.cfg.SeenDB.Enable {
				_ = s.seenDB.Upsert(dedupe, job.URL, db.StatusPendingUpscale, db.SeenResolutionCV2Upscale, strings.TrimSpace(job.MetaResolution))
			} else {
				fmt.Fprintf(os.Stderr, "警告: metadata_large 需启用 metadata_seen_db，否则无法记录待 CV2 URL: %s\n", job.URL)
			}
			return
		}
		r := s.downloadExtract(job, s.upscaleDir)
		switch s.classifyDownload(r, s.upscaleDir) {
		case downloadOutcomeFail:
			s.markFailed(dedupe, job.URL, db.SeenResolutionCV2Upscale, formatCV2Detail(s.resolutionNote(job, r)))
			return
		case downloadOutcomeExists, downloadOutcomeOK:
			// 继续放大；最终 media 文件名与直存一致：{sha1(url)}.{ext}
		}
		if err := s.upscaleToMedia(job, r, dedupe); err != nil {
			s.markFailed(dedupe, job.URL, db.SeenResolutionCV2Upscale, formatCV2Detail(job.MetaResolution+";"+err.Error()))
		}
		return
	}

	// JSONL 无可用 WxH：大图批推迟给 small 探测；小图批/重试则下载探测
	if lg && !retry {
		if s.seenDB != nil && s.cfg.SeenDB.Enable {
			_ = s.seenDB.Upsert(dedupe, job.URL, db.StatusPendingUpscale, db.SeenResolutionCV2Upscale, "nometa")
		} else {
			fmt.Fprintf(os.Stderr, "警告: metadata_large 需启用 metadata_seen_db，否则无法记录无 resolution 行: %s\n", job.URL)
		}
		return
	}

	r := s.downloadExtract(job, s.upscaleDir)
	switch s.classifyDownload(r, s.upscaleDir) {
	case downloadOutcomeFail:
		s.markFailed(dedupe, job.URL, db.SeenResolutionCV2Upscale, formatCV2Detail(s.resolutionNote(job, r)))
		return
	case downloadOutcomeExists, downloadOutcomeOK:
	}
	fileName := s.mediaFileName(job, r)
	src := filepath.Join(s.upscaleDir, fileName)
	w, h, err := imgmeta.DimensionsFromFile(src)
	if err != nil {
		_ = os.Remove(src)
		s.markFailed(dedupe, job.URL, db.SeenResolutionCV2Upscale, formatCV2Detail("decode:"+err.Error()))
		return
	}
	dest := filepath.Join(s.outputDir, fileName)
	_ = os.Remove(dest)
	if s.tierFromDims(w, h) {
		if err := os.Rename(src, dest); err != nil {
			_ = os.Remove(src)
			s.markFailed(dedupe, job.URL, db.SeenResolutionCV2Upscale, formatCV2Detail(imgmeta.FormatResolution(w, h)+";"+err.Error()))
			return
		}
		s.markOK(dedupe, job.URL, db.SeenResolutionLargeDirect)
		return
	}
	if err := s.runCubicThenFinalize(job, r, src, dest, dedupe); err != nil {
		s.markFailed(dedupe, job.URL, db.SeenResolutionCV2Upscale, formatCV2Detail(imgmeta.FormatResolution(w, h)+";"+err.Error()))
	}
}

// upscaleTempPath 生成放大临时路径。OpenCV imwrite 按最终扩展名选编码器，须为 *.up.tmp.jpg 而非 *.jpg.up.tmp。
func upscaleTempPath(finalPath string) string {
	ext := filepath.Ext(finalPath)
	if ext == "" {
		return finalPath + ".up.tmp.jpg"
	}
	return strings.TrimSuffix(finalPath, ext) + ".up.tmp" + ext
}

func (s *downloadSession) upscaleToMedia(job downloadJob, r *downloader.DownloadResult, dedupe string) error {
	fileName := s.mediaFileName(job, r)
	src := filepath.Join(s.upscaleDir, fileName)
	dest := filepath.Join(s.outputDir, fileName)
	tmp := upscaleTempPath(dest)
	_ = os.Remove(tmp)
	_ = os.Remove(dest)

	s.upscaleSem <- struct{}{}
	err := upscale.RunCubic2x(s.python, s.script, src, tmp)
	<-s.upscaleSem
	if err != nil {
		_ = os.Remove(tmp)
		_ = os.Remove(src)
		return err
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		_ = os.Remove(src)
		return err
	}
	_ = os.Remove(src)
	s.markOK(dedupe, job.URL, db.SeenResolutionCV2Upscale)
	return nil
}

func (s *downloadSession) runCubicThenFinalize(job downloadJob, r *downloader.DownloadResult, src, dest, dedupe string) error {
	tmp := upscaleTempPath(dest)
	_ = os.Remove(tmp)
	_ = os.Remove(dest)
	s.upscaleSem <- struct{}{}
	err := upscale.RunCubic2x(s.python, s.script, src, tmp)
	<-s.upscaleSem
	if err != nil {
		_ = os.Remove(tmp)
		_ = os.Remove(src)
		return err
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		_ = os.Remove(src)
		return err
	}
	_ = os.Remove(src)
	s.markOK(dedupe, job.URL, db.SeenResolutionCV2Upscale)
	return nil
}
