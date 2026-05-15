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
	HasMetaSize    bool
	MetaW, MetaH   int
	MetaResolution string
	LineIndex      int64
	ByteOffset     int64
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
		ImageURL   string `json:"image_url"`
		Resolution string `json:"resolution"`
		ImageKey   string `json:"image_key"`
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
		MetaResolution: strings.TrimSpace(row.Resolution),
	}
	if w, h, err := parseWXH(j.MetaResolution); err == nil && w > 0 && h > 0 {
		j.HasMetaSize = true
		j.MetaW, j.MetaH = w, h
	}
	return j, true
}

func downloadJobFromURL(url, resolution string, lineIndex int64) downloadJob {
	j := downloadJob{
		URL:            url,
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

func emitFailedJobsFromSeenDB(seenDB *db.SeenDB, emit func(downloadJob)) (int, error) {
	rows, err := seenDB.ListFailed()
	if err != nil {
		return 0, err
	}
	for i, row := range rows {
		emit(downloadJobFromURL(row.ImageURL, row.Resolution, int64(i)))
	}
	return len(rows), nil
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
	inflight     sync.Map
	upscaleSem   chan struct{}
	python       string
	script       string
	appendFailed bool
	failedChan   chan<- string
}

func (s *downloadSession) markFailed(dedupeKey, imageURL, resolution string) {
	if s.seenDB != nil && s.cfg.SeenDB.Enable {
		_ = s.seenDB.Upsert(dedupeKey, imageURL, "failed", resolution)
	}
	if s.appendFailed {
		s.failedChan <- imageURL
	}
}

func (s *downloadSession) markOK(dedupeKey, imageURL, resolution string) {
	if s.seenDB != nil && s.cfg.SeenDB.Enable {
		_ = s.seenDB.Upsert(dedupeKey, imageURL, "ok", resolution)
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
	return downloader.BaseNameForURL(s.cfg, job.URL)
}

func (s *downloadSession) shouldSkip(dedupe string) bool {
	if s.seenDB != nil && s.cfg.SeenDB.Enable {
		// ok：已下载并上传 S3 后本地文件会删除，仅以 seen.db 为准，不重复下载
		if ok, err := s.seenDB.IsOK(dedupe); err == nil && ok {
			return true
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

func (s *downloadSession) finishJob(job downloadJob, dedupe string) {
	s.inflight.Delete(dedupe)
	if s.checkpoint != nil {
		s.checkpoint.complete(job.LineIndex, job.ByteOffset)
	}
}

func (s *downloadSession) ProcessJob(job downloadJob) {
	dedupe := metadata.SeenDedupeKey(s.cfg, job.URL)

	if s.shouldSkip(dedupe) {
		if s.checkpoint != nil {
			s.checkpoint.complete(job.LineIndex, job.ByteOffset)
		}
		return
	}
	defer s.finishJob(job, dedupe)

	if downloader.IsSkipURL(job.URL) {
		s.markFailed(dedupe, job.URL, "skip_url")
		return
	}

	if s.cfg.UpscaleScript == "" && (!job.HasMetaSize || !s.tierFromDims(job.MetaW, job.MetaH)) {
		s.markFailed(dedupe, job.URL, "no_upscale_script")
		return
	}

	if job.HasMetaSize && s.tierFromDims(job.MetaW, job.MetaH) {
		r := s.dl.Download(job.URL, s.outputDir)
		switch s.classifyDownload(r, s.outputDir) {
		case downloadOutcomeExists, downloadOutcomeOK:
			s.markOK(dedupe, job.URL, s.resolutionNote(job, r))
		default:
			s.markFailed(dedupe, job.URL, s.resolutionNote(job, r))
		}
		return
	}

	if err := os.MkdirAll(s.upscaleDir, 0755); err != nil {
		s.markFailed(dedupe, job.URL, "mkdir_upscale:"+err.Error())
		return
	}

	if job.HasMetaSize && !s.tierFromDims(job.MetaW, job.MetaH) {
		r := s.dl.Download(job.URL, s.upscaleDir)
		switch s.classifyDownload(r, s.upscaleDir) {
		case downloadOutcomeFail:
			s.markFailed(dedupe, job.URL, s.resolutionNote(job, r))
			return
		case downloadOutcomeExists, downloadOutcomeOK:
			// 继续放大；最终 media 文件名与直存一致：{sha1(url)}.{ext}
		}
		if err := s.upscaleToMedia(job, r, dedupe); err != nil {
			s.markFailed(dedupe, job.URL, job.MetaResolution+";"+err.Error())
		}
		return
	}

	r := s.dl.Download(job.URL, s.upscaleDir)
	switch s.classifyDownload(r, s.upscaleDir) {
	case downloadOutcomeFail:
		s.markFailed(dedupe, job.URL, s.resolutionNote(job, r))
		return
	case downloadOutcomeExists, downloadOutcomeOK:
	}
	fileName := s.mediaFileName(job, r)
	src := filepath.Join(s.upscaleDir, fileName)
	w, h, err := imgmeta.DimensionsFromFile(src)
	if err != nil {
		_ = os.Remove(src)
		s.markFailed(dedupe, job.URL, "decode:"+err.Error())
		return
	}
	dest := filepath.Join(s.outputDir, fileName)
	_ = os.Remove(dest)
	if s.tierFromDims(w, h) {
		if err := os.Rename(src, dest); err != nil {
			_ = os.Remove(src)
			s.markFailed(dedupe, job.URL, imgmeta.FormatResolution(w, h)+";"+err.Error())
			return
		}
		fr := imgmeta.FormatResolution(w, h)
		s.markOK(dedupe, job.URL, fr)
		return
	}
	if err := s.runCubicThenFinalize(job, r, src, dest, dedupe); err != nil {
		s.markFailed(dedupe, job.URL, imgmeta.FormatResolution(w, h)+";"+err.Error())
	}
}

func (s *downloadSession) upscaleToMedia(job downloadJob, r *downloader.DownloadResult, dedupe string) error {
	fileName := s.mediaFileName(job, r)
	src := filepath.Join(s.upscaleDir, fileName)
	dest := filepath.Join(s.outputDir, fileName)
	tmp := filepath.Join(s.outputDir, fileName+".up.tmp")
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
	fw, fh, e2 := imgmeta.DimensionsFromFile(dest)
	fr := imgmeta.FormatResolution(fw, fh)
	if e2 != nil {
		fr = job.MetaResolution + "_upscaled"
	}
	s.markOK(dedupe, job.URL, fr)
	return nil
}

func (s *downloadSession) runCubicThenFinalize(job downloadJob, r *downloader.DownloadResult, src, dest, dedupe string) error {
	fileName := s.mediaFileName(job, r)
	tmp := filepath.Join(s.outputDir, fileName+".up.tmp")
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
	fw, fh, e2 := imgmeta.DimensionsFromFile(dest)
	fr := imgmeta.FormatResolution(fw, fh)
	if e2 != nil {
		fr = "upscaled"
	}
	s.markOK(dedupe, job.URL, fr)
	return nil
}
