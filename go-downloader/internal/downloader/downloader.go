package downloader

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unsplash_downloads/go-downloader/internal/config"
	"github.com/unsplash_downloads/go-downloader/internal/proxy"
)

var photoID500px = regexp.MustCompile(`500px\.org/photo/(\d+)`)

// Downloader 下载器
type Downloader struct {
	cfg        *config.Config
	httpClient *http.Client // CDN 下载（可走代理）
	apiClient  *http.Client // api.500px.com 刷新 URL（优先直连）
	stats      *Stats
	userAgents []string
}

// Stats 统计信息
type Stats struct {
	Success int64
	Failed  int64
	Skipped int64
	mu      sync.RWMutex
}

// DownloadExtractOpts extract 模式：fileName 固定为 metadata image_key basename；fetch URL 可刷新。
type DownloadExtractOpts struct {
	IdentityURL     string // metadata image_url（seen 去重键，不变）
	InitialFetchURL string // 首次 HTTP 使用的 URL（通常同 IdentityURL）
	FileName        string // 落盘 basename，来自 image_key
	PhotoID         string // 500px legacy id，用于 Referer 与 m=4096 刷新
}

// DownloadResult 下载结果
type DownloadResult struct {
	Success        bool
	FileName       string
	Timestamp      string
	Resolution     string
	Error          error
	ObjectID       string
	SkippedLowRes  bool // 因最短边 < MinSidePixels 跳过，不落盘不写 metadata，计入 Skipped
	SkipFailedList bool // 不计入 failed_urls（已有文件、URL 过滤等，非 HTTP 失败）
}

// NewDownloader 创建新的下载器（可选按 config 从 YAML 加载 HTTP 代理，轮询使用）。
func NewDownloader(cfg *config.Config) *Downloader {
	poolPerHost := cfg.HTTPPoolMaxsize
	if poolPerHost <= 0 {
		poolPerHost = maxInt(cfg.Workers*2, 64)
	}
	if poolPerHost < cfg.Workers {
		poolPerHost = cfg.Workers
	}

	tr := &http.Transport{
		MaxIdleConns:        maxInt(cfg.Workers*4, poolPerHost*2),
		MaxIdleConnsPerHost: poolPerHost,
		MaxConnsPerHost:     poolPerHost,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	var proxyURLs []*url.URL
	if cfg.UseProxy && strings.TrimSpace(cfg.ProxiesYAML) != "" {
		p := strings.TrimSpace(cfg.ProxiesYAML)
		if !filepath.IsAbs(p) {
			p = filepath.Join(cfg.ProjectRoot, p)
		}
		loaded, err := proxy.LoadFromYAML(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warning: 加载代理 YAML 失败，将直连: %v\n", err)
		} else {
			proxyURLs = loaded
			var idx uint64
			tr.Proxy = func(*http.Request) (*url.URL, error) {
				i := atomic.AddUint64(&idx, 1)
				return proxyURLs[int(i-1)%len(proxyURLs)], nil
			}
			fmt.Fprintf(os.Stderr, "已加载 %d 个 HTTP 代理（轮询）\n", len(proxyURLs))
		}
	}

	timeout := time.Duration(cfg.Timeout) * time.Second
	httpClient := &http.Client{
		Timeout:   timeout,
		Transport: tr,
	}
	// api.500px.com：优先直连；数据中心 IP 常被 403，失败时回退走 CDN 同款代理轮询。
	apiClient := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy:               nil,
			MaxIdleConns:        32,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
			DisableKeepAlives:   false,
		},
	}

	return &Downloader{
		cfg:        cfg,
		httpClient: httpClient,
		apiClient:  apiClient,
		stats:      &Stats{},
		userAgents: loadUserAgents(cfg),
	}
}

// Download 下载单个 URL。seen_db 去重在 metadata 写入阶段处理（与 Python 一致）。
func (d *Downloader) Download(url string, downloadDir string) *DownloadResult {
	return d.DownloadExtract(DownloadExtractOpts{
		IdentityURL:     url,
		InitialFetchURL: url,
		FileName:        BaseNameForURL(d.cfg, url),
		PhotoID:         PhotoIDFrom500px(url, ""),
	}, downloadDir)
}

// DownloadExtract extract metadata 下载：fileName 固定，HTTP URL 失败时刷新 m=4096 CDN 链接。
func (d *Downloader) DownloadExtract(opts DownloadExtractOpts, downloadDir string) *DownloadResult {
	identityURL := strings.TrimSpace(opts.IdentityURL)
	if identityURL == "" {
		identityURL = strings.TrimSpace(opts.InitialFetchURL)
	}
	if IsSkipURL(identityURL) {
		atomic.AddInt64(&d.stats.Skipped, 1)
		return &DownloadResult{Success: false, SkipFailedList: true}
	}

	fileName := strings.TrimSpace(opts.FileName)
	if fileName == "" {
		fileName = FileNameFromImageKey(d.cfg, "", identityURL)
	}
	objectID := ObjectIDFromFileName(fileName)
	finalPath := filepath.Join(downloadDir, fileName)

	if d.cfg.DiskGlobFallback {
		matches, _ := filepath.Glob(filepath.Join(downloadDir, fmt.Sprintf("%s.*", objectID)))
		if HasCompleteDownload(matches) {
			atomic.AddInt64(&d.stats.Skipped, 1)
			return &DownloadResult{Success: false, FileName: fileName, ObjectID: objectID, SkipFailedList: true}
		}
	} else if _, err := os.Stat(finalPath); err == nil {
		atomic.AddInt64(&d.stats.Skipped, 1)
		return &DownloadResult{Success: false, FileName: fileName, ObjectID: objectID, SkipFailedList: true}
	}

	tmpPath := filepath.Join(downloadDir, fmt.Sprintf("%s.part", objectID))
	photoID := PhotoIDFrom500px(identityURL, opts.PhotoID)

	initialURL := strings.TrimSpace(opts.InitialFetchURL)
	if initialURL == "" {
		initialURL = identityURL
	}

	var result *DownloadResult
	attempts := d.cfg.Retries + 1
	for i := 0; i < attempts; i++ {
		if initialURL != "" {
			result = d.downloadHTTP(initialURL, tmpPath, fileName, photoID)
			if result != nil && result.Success {
				break
			}
		}
		if photoID != "" {
			if fresh, err := d.refresh500pxCDN4096(photoID); err == nil && fresh != "" && fresh != initialURL {
				result = d.downloadHTTP(fresh, tmpPath, fileName, photoID)
				if result != nil && result.Success {
					break
				}
			}
		}
		if i < attempts-1 {
			time.Sleep(time.Duration(i+1) * 100 * time.Millisecond)
		}
	}

	if result != nil && result.Success {
		atomic.AddInt64(&d.stats.Success, 1)
	} else if result != nil && result.SkippedLowRes {
		atomic.AddInt64(&d.stats.Skipped, 1)
	} else {
		atomic.AddInt64(&d.stats.Failed, 1)
		os.Remove(tmpPath)
	}
	if result == nil {
		return &DownloadResult{Success: false, FileName: fileName, ObjectID: objectID}
	}
	return result
}

// refresh500pxCDN4096 刷新 m=4096 CDN URL：先直连 api.500px.com，403/失败时走代理并重试。
func (d *Downloader) refresh500pxCDN4096(photoID string) (string, error) {
	if fresh, err := Refresh500pxCDN4096(d.apiClient, photoID); err == nil && fresh != "" {
		return fresh, nil
	} else if d.httpClient == nil || d.httpClient == d.apiClient {
		return "", err
	}
	var lastErr error
	const proxyTries = 3
	for i := 0; i < proxyTries; i++ {
		fresh, err := Refresh500pxCDN4096(d.httpClient, photoID)
		if err == nil && fresh != "" {
			return fresh, nil
		}
		lastErr = err
		if err != nil && !apiErrRetryable(err) {
			return "", err
		}
	}
	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("500px API refresh failed for photo %s", photoID)
}

// downloadHTTP 执行 HTTP 下载。targetFileName 为最终 basename（与 metadata image_key 一致）。
func (d *Downloader) downloadHTTP(url, tmpPath, targetFileName, refererPhotoID string) *DownloadResult {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return &DownloadResult{Success: false, Error: err}
	}
	if ua := d.pickUserAgent(); ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	pid := strings.TrimSpace(refererPhotoID)
	if pid == "" {
		if m := photoID500px.FindStringSubmatch(url); len(m) >= 2 {
			pid = m[1]
		}
	}
	if pid != "" {
		req.Header.Set("Referer", "https://500px.com/photo/"+pid)
	} else {
		req.Header.Set("Referer", "https://500px.com/")
	}
	req.Header.Set("Origin", "https://500px.com")
	req.Header.Set("Accept", "image/avif,image/webp,image/apng,image/*,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return &DownloadResult{Success: false, Error: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return &DownloadResult{Success: false, Error: fmt.Errorf("HTTP %d", resp.StatusCode)}
	}

	fileName := strings.TrimSpace(targetFileName)
	if fileName == "" {
		fileName = filepath.Base(tmpPath)
	}
	objectID := ObjectIDFromFileName(fileName)
	finalPath := filepath.Join(filepath.Dir(tmpPath), fileName)

	// 先读入内存最多 512KB，用于分辨率检测
	prefix := make([]byte, 0, 512*1024)
	buf := make([]byte, 64*1024)
	for len(prefix) < 512*1024 {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			prefix = append(prefix, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return &DownloadResult{Success: false, Error: err}
		}
	}

	res := ReadResolutionFromBytes(prefix)
	minSide := 0
	if res != nil {
		if res.Width < res.Height {
			minSide = res.Width
		} else {
			minSide = res.Height
		}
	}

	// 分辨率过滤：最短边 < MinSidePixels 则丢弃剩余 body，不落盘
	if d.cfg.MinSidePixels > 0 && res != nil && minSide < d.cfg.MinSidePixels {
		_, _ = io.Copy(io.Discard, resp.Body)
		return &DownloadResult{Success: false, SkippedLowRes: true, FileName: fileName, ObjectID: objectID}
	}

	// 通过过滤或无法解析分辨率：落盘
	out, err := os.Create(tmpPath)
	if err != nil {
		return &DownloadResult{Success: false, Error: err}
	}
	if _, err := out.Write(prefix); err != nil {
		out.Close()
		os.Remove(tmpPath)
		return &DownloadResult{Success: false, Error: err}
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		os.Remove(tmpPath)
		return &DownloadResult{Success: false, Error: err}
	}
	if err := out.Close(); err != nil {
		os.Remove(tmpPath)
		return &DownloadResult{Success: false, Error: err}
	}

	resStr := "0x0"
	if res != nil {
		resStr = fmt.Sprintf("%dx%d", res.Width, res.Height)
	}
	timestamp := timestampFromHeaders(resp.Header)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return &DownloadResult{Success: false, Error: err}
	}
	return &DownloadResult{
		Success:    true,
		FileName:   fileName,
		Timestamp:  timestamp,
		Resolution: resStr,
		ObjectID:   objectID,
	}
}

// GetStats 获取统计信息
func (d *Downloader) GetStats() (int64, int64, int64) {
	return atomic.LoadInt64(&d.stats.Success), atomic.LoadInt64(&d.stats.Failed), atomic.LoadInt64(&d.stats.Skipped)
}

// sha1Hex 计算 SHA1 哈希
func sha1Hex(s string) string {
	h := sha1.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

// HasCompleteDownload 是否存在可复用的成品（排除 *.part / *.up.tmp* 等临时文件）。
func HasCompleteDownload(matches []string) bool {
	for _, p := range matches {
		base := strings.ToLower(filepath.Base(p))
		if strings.HasSuffix(base, ".part") || strings.Contains(base, ".up.tmp") {
			continue
		}
		return true
	}
	return false
}

// extFromURL 从 URL 提取扩展名
func extFromURL(url string) string {
	idx := strings.Index(url, "?")
	if idx != -1 {
		url = url[:idx]
	}
	ext := strings.ToLower(filepath.Ext(url))
	if ext != "" {
		ext = ext[1:] // 去掉点号
	}
	return ext
}

// extFromContentType 从 Content-Type 提取扩展名
func extFromContentType(ct string) string {
	ct = strings.ToLower(ct)
	ct = strings.Split(ct, ";")[0]
	ct = strings.TrimSpace(ct)
	switch ct {
	case "image/jpeg":
		return "jpg"
	case "image/png":
		return "png"
	case "image/webp":
		return "webp"
	case "image/gif":
		return "gif"
	case "image/heic":
		return "heic"
	}
	return ""
}

// timestampFromHeaders 从 HTTP headers 获取时间戳
func timestampFromHeaders(headers http.Header) string {
	// 优先使用 Last-Modified，否则使用 Date
	if lastModified := headers.Get("Last-Modified"); lastModified != "" {
		return strings.TrimSpace(lastModified)
	}
	if date := headers.Get("Date"); date != "" {
		return strings.TrimSpace(date)
	}
	// 如果没有，返回当前 UTC 时间
	return time.Now().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
}

func loadUserAgents(cfg *config.Config) []string {
	if strings.TrimSpace(cfg.HTTPUserAgent) != "" {
		return []string{strings.TrimSpace(cfg.HTTPUserAgent)}
	}
	if !cfg.UseUserAgents {
		return nil
	}
	agents := []string{
		"Mozilla/5.0 (compatible; pinterest-downloader)",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
	}
	uaPath := filepath.Join(cfg.ProjectRoot, "config", "user_agents.txt")
	f, err := os.Open(uaPath)
	if err != nil {
		return agents
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		agents = append(agents, line)
	}
	return agents
}

func (d *Downloader) pickUserAgent() string {
	if strings.TrimSpace(d.cfg.HTTPUserAgent) != "" {
		return strings.TrimSpace(d.cfg.HTTPUserAgent)
	}
	if len(d.userAgents) == 0 {
		return "Mozilla/5.0 (compatible; pinterest-downloader)"
	}
	return d.userAgents[rand.Intn(len(d.userAgents))]
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
