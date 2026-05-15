package downloader

import (
	"strings"

	"github.com/unsplash_downloads/go-downloader/internal/config"
)

// IsCrawlHashStyle 与 scripts/download.py 的 image_key_style=crawl_hash 一致。
func IsCrawlHashStyle(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(cfg.ImageKeyStyle), "crawl_hash")
}

// GuessExtFromURL500px 与 scripts/crawl_500px_gallery_dl.py 的 guess_ext_from_url 一致。
func GuessExtFromURL500px(url string) string {
	lower := strings.ToLower(url)
	if strings.Contains(lower, ".png") {
		return "png"
	}
	if strings.Contains(lower, ".webp") || strings.Contains(lower, "webp=true") {
		return "webp"
	}
	return "jpg"
}

// ObjectIDForURL crawl_hash 下为 SHA1(UTF-8 的 URL 字符串)；与 Python crawl.sha1_hex(best_url) 一致（仅 TrimSpace）。
func ObjectIDForURL(cfg *config.Config, url string) string {
	s := strings.TrimSpace(url)
	return sha1Hex(s)
}

// BaseNameForURL 磁盘文件名（无目录）：<sha1>.<ext>。
func BaseNameForURL(cfg *config.Config, url string) string {
	oid := ObjectIDForURL(cfg, url)
	if IsCrawlHashStyle(cfg) {
		return oid + "." + GuessExtFromURL500px(url)
	}
	ext := extFromURL(url)
	if ext == "" {
		return oid + ".jpg"
	}
	return oid + "." + ext
}
