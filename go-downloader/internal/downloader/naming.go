package downloader

import (
	"path/filepath"
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

// FileNameFromImageKey extract metadata 的 image_key basename；缺失时回退 BaseNameForURL(identityURL)。
func FileNameFromImageKey(cfg *config.Config, imageKey, identityURL string) string {
	key := strings.TrimSpace(imageKey)
	if key != "" {
		base := filepath.Base(key)
		if base != "" && base != "." && base != string(filepath.Separator) {
			return base
		}
	}
	return BaseNameForURL(cfg, identityURL)
}

// ObjectIDFromFileName 文件名去掉扩展名，用作 .part 临时文件前缀。
func ObjectIDFromFileName(fileName string) string {
	ext := filepath.Ext(fileName)
	if ext == "" {
		return fileName
	}
	return strings.TrimSuffix(fileName, ext)
}

// PhotoIDFrom500px 从 drscdn URL 或十进制字符串解析 legacy photo id。
func PhotoIDFrom500px(rawURL, explicitID string) string {
	if id := strings.TrimSpace(explicitID); id != "" {
		return id
	}
	if m := photoID500px.FindStringSubmatch(rawURL); len(m) >= 2 {
		return m[1]
	}
	return ""
}
