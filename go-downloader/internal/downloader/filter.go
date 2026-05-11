package downloader

import "strings"

var skipSuffixes = []string{".m3u8", ".mp4"}

// SetSkipSuffixes 用配置覆盖默认跳过后缀。
func SetSkipSuffixes(items []string) {
	if len(items) == 0 {
		skipSuffixes = []string{".m3u8", ".mp4"}
		return
	}
	out := make([]string, 0, len(items))
	for _, s := range items {
		s = strings.ToLower(strings.TrimSpace(s))
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		out = []string{".m3u8", ".mp4"}
	}
	skipSuffixes = out
}

// IsSkipURL 检查 URL 是否应该跳过
func IsSkipURL(url string) bool {
	u := strings.ToLower(strings.Split(strings.TrimSpace(url), "?")[0])
	for _, s := range skipSuffixes {
		if strings.HasSuffix(u, s) {
			return true
		}
	}
	return false
}

// IsValidURL 检查 URL 是否有效
func IsValidURL(url string) bool {
	s := strings.TrimSpace(url)
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}
