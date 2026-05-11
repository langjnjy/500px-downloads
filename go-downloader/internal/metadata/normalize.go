package metadata

import "strings"

// NormalizeImageURLKey 与 scripts/download.py 的 normalize_image_url_key 一致。
func NormalizeImageURLKey(u string) string {
	k := strings.TrimSpace(u)
	for strings.HasSuffix(k, "/") {
		k = strings.TrimSuffix(k, "/")
	}
	if i := strings.Index(k, "?"); i >= 0 {
		k = k[:i]
	}
	return k
}
