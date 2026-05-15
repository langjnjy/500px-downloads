package metadata

import (
	"strings"

	"github.com/unsplash_downloads/go-downloader/internal/config"
)

// SeenDedupeKey 与 extract / crawl_hash 对齐：crawl_hash 用 Trim 后的完整 URL（含 query）做 seen 主键；
// 其它模式沿用 NormalizeImageURLKey（与历史 download 行为一致）。
func SeenDedupeKey(cfg *config.Config, rawURL string) string {
	u := strings.TrimSpace(rawURL)
	if cfg != nil && strings.EqualFold(strings.TrimSpace(cfg.ImageKeyStyle), "crawl_hash") {
		return u
	}
	return NormalizeImageURLKey(rawURL)
}
