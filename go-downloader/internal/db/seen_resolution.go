package db

import "strings"

// extract_metadata 下载写入 seen.route 时使用的路径标识（仅两种取值）。
const (
	SeenResolutionLargeDirect = "large_direct"
	SeenResolutionCV2Upscale  = "cv2_upscale"
)

// FailedRowMatchesRetryPolicy 在 retry_failed 时是否应入队：与 extract_metadata_resolution_policy 一致，
// metadata_large 只重试 route=large_direct；metadata_small 只重试 route=cv2_upscale；full 重试全部 failed。
func FailedRowMatchesRetryPolicy(policyNorm, route string) bool {
	rs := strings.TrimSpace(strings.ToLower(route))
	switch strings.TrimSpace(strings.ToLower(policyNorm)) {
	case "metadata_large":
		return rs == strings.ToLower(SeenResolutionLargeDirect)
	case "metadata_small":
		return rs == strings.ToLower(SeenResolutionCV2Upscale)
	case "full":
		return true
	default:
		return true
	}
}
