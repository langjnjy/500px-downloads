package imgmeta

import (
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"

	_ "golang.org/x/image/webp"
)

// DimensionsFromFile 读取图片宽高（支持 jpeg/png/gif/webp）。
func DimensionsFromFile(path string) (w, h int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	cfg, _, err := image.DecodeConfig(f)
	if err != nil {
		return 0, 0, err
	}
	return cfg.Width, cfg.Height, nil
}

// MeetsMinShortMinLong 最短边 >= minShort 且 长边 >= minLong。
func MeetsMinShortMinLong(w, h, minShort, minLong int) bool {
	if w <= 0 || h <= 0 || minShort <= 0 || minLong <= 0 {
		return false
	}
	short, long := w, h
	if w > h {
		short, long = h, w
	}
	return short >= minShort && long >= minLong
}

// FormatResolution 如 "1920x1080"。
func FormatResolution(w, h int) string {
	return fmt.Sprintf("%dx%d", w, h)
}
