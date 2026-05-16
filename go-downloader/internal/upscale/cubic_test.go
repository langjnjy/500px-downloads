package upscale

import (
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"testing"
)

// 冒烟：OpenCV + CGO 可用，且 2x cubic 输出尺寸正确。
func TestRunCubic2x_smoke(t *testing.T) {
	dir := t.TempDir()
	inPath := filepath.Join(dir, "in.png")
	outPath := filepath.Join(dir, "out.png")

	// 宽 8 高 6 的 RGBA 图（与旧测试一致：宽×高）
	img := image.NewRGBA(image.Rect(0, 0, 8, 6))
	for y := 0; y < 6; y++ {
		for x := 0; x < 8; x++ {
			img.SetRGBA(x, y, color.RGBA{R: byte(x), G: byte(y), B: 40, A: 255})
		}
	}
	f, err := os.Create(inPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := png.Encode(f, img); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	_ = f.Close()
	t.Cleanup(func() {
		_ = os.Remove(inPath)
		_ = os.Remove(outPath)
	})

	if err := RunCubic2x("", "", inPath, outPath); err != nil {
		t.Fatal(err)
	}

	of, err := os.Open(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer of.Close()
	dec, err := png.Decode(of)
	if err != nil {
		t.Fatal(err)
	}
	b := dec.Bounds()
	gotW, gotH := b.Dx(), b.Dy()
	if gotW != 16 || gotH != 12 {
		t.Fatalf("expected 16x12 got %dx%d", gotW, gotH)
	}
}
