package downloader

import (
	"testing"

	"github.com/unsplash_downloads/go-downloader/internal/config"
)

func TestFileNameFromImageKey(t *testing.T) {
	cfg := &config.Config{ImageKeyStyle: "crawl_hash"}
	url := "https://drscdn.500px.org/photo/1/m%3D4096/v2?sig=abc"
	key := "500px-downloads/media/deadbeeffeed.jpg"
	got := FileNameFromImageKey(cfg, key, url)
	if got != "deadbeeffeed.jpg" {
		t.Fatalf("got %q want deadbeeffeed.jpg", got)
	}
	fallback := FileNameFromImageKey(cfg, "", url)
	want := BaseNameForURL(cfg, url)
	if fallback != want {
		t.Fatalf("fallback %q want %q", fallback, want)
	}
}
