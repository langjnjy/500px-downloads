//go:build integration

package downloader

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/unsplash_downloads/go-downloader/internal/config"
)

func TestRefresh500pxCDN4096Fallback(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		ProjectRoot:     root,
		Timeout:         30,
		UseProxy:        true,
		ProxiesYAML:     "config/proxies.yaml",
		HTTPPoolMaxsize: 8,
		Workers:         4,
	}
	dl := NewDownloader(cfg)
	fresh, err := dl.refresh500pxCDN4096("1056265517")
	if err != nil {
		t.Fatalf("refresh failed: %v", err)
	}
	if fresh == "" || !strings.Contains(fresh, "drscdn.500px.org") || !strings.Contains(fresh, "m%3D4096") {
		t.Fatalf("unexpected url: %q", fresh)
	}
}
