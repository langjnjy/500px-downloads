//go:build integration

package downloader

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/unsplash_downloads/go-downloader/internal/config"
)

func TestDownloadExtractFailedSample(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{
		ProjectRoot:      root,
		Timeout:          120,
		UseProxy:         true,
		ProxiesYAML:      "config/proxies.yaml",
		HTTPPoolMaxsize:  40,
		Workers:          40,
		Retries:          3,
		UseUserAgents:    true,
		DiskGlobFallback: false,
		MinSidePixels:    0,
		ImageKeyStyle:    "crawl_hash",
	}
	dl := NewDownloader(cfg)
	tmp := t.TempDir()
	// expired sig from seen.db failed set
	url := "https://drscdn.500px.org/photo/1056265517/m%3D4096/v2?sig=38a6a9212ef35cab8152ff96fe0371710e1860b8a56905276d31c2854d676f"
	res := dl.DownloadExtract(DownloadExtractOpts{
		IdentityURL:     url,
		InitialFetchURL: url,
		FileName:        FileNameFromImageKey(cfg, "", url),
		PhotoID:         "1056265517",
	}, tmp)
	if !res.Success {
		t.Fatalf("download failed: %v skippedLow=%v", res.Error, res.SkippedLowRes)
	}
	files, _ := os.ReadDir(tmp)
	if len(files) == 0 {
		t.Fatal("no output file")
	}
	if !strings.HasSuffix(files[0].Name(), ".jpg") && !strings.HasSuffix(files[0].Name(), ".jpeg") {
		t.Logf("file: %s", files[0].Name())
	}
}
