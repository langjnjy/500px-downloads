package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverExtractMetadataFiles(t *testing.T) {
	root := t.TempDir()
	metaDir := filepath.Join(root, "output", "metadata")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"extract_metadata_10.jsonl",
		"extract_metadata_2.jsonl",
		"extract_metadata_1.jsonl",
		"other.jsonl",
		"extract_metadata_x.jsonl",
	} {
		if err := os.WriteFile(filepath.Join(metaDir, name), []byte("{}"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	got, err := DiscoverExtractMetadataFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(metaDir, "extract_metadata_1.jsonl"),
		filepath.Join(metaDir, "extract_metadata_2.jsonl"),
		filepath.Join(metaDir, "extract_metadata_10.jsonl"),
	}
	if len(got) != len(want) {
		t.Fatalf("got %d files, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("index %d: got %q want %q", i, got[i], want[i])
		}
	}
}
