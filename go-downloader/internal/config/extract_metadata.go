package config

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

var extractMetadataFileRe = regexp.MustCompile(`^extract_metadata_(\d+)\.jsonl$`)

const defaultExtractMetadataDir = "output/metadata"

// DiscoverExtractMetadataFiles 扫描 output/metadata 下 extract_metadata_<n>.jsonl，按 n 升序返回绝对路径。
func DiscoverExtractMetadataFiles(projectRoot string) ([]string, error) {
	dir := filepath.Join(projectRoot, defaultExtractMetadataDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("读取 extract metadata 目录失败: %w", err)
	}
	type numbered struct {
		n    int
		path string
	}
	var found []numbered
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		m := extractMetadataFileRe.FindStringSubmatch(ent.Name())
		if m == nil {
			continue
		}
		n, err := strconv.Atoi(m[1])
		if err != nil {
			continue
		}
		found = append(found, numbered{n: n, path: filepath.Join(dir, ent.Name())})
	}
	sort.Slice(found, func(i, j int) bool { return found[i].n < found[j].n })
	out := make([]string, len(found))
	for i, f := range found {
		out[i] = f.path
	}
	return out, nil
}
