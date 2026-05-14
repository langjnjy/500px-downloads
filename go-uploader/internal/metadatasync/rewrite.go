package metadatasync

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RewriteImageKeysForBasenames 扫描 metadataDir 下 *.metadata.jsonl；若某行 JSON 的 image_key 或
// image_url 的 basename 落在 basenames 中，则将 image_key 设为 prefix + "/" + basename（正斜杠）。
func RewriteImageKeysForBasenames(metadataDir, prefix string, basenames map[string]struct{}) error {
	if len(basenames) == 0 {
		return nil
	}
	prefix = strings.TrimSpace(prefix)
	prefix = strings.TrimRight(prefix, "/")
	if prefix == "" {
		return nil
	}
	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		return fmt.Errorf("read metadata dir %s: %w", metadataDir, err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".metadata.jsonl") {
			continue
		}
		p := filepath.Join(metadataDir, e.Name())
		if err := rewriteOneJSONL(p, prefix, basenames); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
	}
	return nil
}

func basenameFromImageKeyOrURL(obj map[string]interface{}) string {
	if ik, ok := obj["image_key"].(string); ok && ik != "" {
		return filepath.Base(strings.ReplaceAll(ik, "\\", "/"))
	}
	if iu, ok := obj["image_url"].(string); ok && iu != "" {
		u := strings.SplitN(iu, "?", 2)[0]
		return filepath.Base(strings.ReplaceAll(u, "\\", "/"))
	}
	return ""
}

func rewriteOneJSONL(path, prefix string, basenames map[string]struct{}) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()

	outPath := path + ".tmp"
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}

	sc := bufio.NewScanner(in)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)

	changed := false
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var obj map[string]interface{}
		if err := json.Unmarshal(line, &obj); err != nil {
			if _, werr := out.Write(append(append([]byte{}, line...), '\n')); werr != nil {
				_ = out.Close()
				_ = os.Remove(outPath)
				return werr
			}
			continue
		}
		base := basenameFromImageKeyOrURL(obj)
		if base == "" {
			if _, werr := out.Write(append(append([]byte{}, line...), '\n')); werr != nil {
				_ = out.Close()
				_ = os.Remove(outPath)
				return werr
			}
			continue
		}
		if _, hit := basenames[base]; !hit {
			if _, werr := out.Write(append(append([]byte{}, line...), '\n')); werr != nil {
				_ = out.Close()
				_ = os.Remove(outPath)
				return werr
			}
			continue
		}
		newKey := prefix + "/" + base
		obj["image_key"] = newKey
		newLine, err := json.Marshal(obj)
		if err != nil {
			if _, werr := out.Write(append(append([]byte{}, line...), '\n')); werr != nil {
				_ = out.Close()
				_ = os.Remove(outPath)
				return werr
			}
			continue
		}
		if _, err := out.Write(append(newLine, '\n')); err != nil {
			_ = out.Close()
			_ = os.Remove(outPath)
			return err
		}
		changed = true
	}
	if err := sc.Err(); err != nil {
		_ = out.Close()
		_ = os.Remove(outPath)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(outPath)
		return err
	}
	if !changed {
		return os.Remove(outPath)
	}
	if err := os.Rename(outPath, path); err != nil {
		_ = os.Remove(outPath)
		return err
	}
	return nil
}
