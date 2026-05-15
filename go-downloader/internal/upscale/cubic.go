package upscale

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

// RunCubic2x 调用 Python + OpenCV INTER_CUBIC 将图像放大 2 倍写入 outPath。
func RunCubic2x(pythonBin, scriptPath, inPath, outPath string) error {
	pythonBin = strings.TrimSpace(pythonBin)
	if pythonBin == "" {
		pythonBin = "python3"
	}
	scriptPath = filepath.Clean(scriptPath)
	inPath = filepath.Clean(inPath)
	outPath = filepath.Clean(outPath)
	cmd := exec.Command(pythonBin, scriptPath, inPath, outPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg != "" {
			return fmt.Errorf("upscale: %w: %s", err, msg)
		}
		return fmt.Errorf("upscale: %w", err)
	}
	return nil
}
