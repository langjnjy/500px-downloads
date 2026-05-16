package upscale

/*
#cgo pkg-config: opencv4
#cgo CXXFLAGS: -std=c++11
#include <stdlib.h>
extern int upscale_cubic_2x(const char *inpath, const char *outpath);
*/
import "C"

import (
	"fmt"
	"path/filepath"
	"unsafe"
)

// RunCubic2x 使用 CGO + OpenCV（INTER_CUBIC）将图像放大 2 倍写入 outPath。
// pythonBin、scriptPath 已弃用，保留仅为兼容调用方签名。
func RunCubic2x(_, _, inPath, outPath string) error {
	inPath = filepath.Clean(inPath)
	outPath = filepath.Clean(outPath)
	cin := C.CString(inPath)
	cout := C.CString(outPath)
	defer C.free(unsafe.Pointer(cin))
	defer C.free(unsafe.Pointer(cout))
	rc := C.upscale_cubic_2x(cin, cout)
	if rc != 0 {
		return fmt.Errorf("upscale: upscale_cubic_2x rc=%d in=%s out=%s", int(rc), inPath, outPath)
	}
	return nil
}
