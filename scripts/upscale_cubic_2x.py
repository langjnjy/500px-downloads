#!/usr/bin/env python3
"""OpenCV INTER_CUBIC 将图像放大 2 倍。用法: python upscale_cubic_2x.py <输入路径> <输出路径>"""
import sys

try:
    import cv2  # type: ignore
except ImportError as e:
    print("需要安装 opencv-python: pip install opencv-python-headless", file=sys.stderr)
    raise SystemExit(2) from e


def main() -> None:
    if len(sys.argv) != 3:
        print("usage: upscale_cubic_2x.py <in_path> <out_path>", file=sys.stderr)
        raise SystemExit(2)
    inp, outp = sys.argv[1], sys.argv[2]
    im = cv2.imread(inp, cv2.IMREAD_UNCHANGED)
    if im is None:
        print(f"cv2.imread failed: {inp}", file=sys.stderr)
        raise SystemExit(1)
    if im.ndim == 2:
        h, w = im.shape[:2]
    elif im.ndim >= 3:
        h, w = im.shape[:2]
    else:
        print("unsupported array shape", file=sys.stderr)
        raise SystemExit(1)
    nh, nw = h * 2, w * 2
    out = cv2.resize(im, (nw, nh), interpolation=cv2.INTER_CUBIC)
    if not cv2.imwrite(outp, out):
        print(f"cv2.imwrite failed: {outp}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
