#!/usr/bin/env python3
"""文本文件按行去重：保留第一次出现的顺序，默认就地覆盖（先写临时文件再替换）。

未指定输入文件时，默认 ``output/500px_photographers_all_usernames.txt``（相对仓库根）。

示例：

  python3 scripts/dedupe_text_lines.py
  python3 scripts/dedupe_text_lines.py --dry-run

  python3 scripts/dedupe_text_lines.py output/500px_photographers_all_usernames.txt

  python3 scripts/dedupe_text_lines.py -i a.txt -o b.txt
  python3 scripts/dedupe_text_lines.py input.txt --dry-run
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


DEFAULT_INPUT_RELPATH = "output/500px_photographers_all_usernames.txt"


def _dry_run_counts(inp: Path) -> tuple[int, int]:
    seen: set[str] = set()
    total_in = 0
    unique_out = 0
    with inp.open(encoding="utf-8", errors="replace") as fin:
        for line in fin:
            total_in += 1
            key = line.rstrip("\n\r")
            if key in seen:
                continue
            seen.add(key)
            unique_out += 1
    return total_in, unique_out


def main() -> int:
    ap = argparse.ArgumentParser(description="按行去重（保留首次出现顺序）")
    ap.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=None,
        help=f"输入文件（也可用 -i）；省略则默认 {DEFAULT_INPUT_RELPATH}",
    )
    ap.add_argument("-i", "--input-file", type=Path, dest="input_alt", default=None)
    ap.add_argument("-o", "--output", type=Path, default=None, help="输出路径；默认覆盖输入")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="只统计行数，不写文件",
    )
    args = ap.parse_args()

    inp = args.input or args.input_alt
    if inp is None:
        inp = Path(DEFAULT_INPUT_RELPATH)

    inp = inp.expanduser()
    if not inp.is_absolute():
        inp = (_repo_root() / inp).resolve()
    if not inp.is_file():
        print(f"找不到输入文件: {inp}", file=sys.stderr)
        return 2

    if args.dry_run:
        total_in, unique_out = _dry_run_counts(inp)
        dup = total_in - unique_out
        print(
            f"{inp}\n"
            f"  总行数: {total_in}\n"
            f"  去重后: {unique_out}\n"
            f"  去掉重复: {dup}",
            flush=True,
        )
        return 0

    seen: set[str] = set()
    total_in = 0
    unique_written = 0

    out_path = args.output
    if out_path is not None:
        out_path = out_path.expanduser()
        if not out_path.is_absolute():
            out_path = (_repo_root() / out_path).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        dest_dir = str(out_path.parent)
        final_path = out_path
    else:
        dest_dir = str(inp.parent)
        final_path = inp

    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        delete=False,
        prefix=".dedupe_",
        suffix=".tmp",
        dir=dest_dir,
    ) as fout:
        tmp_name = fout.name
        try:
            with inp.open(encoding="utf-8", errors="replace") as fin:
                for line in fin:
                    total_in += 1
                    key = line.rstrip("\n\r")
                    if key in seen:
                        continue
                    seen.add(key)
                    fout.write(key + "\n")
                    unique_written += 1
        except BaseException:
            Path(tmp_name).unlink(missing_ok=True)
            raise

    shutil.move(tmp_name, final_path)

    dup = total_in - unique_written
    if args.output is not None:
        print(
            f"写入 {final_path}：去重后 {unique_written} 行（原 {total_in} 行，去掉重复 {dup}）",
            flush=True,
        )
    else:
        print(
            f"已覆盖 {final_path}：去重后 {unique_written} 行（原 {total_in} 行，去掉重复 {dup}）",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
