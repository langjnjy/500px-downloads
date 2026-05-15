#!/usr/bin/env python3
"""从 ``output/metadata/extract_metadata_{n}.jsonl`` 每行解析 JSON，取出 ``image_url`` 写入 ``extract_urls_{n}``（每行一条 URL）。

默认扫描目录下所有符合 ``extract_metadata_<正整数>.jsonl`` 的文件，有几个处理几个；输出文件已存在则覆盖（``open(..., "w")``）。

无法解析 JSON、或缺少 / 非字符串 / 空白的 ``image_url``：向 stderr 打一行摘要，并向缺失日志写入一条 JSON（含分片文件名、物理行号、原始行文本）。

可选 ``--shards 1-8`` 或 ``7,9`` 仅处理指定编号（仍要求对应 jsonl 存在）。
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, TextIO

_METADATA_GLOB = re.compile(r"^extract_metadata_(\d+)\.jsonl$")


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _default_metadata_dir() -> Path:
    """默认：本仓库常用绝对路径；若不存在则退回仓库 ``output/metadata``。"""
    fixed = Path("/home/ubuntu/500px-downloads/output/metadata")
    if fixed.is_dir():
        return fixed.resolve()
    return (_repo_root() / "output/metadata").resolve()


def _discover_shard_indices(meta_dir: Path) -> List[int]:
    found: List[int] = []
    for p in meta_dir.iterdir():
        if not p.is_file():
            continue
        m = _METADATA_GLOB.match(p.name)
        if m:
            found.append(int(m.group(1)))
    return sorted(set(found))


def _image_url_from_obj(obj: Any) -> Optional[str]:
    if not isinstance(obj, dict):
        return None
    v = obj.get("image_url")
    if not isinstance(v, str):
        return None
    s = v.strip()
    return s or None


def _write_missing_record(
    log_fp: TextIO,
    *,
    shard_name: str,
    line_number: int,
    reason: str,
    raw_line: str,
) -> None:
    rec = {
        "event": "missing_or_bad_image_url",
        "reason": reason,
        "shard": shard_name,
        "line_number": line_number,
        "raw_line": raw_line,
    }
    log_fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
    log_fp.flush()


def _process_one_shard(
    meta_dir: Path,
    shard_index: int,
    log_fp: TextIO,
) -> tuple[int, int, int]:
    shard_name = f"extract_metadata_{shard_index}.jsonl"
    inp = meta_dir / shard_name
    outp = meta_dir / f"extract_urls_{shard_index}"
    ok = bad = 0
    line_number = 0
    if not inp.is_file():
        print(f"跳过（不存在）: {inp}", file=sys.stderr)
        return 0, 0, 0

    # "w"：输出文件已存在则覆盖
    with inp.open(encoding="utf-8", errors="replace") as fin, outp.open(
        "w", encoding="utf-8", newline="\n"
    ) as fout:
        for line_number, line in enumerate(fin, start=1):
            raw = line.rstrip("\n\r")
            if not raw.strip():
                reason = "empty_physical_line"
                print(
                    f"[{shard_name}] line {line_number}: {reason}",
                    file=sys.stderr,
                )
                _write_missing_record(
                    log_fp,
                    shard_name=shard_name,
                    line_number=line_number,
                    reason=reason,
                    raw_line=raw,
                )
                bad += 1
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as e:
                reason = f"json_decode_error:{e}"
                print(
                    f"[{shard_name}] line {line_number}: {reason}",
                    file=sys.stderr,
                )
                _write_missing_record(
                    log_fp,
                    shard_name=shard_name,
                    line_number=line_number,
                    reason=reason,
                    raw_line=raw,
                )
                bad += 1
                continue
            url = _image_url_from_obj(obj)
            if url is None:
                reason = "missing_or_empty_image_url"
                if isinstance(obj, dict) and "image_url" in obj:
                    reason = "image_url_not_nonempty_string"
                print(
                    f"[{shard_name}] line {line_number}: {reason}",
                    file=sys.stderr,
                )
                _write_missing_record(
                    log_fp,
                    shard_name=shard_name,
                    line_number=line_number,
                    reason=reason,
                    raw_line=raw,
                )
                bad += 1
                continue
            fout.write(url + "\n")
            ok += 1
    return ok, bad, line_number


def _parse_shards_spec(spec: str) -> List[int]:
    shards: List[int] = []
    s = (spec or "").strip().replace(" ", "")
    for part in s.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            shards.extend(range(int(a), int(b) + 1))
        else:
            shards.append(int(part))
    return sorted(set(n for n in shards if n >= 1))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="从 extract_metadata_*.jsonl 提取 image_url 到 extract_urls_*（覆盖已存在输出）"
    )
    ap.add_argument(
        "--metadata-dir",
        type=Path,
        default=_default_metadata_dir(),
        help="metadata 目录（默认 /home/ubuntu/500px-downloads/output/metadata，若不存在则用仓库 output/metadata）",
    )
    ap.add_argument(
        "--shards",
        type=str,
        default="auto",
        help='分片：auto=扫描目录下全部 extract_metadata_n.jsonl；或 "1-8"、"7,9"',
    )
    ap.add_argument(
        "--missing-log",
        type=Path,
        default=None,
        help="缺失/坏行日志（JSONL）；默认 metadata-dir/extract_urls_missing_image_url.jsonl",
    )
    args = ap.parse_args()

    root = _repo_root()
    meta_dir = args.metadata_dir.expanduser()
    if not meta_dir.is_absolute():
        meta_dir = (root / meta_dir).resolve()
    else:
        meta_dir = meta_dir.resolve()
    meta_dir.mkdir(parents=True, exist_ok=True)

    miss = args.missing_log
    if miss is None:
        miss = meta_dir / "extract_urls_missing_image_url.jsonl"
    else:
        miss = miss.expanduser()
        if not miss.is_absolute():
            miss = (root / miss).resolve()
    miss.parent.mkdir(parents=True, exist_ok=True)

    sarg = (args.shards or "").strip().lower()
    if sarg in ("", "auto", "all"):
        shards = _discover_shard_indices(meta_dir)
        if not shards:
            print(
                f"未在目录中找到 extract_metadata_<n>.jsonl: {meta_dir}",
                file=sys.stderr,
            )
            return 2
        print(
            f"自动发现分片: {shards}（目录 {meta_dir}）",
            file=sys.stderr,
        )
    else:
        shards = _parse_shards_spec(args.shards)
        if not shards:
            print("未指定有效分片（须为正整数，如 1-8 或 7,9）", file=sys.stderr)
            return 2

    total_ok = total_bad = 0
    with miss.open("w", encoding="utf-8", newline="\n") as log_fp:
        for n in shards:
            ok, bad, last_ln = _process_one_shard(meta_dir, n, log_fp)
            total_ok += ok
            total_bad += bad
            print(
                f"shard{n}: ok_lines={ok} bad_lines={bad} last_line_number={last_ln} -> extract_urls_{n}",
                file=sys.stderr,
            )

    print(
        json.dumps(
            {
                "metadata_dir": str(meta_dir),
                "shards": shards,
                "ok_lines": total_ok,
                "bad_lines": total_bad,
                "missing_log": str(miss),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
