#!/usr/bin/env python3
"""从 ``extract_metadata_{1..8}.jsonl`` 每行解析 JSON，取出 ``image_url`` 写入 ``extract_urls_{n}``（每行一条 URL）。

无法解析 JSON、或缺少 / 非字符串 / 空白的 ``image_url``：向 stderr 打一行摘要，并向缺失日志写入一条 JSON（含分片文件名、物理行号、原始行文本），便于排查 shard 7/8 等异常行。
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional, TextIO


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


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
    if not inp.is_file():
        print(f"跳过（不存在）: {inp}", file=sys.stderr)
        return 0, 0, 0

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


def main() -> int:
    ap = argparse.ArgumentParser(
        description="从 extract_metadata 分片提取 image_url 到 extract_urls_*"
    )
    ap.add_argument(
        "--metadata-dir",
        type=Path,
        default=None,
        help="分片所在目录（默认 {root}/output/metadata）",
    )
    ap.add_argument(
        "--shards",
        type=str,
        default="1-8",
        help="要处理的分片，如 1-8 或 7,8",
    )
    ap.add_argument(
        "--missing-log",
        type=Path,
        default=None,
        help="缺失/坏行日志（JSONL）；默认 metadata-dir/extract_urls_missing_image_url.jsonl",
    )
    args = ap.parse_args()

    root = _repo_root()
    meta_dir = args.metadata_dir
    if meta_dir is None:
        meta_dir = root / "output/metadata"
    else:
        meta_dir = meta_dir.expanduser()
        if not meta_dir.is_absolute():
            meta_dir = (root / meta_dir).resolve()
    meta_dir.mkdir(parents=True, exist_ok=True)

    miss = args.missing_log
    if miss is None:
        miss = meta_dir / "extract_urls_missing_image_url.jsonl"
    else:
        miss = miss.expanduser()
        if not miss.is_absolute():
            miss = (root / miss).resolve()
    miss.parent.mkdir(parents=True, exist_ok=True)

    shards: list[int] = []
    s = (args.shards or "").strip().replace(" ", "")
    for part in s.split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            shards.extend(range(int(a), int(b) + 1))
        else:
            shards.append(int(part))
    shards = sorted(set(n for n in shards if 1 <= n <= 8))
    if not shards:
        print("未指定有效分片（1–8）", file=sys.stderr)
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
            {"shards": shards, "ok_lines": total_ok, "bad_lines": total_bad, "missing_log": str(miss)},
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
