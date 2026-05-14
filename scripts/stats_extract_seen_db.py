#!/usr/bin/env python3
"""统计 extract 专用 seen DB：已完成用户数、写入 metadata 行数累加等。

默认读取 ``config/extract.yaml`` 中的 ``extract_profiles_seen_db``（相对仓库 root）。
表 ``extract_500px_user_profiles_done``：每行对应一个已成功 extract 的 profile（uid）。

- ``metadata_lines``：该用户本次成功 flush 的 **新写入** metadata 行数（与 extract 日志 ``new_metadata_lines`` 一致）。
- ``eligible_count``：该用户 dump 后经分辨率等筛选后的条目数（旧库可能无此列）。

默认 stdout 只输出两行 JSON 中的两个字段：``profiles_done``、``sum_metadata_lines``。
加 ``--verbose`` 可输出完整 JSON（含路径、字节累加、eligible 等）。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, Optional


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_extract_yaml(root: Path) -> Dict[str, Any]:
    import yaml

    p = root / "config" / "extract.yaml"
    if not p.is_file():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _seen_db_path(root: Path, override: Optional[Path]) -> Path:
    if override is not None:
        p = override.expanduser()
        return p.resolve() if p.is_absolute() else (root / p).resolve()
    cfg = _load_extract_yaml(root)
    rel = str(cfg.get("extract_profiles_seen_db") or "output/state/extract_500px_user_profiles_seen.db").strip()
    p = Path(rel)
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def main() -> int:
    ap = argparse.ArgumentParser(description="extract seen DB 用户数与 metadata 行数汇总")
    ap.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite 路径；默认使用 config/extract.yaml 的 extract_profiles_seen_db",
    )
    ap.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="输出完整 JSON（默认仅 profiles_done + sum_metadata_lines）",
    )
    args = ap.parse_args()

    root = _repo_root()
    db_path = _seen_db_path(root, args.db)
    if not db_path.is_file():
        print(f"找不到数据库: {db_path}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='extract_500px_user_profiles_done'"
        )
        if cur.fetchone() is None:
            print(f"表 extract_500px_user_profiles_done 不存在: {db_path}", file=sys.stderr)
            return 2

        cur = conn.execute("PRAGMA table_info(extract_500px_user_profiles_done)")
        cols = {str(row[1]) for row in cur.fetchall()}
        has_eligible = "eligible_count" in cols

        row = conn.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(metadata_lines), 0),
                   COALESCE(SUM(dump_json_bytes), 0)
            FROM extract_500px_user_profiles_done
            """
        ).fetchone()
        n_profiles = int(row[0] or 0)
        sum_metadata_lines = int(row[1] or 0)
        sum_dump_bytes = int(row[2] or 0)

        out: Dict[str, Any] = {
            "db_path": str(db_path),
            "profiles_done": n_profiles,
            "sum_metadata_lines": sum_metadata_lines,
            "sum_dump_json_bytes": sum_dump_bytes,
        }
        if has_eligible:
            r2 = conn.execute(
                "SELECT COALESCE(SUM(eligible_count), 0) FROM extract_500px_user_profiles_done"
            ).fetchone()
            out["sum_eligible_count"] = int(r2[0] or 0)

        # 可选：distinct username 数（通常与 profiles_done 相同，除非 profile_url 重复写法）
        row3 = conn.execute(
            "SELECT COUNT(DISTINCT username) FROM extract_500px_user_profiles_done"
        ).fetchone()
        out["distinct_usernames"] = int(row3[0] or 0)

        if args.verbose:
            print(json.dumps(out, ensure_ascii=False, indent=2))
        else:
            print(
                json.dumps(
                    {
                        "profiles_done": n_profiles,
                        "sum_metadata_lines": sum_metadata_lines,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
