#!/usr/bin/env python3
"""统计 download 专用 seen DB：成功（ok）、失败（failed）与总条数。

默认读取 ``go-downloader/config/download-500px.yaml`` 中的
``metadata_seen_db_path_template``（相对仓库 root）。
表 ``seen``：``status`` 为 ``ok`` 表示已成功下载，``failed`` 表示失败。

默认 stdout 输出 JSON：``total``、``ok``、``failed``。
加 ``--verbose`` 可输出完整 JSON（含路径、其他 status 等）。
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


def _load_download_yaml(root: Path, config_path: Optional[Path]) -> Dict[str, Any]:
    import yaml

    if config_path is not None:
        p = config_path.expanduser()
        cfg_file = p.resolve() if p.is_absolute() else (root / p).resolve()
    else:
        cfg_file = root / "go-downloader" / "config" / "download-500px.yaml"
    if not cfg_file.is_file():
        return {}
    return yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}


def _seen_db_path(root: Path, cfg: Dict[str, Any], override: Optional[Path]) -> Path:
    if override is not None:
        p = override.expanduser()
        return p.resolve() if p.is_absolute() else (root / p).resolve()
    rel = str(cfg.get("metadata_seen_db_path_template") or "output/metadata/seen/500px_seen.db").strip()
    p = Path(rel)
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def main() -> int:
    ap = argparse.ArgumentParser(description="download seen DB 成功/失败/总数统计")
    ap.add_argument(
        "--db",
        type=Path,
        default=None,
        help="SQLite 路径；默认使用 download-500px.yaml 的 metadata_seen_db_path_template",
    )
    ap.add_argument(
        "--config",
        type=Path,
        default=None,
        help="download 配置文件；默认 go-downloader/config/download-500px.yaml",
    )
    ap.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="输出完整 JSON（默认仅 total + ok + failed）",
    )
    args = ap.parse_args()

    root = _repo_root()
    cfg = _load_download_yaml(root, args.config)
    db_path = _seen_db_path(root, cfg, args.db)
    if not db_path.is_file():
        print(f"找不到数据库: {db_path}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='seen'")
        if cur.fetchone() is None:
            print(f"表 seen 不存在: {db_path}", file=sys.stderr)
            return 2

        row = conn.execute(
            """
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN lower(trim(coalesce(status, ''))) = 'ok' THEN 1 ELSE 0 END) AS ok,
              SUM(CASE WHEN lower(trim(coalesce(status, ''))) = 'failed' THEN 1 ELSE 0 END) AS failed
            FROM seen
            """
        ).fetchone()
        total = int(row[0] or 0)
        ok = int(row[1] or 0)
        failed = int(row[2] or 0)
        other = total - ok - failed

        out: Dict[str, Any] = {
            "db_path": str(db_path),
            "total": total,
            "ok": ok,
            "failed": failed,
        }
        if other:
            out["other_status"] = other

        if args.verbose:
            status_rows = conn.execute(
                """
                SELECT lower(trim(coalesce(status, ''))) AS st, COUNT(*) AS n
                FROM seen
                GROUP BY 1
                ORDER BY n DESC
                """
            ).fetchall()
            out["status_breakdown"] = {str(st or "(empty)"): int(n) for st, n in status_rows}
            print(json.dumps(out, ensure_ascii=False, indent=2))
        else:
            print(
                json.dumps(
                    {"total": total, "ok": ok, "failed": failed},
                    ensure_ascii=False,
                    indent=2,
                )
            )
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
