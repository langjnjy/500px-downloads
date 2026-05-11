#!/usr/bin/env python3
"""把 combo_seen 库里「旧格式」键迁移为显式 RELEVANCE。

旧键仅有 ``s:|m:``；新键为 ``s:|m:|sort:RELEVANCE``。插入新行并保留原 ``completed_at``；
原短键保留不动（可与批量脚本的 legacy 兼容并存）。

**前提**：这些短键确实是默认排序 / RELEVANCE 下跑出来的。若曾为 ``--no-sort`` 单独跑过，
同一短键不应迁移为 RELEVANCE（需手工删行或换库）。

用法::

  python3 scripts/migrate_combo_seen_legacy_to_relevance.py output/500px_multiselect_combo_seen.sqlite
  python3 scripts/migrate_combo_seen_legacy_to_relevance.py path/to.sqlite --dry-run
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description="combo_seen：短键补齐 |sort:RELEVANCE")
    ap.add_argument(
        "db",
        type=Path,
        help="combo_seen SQLite（seen_combo 表）",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将插入多少条，不写库",
    )
    args = ap.parse_args()
    db = args.db.expanduser().resolve()
    if not db.is_file():
        print(f"找不到数据库: {db}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(str(db))
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='seen_combo'"
        ).fetchone()
        if row is None:
            print("库中无 seen_combo 表", file=sys.stderr)
            return 2

        rows = conn.execute(
            """
            SELECT combo_key, completed_at FROM seen_combo
            WHERE combo_key NOT LIKE '%|sort:%'
            """
        ).fetchall()
    finally:
        conn.close()

    to_add: list[tuple[str, str]] = []
    for key, completed_at in rows:
        new_key = f"{key}|sort:RELEVANCE"
        to_add.append((new_key, str(completed_at)))

    print(f"短键（无 |sort:）共 {len(rows)} 条；将插入显式 RELEVANCE 键 {len(to_add)} 条", flush=True)
    if args.dry_run:
        for nk, _ in to_add[:10]:
            print(f"  + {nk}")
        if len(to_add) > 10:
            print(f"  ... 另有 {len(to_add) - 10} 条")
        return 0

    conn = sqlite3.connect(str(db))
    try:
        n = 0
        for new_key, completed_at in to_add:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO seen_combo (combo_key, completed_at)
                VALUES (?, ?)
                """,
                (new_key, completed_at),
            )
            n += cur.rowcount
        conn.commit()
        print(f"实际新插入行数（IGNORE 已存在键时为 0）: {n}", flush=True)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
