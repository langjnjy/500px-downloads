#!/usr/bin/env python3
"""按 YAML 或「多选幂集」组合多次调用 fetch_500px_photographers_all.py。

- 默认读取仓库内 ``config/run_500px_photographer_filter_batches.yaml``（若存在），可用 ``--config PATH`` /
  ``--no-config`` 覆盖；命令行参数优先于配置文件。
- 每个子进程都会带上 ``--max-pages``（默认 50）：超过 50 页只取前 50 页；不足则有多少页拿多少。
- ``combo_seen_db`` 的组合键含 **sort**（由 ``fetch_extra_args`` 里的 ``--sort`` / ``--no-sort`` 推断，默认 RELEVANCE）。
  旧库仅有 ``s:|m:`` 的行在 **RELEVANCE** 下仍视为已见，避免重复跑历史数据。
- ``--multiselect-combos-from-yaml``：从 filter id YAML 读 specialty / member id，
  生成 **网页多选语义** 下的所有组合：specialty 子集 × member 子集（member 仅使用 id≠0 的项；空子集=不在请求里加该维 filter）。
  默认 **包含**「两维都不选」的组合；若需与旧行为一致可 ``--exclude-both-empty-combo``。

  python3 scripts/run_500px_photographer_filter_batches.py

  python3 scripts/run_500px_photographer_filter_batches.py --no-config \\
    --multiselect-combos-from-yaml config/500px_usersearch_filter_ids.yaml

手写 runs：``--batches-yaml ...``。传给 fetch 的额外参数写在配置的 ``fetch_extra_args``，或留在命令行末尾。

运行时在 ``output/logs/photographers_filter_batches_<时间戳>.log`` 写详细日志（行首带时间）；
单文件超过 10MB 自动轮转（同前缀 ``.1``、``.2`` …）。控制台只保留简短进度。
"""

from __future__ import annotations

import argparse
import itertools
import logging
import shlex
import sqlite3
import subprocess
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import yaml


DEFAULT_RUN_CONFIG_RELPATH = "config/run_500px_photographer_filter_batches.yaml"

LOG_DIR_RELPATH = "output/logs"
LOG_FILE_PREFIX = "photographers_filter_batches"
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 100


def _init_batch_logger(root: Path) -> tuple[logging.Logger, Path]:
    """文件日志：带时间戳文件名，单文件超 10MB 轮转；控制台不挂 logging（另行简短 print）。"""
    log_dir = root / LOG_DIR_RELPATH
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"{LOG_FILE_PREFIX}_{ts}.log"
    log = logging.getLogger("500px.filter_batches")
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    fh = RotatingFileHandler(
        str(log_path),
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    fh.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    log.addHandler(fh)
    log.propagate = False
    return log, log_path


def _cmd_join(cmd: Sequence[str]) -> str:
    try:
        return shlex.join(list(cmd))
    except Exception:
        return " ".join(cmd)


def _extract_config_cli(argv: Sequence[str]) -> Tuple[bool, Optional[str], List[str]]:
    """解析 ``--no-config`` / ``--config``，返回 (no_config, config_path_or_none, 剩余 argv)。"""
    no_config = False
    config_path: Optional[str] = None
    out: List[str] = []
    i = 0
    av = list(argv)
    while i < len(av):
        a = av[i]
        if a == "--no-config":
            no_config = True
            i += 1
        elif a == "--config":
            if i + 1 >= len(av):
                raise ValueError("--config 需要指向 YAML 文件的路径")
            config_path = av[i + 1]
            i += 2
        elif a.startswith("--config="):
            config_path = a.split("=", 1)[1]
            if not config_path:
                raise ValueError("--config= 的路径不能为空")
            i += 1
        else:
            out.append(a)
            i += 1
    return no_config, config_path, out


def _argv_declares_multiselect(argv: Sequence[str]) -> bool:
    for a in argv:
        if a == "--multiselect-combos-from-yaml" or a.startswith("--multiselect-combos-from-yaml="):
            return True
    return False


def _argv_declares_batches_yaml(argv: Sequence[str]) -> bool:
    for a in argv:
        if a == "--batches-yaml" or a.startswith("--batches-yaml="):
            return True
    return False


def _load_run_config_file(*, root: Path, no_config: bool, config_override: Optional[str]) -> Dict[str, Any]:
    if no_config:
        return {}
    if config_override:
        p = Path(config_override)
        if not p.is_file():
            print(f"找不到配置文件: {p.resolve()}", file=sys.stderr)
            raise SystemExit(2)
        return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    default_p = root / DEFAULT_RUN_CONFIG_RELPATH
    if default_p.is_file():
        return yaml.safe_load(default_p.read_text(encoding="utf-8")) or {}
    return {}


def _defaults_and_fetch_extra_from_run_config(
    cfg: Dict[str, Any], root: Path
) -> Tuple[Dict[str, Any], List[str]]:
    """把运行配置 YAML 转成 argparse set_defaults 用的字典 + fetch 子进程额外参数。"""
    ms = cfg.get("multiselect") if isinstance(cfg.get("multiselect"), dict) else {}
    by = cfg.get("batches_yaml") if isinstance(cfg.get("batches_yaml"), dict) else {}
    lim = cfg.get("limits") if isinstance(cfg.get("limits"), dict) else {}

    fetch_extra: List[str] = []
    raw_fe = cfg.get("fetch_extra_args")
    if isinstance(raw_fe, list):
        fetch_extra = [str(x) for x in raw_fe]
    elif isinstance(raw_fe, str) and raw_fe.strip():
        fetch_extra = shlex.split(raw_fe)

    out: Dict[str, Any] = {}

    mode = str(cfg.get("mode") or "multiselect").strip().lower()
    if mode in ("batches", "batches_yaml", "runs"):
        p = by.get("path")
        if isinstance(p, str) and p.strip():
            out["batches_yaml"] = Path(p.strip())
        out["multiselect_combos_from_yaml"] = None
    else:
        p = ms.get("filter_ids_yaml") or ms.get("filter_ids")
        if isinstance(p, str) and p.strip():
            out["multiselect_combos_from_yaml"] = Path(p.strip())
        # batches_yaml 仅在切换到 runs 模式时使用；多选模式下去掉默认手写 batches 干扰
        out["batches_yaml"] = None

    outp = cfg.get("output")
    if isinstance(outp, str) and outp.strip():
        out["output"] = Path(outp.strip())

    cdb = ms.get("combo_seen_db") if isinstance(ms, dict) else None
    if cdb is None:
        cdb = cfg.get("combo_seen_db")
    if isinstance(cdb, str) and cdb.strip():
        out["combo_seen_db"] = Path(cdb.strip())

    out["max_pages"] = int(lim.get("max_pages", cfg.get("max_pages", 50)))
    out["max_batches"] = int(lim.get("max_batches", cfg.get("max_batches", 0)))
    out["combo_offset"] = int(lim.get("combo_offset", cfg.get("combo_offset", 0)))
    excl = ms.get("exclude_both_empty_combo")
    if excl is None:
        excl = cfg.get("exclude_both_empty_combo", False)
    out["exclude_both_empty_combo"] = bool(excl)
    out["dry_run"] = bool(cfg.get("dry_run", False))

    return out, fetch_extra


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_filter_ids_yaml(path: Path) -> Tuple[List[str], List[str]]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    spec: List[str] = []
    for row in obj.get("specialties") or []:
        if isinstance(row, dict) and row.get("id") is not None:
            spec.append(str(row["id"]).strip())
    mem: List[str] = []
    for row in obj.get("member_types") or []:
        if not isinstance(row, dict) or row.get("id") is None:
            continue
        mid = str(row["id"]).strip()
        if mid == "0":
            continue
        mem.append(mid)
    mem = sorted(set(mem), key=lambda x: int(x) if x.isdigit() else x)
    return spec, mem


def _powerset_including_empty(ids: Sequence[str]) -> Iterator[Tuple[str, ...]]:
    """含空元组：表示该维「不添加任何 filter」。"""
    s = list(ids)
    yield tuple()
    for r in range(1, len(s) + 1):
        yield from itertools.combinations(s, r)


def _iter_multiselect_combo_runs(
    spec_ids: Sequence[str],
    mem_ids: Sequence[str],
    *,
    exclude_both_empty: bool = False,
) -> Iterator[Tuple[Tuple[str, ...], Tuple[str, ...]]]:
    """(specialty 多选子集, member 多选子集)。空 specialty = 不加 SPECIALTIES；空 member = 不加 MEMBER_TYPE。"""
    spec_iter = _powerset_including_empty(spec_ids)
    mem_iter = _powerset_including_empty(mem_ids)
    for ss, ms in itertools.product(spec_iter, mem_iter):
        if exclude_both_empty and not ss and not ms:
            continue
        yield ss, ms


def _combo_run_name(ss: Tuple[str, ...], ms: Tuple[str, ...], index: int) -> str:
    sp = "none" if not ss else "-".join(ss)
    mp = "none" if not ms else "-".join(ms)
    return f"combo_{index}_s_{sp}_m_{mp}"


def _filters_from_combo(
    ss: Tuple[str, ...], ms: Tuple[str, ...]
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for sid in ss:
        out.append({"key": "SPECIALTIES", "value": sid})
    for mid in ms:
        out.append({"key": "MEMBER_TYPE", "value": mid})
    return out


def _combo_filter_key(ss: Tuple[str, ...], ms: Tuple[str, ...]) -> str:
    """仅 filters 的稳定键（旧版 seen 库仅存此格式，无 sort 后缀）。"""
    s_part = ",".join(sorted(ss))
    m_part = ",".join(sorted(ms))
    return f"s:{s_part}|m:{m_part}"


def _combo_stable_key(ss: Tuple[str, ...], ms: Tuple[str, ...], sort_sig: str) -> str:
    """与迭代次序无关的稳定键；含 sort，便于同一 filters 换排序再跑一轮。"""
    return f"{_combo_filter_key(ss, ms)}|sort:{sort_sig}"


def _usersearch_sort_signature(fetch_argv: Sequence[str]) -> str:
    """从传给 fetch 的 argv 推断 GraphQL sort；与 fetch 默认一致（无 flag -> RELEVANCE）。"""
    av = list(fetch_argv)
    i = 0
    while i < len(av):
        a = av[i]
        if a == "--no-sort":
            return "null"
        if a == "--sort" and i + 1 < len(av):
            return str(av[i + 1]).strip().upper()
        if a.startswith("--sort="):
            return a.split("=", 1)[1].strip().upper()
        i += 1
    return "RELEVANCE"


def _combo_is_seen(
    conn: sqlite3.Connection,
    stable_key: str,
    *,
    legacy_filter_key: Optional[str],
) -> bool:
    """legacy_filter_key：旧库仅有 s:|m: 且本次为默认 RELEVANCE 时，与之等价视为已见。"""
    if legacy_filter_key is None:
        row = conn.execute(
            "SELECT 1 FROM seen_combo WHERE combo_key = ? LIMIT 1",
            (stable_key,),
        ).fetchone()
        return row is not None
    row = conn.execute(
        """
        SELECT 1 FROM seen_combo
        WHERE combo_key IN (?, ?)
        LIMIT 1
        """,
        (stable_key, legacy_filter_key),
    ).fetchone()
    return row is not None


def _open_combo_seen_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_combo (
            combo_key TEXT PRIMARY KEY NOT NULL,
            completed_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()
    return conn


def _combo_seen_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM seen_combo").fetchone()
    return int(row[0]) if row else 0


def _combo_mark_seen(conn: sqlite3.Connection, combo_key: str) -> None:
    conn.execute("INSERT OR IGNORE INTO seen_combo (combo_key) VALUES (?)", (combo_key,))
    conn.commit()


def _build_fetch_cmd(
    *,
    root: Path,
    fetch_script: Path,
    out: Path,
    filters: List[Dict[str, str]],
    extra_tail: List[str],
    max_pages_args: List[str],
) -> List[str]:
    cmd: List[str] = [
        sys.executable,
        str(fetch_script),
        "--output",
        str(out),
    ]
    for f in filters:
        if not isinstance(f, dict):
            continue
        k, v = f.get("key"), f.get("value")
        if k is None or v is None:
            continue
        cmd.extend(["--filter", str(k).strip(), str(v)])
    cmd.extend(extra_tail)
    cmd.extend(max_pages_args)
    return cmd


def _estimate_combo_count(n_spec: int, n_mem: int, exclude_both_empty: bool) -> int:
    total = (2**n_spec) * (2**n_mem)
    if exclude_both_empty:
        total -= 1
    return total


def main() -> int:
    root = _repo_root()
    log, log_path = _init_batch_logger(root)
    print(f"日志文件: {log_path}", flush=True)
    log.info("启动 argv=%s", sys.argv)

    fetch_script = root / "scripts" / "fetch_500px_photographers_all.py"
    default_batches = root / "config" / "500px_usersearch_filter_batches.example.yaml"

    try:
        no_config, cfg_override, argv_rest = _extract_config_cli(sys.argv[1:])
    except ValueError as exc:
        log.error("%s", exc)
        print(str(exc), file=sys.stderr)
        return 2

    cfg_raw = _load_run_config_file(
        root=root, no_config=no_config, config_override=cfg_override
    )
    yaml_defaults, fetch_extra_yaml = _defaults_and_fetch_extra_from_run_config(
        cfg_raw, root
    )

    ap = argparse.ArgumentParser(
        description="批量运行 500px userSearch（多组 filters），追加写入同一 output",
        epilog=(
            f"默认配置（若存在）: {DEFAULT_RUN_CONFIG_RELPATH} ；"
            "可用 --config / --no-config 控制。命令行覆盖配置文件中的同名字段。"
        ),
    )
    ap.add_argument(
        "--batches-yaml",
        type=Path,
        default=None,
        help=f"手写 runs 的 YAML（与 --multiselect-combos-from-yaml 二选一；不设 combo 时默认 {default_batches.name}）",
    )
    ap.add_argument(
        "--multiselect-combos-from-yaml",
        type=Path,
        default=None,
        help="从该文件读 specialty/member id，生成多选幂集 × 幂集组合",
    )
    ap.add_argument(
        "--exclude-both-empty-combo",
        action="store_true",
        help="排除「不加 SPECIALTIES 且不加 MEMBER_TYPE」的组合（与旧版默认一致）",
    )
    ap.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="多选模式下最多执行多少组合，0 表示不限制（组合可达百万级，慎用）",
    )
    ap.add_argument(
        "--combo-offset",
        type=int,
        default=0,
        help="多选模式下跳过前 N 个组合（与 --max-batches 配合分段跑）",
    )
    ap.add_argument(
        "--combo-seen-db",
        type=Path,
        default=None,
        help="多选模式：SQLite 路径；子进程成功（退出码 0）后写入组合键，下次运行自动跳过",
    )
    ap.add_argument(
        "--max-pages",
        type=int,
        default=50,
        help="每个子进程传给 fetch 的 --max-pages（默认 50）",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=root / "output" / "500px_photographers_all_usernames.txt",
        help="传给 fetch 脚本的 --output（追加模式）",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="只打印将执行的命令，不运行",
    )
    ap.set_defaults(**yaml_defaults)
    args, fetch_extra_cli = ap.parse_known_args(argv_rest)

    force_m = _argv_declares_multiselect(argv_rest)
    force_b = _argv_declares_batches_yaml(argv_rest)
    if force_m and force_b:
        msg = "错误: 不要同时在命令行指定 --multiselect-combos-from-yaml 与 --batches-yaml"
        log.error(msg)
        print(msg, file=sys.stderr)
        return 2
    if force_m:
        args.batches_yaml = None
    if force_b and not force_m:
        args.multiselect_combos_from_yaml = None

    if args.combo_seen_db is not None and args.multiselect_combos_from_yaml is None:
        msg = "提示: --combo-seen-db 仅在 --multiselect-combos-from-yaml 模式下生效，已忽略。"
        log.warning(msg)
        print(msg, file=sys.stderr)

    out = args.output if args.output.is_absolute() else (root / args.output).resolve()
    extra_tail = list(fetch_extra_yaml) + list(fetch_extra_cli)
    max_pages_args = ["--max-pages", str(args.max_pages)]
    combo_sort_sig = _usersearch_sort_signature(extra_tail)
    log.info(
        "output=%s max_pages=%s sort_sig=%s dry_run=%s",
        out,
        args.max_pages,
        combo_sort_sig,
        args.dry_run,
    )

    if args.multiselect_combos_from_yaml is not None:
        id_path = (
            args.multiselect_combos_from_yaml
            if args.multiselect_combos_from_yaml.is_absolute()
            else (root / args.multiselect_combos_from_yaml).resolve()
        )
        if not id_path.is_file():
            log.error("找不到 ID 配置: %s", id_path)
            print(f"找不到 ID 配置: {id_path}", file=sys.stderr)
            return 2
        spec_ids, mem_ids = _load_filter_ids_yaml(id_path)
        if not spec_ids:
            log.error("%s 中无 specialties.id", id_path)
            print(f"{id_path} 中无 specialties.id", file=sys.stderr)
            return 2
        est = _estimate_combo_count(
            len(spec_ids), len(mem_ids), args.exclude_both_empty_combo
        )
        log.info(
            "多选 specialties=%d member_types=%d 理论批次≈%d sort=%s id_yaml=%s",
            len(spec_ids),
            len(mem_ids),
            est,
            combo_sort_sig,
            id_path,
        )
        print(
            f"多选 理论批次≈{est} sort={combo_sort_sig}（详情见日志）",
            flush=True,
        )

        seen_conn: Optional[sqlite3.Connection] = None
        seen_path: Optional[Path] = None
        if args.combo_seen_db is not None:
            seen_path = (
                args.combo_seen_db
                if args.combo_seen_db.is_absolute()
                else (root / args.combo_seen_db).resolve()
            )
            seen_conn = _open_combo_seen_db(seen_path)
            n_seen = _combo_seen_count(seen_conn)
            log.info("combo_seen_db=%s 已有=%d sort=%s", seen_path, n_seen, combo_sort_sig)
            print(f"combo_seen 已有 {n_seen} 条", flush=True)
        elif args.max_batches == 0 and est > 50_000:
            warn = (
                "组合数极大且未设置 combo_seen_db；中断后需手动 offset 或承担重复抓取。"
            )
            log.warning(warn)
            print(f"警告: {warn}", file=sys.stderr)

        skip_leading = args.combo_offset
        window_cap = args.max_batches
        slots_in_window = 0
        skipped_seen = 0
        ran_ok = 0

        combo_iter = _iter_multiselect_combo_runs(
            spec_ids,
            mem_ids,
            exclude_both_empty=args.exclude_both_empty_combo,
        )

        try:
            for idx, (ss, ms) in enumerate(combo_iter):
                if skip_leading > 0:
                    skip_leading -= 1
                    continue
                if window_cap > 0 and slots_in_window >= window_cap:
                    break
                slots_in_window += 1

                filter_key = _combo_filter_key(ss, ms)
                combo_key = _combo_stable_key(ss, ms, combo_sort_sig)
                legacy_key = filter_key if combo_sort_sig == "RELEVANCE" else None
                name = _combo_run_name(ss, ms, idx)
                filters = _filters_from_combo(ss, ms)

                if seen_conn is not None and _combo_is_seen(
                    seen_conn, combo_key, legacy_filter_key=legacy_key
                ):
                    skipped_seen += 1
                    if skipped_seen <= 3 or skipped_seen % 50000 == 0:
                        log.info(
                            "跳过已见库 累计=%d key=%s",
                            skipped_seen,
                            combo_key,
                        )
                    if skipped_seen % 50000 == 0:
                        print(f"已跳过 seen {skipped_seen} 组合", flush=True)
                    continue

                cmd = _build_fetch_cmd(
                    root=root,
                    fetch_script=fetch_script,
                    out=out,
                    filters=filters,
                    extra_tail=extra_tail,
                    max_pages_args=max_pages_args,
                )

                denom = window_cap if window_cap > 0 else est
                log.info(
                    "开始批次 slot=%s/%s idx=%s name=%s key=%s",
                    slots_in_window,
                    denom,
                    idx,
                    name,
                    combo_key,
                )
                log.info("cmd %s", _cmd_join(cmd))
                print(
                    f"[{slots_in_window}/{denom}] {name}",
                    flush=True,
                )
                if args.dry_run:
                    continue
                r = subprocess.run(cmd, cwd=str(root))
                if r.returncode != 0:
                    err = (
                        f"批次失败 exit={r.returncode} name={name}（未写入 seen，可重跑）"
                    )
                    log.error(err)
                    print(f"[错误] {err}", file=sys.stderr)
                    return r.returncode
                log.info("批次成功 name=%s", name)
                if seen_conn is not None:
                    _combo_mark_seen(seen_conn, combo_key)
                ran_ok += 1
        finally:
            if seen_conn is not None:
                seen_conn.close()

        summary = (
            f"多选结束 本窗口组合位={slots_in_window} 成功抓取={ran_ok} 跳过seen={skipped_seen}"
        )
        log.info(summary)
        print(summary, flush=True)
        if args.dry_run:
            log.info("dry-run 未执行抓取")
            print("（dry-run）", flush=True)
            return 0
        log.info("每批最多 %s 页；输出请离线去重", args.max_pages)
        print(f"每批最多 {args.max_pages} 页；输出请离线去重。", flush=True)
        return 0

    by = args.batches_yaml if args.batches_yaml is not None else default_batches
    by = by if by.is_absolute() else (root / by).resolve()
    if not by.is_file():
        log.error("找不到配置: %s", by)
        print(f"找不到配置: {by}", file=sys.stderr)
        return 2
    obj = yaml.safe_load(by.read_text(encoding="utf-8")) or {}
    loaded = obj.get("runs")
    if not isinstance(loaded, list) or not loaded:
        log.error("YAML runs 须为非空: %s", by)
        print(f"YAML 中 runs 须为非空列表: {by}", file=sys.stderr)
        return 2
    runs: List[Dict[str, Any]] = loaded
    log.info("runs 模式 batches_yaml=%s 共 %d 条", by, len(runs))

    for i, run in enumerate(runs):
        if not isinstance(run, dict):
            continue
        name = str(run.get("name") or f"run_{i}").strip() or f"run_{i}"
        filters = run.get("filters")
        if filters is None:
            filters = []
        if not isinstance(filters, list):
            log.warning("跳过 %s: filters 非列表", name)
            print(f"[跳过] {name}: filters 不是列表", file=sys.stderr)
            continue

        cmd = _build_fetch_cmd(
            root=root,
            fetch_script=fetch_script,
            out=out,
            filters=filters,
            extra_tail=extra_tail,
            max_pages_args=max_pages_args,
        )

        log.info("开始 run %s/%s name=%s", i + 1, len(runs), name)
        log.info("cmd %s", _cmd_join(cmd))
        print(f"[{i + 1}/{len(runs)}] {name}", flush=True)
        if args.dry_run:
            continue
        r = subprocess.run(cmd, cwd=str(root))
        if r.returncode != 0:
            err = f"批次失败 exit={r.returncode} name={name}"
            log.error(err)
            print(f"[错误] {err}", file=sys.stderr)
            return r.returncode
        log.info("批次成功 name=%s", name)

    if args.dry_run:
        log.info("dry-run 未执行抓取")
        print("（dry-run）", flush=True)
        return 0
    tail = f"全部批次结束（每批最多 {args.max_pages} 页）。请对输出离线去重。"
    log.info(tail)
    print(tail, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
