#!/usr/bin/env python3
"""通过 gallery-dl --dump-json 从用户作品流（photostream）元数据里收集用户名。

gallery-dl 对 500px 使用 api.500px.com/graphql（OtherPhotosQuery 等）+ v1/photos REST；
需有效 Cookie（含 x-csrf-token）。部分账号 userByUsername 会 INTERNAL_SERVER_ERROR，gallery-dl 会失败或返回空。

默认读取 ``config/discover_500px_users_gallery_dl.yaml``（若存在）；可用 ``--config PATH`` / ``--no-config``；
命令行参数覆盖配置文件。

适用场景：
  - 有一批种子用户名 / 主页 URL；
  - 对每个用户抓若干页作品 JSON，递归提取 ``user.username``、``liked_by[].username`` 等。

局限：
  - **没有** Pinterest 式「关键词搜索 URL」；扩量仍依赖 seeds、点赞流 GraphQL、或 userSearch 目录脚本。
  - 单用户流里多数是同一 ``user``；``liked_by`` 非空时才会显著扩散。

示例：

  python3 scripts/discover_500px_users_gallery_dl.py

  python3 scripts/discover_500px_users_gallery_dl.py --no-config \\
    --cookies config/500px_cookies.txt --seeds-file seeds.txt
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
import sys
import time
from io import TextIOWrapper
from pathlib import Path
from shutil import which
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import yaml

_SCRIPT_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRIPT_DIR.parent

DEFAULT_CONFIG_RELPATH = "config/discover_500px_users_gallery_dl.yaml"


def _repo_root() -> Path:
    return _REPO_ROOT


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


def _coerce_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("1", "true", "yes", "on", "y"):
            return True
        if s in ("0", "false", "no", "off", "n", ""):
            return False
    return bool(v)


def _resolve_path_opt(root: Path, v: Any) -> Optional[Path]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    p = Path(s)
    return p if p.is_absolute() else (root / p).resolve()


def _defaults_from_yaml(cfg: Dict[str, Any], root: Path) -> Dict[str, Any]:
    """把 YAML 顶层键映射为 argparse 的 set_defaults。"""
    if not cfg:
        return {}
    out: Dict[str, Any] = {}
    if cfg.get("cookies") not in (None, ""):
        out["cookies"] = _resolve_path_opt(root, cfg.get("cookies"))
    if cfg.get("seeds_file") not in (None, ""):
        out["seeds_file"] = _resolve_path_opt(root, cfg.get("seeds_file"))
    if cfg.get("output") not in (None, ""):
        out["output"] = _resolve_path_opt(root, cfg.get("output"))
    if "seen_db" in cfg:
        out["seen_db"] = _resolve_path_opt(root, cfg.get("seen_db"))
    if cfg.get("max_per_user") is not None:
        out["max_per_user"] = int(cfg["max_per_user"])
    if cfg.get("sleep") is not None:
        out["sleep"] = float(cfg["sleep"])
    if cfg.get("gallery_dl_path") is not None:
        out["gallery_dl_path"] = str(cfg.get("gallery_dl_path") or "").strip() or "gallery-dl"
    if cfg.get("gallery_dl_timeout_seconds") is not None:
        out["gallery_dl_timeout"] = int(cfg["gallery_dl_timeout_seconds"])
    if "overwrite_output" in cfg:
        out["overwrite_output"] = _coerce_bool(cfg.get("overwrite_output"), False)
    if cfg.get("progress_every") is not None:
        out["progress_every"] = int(cfg["progress_every"])
    return out


def _load_config_file(
    *, root: Path, no_config: bool, config_override: Optional[str]
) -> Dict[str, Any]:
    if no_config:
        return {}
    if config_override:
        p = Path(config_override)
        if not p.is_file():
            print(f"找不到配置文件: {p.resolve()}", file=sys.stderr)
            raise SystemExit(2)
        return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    default_p = root / DEFAULT_CONFIG_RELPATH
    if default_p.is_file():
        return yaml.safe_load(default_p.read_text(encoding="utf-8")) or {}
    return {}


def parse_username_line(line: str) -> Optional[str]:
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    if s.startswith("@"):
        return s[1:].strip() or None
    m = re.search(r"500px\.com/(?:p/)?([^/?#]+)/?", s)
    if m:
        return m.group(1).strip() or None
    if re.fullmatch(r"[A-Za-z0-9_.-]+", s):
        return s
    return None


def open_seen_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS px500_discovered_users (
            username TEXT PRIMARY KEY,
            source TEXT NOT NULL,
            seen_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def seen_contains(conn: Optional[sqlite3.Connection], user: str) -> bool:
    if conn is None:
        return False
    row = conn.execute(
        "SELECT 1 FROM px500_discovered_users WHERE username=? LIMIT 1",
        (user,),
    ).fetchone()
    return row is not None


def seen_insert(conn: sqlite3.Connection, user: str, source: str) -> None:
    import datetime as dt

    now = dt.datetime.now(dt.timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO px500_discovered_users(username, source, seen_at)
        VALUES (?, ?, ?)
        """,
        (user, source, now),
    )
    conn.commit()


def resolve_gallery_dl_path(configured: str) -> str:
    if configured and configured != "gallery-dl":
        return configured
    p = which("gallery-dl")
    if p:
        return p
    fb = Path.home() / ".local" / "bin" / "gallery-dl"
    if fb.is_file() and fb.stat().st_mode & 0o111:
        return str(fb)
    return "gallery-dl"


def iter_usernames_from_gallery_dl_json(root: Any) -> Iterable[str]:
    """从 gallery-dl --dump-json 输出中收集可能的用户名。"""
    stack: List[Any] = [root]
    while stack:
        obj = stack.pop()
        if isinstance(obj, dict):
            user = obj.get("user")
            if isinstance(user, dict):
                un = user.get("username")
                if isinstance(un, str) and un.strip():
                    yield un.strip()
            ph = obj.get("photographer")
            if isinstance(ph, dict):
                un = ph.get("username")
                if isinstance(un, str) and un.strip():
                    yield un.strip()
            lb = obj.get("liked_by")
            if isinstance(lb, list):
                for x in lb:
                    if isinstance(x, dict):
                        un = x.get("username")
                        if isinstance(un, str) and un.strip():
                            yield un.strip()
            for v in obj.values():
                stack.append(v)
        elif isinstance(obj, list):
            stack.extend(obj)


def profile_url(username: str) -> str:
    return f"https://500px.com/{username.strip()}"


def run_gallery_dl_json(
    *,
    gallery_dl: str,
    cookies: Path,
    url: str,
    max_items: int,
    cwd: Path,
    timeout_sec: int,
) -> tuple[List[Any], str]:
    args = [
        gallery_dl,
        "--cookies",
        str(cookies),
        "--dump-json",
    ]
    if max_items > 0:
        args += ["--range", f"1-{max_items}"]
    args.append(url)
    try:
        proc = subprocess.run(
            args,
            cwd=str(cwd),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(timeout_sec, 1),
            check=False,
        )
    except subprocess.TimeoutExpired:
        return [], "timeout"
    err = proc.stderr.decode("utf-8", errors="ignore").strip()
    if proc.returncode != 0:
        return [], err or f"exit {proc.returncode}"
    try:
        data = json.loads(proc.stdout.decode("utf-8", errors="ignore"))
    except json.JSONDecodeError as e:
        return [], f"json: {e}"
    if not isinstance(data, list):
        return [], "unexpected json root"
    return data, ""


def main() -> int:
    root = _repo_root()
    try:
        no_config, cfg_override, argv_rest = _extract_config_cli(sys.argv[1:])
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    cfg_raw = _load_config_file(root=root, no_config=no_config, config_override=cfg_override)
    yaml_defaults = _defaults_from_yaml(cfg_raw, root)

    ap = argparse.ArgumentParser(
        description="500px：用 gallery-dl --dump-json 从用户作品元数据收集用户名",
        epilog=(
            f"默认配置（若存在）: {DEFAULT_CONFIG_RELPATH} ；"
            "可用 --config / --no-config。命令行覆盖 YAML。"
        ),
    )
    ap.add_argument(
        "--cookies",
        type=Path,
        default=None,
        help="Netscape Cookie（500px.com + x-csrf-token）",
    )
    ap.add_argument("--seeds-file", type=Path, default=None, help="种子用户名或 URL，每行一条")
    ap.add_argument(
        "--output",
        type=Path,
        default=root / "output" / "500px_discovered_gallery_dl_usernames.txt",
    )
    ap.add_argument(
        "--max-per-user",
        type=int,
        default=50,
        help="每个种子用户最多解析多少条作品（gallery-dl --range；0=不限）",
    )
    ap.add_argument("--sleep", type=float, default=0.5, help="种子之间的间隔秒数")
    ap.add_argument("--gallery-dl-path", type=str, default="gallery-dl")
    ap.add_argument(
        "--gallery-dl-timeout",
        type=int,
        default=600,
        help="单次 gallery-dl 子进程超时秒数",
    )
    ap.add_argument("--seen-db", type=Path, default=None, help="SQLite 去重（px500_discovered_users 表）")
    ap.add_argument("--overwrite-output", action="store_true")
    ap.add_argument(
        "--progress-every",
        type=int,
        default=1,
        help="每处理多少个种子打印一行进度（默认 1；大批次可调大到 50）",
    )
    ap.set_defaults(**yaml_defaults)
    args = ap.parse_args(argv_rest)

    if args.cookies is None:
        print(
            "缺少 --cookies：请在 YAML（cookies:）或命令行指定。",
            file=sys.stderr,
        )
        return 2
    if args.seeds_file is None:
        print(
            "缺少 --seeds-file：请在 YAML（seeds_file:）或命令行指定。",
            file=sys.stderr,
        )
        return 2

    ck = args.cookies.expanduser().resolve()
    if not ck.is_file():
        print(f"找不到 Cookie: {ck}", file=sys.stderr)
        return 2

    sf = args.seeds_file.expanduser().resolve()
    if not sf.is_file():
        print(f"找不到 seeds: {sf}", file=sys.stderr)
        return 2

    out_path = (
        args.output
        if args.output.is_absolute()
        else (_repo_root() / args.output).resolve()
    )

    seeds: Set[str] = set()
    for line in sf.read_text(encoding="utf-8").splitlines():
        u = parse_username_line(line)
        if u:
            seeds.add(u)

    if not seeds:
        print("无有效种子", file=sys.stderr)
        return 2

    gdl = resolve_gallery_dl_path(args.gallery_dl_path)

    seen_conn: Optional[sqlite3.Connection] = None
    if args.seen_db is not None:
        dbp = (
            args.seen_db
            if args.seen_db.is_absolute()
            else (_repo_root() / args.seen_db).resolve()
        )
        seen_conn = open_seen_db(dbp)

    already_file: Set[str] = set()
    if out_path.is_file() and not args.overwrite_output:
        for line in out_path.read_text(encoding="utf-8").splitlines():
            p = parse_username_line(line)
            if p:
                already_file.add(p)

    collected: Set[str] = set()
    err_n = 0
    written_run = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if args.overwrite_output else "a"
    seeds_sorted = sorted(seeds, key=str.lower)
    n_seeds = len(seeds_sorted)
    prog_every = max(1, args.progress_every)

    def flush_new_names(data_json: List[Any], out_fp: TextIOWrapper) -> int:
        nonlocal written_run
        n = 0
        for name in iter_usernames_from_gallery_dl_json(data_json):
            collected.add(name)
            if name in already_file:
                continue
            if seen_conn and seen_contains(seen_conn, name):
                continue
            out_fp.write(name + "\n")
            already_file.add(name)
            if seen_conn:
                seen_insert(seen_conn, name, "gallery_dl_dump_json")
            n += 1
        if n:
            out_fp.flush()
            written_run += n
        return n

    last_i = -1
    try:
        with out_path.open(mode, encoding="utf-8") as out_fp:
            for i, user in enumerate(seeds_sorted):
                last_i = i
                url = profile_url(user)
                data, err = run_gallery_dl_json(
                    gallery_dl=gdl,
                    cookies=ck,
                    url=url,
                    max_items=args.max_per_user,
                    cwd=root,
                    timeout_sec=args.gallery_dl_timeout,
                )
                batch_new = 0
                if err:
                    print(f"[跳过] {user}: {err[:300]}", file=sys.stderr)
                    err_n += 1
                else:
                    batch_new = flush_new_names(data, out_fp)

                if (i + 1) % prog_every == 0 or i == 0:
                    print(
                        f"[进度 {i + 1}/{n_seeds}] {user} "
                        f"本批新增写入 {batch_new} 行（本轮累计写入 {written_run}）",
                        flush=True,
                    )

                if args.sleep > 0 and i + 1 < n_seeds:
                    time.sleep(args.sleep)
    except KeyboardInterrupt:
        done = last_i + 1 if last_i >= 0 else 0
        print(
            f"\n已中断：已写入本轮新增 {written_run} 行 -> {out_path} "
            f"（处理约 {min(done, n_seeds)}/{n_seeds} 个种子）",
            file=sys.stderr,
            flush=True,
        )
        if seen_conn:
            seen_conn.close()
        return 130

    if seen_conn:
        seen_conn.close()

    print(
        f"种子 {n_seeds} 个，解析得到不同用户名 {len(collected)} 个；"
        f"本轮新写入 {written_run} 行 -> {out_path}（失败种子约 {err_n}）",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
