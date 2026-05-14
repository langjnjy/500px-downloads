#!/usr/bin/env python3
"""
仅元数据提取：对每个 500px 用户主页执行 ``gallery-dl --no-download --dump-json``，
解析后与 ``scripts/download.py`` 中 **精简字段** 写入按日 ``metadata.jsonl``（每行仅：
``image_key``, ``image_url``, ``resolution``, ``timestamps``, ``description``, ``tags``, ``id``）；成功后写入 extract 专用 SQLite seen。

- 默认 **分片** 写入 ``extract_metadata_1.jsonl`` …（约 **300 万行** metadata 一片，见 ``extract_metadata_sharding.max_shard_lines``；可按用户数/字节叠加）；关闭分片时仍写 ``{UTC日}.metadata.jsonl``。

- 未分片时当日 ``metadata.jsonl`` 由 ``MetadataSink`` **追加写入**；分片时每个分片文件同样 **追加**。

性能要点（见 ``config/extract.yaml``）：
  - 默认 stdout 走内存（不落临时 .json），省掉大文件写读；
  - 可选 ``orjson`` 解析 dump-json；
  - 启动时一次性加载 seen 集合，避免对十几万用户逐条 ``SELECT``；
  - ``commit_flush_and_finalize_profile``：单 uid 在持锁下完成 metadata 缓冲、刷盘与分片计数，避免多 worker 交叉 flush 同一缓冲。

代理策略与 ``download.py`` 一致（``use_proxies`` + ``proxies_file`` + ``ProxyRoundRobin``）。

默认配置：``config/extract.yaml``。
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import datetime as dt
import importlib.util
import json
import logging
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_CONFIG = (_REPO_ROOT / "config" / "extract.yaml").resolve()


class _UtcFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        return dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).strftime(
            datefmt or "%Y-%m-%dT%H:%M:%SZ"
        )


def setup_logging(root: Path, *, append_master_log: bool = True) -> Tuple[logging.Logger, Path, Optional[Path]]:
    """
    详细日志：``output/logs/extract_500px_{UTC}_{pid}.log``（每 run 新文件）。
    若 ``append_master_log``：同时 **追加** 写入 ``output/logs/extract_500px_appended.log``（跨 run 累计）。
    """
    logs_dir = Path(root) / "output" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_path = logs_dir / f"extract_500px_{ts}_{os.getpid()}.log"
    lg = logging.getLogger("extract_500px")
    lg.setLevel(logging.DEBUG)
    lg.handlers.clear()
    fmt = _UtcFormatter(fmt="%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    lg.addHandler(fh)
    lg.addHandler(sh)
    master_path: Optional[Path] = None
    if append_master_log:
        master_path = logs_dir / "extract_500px_appended.log"
        mfh = logging.FileHandler(master_path, mode="a", encoding="utf-8")
        mfh.setLevel(logging.INFO)
        mfh.setFormatter(fmt)
        lg.addHandler(mfh)
    lg.propagate = False
    lg.info("log_file=%s", log_path)
    if master_path is not None:
        lg.info("append_master_log=%s", master_path)
    return lg, log_path, master_path


def _flush_loggers(lg: logging.Logger) -> None:
    for h in lg.handlers:
        try:
            h.flush()
        except Exception:
            pass


def _append_run_summary_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    obj = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return obj if isinstance(obj, dict) else {}


def load_crawl(scripts_dir: Path) -> Any:
    path = scripts_dir / "crawl_500px_gallery_dl.py"
    spec = importlib.util.spec_from_file_location("crawl_500px_gd_extract", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed to load: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def load_download_module(scripts_dir: Path) -> Any:
    path = scripts_dir / "download.py"
    spec = importlib.util.spec_from_file_location("download_500px_for_extract", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed to load: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _cfg_bool(cfg: Dict[str, Any], key: str, default: bool) -> bool:
    if key not in cfg or cfg[key] is None:
        return default
    s = str(cfg[key]).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    if s in ("1", "true", "yes", "on"):
        return True
    return default


def _build_json_loads(cfg: Dict[str, Any], logger: logging.Logger) -> Callable[[str], Any]:
    """大 JSON 解析：优先 orjson（需 ``pip install orjson``），否则 ``json.loads``。"""
    if not _cfg_bool(cfg, "extract_use_orjson", True):
        return json.loads
    try:
        import orjson as _orjson  # type: ignore[import-not-found]

        def _loads(s: str) -> Any:
            return _orjson.loads(s.encode("utf-8"))

        logger.info("extract_json_parser=orjson")
        return _loads
    except ImportError:
        logger.info("extract_json_parser=json (install orjson for faster parse)")
        return json.loads


def canonical_extract_profile_url(dl: Any, profile_url: str) -> str:
    """
    将 ``uid`` 行或各种 500px 主页 URL 规范为 ``https://500px.com/p/<username>``，
    与 seen DB 主键一致，避免同一用户因 URL 写法不同被重复跑。
    """
    s = (profile_url or "").strip()
    if not s:
        return ""
    un = dl.profile_url_to_username(s)
    if un and un != "unknown":
        return f"https://500px.com/p/{un}"
    return s.rstrip("/")


def _dedupe_gallery_items_by_photo_id(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """同一 dump 中若出现多条相同 ``id``，只保留首次（防御性）。"""
    seen: Set[Any] = set()
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        pid = it.get("id")
        if pid is None:
            out.append(it)
            continue
        if pid in seen:
            continue
        seen.add(pid)
        out.append(it)
    return out


def _plan_slim_extract_row(
    dl: Any,
    crawl: Any,
    item: Dict[str, Any],
    *,
    min_short: int,
    min_long: int,
    image_key_style: str,
    image_key_prefix: str,
) -> Optional[Tuple[str, str]]:
    """单条 gallery 作品 → 仅 extract 需要的 7 个字段的 JSONL 行。"""
    best = crawl.pick_best_url_from_gallery_item(
        item, min_short=max(0, min_short), min_long=max(0, min_long)
    )
    if not best:
        return None
    best_url, width, height = best
    raw_id = item.get("id")
    if raw_id is None:
        return None
    photo_id = str(raw_id).strip()
    if not photo_id:
        return None
    if not dl._item_username(item):
        return None
    image_key, _ = dl.image_key_for_item(
        crawl,
        style=image_key_style,
        prefix=image_key_prefix,
        photo_id=photo_id,
        best_url=best_url,
        item=item,
    )
    user = item.get("user") if isinstance(item.get("user"), dict) else {}
    tags_val = item.get("tags")
    tags_list: Optional[List[Any]] = tags_val if isinstance(tags_val, list) else None
    row = {
        "image_key": image_key,
        "image_url": best_url,
        "resolution": f"{int(width)}x{int(height)}",
        "timestamps": dl._timestamps_from_500px(item, user),
        "description": item.get("description"),
        "tags": tags_list,
        "id": item.get("id"),
    }
    line_body = json.dumps(row, ensure_ascii=False)
    # 保证一条物理行一条 JSON（避免 description 等字段含裸换行导致 JSONL 与换行计数不一致）
    if "\n" in line_body or "\r" in line_body:
        line_body = line_body.replace("\r\n", "\n").replace("\r", "\n")
        line_body = "".join(line_body.splitlines())
    return image_key, line_body + "\n"


def slim_metadata_pairs_from_items(
    dl: Any,
    crawl: Any,
    items: List[Dict[str, Any]],
    *,
    min_short: int,
    min_long: int,
    image_key_style: str,
    image_key_prefix: str,
) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        row = _plan_slim_extract_row(
            dl,
            crawl,
            item,
            min_short=min_short,
            min_long=min_long,
            image_key_style=image_key_style,
            image_key_prefix=image_key_prefix,
        )
        if row:
            out.append(row)
    return out


def _safe_dump_filename(username: str) -> str:
    u = username.strip() or "unknown"
    out_ch: List[str] = []
    for c in u:
        if c.isalnum() or c in "._-":
            out_ch.append(c)
        else:
            out_ch.append("_")
    s = "".join(out_ch).strip("_") or "unknown"
    return s[:200]


class ExtractSeenDB:
    """extract 阶段已成功处理的用户主页（与 download 的 seen 分离）。"""

    def __init__(self, db_path: Path, *, cfg: Optional[Dict[str, Any]] = None):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        c = cfg or {}
        syn = str(c.get("seen_db_synchronous") or "NORMAL").strip().upper()
        if syn in ("OFF", "0", "NO", "FALSE"):
            self.conn.execute("PRAGMA synchronous=OFF")
        elif syn in ("FULL", "2", "YES", "TRUE"):
            self.conn.execute("PRAGMA synchronous=FULL")
        else:
            self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS extract_500px_user_profiles_done (
                profile_url TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                completed_at_utc TEXT NOT NULL,
                metadata_lines INTEGER NOT NULL,
                dump_json_bytes INTEGER NOT NULL,
                eligible_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self._migrate_extract_seen_columns()
        self.conn.commit()

    def _migrate_extract_seen_columns(self) -> None:
        cur = self.conn.execute("PRAGMA table_info(extract_500px_user_profiles_done)")
        cols = {str(row[1]) for row in cur.fetchall()}
        if "eligible_count" not in cols:
            self.conn.execute(
                "ALTER TABLE extract_500px_user_profiles_done "
                "ADD COLUMN eligible_count INTEGER NOT NULL DEFAULT 0"
            )

    def all_done_profile_urls(self, dl: Any) -> Set[str]:
        """一次性取出已成功用户（profile_url 经 canonical 后与待跑列表对齐）。"""
        cur = self.conn.execute("SELECT profile_url FROM extract_500px_user_profiles_done")
        out: Set[str] = set()
        for row in cur:
            if not row or not row[0]:
                continue
            out.add(canonical_extract_profile_url(dl, str(row[0])))
        return out

    def is_done(self, profile_url: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM extract_500px_user_profiles_done WHERE profile_url = ? LIMIT 1",
            (profile_url.strip(),),
        ).fetchone()
        return row is not None

    def mark_done(
        self,
        profile_url: str,
        username: str,
        metadata_lines: int,
        dump_json_bytes: int,
        *,
        eligible_count: int,
    ) -> None:
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.conn.execute(
            """
            INSERT OR REPLACE INTO extract_500px_user_profiles_done(
                profile_url, username, completed_at_utc, metadata_lines, dump_json_bytes, eligible_count
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                profile_url.strip(),
                username.strip(),
                now,
                int(metadata_lines),
                int(dump_json_bytes),
                max(0, int(eligible_count)),
            ),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


def _gallery_dl_dump_cmd(
    crawl: Any,
    *,
    gallery_dl_bin: str,
    cookies_path: Optional[Path],
    cookies_from_browser: Optional[str],
    proxy_url: Optional[str],
    extra_args: List[str],
    profile_url: str,
    retries: int,
    http_timeout: float,
    sleep_429: float,
    sleep_retries: str,
) -> List[str]:
    cmd = crawl._gallery_dl_argv_prefix(
        gallery_dl_bin,
        cookies_path,
        cookies_from_browser,
        proxy_url,
    )
    for a in extra_args:
        s = str(a).strip()
        if s:
            cmd.append(s)
    cmd.extend(
        [
            "-R",
            str(max(0, int(retries))),
            "--http-timeout",
            str(max(1.0, float(http_timeout))),
            "--sleep-retries",
            (sleep_retries or "exp=2.0").strip() or "exp=2.0",
            "--sleep-429",
            str(max(0.0, float(sleep_429))),
            "-q",
            "--no-download",
            "--dump-json",
            profile_url.strip(),
        ]
    )
    return cmd


_SHARD_FILE_RE = re.compile(r"^extract_metadata_(\d+)\.jsonl$", re.IGNORECASE)
# 分片默认：约 300 万行 metadata 一行一条；profiles_per_shard 未配置时为 0（不按用户数切，避免先于行数上限切分）
_DEFAULT_SHARD_MAX_LINES = 3_000_000


def _sharding_cfg_dict(cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """返回分片配置 dict；``extract_metadata_sharding: false`` 或 ``enabled: false`` 时关闭分片。"""
    if "extract_metadata_sharding" not in cfg:
        return {}
    raw = cfg.get("extract_metadata_sharding")
    if raw is False or raw is None:
        return None
    return raw if isinstance(raw, dict) else {}


def _discover_max_shard_index(meta_dir: Path) -> int:
    m = 0
    try:
        for p in meta_dir.iterdir():
            if not p.is_file():
                continue
            mo = _SHARD_FILE_RE.match(p.name)
            if mo:
                m = max(m, int(mo.group(1)))
    except OSError:
        pass
    return m


def _count_newlines_in_file(path: Path) -> int:
    """流式数换行符；用于分片启动时与磁盘上已有 JSONL 行数对齐。"""
    n = 0
    with path.open("rb", buffering=1024 * 1024) as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            n += chunk.count(b"\n")
    return n


def _file_newline_count_and_size(path: Path) -> Optional[Tuple[int, int]]:
    """返回 ``(换行数, 字节大小)``；非文件或不可读时返回 ``None``。"""
    if not path.is_file():
        return None
    try:
        sz = int(path.stat().st_size)
    except OSError:
        return None
    if sz <= 0:
        return (0, 0)
    try:
        return (_count_newlines_in_file(path), sz)
    except OSError:
        return None


def _metadata_display_preview(cfg: Dict[str, Any], meta_dir: Path, meta_path_single: Path) -> str:
    sd = _sharding_cfg_dict(cfg)
    if sd is not None and _cfg_bool(sd, "enabled", True):
        tpl = str(sd.get("filename_template") or "extract_metadata_{shard}.jsonl").strip()
        return str(meta_dir / tpl.format(shard="*")) + " (sharded)"
    return str(meta_path_single)


class ShardedExtractMetadataSink:
    """
    分片 JSONL：``extract_metadata_{shard}.jsonl``（shard 从 1 递增）。
    轮转条件（满足任一即切到下一片，在**当前 uid 已成功 flush 之后**判断）：
      - 本分片内已成功处理的用户数 ≥ ``profiles_per_shard``（``profiles_per_shard <= 0`` 表示不按用户数轮转）；
      - 本分片累计写入字节（近似）≥ ``max_shard_bytes``（0 表示不限制）；
      - 本分片累计 metadata 行数 ≥ ``max_shard_lines``（0 表示不限制）。
    状态持久化在 ``state_path``，便于进程重启后继续当前分片计数。

    启动时（见 ``_load_state_or_init``）：
      - 用当前分片文件物理换行数覆盖 ``lines_in_shard``，与 state 不一致时打日志并写回 state；
      - 若 state 指向的「下一片」实为占位（无文件、空文件、无换行垃圾、或行数 ≤ ``placeholder_max_lines``），
        且前一片未满 ``max_shard_lines``，则删除占位文件并回退到前一片继续写。
    """

    def __init__(
        self,
        meta_dir: Path,
        state_path: Path,
        *,
        filename_template: str,
        profiles_per_shard: int,
        max_shard_bytes: int,
        max_shard_lines: int,
        flush_interval_sec: float,
        flush_every_n_lines: int,
        logger: logging.Logger,
        placeholder_max_lines: int = 50,
    ) -> None:
        if "{shard}" not in filename_template:
            raise ValueError("filename_template must contain {shard}")
        self.meta_dir = meta_dir
        self.state_path = state_path
        self.filename_template = filename_template
        self.profiles_per_shard = max(0, int(profiles_per_shard))
        self.max_shard_bytes = max(0, int(max_shard_bytes))
        self.max_shard_lines = max(0, int(max_shard_lines))
        self._placeholder_max_lines = max(0, int(placeholder_max_lines))
        self.flush_interval_sec = max(0.1, float(flush_interval_sec))
        self.flush_every_n_lines = max(1, int(flush_every_n_lines))
        self._logger = logger
        self._lock = threading.Lock()
        self._buf: List[str] = []
        self._last_flush = time.monotonic()
        self.written_image_keys: Set[str] = set()
        self._shard = 1
        self._profiles_in_shard = 0
        self._lines_in_shard = 0
        self._bytes_in_shard = 0
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        if self.profiles_per_shard <= 0 and self.max_shard_bytes <= 0 and self.max_shard_lines <= 0:
            raise ValueError(
                "sharding requires at least one of profiles_per_shard>0, max_shard_bytes>0, max_shard_lines>0"
            )
        self._load_state_or_init()

    def _shard_path(self, shard: int) -> Path:
        return self.meta_dir / self.filename_template.format(shard=shard)

    def _sync_active_shard_line_count_with_disk(self) -> None:
        """用当前分片文件物理换行数对齐 ``lines_in_shard``（与 JSONL 一条一换行约定一致）。"""
        if self.max_shard_lines <= 0:
            return
        p = self._shard_path(self._shard)
        info = _file_newline_count_and_size(p)
        if info is None:
            return
        nl, sz = info
        old_l, old_b = self._lines_in_shard, self._bytes_in_shard
        if nl == old_l and sz == old_b:
            return
        if nl != old_l:
            self._logger.warning(
                "extract_metadata_shard_lines_resync old_state=%s disk_newlines=%s file=%s",
                old_l,
                nl,
                p.name,
            )
        self._lines_in_shard = nl
        self._bytes_in_shard = sz
        self._persist_state_unlocked()

    def _fallback_to_prior_shard_if_current_empty_and_prior_incomplete(self) -> None:
        if self.max_shard_lines <= 0:
            return
        cur = self._shard_path(self._shard)
        cur_info = _file_newline_count_and_size(cur)
        abandon_cur = False
        if cur_info is None:
            abandon_cur = True
        else:
            nl, sz = cur_info
            if nl >= self.max_shard_lines:
                return
            if sz <= 0:
                abandon_cur = True
            elif nl == 0 and sz > 0:
                abandon_cur = True
            elif self._placeholder_max_lines > 0 and nl <= self._placeholder_max_lines:
                abandon_cur = True
            else:
                return
        if not abandon_cur:
            return
        try:
            if cur.is_file():
                cur.unlink()
                self._logger.info("extract_metadata_shard_removed_nonfinal_next path=%s", cur.name)
        except OSError as e:
            self._logger.warning("extract_metadata_shard_remove_next_failed path=%s err=%s", cur, e)
        prev_n = self._shard - 1
        if prev_n < 1:
            return
        prev_path = self._shard_path(prev_n)
        try:
            if not prev_path.is_file() or prev_path.stat().st_size <= 0:
                return
        except OSError:
            return
        try:
            lines_prev = _count_newlines_in_file(prev_path)
        except OSError as e:
            self._logger.warning("extract_metadata_shard_prior_line_count_failed path=%s err=%s", prev_path, e)
            return
        if lines_prev >= self.max_shard_lines:
            return
        self._shard = prev_n
        self._profiles_in_shard = 0
        self._lines_in_shard = lines_prev
        try:
            self._bytes_in_shard = prev_path.stat().st_size
        except OSError:
            self._bytes_in_shard = 0
        self._logger.info(
            "extract_metadata_shard_resume_incomplete_prior path=%s lines=%s max_shard_lines=%s",
            self.active_path().name,
            self._lines_in_shard,
            self.max_shard_lines,
        )
        self._persist_state_unlocked()

    def active_path(self) -> Path:
        return self.meta_dir / self.filename_template.format(shard=self._shard)

    def metadata_log_label(self) -> str:
        return f"{self.meta_dir}/{self.filename_template.format(shard='*')} active={self.active_path().name}"

    def _persist_state_unlocked(self) -> None:
        payload = {
            "active_shard": self._shard,
            "profiles_in_shard": self._profiles_in_shard,
            "lines_in_shard": self._lines_in_shard,
            "bytes_in_shard": self._bytes_in_shard,
            "updated_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.state_path)

    def _load_state_or_init(self) -> None:
        if self.state_path.is_file():
            try:
                data = json.loads(self.state_path.read_text(encoding="utf-8"))
                self._shard = max(1, int(data.get("active_shard") or 1))
                self._profiles_in_shard = max(0, int(data.get("profiles_in_shard") or 0))
                self._lines_in_shard = max(0, int(data.get("lines_in_shard") or 0))
                self._bytes_in_shard = max(0, int(data.get("bytes_in_shard") or 0))
                self._logger.info(
                    "extract_metadata_shard_resume active=%s profiles_in_shard=%s lines=%s bytes=%s",
                    self.active_path().name,
                    self._profiles_in_shard,
                    self._lines_in_shard,
                    self._bytes_in_shard,
                )
                self._fallback_to_prior_shard_if_current_empty_and_prior_incomplete()
                self._sync_active_shard_line_count_with_disk()
                return
            except Exception as e:
                self._logger.warning("extract_metadata_shard_state_corrupt err=%s rebuilding", e)
        mx = _discover_max_shard_index(self.meta_dir)
        self._shard = max(1, mx)
        self._profiles_in_shard = 0
        self._lines_in_shard = 0
        p = self.active_path()
        try:
            self._bytes_in_shard = p.stat().st_size if p.is_file() else 0
        except OSError:
            self._bytes_in_shard = 0
        if mx >= 1:
            self._logger.info(
                "extract_metadata_shard_no_state_file start_shard=%s bytes_on_disk=%s (profiles/lines reset to 0)",
                self.active_path().name,
                self._bytes_in_shard,
            )
        self._persist_state_unlocked()
        self._fallback_to_prior_shard_if_current_empty_and_prior_incomplete()
        self._sync_active_shard_line_count_with_disk()

    def load_existing_image_keys_from_disk(self) -> None:
        for p in sorted(self.meta_dir.iterdir()):
            if not p.is_file() or not _SHARD_FILE_RE.match(p.name):
                continue
            self._logger.info("extract_metadata_loading_image_keys begin path=%s", p.name)
            n_lines = 0
            n_keys = 0
            try:
                with p.open("r", encoding="utf-8") as f:
                    for line in f:
                        n_lines += 1
                        if n_lines % 500_000 == 0:
                            self._logger.info(
                                "extract_metadata_loading_image_keys progress path=%s lines=%s keys_unique=%s",
                                p.name,
                                n_lines,
                                len(self.written_image_keys),
                            )
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            obj = json.loads(line)
                        except Exception:
                            continue
                        k = str(obj.get("image_key") or "").strip()
                        if k:
                            self.written_image_keys.add(k)
                            n_keys += 1
            except OSError:
                continue
            self._logger.info(
                "extract_metadata_loading_image_keys done path=%s lines_read=%s keys_added_attempts=%s set_size=%s",
                p.name,
                n_lines,
                n_keys,
                len(self.written_image_keys),
            )

    def _commit_profile_rows_unlocked(self, pairs: List[Tuple[str, str]]) -> Tuple[int, int]:
        if not pairs:
            return (0, 0)
        to_add: List[str] = []
        for image_key, line in pairs:
            if image_key in self.written_image_keys:
                continue
            self.written_image_keys.add(image_key)
            to_add.append(line)
        if to_add:
            self._buf.extend(to_add)
        added = len(to_add)
        bsum = sum(len(x.encode("utf-8")) for x in to_add)
        self._maybe_flush_unlocked()
        return (added, bsum)

    def commit_profile_rows(self, pairs: List[Tuple[str, str]]) -> Tuple[int, int]:
        """返回 ``(本 uid 新纳入缓冲的 metadata 行数, 对应 UTF-8 字节近似)``。"""
        with self._lock:
            return self._commit_profile_rows_unlocked(pairs)

    def commit_flush_and_finalize_profile(self, pairs: List[Tuple[str, str]]) -> int:
        """单 uid：持锁完成 commit、最终 flush 与分片计数/轮转，避免 worker 间交叉 flush 与计数丢失。"""
        with self._lock:
            added, bsum = self._commit_profile_rows_unlocked(pairs)
            self._flush_unlocked()
            self._after_profile_success_unlocked(added, bsum)
        return int(added)

    def flush(self) -> None:
        with self._lock:
            self._flush_unlocked()

    def _after_profile_success_unlocked(self, new_lines: int, new_bytes: int) -> None:
        self._profiles_in_shard += 1
        self._lines_in_shard += max(0, int(new_lines))
        self._bytes_in_shard += max(0, int(new_bytes))
        self._persist_state_unlocked()
        if self._should_rotate_unlocked():
            self._rotate_unlocked()

    def after_profile_success(self, new_lines: int, new_bytes: int) -> None:
        """仅在其他路径已 flush 后需要补计数时使用；正常路径请用 ``commit_flush_and_finalize_profile``。"""
        with self._lock:
            self._after_profile_success_unlocked(new_lines, new_bytes)

    def _should_rotate_unlocked(self) -> bool:
        if self.profiles_per_shard > 0 and self._profiles_in_shard >= self.profiles_per_shard:
            return True
        if self.max_shard_bytes > 0 and self._bytes_in_shard >= self.max_shard_bytes:
            return True
        if self.max_shard_lines > 0 and self._lines_in_shard >= self.max_shard_lines:
            return True
        return False

    def _rotate_unlocked(self) -> None:
        self._logger.info(
            "extract_metadata_shard_rotate closed_shard=%s profiles=%s lines=%s bytes=%s -> next_shard=%s",
            self.active_path().name,
            self._profiles_in_shard,
            self._lines_in_shard,
            self._bytes_in_shard,
            self._shard + 1,
        )
        self._shard += 1
        self._profiles_in_shard = 0
        self._lines_in_shard = 0
        self._bytes_in_shard = 0
        self._persist_state_unlocked()

    def _maybe_flush_unlocked(self) -> None:
        if not self._buf:
            return
        now = time.monotonic()
        if len(self._buf) >= self.flush_every_n_lines or (now - self._last_flush) >= self.flush_interval_sec:
            self._flush_unlocked()

    def _flush_unlocked(self) -> None:
        if not self._buf:
            return
        chunk = "".join(self._buf)
        self._buf.clear()
        path = self.active_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(chunk)
        self._last_flush = time.monotonic()


def worker_extract(
    profile_url: str,
    *,
    cfg: Dict[str, Any],
    root: Path,
    crawl: Any,
    dl: Any,
    logger: logging.Logger,
    dump_staging: Path,
    meta_sink: Any,
    seen_db: ExtractSeenDB,
    seen_lock: threading.Lock,
    failed_log: Any,
    proxy_pool: Optional[Any],
    images_recorded_total: List[int],
    stats_lock: threading.Lock,
    json_loads: Callable[[str], Any],
) -> Tuple[str, bool, str]:
    gallery_dl_bin = str(cfg.get("gallery_dl_bin") or "gallery-dl").strip() or "gallery-dl"
    retries = max(0, int(cfg.get("gallery_dl_retries", 3) or 3))
    http_timeout = float(cfg.get("http_timeout", 120) or 120)
    sleep_429 = float(cfg.get("sleep_429", 60) or 60)
    sleep_retries = str(cfg.get("sleep_retries") or "exp=2.0").strip()
    job_timeout = float(cfg.get("gallery_dl_job_timeout_sec") or 0) or 0.0
    extra = cfg.get("gallery_dl_extra_args")
    extra_args: List[str] = [str(x) for x in extra] if isinstance(extra, list) else []

    uname = dl.profile_url_to_username(profile_url)
    safe = _safe_dump_filename(uname)
    dump_path = (dump_staging / f"{safe}.json").resolve()
    stdout_to_memory = _cfg_bool(cfg, "dump_json_stdout_to_memory", True)

    cookies_path: Optional[Path] = None
    cookies_from_browser: Optional[str] = None
    if _cfg_bool(cfg, "use_cookies", False):
        ycfb = cfg.get("cookies_from_browser")
        if ycfb is not None and str(ycfb).strip():
            cookies_from_browser = str(ycfb).strip()
        else:
            cf = cfg.get("cookies_file")
            if cf:
                cp = dl.resolve_relative_to_root(root, str(cf))
                if cp.is_file():
                    cookies_path = cp
                else:
                    logger.warning("cookies_file not found: %s", cp)

    min_short = dl._cfg_nonneg_int(cfg, "min_short", int(getattr(crawl, "MIN_SHORT_SIDE", 0)))
    min_long = dl._cfg_nonneg_int(cfg, "min_long", int(getattr(crawl, "MIN_LONG_SIDE", 0)))
    image_key_style = str(cfg.get("image_key_style") or "crawl_hash").strip()
    image_key_prefix = str(cfg.get("image_key_prefix") or "500px-downloads/media").strip() or "500px-downloads/media"

    last_err = ""
    dump_bytes = 0
    for attempt in range(retries + 1):
        proxy_url = proxy_pool.next() if proxy_pool is not None else None
        cmd = _gallery_dl_dump_cmd(
            crawl,
            gallery_dl_bin=gallery_dl_bin,
            cookies_path=cookies_path,
            cookies_from_browser=cookies_from_browser,
            proxy_url=proxy_url,
            extra_args=extra_args,
            profile_url=profile_url,
            retries=retries,
            http_timeout=http_timeout,
            sleep_429=sleep_429,
            sleep_retries=sleep_retries,
        )
        dump_staging.mkdir(parents=True, exist_ok=True)
        try:
            if not stdout_to_memory and dump_path.exists():
                dump_path.unlink()
        except OSError:
            pass
        try:
            if stdout_to_memory:
                r = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=job_timeout if job_timeout > 0 else None,
                )
            else:
                with dump_path.open("w", encoding="utf-8") as outf:
                    r = subprocess.run(
                        cmd,
                        stdout=outf,
                        stderr=subprocess.PIPE,
                        text=True,
                        timeout=job_timeout if job_timeout > 0 else None,
                    )
            if r.returncode != 0:
                last_err = (r.stderr or "")[:2000] or f"exit={r.returncode}"
                logger.warning(
                    "dump_json_cmd_fail url=%s attempt=%s/%s err=%s",
                    profile_url,
                    attempt + 1,
                    retries + 1,
                    last_err[:400],
                )
                continue
        except subprocess.TimeoutExpired:
            last_err = f"timeout_sec={job_timeout}"
            logger.warning("dump_json_timeout url=%s attempt=%s", profile_url, attempt + 1)
            continue
        except Exception as e:
            last_err = repr(e)
            logger.warning("dump_json_exc url=%s err=%s", profile_url, e)
            continue

        if stdout_to_memory:
            raw = r.stdout or ""
        else:
            try:
                raw = dump_path.read_text(encoding="utf-8")
            except OSError as e:
                last_err = repr(e)
                continue

        dump_bytes = len(raw.encode("utf-8"))
        items = crawl.parse_gallery_dl_dump_stdout_text(raw, loads=json_loads)
        if not items:
            items = _fallback_g_items(crawl, profile_url, gallery_dl_bin, cookies_path, cookies_from_browser, proxy_url, extra_args)

        items = _dedupe_gallery_items_by_photo_id(items)

        pairs = slim_metadata_pairs_from_items(
            dl,
            crawl,
            items,
            min_short=min_short,
            min_long=min_long,
            image_key_style=image_key_style,
            image_key_prefix=image_key_prefix,
        )
        n_new = meta_sink.commit_flush_and_finalize_profile(pairs)
        eligible_n = len(pairs)

        with seen_lock:
            seen_db.mark_done(
                profile_url,
                uname,
                n_new,
                dump_bytes,
                eligible_count=eligible_n,
            )

        with stats_lock:
            images_recorded_total[0] += int(n_new)

        if not stdout_to_memory:
            try:
                dump_path.unlink(missing_ok=True)
            except OSError:
                pass

        logger.info(
            "extract_ok url=%s user=%s new_metadata_lines=%s eligible_count=%s dump_bytes=%s",
            profile_url,
            uname,
            n_new,
            eligible_n,
            dump_bytes,
        )
        return profile_url, True, ""

    failed_log.append(profile_url, "dump_json_file", last_err[:3500])
    try:
        if dump_path.exists():
            dump_path.unlink()
    except OSError:
        pass
    return profile_url, False, last_err


def _fallback_g_items(
    crawl: Any,
    profile_url: str,
    gallery_dl_bin: str,
    cookies_path: Optional[Path],
    cookies_from_browser: Optional[str],
    proxy_url: Optional[str],
    extra_args: List[str],
) -> List[Dict[str, Any]]:
    """与 crawl.run_gallery_dl_dump_json 的 -g 回退一致（仅 URL 行）。"""
    cmd = crawl._gallery_dl_argv_prefix(gallery_dl_bin, cookies_path, cookies_from_browser, proxy_url)
    for a in extra_args:
        s = str(a).strip()
        if s:
            cmd.append(s)
    cmd.extend(["-g", profile_url.strip()])
    res_g = subprocess.run(cmd, capture_output=True, text=True)
    if res_g.returncode != 0:
        return []
    out: List[Dict[str, Any]] = []
    for line in (res_g.stdout or "").splitlines():
        u = line.strip()
        if u.startswith("http"):
            out.append({"url": u})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="500px 用户主页仅 dump-json 提取元数据写入精简 metadata JSONL（不下载图片）。",
        epilog=f"默认配置: {_DEFAULT_CONFIG}",
    )
    ap.add_argument("--config", type=str, default=str(_DEFAULT_CONFIG), help="YAML 配置路径")
    args = ap.parse_args()
    cfg_path = Path(args.config).resolve()
    cfg = load_yaml(cfg_path)
    root = Path(str(cfg.get("root") or "").strip() or str(_REPO_ROOT)).resolve()
    append_master = _cfg_bool(cfg, "extract_append_master_log", True)
    logger, log_path, master_path = setup_logging(root, append_master_log=append_master)
    run_t0 = time.perf_counter()

    def emit_summary(
        *,
        status: str,
        workers: int,
        meta_path: Optional[Path],
        use_proxies: bool,
        proxy_count: int,
        stdout_memory: bool,
        urls_listed: int,
        urls_pending: int,
        profiles_ok: int,
        profiles_fail: int,
        metadata_lines: int,
        skip_done: bool,
        extra: str = "",
        metadata_display: Optional[str] = None,
    ) -> None:
        elapsed = time.perf_counter() - run_t0
        total = profiles_ok + profiles_fail
        prate = total / elapsed if elapsed > 0 and total else 0.0
        lrate = metadata_lines / elapsed if elapsed > 0 else 0.0
        logger.info("======== extract_run_summary ========")
        logger.info("run_status=%s%s", status, (" " + extra) if extra else "")
        logger.info("elapsed_sec=%.3f config=%s root=%s", elapsed, cfg_path, root)
        logger.info(
            "workers=%s use_proxies=%s proxy_count=%s dump_json_stdout_to_memory=%s skip_completed_profiles=%s",
            workers,
            use_proxies,
            proxy_count,
            stdout_memory,
            skip_done,
        )
        logger.info(
            "urls_listed=%s urls_pending_this_run=%s profiles_ok=%s profiles_fail=%s metadata_lines_new=%s",
            urls_listed,
            urls_pending,
            profiles_ok,
            profiles_fail,
            metadata_lines,
        )
        logger.info("profiles_per_sec=%.4f metadata_lines_per_sec=%.2f", prate, lrate)
        logger.info("detail_log=%s", log_path)
        if master_path is not None:
            logger.info("append_master_log=%s", master_path)
        mt = (metadata_display or (str(meta_path) if meta_path else None))
        if mt is not None:
            logger.info("metadata_target=%s (append)", mt)
        logger.info("======================================")
        if "extract_run_summary_jsonl" in cfg and not str(cfg.get("extract_run_summary_jsonl") or "").strip():
            rel = ""
        else:
            rel = str(cfg.get("extract_run_summary_jsonl") or "output/logs/extract_run_summary.jsonl").strip()
        if rel:
            sp = (root / rel).resolve() if not Path(rel).is_absolute() else Path(rel).resolve()
            row = {
                "event": "extract_run_end",
                "run_status": status,
                "finished_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "elapsed_sec": round(elapsed, 3),
                "workers": workers,
                "use_proxies": use_proxies,
                "proxy_count": proxy_count,
                "dump_json_stdout_to_memory": stdout_memory,
                "skip_completed_profiles": skip_done,
                "urls_listed": urls_listed,
                "urls_pending_this_run": urls_pending,
                "profiles_ok": profiles_ok,
                "profiles_fail": profiles_fail,
                "metadata_lines_new": metadata_lines,
                "profiles_per_sec": round(prate, 6),
                "metadata_lines_per_sec": round(lrate, 4),
                "metadata_jsonl": mt,
                "detail_log": str(log_path),
                "append_master_log": str(master_path) if master_path else None,
                "config": str(cfg_path),
                "root": str(root),
            }
            if extra:
                row["extra"] = extra
            try:
                _append_run_summary_jsonl(sp, row)
            except OSError as e:
                logger.warning("extract_run_summary_jsonl_write_failed path=%s err=%s", sp, e)
        _flush_loggers(logger)

    scripts_dir = Path(__file__).resolve().parent
    crawl = load_crawl(scripts_dir)
    dl = load_download_module(scripts_dir)

    if master_path is not None:
        logger.info(
            "----- extract run begin pid=%s utc=%s -----",
            os.getpid(),
            dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )

    seen_rel = str(cfg.get("extract_profiles_seen_db") or "output/state/extract_500px_user_profiles_seen.db").strip()
    seen_path = (root / seen_rel).resolve() if not Path(seen_rel).is_absolute() else Path(seen_rel).resolve()
    failed_rel = str(cfg.get("extract_failed_profiles_log") or "output/state/extract_500px_user_profiles_failed.jsonl").strip()
    failed_path = (root / failed_rel).resolve() if not Path(failed_rel).is_absolute() else Path(failed_rel).resolve()

    urls_raw = dl.load_profile_urls(cfg, root=root)
    seen_db = ExtractSeenDB(seen_path, cfg=cfg)
    seen_lock = threading.Lock()
    failed_log = dl.FailedProfileLog(failed_path)

    # 规范 URL + 去重，保证每个 uid 只对应一条任务与 seen 主键
    urls_all: List[str] = []
    _seen_urls: Set[str] = set()
    for u in urls_raw:
        c = canonical_extract_profile_url(dl, u)
        if not c or c in _seen_urls:
            continue
        _seen_urls.add(c)
        urls_all.append(c)

    skip_done = _cfg_bool(cfg, "skip_completed_profiles", True)
    if skip_done:
        done_set = seen_db.all_done_profile_urls(dl)
        urls = [u for u in urls_all if u not in done_set]
        skipped = len(urls_all) - len(urls)
        if skipped:
            logger.info("skip_seen count=%s done_in_db=%s pending=%s", skipped, len(done_set), len(urls))
    else:
        urls = urls_all

    meta_rel = str(cfg.get("metadata_dir") or "output/metadata").strip() or "output/metadata"
    meta_dir = dl.resolve_relative_to_root(root, meta_rel)
    meta_dir.mkdir(parents=True, exist_ok=True)
    day = crawl.utc_day()
    meta_path = meta_dir / f"{day}.metadata.jsonl"
    metadata_display = _metadata_display_preview(cfg, meta_dir, meta_path)

    workers = max(1, int(cfg.get("workers", 48) or 48))
    stdout_memory = _cfg_bool(cfg, "dump_json_stdout_to_memory", True)
    use_proxies = _cfg_bool(cfg, "use_proxies", False)

    if not urls:
        logger.error("没有待处理的用户主页 URL（列表为空或已全部在 extract seen DB 中）")
        seen_db.close()
        emit_summary(
            status="no_pending",
            workers=workers,
            meta_path=meta_path,
            use_proxies=use_proxies,
            proxy_count=0,
            stdout_memory=stdout_memory,
            urls_listed=len(urls_all),
            urls_pending=0,
            profiles_ok=0,
            profiles_fail=0,
            metadata_lines=0,
            skip_done=skip_done,
            metadata_display=metadata_display,
        )
        return 1

    dump_rel = str(cfg.get("dump_staging_dir") or "output/extract_staging/dumps").strip() or "output/extract_staging/dumps"
    dump_staging = dl.resolve_relative_to_root(root, dump_rel)
    dump_staging.mkdir(parents=True, exist_ok=True)

    flush_interval = float(cfg.get("metadata_flush_interval_sec", 3.0) or 3.0)
    flush_every = int(cfg.get("metadata_flush_every_n_lines", 200) or 200)

    sh_cfg = _sharding_cfg_dict(cfg)
    use_sharding = sh_cfg is not None and _cfg_bool(sh_cfg, "enabled", True)
    if use_sharding:
        st_rel = str(sh_cfg.get("state_file") or "output/metadata/extract_metadata_shard_state.json").strip()
        st_path = (root / st_rel).resolve() if not Path(st_rel).is_absolute() else Path(st_rel).resolve()
        tpl = str(sh_cfg.get("filename_template") or "extract_metadata_{shard}.jsonl").strip()
        if "profiles_per_shard" in sh_cfg:
            pps = int(sh_cfg["profiles_per_shard"])
        else:
            pps = 0
        msb = int(sh_cfg["max_shard_bytes"]) if "max_shard_bytes" in sh_cfg else 0
        if "max_shard_lines" in sh_cfg:
            msl = int(sh_cfg["max_shard_lines"])
        else:
            msl = _DEFAULT_SHARD_MAX_LINES
        ph = max(0, int(sh_cfg.get("placeholder_max_lines", 50)))
        if pps <= 0 and msb <= 0 and msl <= 0:
            pps = 20_000
            logger.warning(
                "extract_metadata_sharding: all limits were 0/disabled; using profiles_per_shard=%s",
                pps,
            )
        meta_sink = ShardedExtractMetadataSink(
            meta_dir,
            st_path,
            filename_template=tpl,
            profiles_per_shard=pps,
            max_shard_bytes=msb,
            max_shard_lines=msl,
            flush_interval_sec=flush_interval,
            flush_every_n_lines=flush_every,
            logger=logger,
            placeholder_max_lines=ph,
        )
        metadata_display = meta_sink.metadata_log_label()
    else:
        meta_sink = dl.MetadataSink(
            meta_path,
            flush_interval_sec=flush_interval,
            flush_every_n_lines=flush_every,
            flush_after_each_profile=False,
        )
    meta_sink.load_existing_image_keys_from_disk()

    proxies_rel = str(cfg.get("proxies_file") or "config/proxies.yaml").strip()
    proxy_pool: Optional[Any] = None
    proxy_count = 0
    if use_proxies:
        try:
            purls = dl.load_proxy_urls_for_gallery_dl(root, proxies_rel)
        except Exception as e:
            logger.error("proxies_load_failed path=%s err=%s", proxies_rel, e)
            seen_db.close()
            emit_summary(
                status="proxy_load_failed",
                workers=workers,
                meta_path=meta_path,
                use_proxies=True,
                proxy_count=0,
                stdout_memory=stdout_memory,
                urls_listed=len(urls_all),
                urls_pending=len(urls),
                profiles_ok=0,
                profiles_fail=0,
                metadata_lines=0,
                skip_done=skip_done,
                extra=str(e)[:500],
                metadata_display=metadata_display,
            )
            return 1
        if not purls:
            logger.error("use_proxies true but proxies list empty: %s", dl.resolve_relative_to_root(root, proxies_rel))
            seen_db.close()
            emit_summary(
                status="proxy_list_empty",
                workers=workers,
                meta_path=meta_path,
                use_proxies=True,
                proxy_count=0,
                stdout_memory=stdout_memory,
                urls_listed=len(urls_all),
                urls_pending=len(urls),
                profiles_ok=0,
                profiles_fail=0,
                metadata_lines=0,
                skip_done=skip_done,
                metadata_display=metadata_display,
            )
            return 1
        proxy_pool = dl.ProxyRoundRobin(purls)
        proxy_count = len(proxy_pool)
        logger.info("proxies_enabled count=%s file=%s", proxy_count, dl.resolve_relative_to_root(root, proxies_rel))

    images_recorded_total: List[int] = [0]
    stats_lock = threading.Lock()
    stats_interval = float(cfg.get("extract_stats_interval_sec", 600) or 0)
    stats_stop: Optional[threading.Event] = None
    stats_thr: Optional[threading.Thread] = None

    def _stats_loop() -> None:
        while stats_stop is not None and not stats_stop.wait(timeout=stats_interval):
            with stats_lock:
                n = images_recorded_total[0]
            logger.info("extract_stats metadata_lines_total=%s", n)

    if stats_interval > 0:
        stats_stop = threading.Event()
        stats_thr = threading.Thread(target=_stats_loop, name="extract-stats", daemon=True)
        stats_thr.start()

    logger.info(
        "extract_start workers=%s urls_pending=%s urls_listed=%s metadata=%s (append) "
        "dump_staging=%s stdout_memory=%s flush_every_n=%s flush_interval_sec=%s per_uid_flush=always",
        workers,
        len(urls),
        len(urls_all),
        metadata_display,
        dump_staging,
        stdout_memory,
        flush_every,
        flush_interval,
    )

    json_loads = _build_json_loads(cfg, logger)

    ok_n = 0
    fail_n = 0
    with futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [
            ex.submit(
                worker_extract,
                u,
                cfg=cfg,
                root=root,
                crawl=crawl,
                dl=dl,
                logger=logger,
                dump_staging=dump_staging,
                meta_sink=meta_sink,
                seen_db=seen_db,
                seen_lock=seen_lock,
                failed_log=failed_log,
                proxy_pool=proxy_pool,
                images_recorded_total=images_recorded_total,
                stats_lock=stats_lock,
                json_loads=json_loads,
            )
            for u in urls
        ]
        for fut in futures.as_completed(futs):
            try:
                _url, ok, _err = fut.result()
                if ok:
                    ok_n += 1
                else:
                    fail_n += 1
            except Exception as e:
                fail_n += 1
                logger.exception("worker_crash err=%s", e)

    if stats_stop is not None:
        stats_stop.set()
    if stats_thr is not None:
        stats_thr.join(timeout=2.0)

    meta_sink.flush()
    seen_db.close()
    st = "success" if fail_n == 0 else "completed_with_errors"
    emit_summary(
        status=st,
        workers=workers,
        meta_path=meta_path,
        use_proxies=use_proxies,
        proxy_count=proxy_count,
        stdout_memory=stdout_memory,
        urls_listed=len(urls_all),
        urls_pending=len(urls),
        profiles_ok=ok_n,
        profiles_fail=fail_n,
        metadata_lines=images_recorded_total[0],
        skip_done=skip_done,
        metadata_display=metadata_display,
    )
    return 0 if fail_n == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
