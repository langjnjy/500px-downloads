#!/usr/bin/env python3
"""
使用 gallery-dl -d 并发下载多个 500px 用户主页作品，并追加写入 metadata JSONL。

- 成功：gallery-dl 退出码 0，且每条 planned 均完成落盘与 materialize（len(pairs)==len(planned)）后写入 SQLite seen；
  仅因 image_key 与当日 jsonl 重复导致新写入行数变少时仍计成功。失败写入 JSONL，可设 retry_failed_only 重试。
- metadata 使用单锁 + 内存缓冲，按条数/时间间隔 flush，可选每个用户完成后强制 flush。
- 每个用户主页：metadata_source=dump_json 时先 gallery-dl --dump-json 再下载（500px 推荐）；
  metadata_source=sidecar 时仅一次下载并读 staging/*.json（仅当确有逐图 JSON 时可用；500px 多为单文件 info.json，不适用）。
- crawl_hash（默认）：gallery-dl -d 指向 gallery_dl_dest（常用 output/media，与最终平铺同盘），文件先落在
  gallery_dl_dest/500px/<username>/；再按 best_url 的 sha1 平移到 final_media_dir（可与 gallery_dl_dest 相同）。
  元数据来自 dump-json 条目或 sidecar 的 *.json（与 pick_best 一致），不依赖其它侧车。metadata 的 image_key 为 {image_key_prefix}/{sha1}{ext}。

- 可选：use_proxies + proxies_file，从 YAML 顺序循环取代理传给 gallery-dl --proxy（与 use_cookies 可同时开）。
- 落盘目录由 YAML 的 root、final_media_dir、metadata_dir 控制（绝对路径或相对 root）。

依赖：同目录 crawl_500px_gallery_dl.py（复用 pick_best_url_from_gallery_item / run_gallery_dl_dump_json 等）。

入口脚本：``scripts/download.py``；默认配置 ``config/download.yaml``。
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
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
# 省略 --config 时使用仓库内 config/download.yaml（绝对路径，与 cwd 无关）
_DEFAULT_CONFIG = (_REPO_ROOT / "config" / "download.yaml").resolve()


class _UtcFormatter(logging.Formatter):
    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        return dt.datetime.fromtimestamp(record.created, tz=dt.timezone.utc).strftime(
            datefmt or "%Y-%m-%dT%H:%M:%SZ"
        )


def setup_logging(root: Path) -> Tuple[logging.Logger, Path]:
    """{root}/output/logs/download_500px_{UTC时间戳}Z_{pid}.log"""
    logs_dir = Path(root) / "output" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_path = logs_dir / f"download_500px_{ts}_{os.getpid()}.log"
    lg = logging.getLogger("download_500px")
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
    lg.propagate = False
    lg.info("log_file=%s", log_path)
    return lg, log_path


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    obj = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return obj if isinstance(obj, dict) else {}


def resolve_relative_to_root(root: Path, rel_or_abs: str) -> Path:
    """绝对路径原样解析；否则相对 {root}（与 crawl 的 output/ 等路径习惯一致，非相对 YAML 所在目录）。"""
    p = Path(rel_or_abs.strip())
    if p.is_absolute():
        return p.resolve()
    return (root / p).resolve()


def load_proxy_urls_for_gallery_dl(root: Path, rel_or_abs: str) -> List[str]:
    """
    从 proxies.yaml 解析 gallery-dl --proxy 用的 URL（http://user:pass@host:port）。
    按 host:port 去重，保留文件中首次出现顺序（与 prepare_and_test_proxies 一致）。
    """
    path = resolve_relative_to_root(root, (rel_or_abs or "config/proxies.yaml").strip())
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = data.get("proxies")
    if not isinstance(rows, list):
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for item in rows:
        if not isinstance(item, dict):
            continue
        host = str(item.get("host") or "").strip()
        if not host:
            continue
        try:
            port_i = int(item.get("port"))
        except (TypeError, ValueError):
            continue
        key = f"{host}:{port_i}"
        if key in seen:
            continue
        seen.add(key)
        username = str(item.get("username") or "").strip()
        password = str(item.get("password") or "").strip()
        uq = urllib.parse.quote(username, safe="")
        pq = urllib.parse.quote(password, safe="")
        out.append(f"http://{uq}:{pq}@{host}:{port_i}")
    return out


class ProxyRoundRobin:
    """每个用户任务顺序取下一个代理；用完后从列表头循环（支持 12 worker 时 1–12、13–24 …）。"""

    def __init__(self, urls: List[str]) -> None:
        self._urls = [u.strip() for u in urls if u and str(u).strip()]
        self._lock = threading.Lock()
        self._next = 0

    def __len__(self) -> int:
        return len(self._urls)

    def next(self) -> Optional[str]:
        if not self._urls:
            return None
        with self._lock:
            url = self._urls[self._next % len(self._urls)]
            self._next += 1
            return url


def load_crawl_500px(scripts_dir: Path) -> Any:
    path = scripts_dir / "crawl_500px_gallery_dl.py"
    if not path.is_file():
        raise SystemExit(f"crawl_500px_gallery_dl.py not found next to this script: {path}")
    spec = importlib.util.spec_from_file_location("crawl_500px_gd_user_dl", path)
    if spec is None or spec.loader is None:
        raise SystemExit(f"failed to load module spec: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def profile_url_to_username(url: str) -> str:
    u = url.strip()
    if not u:
        return "unknown"
    if "://" not in u:
        return urllib.parse.unquote(u).strip() or "unknown"
    parsed = urllib.parse.urlparse(u)
    parts = [p for p in (parsed.path or "").split("/") if p]
    if len(parts) >= 2 and parts[0].lower() == "p":
        return urllib.parse.unquote(parts[1]).strip() or "unknown"
    return "unknown"


def _line_to_profile_url(line: str) -> str:
    """一行一个 uid 或完整主页 URL；无 scheme 时视为 500px 用户名。"""
    s = line.strip()
    if not s:
        return ""
    if "://" in s:
        return s
    return f"https://500px.com/p/{s}"


def _page_html_from_500px_source(source: Dict[str, Any]) -> str:
    rel = str(source.get("url") or "").strip()
    if rel.startswith("/"):
        return "https://500px.com" + rel
    if rel.startswith("http"):
        return rel
    return ""


def _slug_from_500px_source(source: Dict[str, Any]) -> Any:
    """对齐 Unsplash 的 slug 语义：优先 API slug，否则从 /photo/<id>/<slug> 取末段。"""
    if source.get("slug") is not None and str(source.get("slug")).strip():
        return source.get("slug")
    rel = str(source.get("url") or "").strip().strip("/")
    parts = [p for p in rel.split("/") if p]
    if len(parts) >= 3 and parts[0] == "photo":
        return "/".join(parts[2:]) if len(parts) > 2 else parts[-1]
    if len(parts) >= 1:
        return parts[-1]
    return None


def _portfolio_url_from_500px_user(user: Dict[str, Any]) -> Any:
    about = str(user.get("about") or "")
    m = re.search(r"https?://[^\s\"'<>]+", about)
    if m:
        return m.group(0).rstrip(").,;]")
    return None


def _user_location_string(user: Dict[str, Any]) -> Any:
    if isinstance(user.get("location"), str) and user.get("location", "").strip():
        return str(user.get("location")).strip()
    bits = [user.get("city"), user.get("country")]
    s = ", ".join(str(x).strip() for x in bits if x and str(x).strip())
    return s if s else None


def _updated_at_from_500px(source: Dict[str, Any]) -> Any:
    for k in ("updated_at", "feature_date", "editors_choice_date", "highest_rating_date"):
        v = source.get(k)
        if v is not None and str(v).strip():
            return v
    return None


def _iso_utc_to_unix(s: Any) -> Optional[int]:
    """将 API 常见 ISO8601 串转为 UTC Unix 秒；无法解析则返回 None。"""
    if not isinstance(s, str):
        return None
    t = s.strip()
    if not t:
        return None
    if t.endswith("Z"):
        t = t[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(t)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        return None


def _timestamps_from_500px(source: Dict[str, Any], user: Dict[str, Any]) -> Dict[str, Any]:
    """作品与用户相关时间：ISO 字符串 + 可解析时的 *_unix（秒）。"""
    recorded_iso = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pairs: List[Tuple[str, Any]] = [
        ("created_at", source.get("created_at")),
        ("updated_at", _updated_at_from_500px(source)),
        ("taken_at", source.get("taken_at")),
        ("highest_rating_date", source.get("highest_rating_date")),
        ("feature_date", source.get("feature_date")),
        ("editors_choice_date", source.get("editors_choice_date")),
        ("user_registration_at", user.get("registration_date")),
    ]
    out: Dict[str, Any] = {"recorded_at": recorded_iso}
    out["recorded_at_unix"] = int(dt.datetime.now(dt.timezone.utc).timestamp())
    for key, val in pairs:
        if val is None:
            continue
        if isinstance(val, str) and not val.strip():
            continue
        out[key] = val
        u = _iso_utc_to_unix(val)
        if u is not None:
            out[f"{key}_unix"] = u
    return out


def _user_about_raw(user: Dict[str, Any]) -> Optional[str]:
    """摄影师简介原文（与 portfolio_url 从 about 里抽 URL 互不替代）。"""
    a = user.get("about")
    if isinstance(a, str) and a.strip():
        return a.strip()
    return None


def _likes_from_500px(source: Dict[str, Any]) -> Any:
    if source.get("likes") is not None:
        return source.get("likes")
    if source.get("positive_votes_count") is not None:
        return source.get("positive_votes_count")
    return source.get("votes_count")


def _topic_submissions_extras_from_500px(source: Dict[str, Any]) -> Any:
    """Unsplash 同名字段多为对象；此处收纳 gallery-dl 提供的其余可序列化元数据。"""
    keys = (
        "comments_count",
        "votes_count",
        "positive_votes_count",
        "times_viewed",
        "feature",
        "feature_date",
        "editors_choice",
        "editors_choice_date",
        "highest_rating",
        "highest_rating_date",
        "rating",
        "nsfw",
        "has_nsfw_tags",
        "camera",
        "lens",
        "focal_length",
        "aperture",
        "shutter_speed",
        "iso",
        "watermark",
        "privacy",
        "privacy_level",
        "status",
        "aigc",
        "category",
        "subcategory",
        "taken_at",
        "latitude",
        "longitude",
        "location_details",
        "image_format",
        "images",
        "image_url",
        "editored_by",
        "licensing_info",
        "licensing_status",
        "licensing_type",
        "licensing_usage",
        "store_height",
        "store_width",
        "store_license",
        "user_id",
        "profile",
        "url",
    )
    out: Dict[str, Any] = {}
    for k in keys:
        if k not in source:
            continue
        v = source.get(k)
        if v is None:
            continue
        out[k] = v
    return out if out else None


def build_metadata_json_line(
    *,
    image_key: str,
    title: str,
    width: int,
    height: int,
    image_url: str,
    source: Dict[str, Any],
) -> str:
    """与 gallery_dl_user_download.py / crawl_unsplash.write_metadata 同一行结构（字段名与层级一致）。"""
    user = source.get("user") if isinstance(source.get("user"), dict) else {}
    page_html = _page_html_from_500px_source(source)
    links_html = page_html
    links_download = image_url
    raw_name = source.get("name")
    name_str = raw_name.strip() if isinstance(raw_name, str) else ""
    tags_val = source.get("tags")
    tags_list = tags_val if isinstance(tags_val, list) else None
    user_about = _user_about_raw(user)
    row = {
        "image_key": image_key,
        "image_url": image_url,
        "title": title,
        "name": name_str or None,
        "tags": tags_list,
        "resolution": f"{width}x{height}",
        "id": source.get("id"),
        "slug": _slug_from_500px_source(source),
        "created_at": source.get("created_at"),
        "updated_at": _updated_at_from_500px(source),
        "timestamps": _timestamps_from_500px(source, user),
        "description": source.get("description"),
        "alt_description": source.get("alt_description") or source.get("name"),
        "original_width": source.get("width"),
        "original_height": source.get("height"),
        "color": source.get("color"),
        "blur_hash": source.get("blur_hash"),
        "likes": _likes_from_500px(source),
        "user": {
            "name": user.get("name") if user.get("name") else user.get("fullname"),
            "username": user.get("username"),
            "about": user_about,
            "location": _user_location_string(user),
            "portfolio_url": user.get("portfolio_url") if user.get("portfolio_url") else _portfolio_url_from_500px_user(user),
        },
        "links": {
            "html": links_html,
            "download": links_download,
        },
        "topic_submissions": _topic_submissions_extras_from_500px(source),
        "premium": source.get("premium"),
        "plus": source.get("plus"),
        "sponsorship": source.get("sponsorship"),
    }
    return json.dumps(row, ensure_ascii=False) + "\n"


class ProfileSeenDB:
    """已成功整站下载的用户主页（对齐 output/state/*.db 习惯路径）。"""

    def __init__(self, db_path: Path, *, cfg: Optional[Dict[str, Any]] = None):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        c = cfg or {}
        syn = str(c.get("seen_db_synchronous") or "NORMAL").strip().upper()
        if syn in ("OFF", "0", "NO", "FALSE"):
            self.conn.execute("PRAGMA synchronous=OFF")
        elif syn in ("FULL", "2", "YES", "TRUE"):
            self.conn.execute("PRAGMA synchronous=FULL")
        else:
            self.conn.execute("PRAGMA synchronous=NORMAL")
        ck = c.get("seen_db_cache_kb")
        if ck is not None:
            try:
                self.conn.execute("PRAGMA cache_size=?", (int(ck),))
            except (TypeError, ValueError):
                pass
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gallery_dl_user_profiles_done (
                profile_url TEXT PRIMARY KEY,
                username TEXT NOT NULL,
                dest_dir TEXT NOT NULL,
                completed_at_utc TEXT NOT NULL,
                metadata_lines INTEGER NOT NULL,
                gallery_dl_exit_code INTEGER NOT NULL
            )
            """
        )
        self.conn.commit()

    def is_done(self, profile_url: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM gallery_dl_user_profiles_done WHERE profile_url = ? LIMIT 1",
            (profile_url.strip(),),
        ).fetchone()
        return row is not None

    def mark_done(
        self,
        profile_url: str,
        username: str,
        dest_dir: str,
        metadata_lines: int,
        gallery_dl_exit_code: int,
    ) -> None:
        now = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.conn.execute(
            """
            INSERT OR REPLACE INTO gallery_dl_user_profiles_done(
                profile_url, username, dest_dir, completed_at_utc, metadata_lines, gallery_dl_exit_code
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                profile_url.strip(),
                username.strip(),
                dest_dir,
                now,
                int(metadata_lines),
                int(gallery_dl_exit_code),
            ),
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


class MetadataSink:
    """
    并发安全：同一 lock 内维护 written_image_keys 与缓冲；避免 metadata 重复行与 flush 竞态。
    """

    def __init__(
        self,
        meta_path: Path,
        *,
        flush_interval_sec: float,
        flush_every_n_lines: int,
        flush_after_each_profile: bool,
    ):
        self.meta_path = meta_path
        self.flush_interval_sec = max(0.1, float(flush_interval_sec))
        self.flush_every_n_lines = max(1, int(flush_every_n_lines))
        self.flush_after_each_profile = bool(flush_after_each_profile)
        self._lock = threading.Lock()
        self._buf: List[str] = []
        self._last_flush = time.monotonic()
        self.written_image_keys: Set[str] = set()
        self.meta_path.parent.mkdir(parents=True, exist_ok=True)

    def load_existing_image_keys_from_disk(self) -> None:
        """流式读取，避免当日 metadata 极大时一次性 read_text 占满内存。"""
        if not self.meta_path.is_file():
            return
        with self.meta_path.open("r", encoding="utf-8") as f:
            for line in f:
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

    def commit_profile_rows(self, pairs: List[Tuple[str, str]]) -> int:
        """pairs: (image_key, jsonl_line). 返回新写入行数（不含跳过重复 image_key）。"""
        if not pairs:
            return 0
        with self._lock:
            to_add: List[str] = []
            for image_key, line in pairs:
                if image_key in self.written_image_keys:
                    continue
                self.written_image_keys.add(image_key)
                to_add.append(line)
            if to_add:
                self._buf.extend(to_add)
            added = len(to_add)
            self._maybe_flush_unlocked()
        return added

    def commit_flush_and_finalize_profile(self, pairs: List[Tuple[str, str]]) -> int:
        """单 uid：持锁完成 commit（含中途刷盘条件）与最终 flush，避免与其它 worker 交叉 flush 同一缓冲。"""
        with self._lock:
            if not pairs:
                self._flush_unlocked()
                return 0
            to_add: List[str] = []
            for image_key, line in pairs:
                if image_key in self.written_image_keys:
                    continue
                self.written_image_keys.add(image_key)
                to_add.append(line)
            if to_add:
                self._buf.extend(to_add)
            added = len(to_add)
            self._maybe_flush_unlocked()
            self._flush_unlocked()
            return added

    def flush_after_profile(self) -> None:
        if not self.flush_after_each_profile:
            return
        with self._lock:
            self._flush_unlocked()

    def flush(self) -> None:
        with self._lock:
            self._flush_unlocked()

    def _maybe_flush_unlocked(self) -> None:
        if not self._buf:
            return
        now = time.monotonic()
        if (
            len(self._buf) >= self.flush_every_n_lines
            or (now - self._last_flush) >= self.flush_interval_sec
        ):
            self._flush_unlocked()

    def _flush_unlocked(self) -> None:
        if not self._buf:
            return
        chunk = "".join(self._buf)
        self._buf.clear()
        with self.meta_path.open("a", encoding="utf-8") as f:
            f.write(chunk)
        self._last_flush = time.monotonic()


class FailedProfileLog:
    """失败记录 JSONL；单锁顺序追加。"""

    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, profile_url: str, stage: str, error_snippet: str) -> None:
        rec = {
            "profile_url": profile_url.strip(),
            "failed_at_utc": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "stage": stage,
            "error": (error_snippet or "")[:4000],
        }
        line = json.dumps(rec, ensure_ascii=False) + "\n"
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(line)
                f.flush()


def load_urls_from_failed_log(path: Path) -> List[str]:
    if not path.is_file():
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        u = str(obj.get("profile_url") or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def load_profile_urls(cfg: Dict[str, Any], *, root: Path) -> List[str]:
    if _cfg_bool(cfg, "retry_failed_only", False):
        rel = str(cfg.get("failed_profiles_log") or "output/state/gallery_dl_500px_user_profiles_failed.jsonl").strip()
        fp = resolve_relative_to_root(root, rel) if not Path(rel).is_absolute() else Path(rel).resolve()
        return load_urls_from_failed_log(fp)

    out: List[str] = []
    raw_list = cfg.get("profile_urls")
    if isinstance(raw_list, list):
        for x in raw_list:
            s = str(x).strip()
            if s and not s.startswith("#"):
                u = _line_to_profile_url(s)
                if u:
                    out.append(u)
    rel = str(cfg.get("profile_urls_file") or "").strip()
    if rel:
        fp = resolve_relative_to_root(root, rel)
        if not fp.is_file():
            raise SystemExit(f"profile_urls_file not found: {fp}")
        for line in fp.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            u = _line_to_profile_url(s)
            if u:
                out.append(u)
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def _cfg_bool(cfg: Dict[str, Any], key: str, default: bool) -> bool:
    v = cfg.get(key, default)
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    s = str(v).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    if s in ("1", "true", "yes", "on"):
        return True
    return default


def _cfg_nonneg_int(cfg: Dict[str, Any], key: str, default: int) -> int:
    """键缺失或非法时用 default；显式 0 保留（与分辨率门槛 YAML 语义一致）。"""
    if key not in cfg or cfg[key] is None:
        return default
    try:
        return max(0, int(cfg[key]))
    except (TypeError, ValueError):
        return default


def build_gallery_dl_download_cmd(
    crawl: Any,
    *,
    gallery_dl_bin: str,
    cookies_path: Optional[Path],
    cookies_from_browser: Optional[str],
    proxy_url: Optional[str],
    retries: int,
    http_timeout: float,
    sleep_429: float,
    sleep_retries: str,
    filename_format: str,
    dest: Path,
    url: str,
    extra_args: List[str],
) -> List[str]:
    cmd: List[str] = crawl._gallery_dl_argv_prefix(
        gallery_dl_bin,
        cookies_path,
        cookies_from_browser,
        proxy_url,
    )
    cmd.extend(
        [
            "-R",
            str(max(0, retries)),
            "--http-timeout",
            str(max(1.0, float(http_timeout))),
            "--sleep-retries",
            str(sleep_retries).strip() or "exp=2.0",
            "--sleep-429",
            str(max(0.0, float(sleep_429))),
            "-q",
            "-f",
            filename_format,
            "-d",
            str(dest),
        ]
    )
    for a in extra_args:
        s = str(a).strip()
        if s:
            cmd.append(s)
    cmd.append(url.strip())
    return cmd


def image_key_for_item(
    crawl: Any,
    *,
    style: str,
    prefix: str,
    photo_id: str,
    best_url: str,
    item: Dict[str, Any],
) -> Tuple[str, str]:
    ext = crawl.guess_ext_from_url(best_url)
    pfx = prefix.strip().strip("/")
    if style.strip().lower() == "crawl_hash":
        oid = crawl.sha1_hex(best_url)
        fn = f"{oid}{ext}"
        return f"{pfx}/{fn}", oid
    ext_item = str(item.get("extension") or "").strip().lower() or ext.lstrip(".")
    if ext_item and not ext_item.startswith("."):
        ext_item = "." + ext_item
    if not ext_item:
        ext_item = ext if ext.startswith(".") else f".{ext}"
    fn = f"{photo_id}{ext_item}"
    return f"{pfx}/{fn}", fn


def _item_username(item: Dict[str, Any]) -> str:
    u = item.get("user")
    if isinstance(u, dict):
        return str(u.get("username") or "").strip()
    return ""


def plan_profile_metadata(
    crawl: Any,
    *,
    items: List[Dict[str, Any]],
    min_short: int,
    min_long: int,
    image_key_style: str,
    image_key_prefix: str,
) -> List[Tuple[str, str, str, str, Path]]:
    """
    由单次 dump-json 的条目生成 (image_key, jsonl, photo_id, staging_username, dst_filename)。
    staging_username 用于定位 gallery-dl 落盘目录 500px/<username>/；最后一项为 crawl_hash
    时平铺到 final_media_dir 下的文件名（仅 basename）。
    """
    out: List[Tuple[str, str, str, str, Path]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        row = _plan_row_from_gallery_item(
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


def _plan_row_from_gallery_item(
    crawl: Any,
    item: Dict[str, Any],
    *,
    min_short: int,
    min_long: int,
    image_key_style: str,
    image_key_prefix: str,
    staging_username_override: Optional[str] = None,
) -> Optional[Tuple[str, str, str, str, Path]]:
    """单条 gallery-dl 条目或侧车 JSON 对象 → 一行 metadata 与 materialize 元组；无则 None。"""
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
    st_user = (staging_username_override or "").strip() or _item_username(item)
    if not st_user:
        return None
    image_key, _ = image_key_for_item(
        crawl,
        style=image_key_style,
        prefix=image_key_prefix,
        photo_id=photo_id,
        best_url=best_url,
        item=item,
    )
    title = crawl.pick_title(item, best_url=best_url)
    line = build_metadata_json_line(
        image_key=image_key,
        title=title,
        width=width,
        height=height,
        image_url=best_url,
        source=item,
    )
    ext = crawl.guess_ext_from_url(best_url)
    if image_key_style.strip().lower() == "crawl_hash":
        oid = crawl.sha1_hex(best_url)
        if not ext.startswith("."):
            ext = "." + ext if ext else ".jpg"
        dst_name = f"{oid}{ext}"
    else:
        ext_item = str(item.get("extension") or "jpg").strip().lower() or "jpg"
        if ext_item and not ext_item.startswith("."):
            ext_item = "." + ext_item
        dst_name = f"{photo_id}{ext_item}"
    return (image_key, line, photo_id, st_user, Path(dst_name))


def plan_profile_metadata_from_sidecar_dir(
    crawl: Any,
    *,
    gallery_dl_base: Path,
    staging_username: str,
    min_short: int,
    min_long: int,
    image_key_style: str,
    image_key_prefix: str,
    logger: logging.Logger,
) -> List[Tuple[str, str, str, str, Path]]:
    """
    下载完成后从 gallery_dl_base/500px/<staging_username>/*.json 读取侧车。

    注意：gallery-dl 对 500px 用户集的 ``--write-info-json`` 通常只写**一个** ``info.json``（目录级），
    **不是**每张图 ``<id>.jpg.json``，无法还原整站作品列表；500px 请用 ``metadata_source: dump_json``。
    本函数仅适用于确有逐图 JSON 的站点/版本；否则上层会按 .jpg 数量与 planned 不一致判失败并清理 staging。
    """
    d = gallery_dl_base / "500px" / staging_username
    if not d.is_dir():
        return []
    out: List[Tuple[str, str, str, str, Path]] = []
    for p in sorted(d.glob("*.json"), key=lambda x: x.name):
        try:
            item = json.loads(p.read_text(encoding="utf-8"))
        except Exception as e:
            logger.debug("sidecar_json_skip path=%s err=%s", p, e)
            continue
        if not isinstance(item, dict):
            continue
        row = _plan_row_from_gallery_item(
            crawl,
            item,
            min_short=min_short,
            min_long=min_long,
            image_key_style=image_key_style,
            image_key_prefix=image_key_prefix,
            staging_username_override=staging_username,
        )
        if row:
            out.append(row)
    return out


def staging_usernames_from_items(items: List[Dict[str, Any]]) -> Set[str]:
    s: Set[str] = set()
    for item in items:
        if isinstance(item, dict):
            u = _item_username(item)
            if u:
                s.add(u)
    return s


def _cleanup_500px_staging(media_base: Path, usernames: Set[str], logger: logging.Logger) -> None:
    """删除本次 dump 涉及的 500px/<user>/ 下残留文件（crawl_hash 平移后遗留）。"""
    px_root = media_base / "500px"
    if not px_root.is_dir():
        return
    for name in usernames:
        d = px_root / name
        if not d.is_dir():
            continue
        try:
            for p in d.iterdir():
                if p.is_file():
                    p.unlink()
            d.rmdir()
        except OSError as e:
            logger.warning("staging_cleanup dir=%s err=%s", d, e)
    try:
        if px_root.is_dir() and not any(px_root.iterdir()):
            px_root.rmdir()
    except OSError:
        pass


def materialize_crawl_hash_downloads(
    *,
    media_dir: Path,
    gallery_dl_base: Path,
    planned: List[Tuple[str, str, str, str, Path]],
    logger: logging.Logger,
) -> List[Tuple[str, str]]:
    """
    gallery-dl 将文件写入 gallery_dl_base/500px/<username>/<id>.<ext>；
    平移到 media_dir / <sha1 文件名>，返回成功写入的 (image_key, jsonl) 列表。
    """
    pairs: List[Tuple[str, str]] = []
    for image_key, line, photo_id, st_user, dst_tail in planned:
        # Unsplash extractor 固定 extension jpg
        src = gallery_dl_base / "500px" / st_user / f"{photo_id}.jpg"
        if not src.is_file():
            alt = gallery_dl_base / "500px" / st_user / f"{photo_id}.jpeg"
            src = alt if alt.is_file() else src
        if not src.is_file():
            logger.warning("missing_gallery_dl_file expected=%s image_key=%s", src, image_key)
            continue
        dst = (media_dir / dst_tail.name).resolve()
        dst.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(src, dst)
        except OSError as e:
            logger.warning("rename_failed src=%s dst=%s err=%s", src, dst, e)
            continue
        pairs.append((image_key, line))
    return pairs


def materialize_id_file_downloads(
    *,
    gallery_dest: Path,
    planned: List[Tuple[str, str, str, str, Path]],
    logger: logging.Logger,
) -> List[Tuple[str, str]]:
    """id_file：保留 gallery-dl 默认路径，仅校验文件存在即计入 metadata。"""
    pairs: List[Tuple[str, str]] = []
    for image_key, line, photo_id, st_user, _dst_tail in planned:
        src = gallery_dest / "500px" / st_user / f"{photo_id}.jpg"
        if not src.is_file():
            alt = gallery_dest / "500px" / st_user / f"{photo_id}.jpeg"
            src = alt if alt.is_file() else src
        if not src.is_file():
            logger.warning("missing_gallery_dl_file expected=%s image_key=%s", src, image_key)
            continue
        pairs.append((image_key, line))
    return pairs


def worker_job(
    profile_url: str,
    *,
    cfg: Dict[str, Any],
    root: Path,
    crawl: Any,
    logger: logging.Logger,
    meta_sink: MetadataSink,
    seen_db: ProfileSeenDB,
    seen_lock: threading.Lock,
    failed_log: FailedProfileLog,
    images_recorded_total: List[int],
    stats_lock: threading.Lock,
    proxy_pool: Optional[ProxyRoundRobin] = None,
) -> Tuple[str, bool, str]:
    gallery_dl_bin = str(cfg.get("gallery_dl_bin") or "gallery-dl").strip() or "gallery-dl"
    retries = int(cfg.get("gallery_dl_retries", 3) or 3)
    http_timeout = float(cfg.get("http_timeout", 120) or 120)
    sleep_429 = float(cfg.get("sleep_429", 60) or 60)
    sleep_retries = str(cfg.get("sleep_retries") or "exp=2.0").strip()
    filename_fmt = str(cfg.get("gallery_dl_filename_format") or "{id}.{extension}").strip() or "{id}.{extension}"
    dest_base = str(cfg.get("dest_base") or "tmp/download_500px_users").strip() or "tmp/download_500px_users"
    job_timeout = float(cfg.get("gallery_dl_job_timeout_sec") or 0) or 0.0
    extra = cfg.get("gallery_dl_extra_args")
    extra_args: List[str] = [str(x) for x in extra] if isinstance(extra, list) else []

    proxy_url: Optional[str] = proxy_pool.next() if proxy_pool is not None else None

    cookies_path: Optional[Path] = None
    cookies_from_browser: Optional[str] = None
    if _cfg_bool(cfg, "use_cookies", False):
        ycfb = cfg.get("cookies_from_browser")
        if ycfb is not None and str(ycfb).strip():
            cookies_from_browser = str(ycfb).strip()
        else:
            cf = cfg.get("cookies_file")
            if cf:
                cp = resolve_relative_to_root(root, str(cf))
                if cp.is_file():
                    cookies_path = cp
                else:
                    logger.warning("cookies_file not found: %s", cp)

    uname = profile_url_to_username(profile_url)
    min_short = _cfg_nonneg_int(
        cfg,
        "min_short",
        int(getattr(crawl, "MIN_SHORT_SIDE", 0)),
    )
    min_long = _cfg_nonneg_int(
        cfg,
        "min_long",
        int(getattr(crawl, "MIN_LONG_SIDE", 0)),
    )
    image_key_style = str(cfg.get("image_key_style") or "crawl_hash").strip()
    image_key_prefix = str(cfg.get("image_key_prefix") or "500px-downloads/media").strip() or "500px-downloads/media"
    ikl = image_key_style.strip().lower()
    metadata_source = str(cfg.get("metadata_source") or "dump_json").strip().lower()

    if ikl == "crawl_hash":
        gd_rel = str(cfg.get("gallery_dl_dest") or "output/media").strip() or "output/media"
        gallery_dl_base = (root / gd_rel).resolve() if not Path(gd_rel).is_absolute() else Path(gd_rel).resolve()
        flat_rel = str(cfg.get("final_media_dir") or "").strip()
        if flat_rel:
            media_dir = Path(flat_rel).resolve() if Path(flat_rel).is_absolute() else (root / flat_rel).resolve()
        else:
            media_dir = gallery_dl_base
    else:
        gallery_dl_base = (root / dest_base / f"download_user_{uname}").resolve()
        media_dir = gallery_dl_base

    gallery_dl_base.mkdir(parents=True, exist_ok=True)

    extra_args_for_dl: List[str] = list(extra_args)
    if metadata_source == "sidecar":
        if not any(str(a).strip() == "--write-info-json" for a in extra_args_for_dl):
            extra_args_for_dl.append("--write-info-json")

    items: List[Dict[str, Any]] = []
    planned: List[Tuple[str, str, str, str, Path]] = []
    staging_users: Set[str] = set()

    if metadata_source == "dump_json":
        try:
            items = crawl.run_gallery_dl_dump_json(
                profile_url.strip(),
                gallery_dl_bin=gallery_dl_bin,
                cookies_path=cookies_path,
                cookies_from_browser=cookies_from_browser,
                logger=logger,
                extra_args=extra_args,
                proxy_url=proxy_url,
            )
        except Exception as e:
            logger.error("dump_json_failed url=%s err=%s", profile_url, e)
            failed_log.append(profile_url, "dump_json", repr(e)[:3500])
            return profile_url, False, str(e)

        staging_users = staging_usernames_from_items(items)
        planned = plan_profile_metadata(
            crawl,
            items=items,
            min_short=min_short,
            min_long=min_long,
            image_key_style=image_key_style,
            image_key_prefix=image_key_prefix,
        )
    elif metadata_source == "sidecar":
        staging_users = {uname}
    else:
        logger.error("unknown metadata_source=%s (use dump_json or sidecar)", metadata_source)
        failed_log.append(profile_url, "config", f"bad metadata_source: {metadata_source}")
        return profile_url, False, f"bad metadata_source: {metadata_source}"

    cmd = build_gallery_dl_download_cmd(
        crawl,
        gallery_dl_bin=gallery_dl_bin,
        cookies_path=cookies_path,
        cookies_from_browser=cookies_from_browser,
        proxy_url=proxy_url,
        retries=retries,
        http_timeout=http_timeout,
        sleep_429=sleep_429,
        sleep_retries=sleep_retries,
        filename_format=filename_fmt,
        dest=gallery_dl_base,
        url=profile_url,
        extra_args=extra_args_for_dl,
    )
    if metadata_source == "dump_json":
        logger.debug("gallery-dl start url=%s dest=%s planned=%s", profile_url, gallery_dl_base, len(planned))
    else:
        logger.debug("gallery-dl start url=%s dest=%s metadata_source=sidecar", profile_url, gallery_dl_base)
    to = job_timeout if job_timeout > 0 else None
    try:
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=to,
        )
    except subprocess.TimeoutExpired:
        msg = f"gallery-dl subprocess timeout after {job_timeout}s"
        logger.error("gallery-dl timeout url=%s", profile_url)
        failed_log.append(profile_url, "gallery_dl", msg)
        return profile_url, False, msg

    if res.returncode != 0:
        err = (res.stderr or res.stdout or "").strip()
        err_snip = err.replace("\n", " ")[:800]
        logger.error(
            "gallery-dl failed rc=%s url=%s snippet=%s",
            res.returncode,
            profile_url,
            err_snip,
        )
        failed_log.append(profile_url, "gallery_dl", err[:3500])
        return profile_url, False, err[:500]

    if metadata_source == "sidecar":
        planned = plan_profile_metadata_from_sidecar_dir(
            crawl,
            gallery_dl_base=gallery_dl_base,
            staging_username=uname,
            min_short=min_short,
            min_long=min_long,
            image_key_style=image_key_style,
            image_key_prefix=image_key_prefix,
            logger=logger,
        )
        user_dir = gallery_dl_base / "500px" / uname
        n_jpg = sum(1 for p in user_dir.glob("*.jpg") if p.is_file())
        if not planned:
            msg = (
                "sidecar: staging 内无可用 JSON 元数据或未通过分辨率过滤。"
                " 500px 下 --write-info-json 多为单文件 info.json，请改 metadata_source: dump_json"
            )
            logger.error("%s url=%s", msg, profile_url)
            failed_log.append(profile_url, "sidecar_metadata", msg)
            if ikl == "crawl_hash":
                _cleanup_500px_staging(gallery_dl_base, staging_users, logger)
            return profile_url, False, msg
        if n_jpg > 0 and len(planned) < n_jpg:
            msg = (
                f"sidecar: 目录内有 {n_jpg} 张 .jpg，但仅从 JSON 得到 {len(planned)} 条 planned；"
                " 500px 无法靠 info.json 覆盖整站。请设 metadata_source: dump_json"
            )
            logger.error("%s url=%s", msg, profile_url)
            failed_log.append(profile_url, "sidecar_metadata", msg)
            if ikl == "crawl_hash":
                _cleanup_500px_staging(gallery_dl_base, staging_users, logger)
            return profile_url, False, msg

    try:
        if ikl == "crawl_hash":
            pairs = materialize_crawl_hash_downloads(
                media_dir=media_dir,
                gallery_dl_base=gallery_dl_base,
                planned=planned,
                logger=logger,
            )
            _cleanup_500px_staging(gallery_dl_base, staging_users, logger)
        else:
            pairs = materialize_id_file_downloads(
                gallery_dest=gallery_dl_base,
                planned=planned,
                logger=logger,
            )
        n_meta = meta_sink.commit_profile_rows(pairs)
        meta_sink.flush_after_profile()
    except Exception as e:
        logger.error("metadata_failed url=%s err=%s", profile_url, e)
        failed_log.append(profile_url, "metadata", repr(e))
        return profile_url, False, str(e)

    expected = len(planned)
    if expected > 0 and len(pairs) != expected:
        msg = f"incomplete_materialize planned={expected} materialized_files={len(pairs)}"
        logger.error("%s url=%s", msg, profile_url)
        failed_log.append(profile_url, "incomplete_metadata", msg)
        if ikl == "crawl_hash":
            _cleanup_500px_staging(gallery_dl_base, staging_users, logger)
        return profile_url, False, msg
    if expected > 0 and n_meta < len(pairs):
        logger.info(
            "metadata_dedup url=%s committed_new_lines=%s pairs=%s (image_key 已在当日 jsonl 中则跳过)",
            profile_url,
            n_meta,
            len(pairs),
        )

    with seen_lock:
        seen_db.mark_done(
            profile_url,
            username=uname,
            dest_dir=str(gallery_dl_base),
            metadata_lines=n_meta,
            gallery_dl_exit_code=int(res.returncode),
        )

    with stats_lock:
        images_recorded_total[0] += int(n_meta)

    logger.debug("profile_ok url=%s new_metadata_lines=%s", profile_url, n_meta)
    return profile_url, True, ""


def main() -> int:
    ap = argparse.ArgumentParser(
        description="gallery-dl 并发下载 500px 用户作品并写 metadata JSONL。",
        epilog=f"默认配置: {_DEFAULT_CONFIG}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--config",
        type=str,
        default=str(_DEFAULT_CONFIG),
        help=f"YAML 配置路径（默认: {_DEFAULT_CONFIG}）",
    )
    ap.add_argument(
        "--min-short",
        type=int,
        default=None,
        metavar="PX",
        help="覆盖 YAML：短边下限（像素）；0 表示不限制该边。未传则使用配置或 crawl_500px_gallery_dl 默认（当前默认 0）。",
    )
    ap.add_argument(
        "--min-long",
        type=int,
        default=None,
        metavar="PX",
        help="覆盖 YAML：长边下限（像素）；0 表示不限制该边。未传则使用配置或 crawl_500px_gallery_dl 默认（当前默认 0）。",
    )
    args = ap.parse_args()
    cfg_path = Path(args.config).resolve()
    cfg = load_yaml(cfg_path)
    if args.min_short is not None:
        cfg["min_short"] = args.min_short
    if args.min_long is not None:
        cfg["min_long"] = args.min_long

    root = Path(str(cfg.get("root") or "").strip() or str(_REPO_ROOT)).resolve()
    logger, _log_path = setup_logging(root)
    scripts_dir = Path(__file__).resolve().parent
    crawl = load_crawl_500px(scripts_dir)

    seen_rel = str(cfg.get("profiles_seen_db") or "output/state/gallery_dl_500px_user_profiles_seen.db").strip()
    seen_path = (root / seen_rel).resolve() if not Path(seen_rel).is_absolute() else Path(seen_rel).resolve()
    failed_rel = str(
        cfg.get("failed_profiles_log") or "output/state/gallery_dl_500px_user_profiles_failed.jsonl"
    ).strip()
    failed_path = (root / failed_rel).resolve() if not Path(failed_rel).is_absolute() else Path(failed_rel).resolve()

    urls_all = load_profile_urls(cfg, root=root)
    seen_db = ProfileSeenDB(seen_path, cfg=cfg)
    seen_lock = threading.Lock()
    failed_log = FailedProfileLog(failed_path)

    skip_done = _cfg_bool(cfg, "skip_completed_profiles", True)
    if skip_done:
        urls = [u for u in urls_all if not seen_db.is_done(u)]
        skipped = len(urls_all) - len(urls)
        if skipped:
            logger.info("skip_seen count=%s", skipped)
    else:
        urls = urls_all

    if not urls:
        logger.error("没有待处理的用户主页 URL（列表为空或已全部在 seen DB 中）")
        seen_db.close()
        return 1

    workers = max(1, int(cfg.get("workers", 8) or 8))
    use_proxies = _cfg_bool(cfg, "use_proxies", False)
    proxies_rel = str(cfg.get("proxies_file") or "config/proxies.yaml").strip()
    proxy_pool: Optional[ProxyRoundRobin] = None
    if use_proxies:
        try:
            purls = load_proxy_urls_for_gallery_dl(root, proxies_rel)
        except Exception as e:
            logger.error("proxies_load_failed path=%s err=%s", proxies_rel, e)
            seen_db.close()
            return 1
        if not purls:
            logger.error("use_proxies true but proxies list empty: %s", resolve_relative_to_root(root, proxies_rel))
            seen_db.close()
            return 1
        proxy_pool = ProxyRoundRobin(purls)
        logger.info("proxies_enabled count=%s file=%s", len(proxy_pool), resolve_relative_to_root(root, proxies_rel))

    meta_rel = str(cfg.get("metadata_dir") or "output/metadata").strip() or "output/metadata"
    meta_dir = resolve_relative_to_root(root, meta_rel)
    meta_dir.mkdir(parents=True, exist_ok=True)
    day = crawl.utc_day()
    meta_path = meta_dir / f"{day}.metadata.jsonl"

    flush_interval = float(cfg.get("metadata_flush_interval_sec", 3.0) or 3.0)
    flush_every = int(cfg.get("metadata_flush_every_n_lines", 200) or 200)
    flush_after_profile = _cfg_bool(cfg, "metadata_flush_after_each_profile", True)

    meta_sink = MetadataSink(
        meta_path,
        flush_interval_sec=flush_interval,
        flush_every_n_lines=flush_every,
        flush_after_each_profile=flush_after_profile,
    )
    meta_sink.load_existing_image_keys_from_disk()

    stats_interval = float(cfg.get("download_stats_interval_sec", 600) or 0)
    images_recorded_total: List[int] = [0]
    stats_lock = threading.Lock()
    stats_stop: Optional[threading.Event] = None
    stats_thr: Optional[threading.Thread] = None
    stats_anchor: Optional[List[int]] = None

    def _stats_loop() -> None:
        while stats_stop is not None and not stats_stop.wait(timeout=stats_interval):
            with stats_lock:
                now = images_recorded_total[0]
            if stats_anchor is None:
                continue
            delta = now - stats_anchor[0]
            stats_anchor[0] = now
            logger.info(
                "download_stats interval_sec=%.0f new_images_in_period=%s cumulative_new_images=%s",
                stats_interval,
                delta,
                now,
            )

    logger.info(
        "run_begin config=%s pending_urls=%s workers=%s use_proxies=%s download_stats_interval_sec=%s",
        cfg_path,
        len(urls),
        workers,
        use_proxies,
        int(stats_interval) if stats_interval > 0 else 0,
    )

    if stats_interval > 0:
        stats_stop = threading.Event()
        stats_anchor = [0]
        stats_thr = threading.Thread(target=_stats_loop, name="download_stats", daemon=True)
        stats_thr.start()

    ok = 0
    fail = 0
    try:
        with futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [
                ex.submit(
                    worker_job,
                    u,
                    cfg=cfg,
                    root=root,
                    crawl=crawl,
                    logger=logger,
                    meta_sink=meta_sink,
                    seen_db=seen_db,
                    seen_lock=seen_lock,
                    failed_log=failed_log,
                    images_recorded_total=images_recorded_total,
                    stats_lock=stats_lock,
                    proxy_pool=proxy_pool,
                )
                for u in urls
            ]
            for fu in futures.as_completed(futs):
                _url, success, _err = fu.result()
                if success:
                    ok += 1
                else:
                    fail += 1
    finally:
        if stats_stop is not None and stats_thr is not None and stats_anchor is not None:
            stats_stop.set()
            stats_thr.join(timeout=max(5.0, stats_interval + 2.0))
            with stats_lock:
                now = images_recorded_total[0]
            tail = now - stats_anchor[0]
            if tail:
                logger.info(
                    "download_stats interval_sec=%.0f new_images_in_period=%s cumulative_new_images=%s (tail)",
                    stats_interval,
                    tail,
                    now,
                )
        meta_sink.flush()
        seen_db.close()

    total_img = images_recorded_total[0]
    logger.info(
        "run_done profiles_ok=%s profiles_fail=%s new_metadata_lines_total=%s metadata=%s",
        ok,
        fail,
        total_img,
        meta_path,
    )
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
