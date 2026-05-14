#!/usr/bin/env python3
"""
500px + gallery-dl 共用工具：供 scripts/download.py 动态加载。

提供与 unsplash crawl_unsplash 中同名函数兼容的接口：
  _gallery_dl_argv_prefix（可选 proxy_url → --proxy）, run_gallery_dl_dump_json, guess_ext_from_url, sha1_hex,
  pick_best_url_from_gallery_item, pick_title, utc_day, MIN_SHORT_SIDE/MIN_LONG_SIDE（默认 0/0：不按分辨率过滤；YAML 可设 min_short/min_long）
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# 0 表示不按短边/长边过滤；extract/download 的 YAML 可覆盖 min_short / min_long
MIN_SHORT_SIDE = 0
MIN_LONG_SIDE = 0


def utc_day() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()


def guess_ext_from_url(url: str) -> str:
    lower = url.lower()
    if ".png" in lower:
        return ".png"
    if ".webp" in lower or "webp=true" in lower:
        return ".webp"
    return ".jpg"


def _gallery_dl_argv_prefix(
    gallery_dl_bin: str,
    cookies_path: Optional[Path],
    cookies_from_browser: Optional[str] = None,
    proxy_url: Optional[str] = None,
) -> List[str]:
    argv = [gallery_dl_bin]
    pu = (proxy_url or "").strip()
    if pu:
        argv.extend(["--proxy", pu])
    cfb = (cookies_from_browser or "").strip()
    if cfb:
        argv.extend(["--cookies-from-browser", cfb])
    elif cookies_path is not None and cookies_path.is_file():
        argv.extend(["--cookies", str(cookies_path.resolve())])
    return argv


def pick_title(item: Dict[str, Any], best_url: str) -> str:
    for k in ("name", "description", "title"):
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _first_https_from_images(item: Dict[str, Any]) -> str:
    """从 500px GraphQL 条目的 images / image_url 中取最佳直链（优先 size 最大）。"""
    best_url = ""
    best_sz = -1
    images = item.get("images")
    if isinstance(images, list):
        for im in images:
            if not isinstance(im, dict):
                continue
            u = str(im.get("https_url") or im.get("url") or "").strip()
            if not u.startswith("http"):
                continue
            try:
                sz = int(im.get("size") or 0)
            except (TypeError, ValueError):
                sz = 0
            if sz > best_sz:
                best_sz = sz
                best_url = u
    if best_url:
        return best_url
    iu = item.get("image_url")
    if isinstance(iu, list):
        for u in iu:
            s = str(u).strip()
            if s.startswith("http"):
                return s
    elif isinstance(iu, str) and iu.strip().startswith("http"):
        return iu.strip()
    u = item.get("url")
    if isinstance(u, str) and u.startswith("http"):
        return u.strip()
    return ""


def pick_best_url_from_gallery_item(
    item: Dict[str, Any], min_short: int, min_long: int
) -> Optional[Tuple[str, int, int]]:
    width = int(item.get("width") or 0)
    height = int(item.get("height") or 0)
    if width > 0 and height > 0:
        short_side, long_side = sorted((width, height))
        if (min_short > 0 and short_side < min_short) or (min_long > 0 and long_side < min_long):
            return None
    raw_url = _first_https_from_images(item)
    if not raw_url:
        return None
    return raw_url, width, height


def parse_gallery_dl_dump_stdout_text(
    raw: str,
    loads: Optional[Callable[[str], Any]] = None,
) -> List[Dict[str, Any]]:
    """
    解析 gallery-dl --dump-json 写入 stdout（或重定向到文件）的文本，得到与 ``run_gallery_dl_dump_json``
    前半段一致的作品 dict 列表（优先 tag=3 URL 行，否则 tag=2 目录行）。

    ``loads`` 可传入 ``orjson.loads`` 的薄封装等以加速大 JSON（缺省为 ``json.loads``）。
    """
    out: List[Dict[str, Any]] = []
    _MSG_DIRECTORY = 2
    _MSG_URL = 3
    _loads: Callable[[str], Any] = loads if loads is not None else json.loads
    raw_stripped = (raw or "").strip()
    if raw_stripped:
        try:
            parsed = _loads(raw_stripped)
            if isinstance(parsed, dict):
                out.append(parsed)
            elif isinstance(parsed, list):
                url_rows: List[Dict[str, Any]] = []
                dir_rows: List[Dict[str, Any]] = []
                plain_dicts: List[Dict[str, Any]] = []
                for ev in parsed:
                    if isinstance(ev, dict):
                        plain_dicts.append(ev)
                        continue
                    if not isinstance(ev, list) or len(ev) < 2:
                        continue
                    tag = ev[0]
                    if tag == -1:
                        continue
                    if tag == _MSG_URL and len(ev) >= 3 and isinstance(ev[2], dict):
                        meta = dict(ev[2])
                        if isinstance(ev[1], str) and ev[1].startswith("http"):
                            meta.setdefault("url", ev[1])
                        url_rows.append(meta)
                    elif tag == _MSG_DIRECTORY and len(ev) == 2 and isinstance(ev[1], dict):
                        dir_rows.append(dict(ev[1]))
                    elif len(ev) >= 3 and isinstance(ev[2], dict):
                        meta = dict(ev[2])
                        if isinstance(ev[1], str) and ev[1].startswith("http"):
                            meta.setdefault("url", ev[1])
                        url_rows.append(meta)
                    elif len(ev) >= 2 and isinstance(ev[1], dict):
                        dir_rows.append(dict(ev[1]))
                out.extend(url_rows if url_rows else dir_rows)
                if not out and plain_dicts:
                    out.extend(plain_dicts)
        except Exception:
            pass

    if not out:
        for line in (raw or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = _loads(line)
            except Exception:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def run_gallery_dl_dump_json(
    seed_url: str,
    gallery_dl_bin: str,
    cookies_path: Optional[Path] = None,
    cookies_from_browser: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
    extra_args: Optional[List[str]] = None,
    proxy_url: Optional[str] = None,
) -> List[Dict[str, Any]]:
    seed = seed_url.strip()
    cmd = _gallery_dl_argv_prefix(gallery_dl_bin, cookies_path, cookies_from_browser, proxy_url)
    for a in extra_args or []:
        s = str(a).strip()
        if s:
            cmd.append(s)
    cmd.extend(["--dump-json", seed])
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"gallery-dl failed for {seed_url}: {(res.stderr or '')[:300]}")

    stderr_hint = (res.stderr or "").strip()
    if logger and stderr_hint:
        low = stderr_hint.lower()
        suspicious = False
        if "401" in stderr_hint or "unauthorized" in low:
            suspicious = True
        elif "[cookies][warning]" in low or "[cookies][error]" in low:
            suspicious = True
        elif "cookie" in low and (
            "failed" in low or "no such table" in low or "could not" in low or "unable to" in low
        ):
            suspicious = True
        if suspicious:
            logger.warning(
                "gallery-dl stderr (Cookie/API 异常时可对照): %s",
                stderr_hint[:1200],
            )

    out = parse_gallery_dl_dump_stdout_text(res.stdout or "")
    if out:
        return out

    out = []

    cmd_g = _gallery_dl_argv_prefix(gallery_dl_bin, cookies_path, cookies_from_browser, proxy_url)
    for a in extra_args or []:
        s = str(a).strip()
        if s:
            cmd_g.append(s)
    cmd_g.extend(["-g", seed])
    res_g = subprocess.run(cmd_g, capture_output=True, text=True)
    if res_g.returncode != 0:
        return out
    for line in (res_g.stdout or "").splitlines():
        u = line.strip()
        if u.startswith("http"):
            out.append({"url": u})
    return out
