#!/usr/bin/env python3
"""
从 extract metadata JSONL 抽样 image_url，下载到本地临时目录（测试用）。

- 不写入 output/media
- 不更新 seen.db（独立脚本，与 go-downloader 无关）
- 默认使用 config/proxies.yaml 轮询代理；--no-proxy 直连

500px CDN 说明：
  drscdn.500px.org 会拒绝数据中心 IP，且 JSONL 里带 sig 的 URL 会过期。
  浏览器能打开是因为走住宅网络 + 页面会刷新签名。
  本脚本在失败时会调用 api.500px.com 刷新 CDN URL，并换代理重试。

示例:
  python3 scripts/download_sample_images_from_jsonl.py
  python3 scripts/download_sample_images_from_jsonl.py -n 50 -j 8
  python3 scripts/download_sample_images_from_jsonl.py --no-proxy
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]

PHOTO_ID_RE = re.compile(r"500px\.org/photo/(\d+)")
CDN_SIZE_RE = re.compile(r"m%3D(\d+)")


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def parse_row(line: str) -> Optional[Dict[str, Any]]:
    line = line.strip()
    if not line:
        return None
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(obj, dict):
        return None
    url = obj.get("image_url")
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    if not url:
        return None
    image_id = obj.get("id")
    if image_id is not None:
        image_id = str(image_id).strip()
    image_key = obj.get("image_key")
    if image_key is not None:
        image_key = str(image_key).strip()
    return {"image_url": url, "id": image_id or None, "image_key": image_key or None}


def iter_rows_sequential(path: Path, limit: int) -> Iterator[Dict[str, Any]]:
    n = 0
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            row = parse_row(line)
            if row is None:
                continue
            yield row
            n += 1
            if n >= limit:
                return


def reservoir_sample_rows(path: Path, k: int, rng: random.Random) -> List[Dict[str, Any]]:
    reservoir: List[Dict[str, Any]] = []
    seen = 0
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            row = parse_row(line)
            if row is None:
                continue
            seen += 1
            if len(reservoir) < k:
                reservoir.append(row)
            else:
                j = rng.randint(1, seen)
                if j <= k:
                    reservoir[j - 1] = row
    return reservoir


def load_proxies_from_yaml(path: Path) -> List[Dict[str, Any]]:
    if yaml is None:
        raise RuntimeError("需要 PyYAML：pip install PyYAML")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = raw.get("proxies")
    if not isinstance(rows, list):
        return []

    out: List[Dict[str, Any]] = []
    seen_hosts: set[str] = set()
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
        if host in seen_hosts:
            continue
        seen_hosts.add(host)
        username = str(item.get("username") or "").strip()
        password = str(item.get("password") or "").strip()
        server = f"http://{host}:{port_i}"
        out.append(
            {
                "server": server,
                "username": username,
                "password": password,
            }
        )
    return out


def proxy_to_requests_proxies(proxy: Dict[str, Any]) -> Dict[str, str]:
    server = str(proxy["server"]).rstrip("/")
    username = str(proxy.get("username") or "").strip()
    password = str(proxy.get("password") or "").strip()
    if username and password:
        scheme, rest = server.split("://", 1) if "://" in server else ("http", server.replace("http://", ""))
        url = f"{scheme}://{username}:{password}@{rest}"
    else:
        url = server if "://" in server else f"http://{server}"
    return {"http": url, "https": url}


class ProxyPool:
    def __init__(self, proxies: List[Dict[str, Any]]) -> None:
        self._proxies = proxies
        self._lock = threading.Lock()
        self._idx = 0

    def next_requests_proxies(self) -> Dict[str, str]:
        with self._lock:
            p = self._proxies[self._idx % len(self._proxies)]
            self._idx += 1
        return proxy_to_requests_proxies(p)


def build_opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(
        urllib.request.ProxyHandler({}),
        urllib.request.HTTPHandler(),
        urllib.request.HTTPSHandler(),
    )


def _photo_id(url: str, image_id: Optional[str]) -> Optional[str]:
    if image_id:
        return str(image_id)
    m = PHOTO_ID_RE.search(url)
    return m.group(1) if m else None


def headers_for_photo(photo_id: Optional[str], *, for_image: bool = True) -> Dict[str, str]:
    referer = f"https://500px.com/photo/{photo_id}" if photo_id else "https://500px.com/"
    hdrs = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": referer,
        "Origin": "https://500px.com",
    }
    if for_image:
        hdrs.update(
            {
                "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
                "Sec-Fetch-Dest": "image",
                "Sec-Fetch-Mode": "no-cors",
                "Sec-Fetch-Site": "cross-site",
            }
        )
    else:
        hdrs["Accept"] = "application/json, text/plain, */*"
    return hdrs


def _cdn_max_edge(url: str) -> int:
    m = CDN_SIZE_RE.search(url)
    return int(m.group(1)) if m else 0


def _pick_best_photo_cdn_url(photo: Dict[str, Any]) -> Optional[str]:
    candidates: List[Tuple[int, int, str]] = []
    for img in photo.get("images") or []:
        if not isinstance(img, dict):
            continue
        u = img.get("url") or img.get("https_url")
        if not isinstance(u, str) or "/photo/" not in u:
            continue
        size_rank = int(img.get("size") or 0)
        candidates.append((_cdn_max_edge(u), size_rank, u.strip()))
    for u in photo.get("image_url") or []:
        if isinstance(u, str) and "/photo/" in u:
            candidates.append((_cdn_max_edge(u), 999, u.strip()))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
    return candidates[0][2]


def fetch_fresh_cdn_url(photo_id: str, timeout: float) -> Optional[str]:
    """经 api.500px.com 获取新签名 CDN URL（数据中心 IP 通常可访问 API）。"""
    api = f"https://api.500px.com/v1/photos/{photo_id}?image_size=2048"
    hdrs = headers_for_photo(photo_id, for_image=False)
    try:
        if requests is not None:
            resp = requests.get(api, headers=hdrs, timeout=timeout)
            if resp.status_code != 200:
                return None
            photo = resp.json().get("photo")
        else:
            req = urllib.request.Request(api, headers=hdrs, method="GET")
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            photo = payload.get("photo")
        if not isinstance(photo, dict):
            return None
        return _pick_best_photo_cdn_url(photo)
    except Exception:
        return None


def urls_for_row(row: Dict[str, Any], *, prefetch_api: bool, timeout: float) -> List[str]:
    stored = row["image_url"]
    out: List[str] = [stored]
    pid = row.get("id")
    if prefetch_api and pid:
        fresh = fetch_fresh_cdn_url(str(pid), timeout)
        if fresh and fresh not in out:
            out.append(fresh)
    out.sort(key=_cdn_max_edge, reverse=True)
    # 去重保序
    seen: set[str] = set()
    deduped: List[str] = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        deduped.append(u)
    return deduped


def dest_name(row: Dict[str, Any], seq: int) -> str:
    key = row.get("image_key")
    if key:
        name = Path(str(key)).name
        if name:
            return name
    url = row["image_url"]
    path = urlparse(url).path
    name = Path(path).name
    if name and name != "/":
        return name
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()
    return f"{digest}_{seq}.jpg"


def _fetch_bytes(
    url: str,
    hdrs: Dict[str, str],
    timeout: float,
    opener: Optional[urllib.request.OpenerDirector],
    proxies: Optional[Dict[str, str]],
) -> bytes:
    if proxies is not None:
        if requests is None:
            raise RuntimeError("需要 requests：pip install requests")
        resp = requests.get(url, headers=hdrs, proxies=proxies, timeout=timeout)
        if resp.status_code != 200:
            raise urllib.error.HTTPError(url, resp.status_code, resp.reason, resp.headers, None)
        return resp.content
    req = urllib.request.Request(url, headers=hdrs, method="GET")
    with opener.open(req, timeout=timeout) as resp:  # type: ignore[union-attr]
        return resp.read()


def _is_rate_limited(err: str) -> bool:
    return "429" in err


def download_one(
    opener: Optional[urllib.request.OpenerDirector],
    proxy_pool: Optional[ProxyPool],
    seq: int,
    row: Dict[str, Any],
    out_dir: Path,
    timeout: float,
    retries: int,
    prefetch_api: bool,
) -> Tuple[bool, str, str]:
    photo_id = _photo_id(row["image_url"], row.get("id"))
    hdrs = headers_for_photo(photo_id, for_image=True)
    name = dest_name(row, seq)
    dest = out_dir / name
    if dest.exists():
        stem = dest.stem
        suf = dest.suffix or ".bin"
        dest = out_dir / f"{stem}_{seq}{suf}"

    urls = urls_for_row(row, prefetch_api=prefetch_api, timeout=timeout)
    attempts = max(1, retries + 1)
    last_err = "unknown"
    refreshed = prefetch_api

    for attempt in range(attempts):
        if not refreshed and photo_id and attempt > 0:
            fresh = fetch_fresh_cdn_url(str(photo_id), timeout)
            if fresh and fresh not in urls:
                urls.append(fresh)
                urls.sort(key=_cdn_max_edge, reverse=True)
            refreshed = True

        proxies = proxy_pool.next_requests_proxies() if proxy_pool is not None else None
        for url in urls:
            try:
                data = _fetch_bytes(url, hdrs, timeout, opener, proxies)
                if len(data) < 512:
                    last_err = f"body too small ({len(data)} bytes)"
                    continue
                dest.write_bytes(data)
                return True, url, str(dest)
            except urllib.error.HTTPError as e:
                last_err = f"HTTP {e.code}: {e.reason}"
            except urllib.error.URLError as e:
                last_err = f"URL {e.reason!r}"
            except TimeoutError:
                last_err = "timeout"
            except OSError as e:
                last_err = str(e)
            except Exception as e:
                if requests is not None and isinstance(e, requests.exceptions.RequestException):
                    last_err = f"{type(e).__name__}: {e}"
                else:
                    last_err = repr(e)

        if _is_rate_limited(last_err):
            time.sleep(min(2.0, 0.25 * (attempt + 1)))

    return False, row["image_url"], last_err


def resolve_path(root: Path, p: Path) -> Path:
    p = p.expanduser()
    return p.resolve() if p.is_absolute() else (root / p).resolve()


def main() -> int:
    root = _repo_root()
    p = argparse.ArgumentParser(
        description="从 extract metadata JSONL 抽样下载图片到临时目录（测试用，不写 seen.db）。"
    )
    p.add_argument(
        "-i",
        "--input",
        type=Path,
        default=Path("output/metadata/extract_metadata_1.jsonl"),
        help="输入 JSONL（默认 output/metadata/extract_metadata_1.jsonl）",
    )
    p.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("output/tmp"),
        help="图片输出目录（默认 output/tmp，非 output/media）",
    )
    p.add_argument("-n", "--count", type=int, default=100, help="下载条数（默认 100）")
    p.add_argument("-j", "--workers", type=int, default=8, help="并发下载线程数")
    p.add_argument("--retries", type=int, default=8, help="失败后换代理重试次数（默认 8，共 9 次尝试）")
    p.add_argument("--timeout", type=float, default=90.0, help="单张下载超时（秒）")
    p.add_argument(
        "--random",
        action="store_true",
        help="随机蓄水池抽样；默认从文件开头顺序取前 N 条有效 image_url",
    )
    p.add_argument("--seed", type=int, default=42, help="--random 时的随机种子")
    p.add_argument(
        "--proxies-yaml",
        type=Path,
        default=Path("config/proxies.yaml"),
        help="代理列表 YAML（默认 config/proxies.yaml）",
    )
    p.add_argument(
        "--no-proxy",
        action="store_true",
        help="不使用代理，直连（500px CDN 在数据中心 IP 上通常会 403）",
    )
    p.add_argument(
        "--no-prefetch-api",
        action="store_true",
        help="不在下载前调用 api.500px.com 刷新 CDN URL（失败时仍会尝试刷新）",
    )
    args = p.parse_args()

    if args.count < 1:
        print("错误: -n 至少为 1", file=sys.stderr)
        return 2

    inp = resolve_path(root, args.input)
    if not inp.is_file():
        print(f"错误: 找不到输入文件: {inp}", file=sys.stderr)
        return 1

    out_dir = resolve_path(root, args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.perf_counter()
    if args.random:
        print(f">>> 随机抽样 {args.count} 行: {inp}", file=sys.stderr)
        rows = reservoir_sample_rows(inp, args.count, random.Random(args.seed))
    else:
        print(f">>> 顺序读取前 {args.count} 条有效 image_url: {inp}", file=sys.stderr)
        rows = list(iter_rows_sequential(inp, args.count))

    if len(rows) < args.count:
        print(f"警告: 仅得到 {len(rows)} 条有效 URL（请求 {args.count}）。", file=sys.stderr)
    if not rows:
        print("错误: 没有可用的 image_url。", file=sys.stderr)
        return 1

    proxy_pool: Optional[ProxyPool] = None
    opener: Optional[urllib.request.OpenerDirector] = None
    if args.no_proxy:
        opener = build_opener()
        mode = "无代理"
    else:
        proxy_path = resolve_path(root, args.proxies_yaml)
        if not proxy_path.is_file():
            print(f"错误: 找不到代理配置: {proxy_path}", file=sys.stderr)
            return 1
        proxy_rows = load_proxies_from_yaml(proxy_path)
        if not proxy_rows:
            print(f"错误: 代理列表为空: {proxy_path}", file=sys.stderr)
            return 1
        proxy_pool = ProxyPool(proxy_rows)
        mode = f"代理轮询（{len(proxy_rows)} 个）"

    prefetch = not args.no_prefetch_api
    print(
        f">>> 待下载 {len(rows)} 张 -> {out_dir}（{mode}，"
        f"{'预刷新 CDN URL' if prefetch else '失败时刷新 CDN URL'}，不写入 seen.db）",
        file=sys.stderr,
    )

    ok = 0
    fail = 0
    t1 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {
            ex.submit(
                download_one,
                opener,
                proxy_pool,
                seq,
                row,
                out_dir,
                args.timeout,
                args.retries,
                prefetch,
            ): (seq, row)
            for seq, row in enumerate(rows)
        }
        for fut in as_completed(futs):
            seq, row = futs[fut]
            url = row["image_url"]
            try:
                good, u, msg = fut.result()
            except Exception as e:
                good, u, msg = False, url, repr(e)
            if good:
                ok += 1
                if ok <= 3 or ok % 20 == 0:
                    print(f"OK {ok}/{len(rows)} -> {msg}", file=sys.stderr)
            else:
                fail += 1
                if fail <= 10:
                    print(f"FAIL {u} :: {msg}", file=sys.stderr)

    dt = time.perf_counter() - t1
    print(
        f">>> 完成: 成功 {ok} 失败 {fail}，读取耗时 {t1 - t0:.1f}s，下载耗时 {dt:.1f}s",
        file=sys.stderr,
    )
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
