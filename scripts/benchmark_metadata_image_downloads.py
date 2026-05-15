#!/usr/bin/env python3
"""
从 extract_metadata JSONL 读取 image_url，并发下载到 output/media，并输出测速/并发扫描数据。

依据本机 OS：CPU 核数、RLIMIT_NOFILE 软限制，给出建议的最大并发扫描上限（可用 CLI 覆盖）。

用法示例：
  python3 scripts/benchmark_metadata_image_downloads.py \\
    --jsonl output/metadata/extract_metadata_1.jsonl \\
    --media-dir output/media --limit 80 --sweep

  # 多档位 × 每档重复取平均（示例：7 档 × 每档 3 次）
  python3 scripts/benchmark_metadata_image_downloads.py \\
    --limit 256 --benchmark-series 16,32,64,128 --repeat 3 \\
    --report output/metadata/series.json
"""
from __future__ import annotations

import argparse
import concurrent.futures as futures
import json
import os
import random
import resource
import statistics
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    requests = None  # type: ignore[assignment]

try:
    import yaml
except ImportError:
    yaml = None  # type: ignore[assignment]


@dataclass
class HostLimits:
    cpu_count: int
    nofile_soft: int
    nofile_hard: int
    suggested_max_concurrency: int
    rationale: str


def detect_host_limits() -> HostLimits:
    cpu = os.cpu_count() or 1
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # 每个连接约占 1 FD；为解释器/其它句柄留余量
    by_fd = max(8, soft // 8)
    # I/O 型任务：按核数放大，但避免对 CDN 过激
    by_cpu = max(8, cpu * 24)
    # 全局硬顶（可被 --max-concurrency 提高）
    cap = 512
    suggested = min(by_fd, by_cpu, cap)
    rationale = (
        f"min(nofile_soft//8={by_fd}, cpu*24={by_cpu}, cap={cap}) "
        f"from cpu={cpu}, nofile_soft={soft}"
    )
    return HostLimits(
        cpu_count=cpu,
        nofile_soft=soft,
        nofile_hard=hard,
        suggested_max_concurrency=max(4, suggested),
        rationale=rationale,
    )


def concurrency_ladder(max_c: int) -> List[int]:
    """单调递增的并发档位：1,2,4,... 直至 max_c。"""
    out: List[int] = []
    c = 1
    while c < max_c:
        out.append(c)
        c = min(c * 2, max_c)
    if not out or out[-1] != max_c:
        out.append(max_c)
    return sorted(set(out))


@dataclass
class OneResult:
    ok: bool
    bytes_written: int
    seconds: float
    http_status: Optional[int] = None
    error: Optional[str] = None
    image_id: Optional[int] = None
    image_url: Optional[str] = None


def _default_headers(
    referer: Optional[str],
    extra: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    h: Dict[str, str] = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if referer:
        h["Referer"] = referer
    if extra:
        h.update(extra)
    return h


def _proxy_to_requests_proxies(proxy: Dict[str, Any]) -> Dict[str, str]:
    """与 scripts/prepare_and_test_proxies.py 一致：http(s)://user:pass@host:port"""
    server = str(proxy["server"]).rstrip("/")
    if proxy.get("username") and proxy.get("password"):
        u, pw = proxy["username"], proxy["password"]
        if "://" in server:
            scheme, rest = server.split("://", 1)
        else:
            scheme, rest = "http", server.replace("http://", "")
        url = f"{scheme}://{u}:{pw}@{rest}"
    else:
        url = server if "://" in server else f"http://{server}"
    return {"http": url, "https": url}


def load_proxies_from_yaml(path: Path) -> List[Dict[str, Any]]:
    """解析 proxies.yaml；按 host 去重（与 prepare_and_test_proxies 一致）。"""
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
        raw_line = f"{host}:{port_i}:{username}:{password}"
        out.append(
            {
                "server": server,
                "username": username,
                "password": password,
                "raw": raw_line,
            }
        )
    return out


def download_one(
    url: str,
    dest: Path,
    timeout: float,
    image_id: Optional[int],
    extra_headers: Optional[Dict[str, str]],
    requests_proxies: Optional[Dict[str, str]],
) -> OneResult:
    if requests_proxies is not None:
        return _download_one_requests(
            url, dest, timeout, image_id, extra_headers, requests_proxies
        )
    return _download_one_urllib(url, dest, timeout, image_id, extra_headers)


def _download_one_urllib(
    url: str,
    dest: Path,
    timeout: float,
    image_id: Optional[int],
    extra_headers: Optional[Dict[str, str]],
) -> OneResult:
    ref = f"https://500px.com/photo/{image_id}" if image_id else "https://500px.com/"
    hdrs = _default_headers(referer=ref, extra=extra_headers)
    t0 = time.perf_counter()
    req = urllib.request.Request(url, headers=hdrs, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
            body = resp.read()
    except urllib.error.HTTPError as e:
        dt = time.perf_counter() - t0
        return OneResult(
            ok=False,
            bytes_written=0,
            seconds=dt,
            http_status=e.code,
            error=f"HTTPError: {e.code}",
            image_id=image_id,
            image_url=url,
        )
    except Exception as e:  # noqa: BLE001 — 汇总各类网络错误
        dt = time.perf_counter() - t0
        return OneResult(
            ok=False,
            bytes_written=0,
            seconds=dt,
            error=f"{type(e).__name__}: {e}",
            image_id=image_id,
            image_url=url,
        )

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(body)
    dt = time.perf_counter() - t0
    return OneResult(
        ok=True,
        bytes_written=len(body),
        seconds=dt,
        http_status=status if isinstance(status, int) else None,
        image_id=image_id,
        image_url=url,
    )


def _download_one_requests(
    url: str,
    dest: Path,
    timeout: float,
    image_id: Optional[int],
    extra_headers: Optional[Dict[str, str]],
    proxies: Dict[str, str],
) -> OneResult:
    if requests is None:
        return OneResult(
            ok=False,
            bytes_written=0,
            seconds=0.0,
            error="ImportError: requests 未安装",
            image_id=image_id,
            image_url=url,
        )
    ref = f"https://500px.com/photo/{image_id}" if image_id else "https://500px.com/"
    hdrs = _default_headers(referer=ref, extra=extra_headers)
    t0 = time.perf_counter()
    try:
        r = requests.get(url, headers=hdrs, proxies=proxies, timeout=timeout)
        status = r.status_code
        if status != 200:
            dt = time.perf_counter() - t0
            return OneResult(
                ok=False,
                bytes_written=0,
                seconds=dt,
                http_status=status,
                error=f"HTTPError: {status}",
                image_id=image_id,
                image_url=url,
            )
        body = r.content
    except requests.exceptions.RequestException as e:
        dt = time.perf_counter() - t0
        return OneResult(
            ok=False,
            bytes_written=0,
            seconds=dt,
            error=f"{type(e).__name__}: {e}",
            image_id=image_id,
            image_url=url,
        )

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(body)
    dt = time.perf_counter() - t0
    return OneResult(
        ok=True,
        bytes_written=len(body),
        seconds=dt,
        http_status=status,
        image_id=image_id,
        image_url=url,
    )


@dataclass
class BatchStats:
    concurrency: int
    wall_seconds: float
    total_bytes: int
    success: int
    failed: int
    aggregate_mbps: float
    per_file_mbps_median: Optional[float]
    per_file_mbps_p95: Optional[float]
    seconds_median: Optional[float]
    seconds_p95: Optional[float]
    errors_sample: List[str] = field(default_factory=list)
    failed_items: List[Dict[str, Any]] = field(default_factory=list)


def run_batch(
    items: List[Tuple[str, Path, Optional[int], Optional[Dict[str, str]]]],
    concurrency: int,
    timeout: float,
    extra_headers: Optional[Dict[str, str]],
    max_errors_sample: int = 8,
    emit_failure_log: bool = True,
) -> BatchStats:
    t_wall0 = time.perf_counter()
    results: List[OneResult] = []
    with futures.ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        futs = [
            ex.submit(download_one, url, path, timeout, iid, extra_headers, rpx)
            for url, path, iid, rpx in items
        ]
        for fu in futures.as_completed(futs):
            results.append(fu.result())

    wall = time.perf_counter() - t_wall0
    ok = [r for r in results if r.ok]
    bad = [r for r in results if not r.ok]
    total_b = sum(r.bytes_written for r in ok)
    agg_mbps = (total_b * 8 / 1e6) / wall if wall > 0 else 0.0

    mbps_list: List[float] = []
    sec_list: List[float] = []
    for r in ok:
        if r.seconds > 0 and r.bytes_written > 0:
            mbps_list.append((r.bytes_written * 8 / 1e6) / r.seconds)
        sec_list.append(r.seconds)

    def p95(xs: List[float]) -> Optional[float]:
        if not xs:
            return None
        xs = sorted(xs)
        idx = int(round(0.95 * (len(xs) - 1)))
        return xs[idx]

    err_sample: List[str] = []
    for r in bad:
        if len(err_sample) >= max_errors_sample:
            break
        msg = r.error or "unknown"
        err_sample.append(msg)

    failed_items: List[Dict[str, Any]] = []
    for r in bad:
        failed_items.append(
            {
                "image_url": r.image_url,
                "image_id": r.image_id,
                "http_status": r.http_status,
                "error": r.error,
            }
        )

    if bad and emit_failure_log:
        print(f"--- 失败 {len(bad)} 条 image_url ---", flush=True)
        for r in bad:
            u = r.image_url or "(unknown)"
            print(f"  {u}", flush=True)
            print(f"    id={r.image_id}  {r.error}", flush=True)

    return BatchStats(
        concurrency=concurrency,
        wall_seconds=wall,
        total_bytes=total_b,
        success=len(ok),
        failed=len(bad),
        aggregate_mbps=agg_mbps,
        per_file_mbps_median=statistics.median(mbps_list) if mbps_list else None,
        per_file_mbps_p95=p95(mbps_list) if mbps_list else None,
        seconds_median=statistics.median(sec_list) if sec_list else None,
        seconds_p95=p95(sec_list) if sec_list else None,
        errors_sample=err_sample,
        failed_items=failed_items,
    )


def load_items(
    jsonl_path: Path,
    limit: int,
    shuffle: bool,
    seed: int,
) -> List[Tuple[str, Path, Optional[int]]]:
    rows: List[Dict[str, Any]] = []
    with jsonl_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
            if len(rows) >= limit:
                break
    if shuffle:
        rng = random.Random(seed)
        rng.shuffle(rows)

    items: List[Tuple[str, Path, Optional[int]]] = []
    for obj in rows:
        url = obj.get("image_url")
        if not url:
            continue
        key = obj.get("image_key") or ""
        name = Path(str(key)).name
        if not name or name == ".":
            # 退化：用 id
            iid = obj.get("id")
            name = f"{iid}.bin" if iid is not None else f"row_{len(items)}.bin"
        out = Path(name)
        iid = obj.get("id")
        if isinstance(iid, int):
            pass
        else:
            iid = None
        items.append((str(url), out, iid))
    return items


def pick_plateau_concurrency(batches: List[BatchStats], rel_margin: float = 0.05) -> Optional[int]:
    """在成功率 100% 的档位里，取「聚合 Mbps 已达峰值*(1-margin)」的最小并发，作为经验饱和点。"""
    ok_batches = [b for b in batches if b.failed == 0 and b.wall_seconds > 0]
    if not ok_batches:
        return None
    peak = max(b.aggregate_mbps for b in ok_batches)
    if peak <= 0:
        return None
    threshold = peak * (1.0 - rel_margin)
    candidates = [b for b in ok_batches if b.aggregate_mbps >= threshold]
    return min(b.concurrency for b in candidates) if candidates else None


def main() -> int:
    repo = Path(__file__).resolve().parent.parent
    limits = detect_host_limits()

    p = argparse.ArgumentParser(description="JSONL image_url 并发下载测速（按本机 OS 建议默认上限）")
    p.add_argument(
        "--jsonl",
        type=Path,
        default=repo / "output/metadata/extract_metadata_1.jsonl",
        help="metadata JSONL 路径",
    )
    p.add_argument(
        "--media-dir",
        type=Path,
        default=repo / "output/media",
        help="图片保存目录（绝对或相对 cwd；默认仓库 output/media）",
    )
    p.add_argument("--limit", type=int, default=64, help="最多读取前 N 条有效 URL（默认 64）")
    p.add_argument("--timeout", type=float, default=120.0, help="单请求超时秒数")
    p.add_argument(
        "--max-concurrency",
        type=int,
        default=None,
        help=f"扫描上限（默认 {limits.suggested_max_concurrency}，来自本机检测）",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help="只跑单一并发（与 --sweep 互斥；不指定且未 --sweep 时等同 --sweep）",
    )
    p.add_argument(
        "--sweep",
        action="store_true",
        help="按 1,2,4,... 扫描多档并发并输出对比（默认开启，除非指定了 --concurrency）",
    )
    p.add_argument("--no-sweep", action="store_true", help="等价于必须配合 --concurrency 单档")
    p.add_argument("--shuffle", action="store_true", help="抽样前打乱 JSONL 前 limit 条")
    p.add_argument("--seed", type=int, default=42, help="--shuffle 随机种子")
    p.add_argument(
        "--report",
        type=Path,
        default=None,
        help="测速 JSON 报告输出路径（默认 output/metadata/download_benchmark_<ts>.json）",
    )
    p.add_argument(
        "--cookie",
        default=None,
        help="可选：浏览器复制的 Cookie 请求头（部分网络/CDN 无 Cookie 会 403）",
    )
    p.add_argument(
        "--header",
        action="append",
        default=[],
        metavar="KEY: VALUE",
        help="附加 HTTP 头，可重复。例：--header 'Authorization: Bearer xxx'",
    )
    p.add_argument(
        "--proxies-yaml",
        type=Path,
        default=repo / "config/proxies.yaml",
        help="代理列表 YAML（默认仓库 config/proxies.yaml）；与 --no-proxy 互斥",
    )
    p.add_argument(
        "--no-proxy",
        action="store_true",
        help="不使用代理（直连，urllib）",
    )
    p.add_argument(
        "--benchmark-series",
        metavar="C1,C2,...",
        default=None,
        help="指定并发档位顺序测试（与 --sweep/--concurrency 互斥）；例 16,32,64,128",
    )
    p.add_argument(
        "--repeat",
        type=int,
        default=3,
        help="--benchmark-series 下每档并发重复次数，取平均（默认 3）",
    )
    args = p.parse_args()

    max_c = args.max_concurrency if args.max_concurrency is not None else limits.suggested_max_concurrency
    if max_c < 1:
        print("error: --max-concurrency must be >= 1", file=sys.stderr)
        return 2

    jsonl_path = args.jsonl if args.jsonl.is_absolute() else (Path.cwd() / args.jsonl)
    media_dir = args.media_dir if args.media_dir.is_absolute() else (Path.cwd() / args.media_dir)
    media_dir = media_dir.resolve()

    if not jsonl_path.is_file():
        print(f"error: jsonl not found: {jsonl_path}", file=sys.stderr)
        return 2

    series_levels: Optional[List[int]] = None
    if args.benchmark_series:
        raw_parts = [x.strip() for x in args.benchmark_series.split(",") if x.strip()]
        try:
            series_levels = [int(x) for x in raw_parts]
        except ValueError:
            print("error: --benchmark-series 须为逗号分隔整数", file=sys.stderr)
            return 2
        if any(c < 1 for c in series_levels):
            print("error: --benchmark-series 中并发须 >= 1", file=sys.stderr)
            return 2
        if args.repeat < 1:
            print("error: --repeat must be >= 1", file=sys.stderr)
            return 2
        if args.concurrency is not None or args.sweep or args.no_sweep:
            print(
                "error: --benchmark-series 与 --concurrency/--sweep/--no-sweep 不能同时使用",
                file=sys.stderr,
            )
            return 2

    use_sweep = (not series_levels) and (
        args.sweep or (args.concurrency is None and not args.no_sweep)
    )
    if (not series_levels) and args.no_sweep and args.concurrency is None:
        print("error: --no-sweep requires --concurrency", file=sys.stderr)
        return 2

    items_raw = load_items(jsonl_path, args.limit, args.shuffle, args.seed)
    if not items_raw:
        print("error: no items with image_url", file=sys.stderr)
        return 2

    extra_headers: Dict[str, str] = {}
    if args.cookie:
        extra_headers["Cookie"] = args.cookie.strip()
    for raw in args.header:
        if ":" not in raw:
            print(f"error: bad --header (need 'Key: value'): {raw!r}", file=sys.stderr)
            return 2
        k, v = raw.split(":", 1)
        extra_headers[k.strip()] = v.strip()

    base_items: List[Tuple[str, Path, Optional[int]]] = [
        (url, media_dir / rel, iid) for url, rel, iid in items_raw
    ]

    proxies_yaml = (
        None
        if args.no_proxy
        else (
            args.proxies_yaml
            if args.proxies_yaml.is_absolute()
            else (Path.cwd() / args.proxies_yaml)
        ).resolve()
    )
    proxy_rows: List[Dict[str, Any]] = []
    if proxies_yaml is not None:
        if not proxies_yaml.is_file():
            print(f"error: proxies yaml not found: {proxies_yaml}", file=sys.stderr)
            return 2
        proxy_rows = load_proxies_from_yaml(proxies_yaml)
        if not proxy_rows:
            print(f"error: no proxies parsed from {proxies_yaml}", file=sys.stderr)
            return 2
        if requests is None:
            print("error: 使用代理需要 requests：pip install requests", file=sys.stderr)
            return 2

    items: List[Tuple[str, Path, Optional[int], Optional[Dict[str, str]]]]
    if proxy_rows:
        npx = len(proxy_rows)
        items = [
            (u, p, i, _proxy_to_requests_proxies(proxy_rows[idx % npx]))
            for idx, (u, p, i) in enumerate(base_items)
        ]
    else:
        items = [(u, p, i, None) for u, p, i in base_items]

    report: Dict[str, Any] = {
        "jsonl": str(jsonl_path),
        "media_dir": str(media_dir),
        "limit_requested": args.limit,
        "items": len(items),
        "timeout_seconds": args.timeout,
        "host_limits": asdict(limits),
        "shuffle": bool(args.shuffle),
        "seed": args.seed,
        "extra_header_keys": sorted(extra_headers.keys()),
        "use_proxy": bool(proxy_rows),
        "proxies_yaml": str(proxies_yaml) if proxies_yaml else None,
        "proxy_count": len(proxy_rows),
        "batches": [],
    }

    if series_levels:
        mx = max(series_levels)
        if len(items) < mx:
            print(
                f"提示: 最大档位并发 {mx} 大于任务数 {len(items)}，"
                f"高档位实际并行 capped；建议 --limit >= {mx}。",
                flush=True,
            )
        report["mode"] = "benchmark_series"
        report["benchmark_series"] = series_levels
        report["repeat_per_level"] = args.repeat
        summaries: List[Dict[str, Any]] = []
        for c in series_levels:
            eff = min(c, len(items))
            run_dicts: List[Dict[str, Any]] = []
            for rep in range(args.repeat):
                print(
                    f"[series] concurrency={c}  (n={len(items)}, 实际并行≤{eff}) "
                    f"第 {rep + 1}/{args.repeat} 次",
                    flush=True,
                )
                b = run_batch(
                    items,
                    c,
                    args.timeout,
                    extra_headers or None,
                    emit_failure_log=(rep == 0),
                )
                d = {**asdict(b), "effective_parallelism": eff}
                run_dicts.append(d)
                print(
                    f"  wall={b.wall_seconds:.2f}s  ok={b.success} fail={b.failed}  "
                    f"aggregate={b.aggregate_mbps:.2f} Mbit/s",
                    flush=True,
                )
            aggs = [float(x["aggregate_mbps"]) for x in run_dicts]
            walls = [float(x["wall_seconds"]) for x in run_dicts]
            succ = [int(x["success"]) for x in run_dicts]
            fail = [int(x["failed"]) for x in run_dicts]
            row: Dict[str, Any] = {
                "concurrency": c,
                "effective_parallelism": eff,
                "repeat": args.repeat,
                "avg_aggregate_mbps": statistics.mean(aggs),
                "stdev_aggregate_mbps": statistics.stdev(aggs) if len(aggs) > 1 else 0.0,
                "avg_wall_seconds": statistics.mean(walls),
                "stdev_wall_seconds": statistics.stdev(walls) if len(walls) > 1 else 0.0,
                "avg_success": statistics.mean(succ),
                "avg_failed": statistics.mean(fail),
                "runs": run_dicts,
            }
            summaries.append(row)
        report["series_summaries"] = summaries
        report["batches"] = summaries  # 便于旧阅读器：每元素为带 runs 的汇总

        print("\n=== 各并发档位（每档 {} 次平均）===".format(args.repeat), flush=True)
        print(
            f"{'并发':>6} {'实际≤':>8} {'平均Mbps':>12} {'σMbps':>10} "
            f"{'平均墙钟s':>12} {'σs':>8} {'平均ok':>8} {'平均fail':>10}",
            flush=True,
        )
        for row in summaries:
            print(
                f"{row['concurrency']:>6} {row['effective_parallelism']:>8} "
                f"{row['avg_aggregate_mbps']:>12.2f} {row['stdev_aggregate_mbps']:>10.2f} "
                f"{row['avg_wall_seconds']:>12.2f} {row['stdev_wall_seconds']:>8.2f} "
                f"{row['avg_success']:>8.1f} {row['avg_failed']:>10.1f}",
                flush=True,
            )
        peak = max(summaries, key=lambda x: x["avg_aggregate_mbps"])
        report["best_avg_aggregate_mbps"] = {
            "concurrency": peak["concurrency"],
            "avg_aggregate_mbps": peak["avg_aggregate_mbps"],
        }
    elif use_sweep:
        levels = concurrency_ladder(max_c)
        report["concurrency_levels"] = levels
        if levels and max(levels) > len(items):
            print(
                f"提示: 最大并发档 {max(levels)} 大于任务数 {len(items)}，"
                f"实际并行度每档为 min(并发, {len(items)})。"
                f"要测更高并发请增大 --limit（建议 >= --max-concurrency）。",
                flush=True,
            )
        batches: List[BatchStats] = []
        for c in levels:
            eff = min(c, len(items))
            print(f"[sweep] concurrency={c}  (n={len(items)}, 实际并行≤{eff})", flush=True)
            b = run_batch(items, c, args.timeout, extra_headers or None)
            batches.append(b)
            report["batches"].append({**asdict(b), "effective_parallelism": eff})
            print(
                f"  wall={b.wall_seconds:.2f}s  ok={b.success} fail={b.failed}  "
                f"aggregate={b.aggregate_mbps:.2f} Mbit/s  "
                f"per_file_median={b.per_file_mbps_median}  p95={b.per_file_mbps_p95}",
                flush=True,
            )
        plateau = pick_plateau_concurrency(batches)
        report["estimated_saturation_concurrency"] = plateau
        best = max(batches, key=lambda x: x.aggregate_mbps)
        report["best_aggregate_mbps_batch"] = asdict(best)
    else:
        c = int(args.concurrency)  # type: ignore[arg-type]
        eff = min(c, len(items))
        print(f"[single] concurrency={c}  (n={len(items)}, 实际并行≤{eff})", flush=True)
        b = run_batch(items, c, args.timeout, extra_headers or None)
        report["batches"] = [{**asdict(b), "effective_parallelism": eff}]
        print(
            f"  wall={b.wall_seconds:.2f}s  ok={b.success} fail={b.failed}  "
            f"aggregate={b.aggregate_mbps:.2f} Mbit/s",
            flush=True,
        )

    ts = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    report_path = args.report
    if report_path is None:
        report_path = repo / "output/metadata" / f"download_benchmark_{ts}.json"
    else:
        report_path = report_path if report_path.is_absolute() else (Path.cwd() / report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"report written: {report_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
