#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 config/proxies.yaml 读取代理列表并测试可用性（不修改该 YAML）。

格式（与仓库内 config/proxies.yaml 一致）：
  proxies:
    - host: x.x.x.x
      port: 1234
      username: ...
      password: ...

逻辑：
- 使用 requests 请求 test_url（默认 https://ipv4.webshare.io/）检测可用性与延迟，支持并行测试；
- 仅打印结果，不写回 proxies.yaml。

用法（在仓库根）：
  python3 scripts/prepare_and_test_proxies.py
  python3 scripts/prepare_and_test_proxies.py --proxies-yaml config/proxies.yaml -n 50
  python3 scripts/prepare_and_test_proxies.py --test-url https://httpbin.org/ip --timeout 15
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PROXIES_YAML = PROJECT_ROOT / "config" / "proxies.yaml"
DEFAULT_TEST_URL = "https://ipv4.webshare.io/"
DEFAULT_TIMEOUT = 10

try:
    import requests
except ImportError:
    print("请先安装: pip install requests", file=sys.stderr)
    raise SystemExit(1)

try:
    import yaml
except ImportError:
    print("请先安装: pip install PyYAML", file=sys.stderr)
    raise SystemExit(1)


def load_proxies_from_yaml(path: Path) -> list[dict[str, Any]]:
    """解析 proxies.yaml，返回供 test_proxy 使用的 dict 列表（按 host 去重）。"""
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = raw.get("proxies")
    if not isinstance(rows, list):
        return []

    out: list[dict[str, Any]] = []
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


def _proxy_to_requests_proxies(proxy: dict[str, Any]) -> dict[str, str]:
    """转为 requests 的 proxies：http://user:pass@host:port"""
    server = proxy["server"].rstrip("/")
    if proxy.get("username") and proxy.get("password"):
        u, p = proxy["username"], proxy["password"]
        scheme, rest = server.split("://", 1) if "://" in server else ("http", server.replace("http://", ""))
        url = f"{scheme}://{u}:{p}@{rest}"
    else:
        url = server if "://" in server else f"http://{server}"
    return {"http": url, "https": url}


def test_proxy(
    proxy: dict[str, Any],
    test_url: str = DEFAULT_TEST_URL,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """请求 test_url 测可用性与延迟。"""
    result: dict[str, Any] = {
        "success": False,
        "latency_ms": 0.0,
        "error": "",
        "ip": "",
        "proxy": proxy.get("raw") or proxy.get("server", ""),
    }
    proxies = _proxy_to_requests_proxies(proxy)
    start = time.perf_counter()
    try:
        r = requests.get(test_url, proxies=proxies, timeout=timeout)
        elapsed_ms = (time.perf_counter() - start) * 1000
        result["success"] = r.status_code == 200
        result["latency_ms"] = round(elapsed_ms, 2)
        if result["success"]:
            txt = (r.text or "").strip()
            result["ip"] = txt[:64] if txt else "OK"
    except Exception as e:
        result["error"] = repr(e)
    return result


def _short_error_reason(err: str) -> str:
    """将较长的错误 repr 映射成简短原因（用于一行内展示）。"""
    if not err:
        return "unknown error"
    e = err
    if "407 Proxy Authentication Required" in e:
        return "407 Proxy Authentication Required"
    if "Connection timed out" in e or "ReadTimeoutError" in e:
        return "connection timeout"
    if "Connection refused" in e:
        return "connection refused"
    if "Temporary failure in name resolution" in e or "Name or service not known" in e:
        return "DNS resolve failed"
    return e if len(e) <= 80 else e[:77] + "..."


def main() -> int:
    ap = argparse.ArgumentParser(description="从 config/proxies.yaml 测试代理可用性（不修改 YAML）")
    ap.add_argument(
        "--proxies-yaml",
        type=Path,
        default=DEFAULT_PROXIES_YAML,
        help=f"代理列表 YAML（默认: {DEFAULT_PROXIES_YAML}）",
    )
    ap.add_argument("-n", "--count", type=int, default=0, metavar="N", help="测试前 N 个代理，0 表示全部")
    ap.add_argument("--test-url", type=str, default=DEFAULT_TEST_URL, help="检测用的 URL")
    ap.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, metavar="SEC", help="单次请求超时(秒)")
    args = ap.parse_args()

    yaml_path = args.proxies_yaml
    if not yaml_path.is_absolute():
        yaml_path = (PROJECT_ROOT / yaml_path).resolve()
    if not yaml_path.is_file():
        print(f"代理 YAML 不存在: {yaml_path}", file=sys.stderr)
        return 1

    try:
        proxies = load_proxies_from_yaml(yaml_path)
    except Exception as e:
        print(f"读取 YAML 失败: {e}", file=sys.stderr)
        return 1

    if not proxies:
        print(f"未从 {yaml_path} 解析到任何代理（需要顶层 proxies: 列表）", file=sys.stderr)
        return 1

    print(f"[load] {yaml_path} -> {len(proxies)} 条代理（按 host 去重）")

    if args.count > 0:
        proxies = proxies[: args.count]

    results: dict[str, dict[str, Any]] = {}
    lock = threading.Lock()

    def run_one(proxy: dict[str, Any], idx: int) -> None:
        r = test_proxy(proxy, test_url=args.test_url, timeout=args.timeout)
        with lock:
            results[proxy["raw"]] = r
            status = "✓" if r["success"] else "✗"
            if r["success"]:
                lat = f"{r['latency_ms']}ms"
                print(f"[{idx + 1}/{len(proxies)}] {status} {proxy['raw']} - {lat}")
            else:
                reason = _short_error_reason(r.get("error", ""))
                print(f"[{idx + 1}/{len(proxies)}] {status} {proxy['raw']} ({reason})")

    threads: list[threading.Thread] = []
    for i, p in enumerate(proxies):
        t = threading.Thread(target=run_one, args=(p, i))
        t.start()
        threads.append(t)
        time.sleep(0.3)
    for t in threads:
        t.join()

    ok_latencies = [results[raw]["latency_ms"] for raw in results if results[raw]["success"]]
    total_ok = len(ok_latencies)
    total = len(proxies)
    print(f"总计: {total_ok}/{total} 可用")
    if ok_latencies:
        avg = sum(ok_latencies) / len(ok_latencies)
        print(f"平均延迟: {avg:.2f}ms")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
