#!/usr/bin/env python3
"""抓取与 https://500px.com/photographers/all 对应的用户列表（GraphQL userSearch）。

站点该页为 SPA，列表由 api.500px.com/graphql 的 ``userSearch`` 连接提供（Relay 风格分页）。
``featuredPhotographers`` 仅有约 250 条，不是「全部摄影师」目录。

用法示例：

  # 匿名即可（若遇 429 可传已登录 Cookie）
  python3 scripts/fetch_500px_photographers_all.py \\
    --output output/500px_photographers_all_usernames.txt \\
    --page-size 200 --sleep 0.5

  python3 scripts/fetch_500px_photographers_all.py \\
    --cookies config/500px_cookies.txt \\
    --sort MOST_POPULAR \\
    --max-users 100000

  python3 scripts/fetch_500px_photographers_all.py \\
    --proxies-yaml config/proxies.yaml \\
    --proxy-rotate-each-page

  # 按「专业类别」分桶（value 为站点内部数字 ID，不是英文文案）：
  python3 scripts/fetch_500px_photographers_all.py --filter SPECIALTIES 9

  # 多条件（与网页勾选一致，可同时传多个）：
  python3 scripts/fetch_500px_photographers_all.py \\
    --filter SPECIALTIES 1 --filter MEMBER_TYPE 8

  # 批量跑多组过滤并追加到同一输出：见 scripts/run_500px_photographer_filter_batches.py

说明：
  - ``--sort`` 默认 RELEVANCE（与不带 sort 的首页结果一致）；站点 UI 若换排序，可改为
    MOST_POPULAR、MOST_RECENT 等（以服务端校验为准）。
  - 全量约千万级用户，耗时长；请配合 ``--max-pages`` / ``--max-users``、``--seen-db`` 断点续跑。
  - **不把已有输出读入内存去重**；重复跑会产生重复行，跑完后可用 ``sort -u`` 等离线去重。
    若需运行中去重且不占内存，可用 ``--seen-db``（SQLite）。
  - **扩大覆盖面 / 绕过「全站列表」第 51 页搜索 500**：对 ``userSearch`` 加 ``filters``（如 ``SPECIALTIES``、``MEMBER_TYPE``）。
    界面上的英文类别名需对应 **数字 ID**（可用 DevTools 看 GraphQL variables，或参考
    ``config/500px_usersearch_filter_batches.yaml`` 里的探测表）。单桶 ``totalCount`` 较小时，
    分页页数少，往往不会触发深分页 bug；最终对各桶结果 **并集去重** 可接近「更多不重复用户」。
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

GRAPHQL_URL = "https://api.500px.com/graphql"
ORIGIN = "https://500px.com"

USER_SEARCH_QUERY = """\
query UserSearchPhotographersAllQuery($first: Int!, $after: String, $sort: UserSort, $filters: [UserSearchFilter!]) {
  userSearch(first: $first, after: $after, sort: $sort, filters: $filters) {
    totalCount
    pageInfo {
      hasNextPage
      endCursor
      startCursor
    }
    edges {
      cursor
      node {
        username
      }
    }
  }
}
"""

USER_SORT_CHOICES = ("RELEVANCE", "MOST_POPULAR", "MOST_RECENT")


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def build_user_search_filters(
    filter_pairs: Optional[List[List[str]]],
    filters_json: Optional[Path],
) -> Optional[List[Dict[str, str]]]:
    """组装 GraphQL ``UserSearchFilter`` 列表；无项时返回 ``None``（与全站一致）。"""
    items: List[Dict[str, str]] = []
    if filters_json is not None:
        path = (
            filters_json
            if filters_json.is_absolute()
            else (_repo_root() / filters_json).resolve()
        )
        if not path.is_file():
            raise FileNotFoundError(str(path))
        arr = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(arr, list):
            raise ValueError("filters-json 顶层须为 JSON 数组")
        for x in arr:
            if not isinstance(x, dict):
                continue
            k, v = x.get("key"), x.get("value")
            if k is None or v is None:
                continue
            items.append({"key": str(k).strip(), "value": str(v)})
    if filter_pairs:
        for pair in filter_pairs:
            if len(pair) != 2:
                continue
            k, v = pair[0].strip(), pair[1]
            if k:
                items.append({"key": k, "value": str(v)})
    return items if items else None


def load_proxies_yaml(path: Path) -> List[Dict[str, Any]]:
    if not path.is_file():
        raise FileNotFoundError(str(path))
    obj = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    rows = obj.get("proxies") if isinstance(obj, dict) else None
    if not isinstance(rows, list):
        return []
    out: List[Dict[str, Any]] = []
    for r in rows:
        if isinstance(r, dict) and str(r.get("host") or "").strip() and r.get("port") is not None:
            out.append(r)
    return out


def proxy_entry_to_http_url(entry: Dict[str, Any]) -> str:
    host = str(entry["host"]).strip()
    port = int(entry["port"])
    user = str(entry.get("username") or "").strip()
    pwd = str(entry.get("password") or "").strip()
    if user:
        return (
            f"http://{urllib.parse.quote(user, safe='')}:{urllib.parse.quote(pwd, safe='')}"
            f"@{host}:{port}"
        )
    return f"http://{host}:{port}"


def apply_session_proxies(session: requests.Session, proxy_url: str) -> None:
    session.proxies = {"http": proxy_url, "https": proxy_url}


def load_netscape_500px(cookies_path: Path) -> Tuple[str, Optional[str]]:
    pairs: List[str] = []
    csrf: Optional[str] = None
    for raw in cookies_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 7:
            continue
        domain, _flag, _path, _secure, _expiry, name, value = fields[:7]
        if "500px.com" not in domain:
            continue
        pairs.append(f"{name}={value}")
        if name == "x-csrf-token":
            csrf = value
    return "; ".join(pairs), csrf


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


def _first_fallbacks(page_size: int) -> List[int]:
    """同一 ``after`` 游标下遇上游搜索 500 时，依次减小 ``first`` 再试。"""
    chain: List[int] = [page_size]
    for n in (150, 125, 100, 75, 50, 40, 25, 20, 10, 5, 2, 1):
        if n < page_size and 1 <= n <= 200 and n not in chain:
            chain.append(n)
    return chain


def _is_internal_user_search_failure(exc: BaseException) -> bool:
    s = str(exc)
    return (
        "INTERNAL_SERVER_ERROR" in s
        or "Something just went wrong" in s
        or "Internal Server Error" in s
    )


def fetch_user_search_page(
    session: requests.Session,
    csrf: Optional[str],
    sort: Optional[str],
    after: Optional[str],
    page_size: int,
    max_retries: int,
    filters: Optional[List[Dict[str, str]]],
) -> Dict[str, object]:
    """请求一页 ``userSearch``；遇内部搜索 500 时在同一游标上减小 ``first`` 重试。"""
    last_err: Optional[Exception] = None
    for first in _first_fallbacks(page_size):
        variables: Dict[str, object] = {
            "first": first,
            "after": after,
            "sort": sort,
            "filters": filters,
        }
        try:
            if first < page_size:
                print(
                    f"提示: 上游搜索返回 500，同一游标改用 first={first} 重试",
                    file=sys.stderr,
                    flush=True,
                )
            return graphql_post(session, csrf, variables, max_retries)
        except RuntimeError as e:
            last_err = e
            if not _is_internal_user_search_failure(e):
                raise
            if first <= 1:
                raise
            time.sleep(4.0 + random.uniform(0, 3.0))
    assert last_err is not None
    raise last_err


def graphql_post(
    session: requests.Session,
    csrf: Optional[str],
    variables: Dict[str, object],
    max_retries: int,
) -> Dict[str, object]:
    headers = {
        "Origin": ORIGIN,
        "Referer": ORIGIN + "/photographers/all",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if csrf:
        headers["x-csrf-token"] = csrf
    payload = {
        "operationName": "UserSearchPhotographersAllQuery",
        "query": USER_SEARCH_QUERY,
        "variables": variables,
    }
    last_err: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            r = session.post(GRAPHQL_URL, headers=headers, json=payload, timeout=120)
            if r.status_code == 429:
                wait = min(120.0, (2**attempt) + random.uniform(0, 1.5))
                time.sleep(wait)
                continue
            if r.status_code == 403:
                # 多见于短时间请求过多、WAF、或 Cookie/CSRF 与站点会话不一致；可换导出 Cookie 或加长 --sleep
                if attempt >= max_retries:
                    r.raise_for_status()
                wait = min(
                    240.0,
                    25.0 * (1.45**attempt) + random.uniform(3.0, 18.0),
                )
                print(
                    f"提示: GraphQL 403 Forbidden，{wait:.1f}s 后重试 ({attempt + 1}/{max_retries + 1})",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(wait)
                continue
            r.raise_for_status()
            body = r.json()
            if body.get("errors"):
                errs = body["errors"]
                codes = [
                    str(e.get("extensions", {}).get("code", "")) for e in errs if isinstance(e, dict)
                ]
                if "INTERNAL_SERVER_ERROR" in codes and attempt < max_retries:
                    # 错误里常见 search 微服务 /internal/graphql/users 返回 500，退避加长
                    time.sleep(
                        min(240.0, 10.0 * (1.55**attempt) + random.uniform(0, 4))
                    )
                    continue
                raise RuntimeError(
                    "GraphQL errors: " + json.dumps(errs, ensure_ascii=False)[:1200]
                )
            data = body.get("data")
            if not isinstance(data, dict):
                raise RuntimeError(f"GraphQL 无 data: {body!r}"[:800])
            us = data.get("userSearch")
            if us is None:
                raise RuntimeError(f"userSearch 为空: {body!r}"[:800])
            return data
        except (requests.RequestException, RuntimeError) as e:
            last_err = e
            if attempt >= max_retries:
                raise
            time.sleep(min(30.0, 1.2**attempt + random.uniform(0, 0.8)))
    raise last_err  # pragma: no cover


def main() -> int:
    ap = argparse.ArgumentParser(
        description="500px：拉取 photographers/all 对应的 userSearch 用户名录（username）",
    )
    ap.add_argument(
        "--cookies",
        type=Path,
        default=None,
        help="可选 Netscape Cookie（500px.com + x-csrf-token），用于降低匿名限流风险",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="输出路径（每行一个 username）；默认 output/500px_photographers_all_usernames.txt",
    )
    ap.add_argument(
        "--sort",
        choices=USER_SORT_CHOICES,
        default="RELEVANCE",
        help="userSearch 排序（默认 RELEVANCE，与站点「全部」默认列表常见一致）",
    )
    ap.add_argument(
        "--no-sort",
        action="store_true",
        help="不传 sort（GraphQL 变量 sort=null）；用于对照试验；与指定 --sort 同时出现时以此为准",
    )
    ap.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="每页条数，服务端目前最大约 200（默认 200）",
    )
    ap.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="最多请求页数，0 表示无上限（慎用）",
    )
    ap.add_argument(
        "--max-users",
        type=int,
        default=0,
        help="最多写入新用户数（含跳过已存在后计数），0 表示无上限",
    )
    ap.add_argument(
        "--max-no-progress-pages",
        type=int,
        default=5,
        metavar="N",
        help=(
            "连续 N 页无进展则退出：无 seen-db 时看「本页新写入=0」；"
            "有 seen-db 时仅当「edges 为空」计次（避免全在库中误停）。0=关闭"
        ),
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.35,
        help="每页间隔秒数",
    )
    ap.add_argument(
        "--retries",
        type=int,
        default=10,
        help="单页同一 first 下 GraphQL 失败/429/INTERNAL 时重试次数",
    )
    ap.add_argument(
        "--seen-db",
        type=Path,
        default=None,
        help="可选 SQLite：px500_discovered_users 表去重（可与 gallery-dl 发现脚本共用）",
    )
    ap.add_argument(
        "--overwrite-output",
        action="store_true",
        help="覆盖写入 --output（默认追加）",
    )
    ap.add_argument(
        "--proxies-yaml",
        type=Path,
        default=None,
        help="代理列表 YAML（与 config/proxies.yaml 相同格式：proxies: host/port/username/password）",
    )
    ap.add_argument(
        "--proxy-rotate-each-page",
        action="store_true",
        help="每页请求前从列表随机选一个代理；不设则全程固定用第一条代理",
    )
    ap.add_argument(
        "--filter",
        nargs=2,
        metavar=("KEY", "VALUE"),
        action="append",
        default=None,
        help=(
            "userSearch 过滤（UserSearchFilter），可重复。"
            "KEY 为 GraphQL 枚举名，如 SPECIALTIES、MEMBER_TYPE；"
            "VALUE 多为数字 ID（不是网页上的英文类别名）。"
            "示例: --filter SPECIALTIES 9 --filter MEMBER_TYPE 8"
        ),
    )
    ap.add_argument(
        "--filters-json",
        type=Path,
        default=None,
        help='JSON 数组，如 [{"key":"SPECIALTIES","value":"1"}]，与 --filter 合并',
    )
    args = ap.parse_args()

    if args.page_size < 1 or args.page_size > 200:
        print("--page-size 建议在 1–200 之间（当前服务端 >200 易报错）", file=sys.stderr)
        return 2

    sort: Optional[str] = None if args.no_sort else args.sort

    cookie_header = ""
    csrf: Optional[str] = None
    if args.cookies is not None:
        cp = args.cookies.expanduser().resolve()
        if not cp.is_file():
            print(f"找不到 Cookie 文件: {cp}", file=sys.stderr)
            return 2
        cookie_header, csrf = load_netscape_500px(cp)
        if not cookie_header:
            print("Cookie 中无 500px.com 条目", file=sys.stderr)
            return 2
        if not csrf:
            print(
                "警告: 无 x-csrf-token，部分账号可能 403",
                file=sys.stderr,
            )

    try:
        gql_filters = build_user_search_filters(args.filter, args.filters_json)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as e:
        print(f"解析 filters 失败: {e}", file=sys.stderr)
        return 2
    if gql_filters:
        print(f"userSearch filters: {gql_filters}", flush=True)
    else:
        print("userSearch filters: null（未指定 --filter）", flush=True)
    if sort is None:
        print("userSearch sort: null（--no-sort）", flush=True)
    else:
        print(f"userSearch sort: {sort}", flush=True)

    out_path = args.output
    if out_path is None:
        out_path = _repo_root() / "output" / "500px_photographers_all_usernames.txt"
    else:
        out_path = (
            out_path if out_path.is_absolute() else (_repo_root() / out_path).resolve()
        )

    seen_conn: Optional[sqlite3.Connection] = None
    if args.seen_db is not None:
        dbp = (
            args.seen_db
            if args.seen_db.is_absolute()
            else (_repo_root() / args.seen_db).resolve()
        )
        seen_conn = open_seen_db(dbp)

    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
    if cookie_header:
        session.headers["Cookie"] = cookie_header

    proxy_entries: List[Dict[str, Any]] = []
    if args.proxies_yaml is not None:
        py = (
            args.proxies_yaml
            if args.proxies_yaml.is_absolute()
            else (_repo_root() / args.proxies_yaml).resolve()
        )
        try:
            proxy_entries = load_proxies_yaml(py)
        except FileNotFoundError:
            print(f"找不到代理配置: {py}", file=sys.stderr)
            return 2
        if not proxy_entries:
            print(f"代理列表为空: {py}", file=sys.stderr)
            return 2
        session.trust_env = False
        apply_session_proxies(session, proxy_entry_to_http_url(proxy_entries[0]))
        print(
            f"已启用 HTTP 代理（{len(proxy_entries)} 条）："
            + ("每页随机轮换" if args.proxy_rotate_each_page else "固定第一条"),
            flush=True,
        )

    after_cursor: Optional[str] = None

    pages = 0
    new_written = 0
    total_reported: Optional[int] = None
    no_progress_streak = 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "w" if args.overwrite_output else "a"

    try:
        with out_path.open(mode, encoding="utf-8") as out_f:
            while True:
                if args.max_pages and pages >= args.max_pages:
                    break
                if args.max_users and new_written >= args.max_users:
                    break

                if proxy_entries and args.proxy_rotate_each_page:
                    entry = random.choice(proxy_entries)
                    apply_session_proxies(session, proxy_entry_to_http_url(entry))

                data = fetch_user_search_page(
                    session,
                    csrf,
                    sort,
                    after_cursor,
                    args.page_size,
                    args.retries,
                    gql_filters,
                )
                us = data["userSearch"]
                if not isinstance(us, dict):
                    raise RuntimeError(f"userSearch 类型异常: {us!r}")

                if total_reported is None:
                    tc = us.get("totalCount")
                    if isinstance(tc, int):
                        total_reported = tc
                        print(f"服务端 totalCount ≈ {total_reported}", flush=True)

                edges = us.get("edges") or []
                if not isinstance(edges, list):
                    break

                batch_new = 0
                for edge in edges:
                    if not isinstance(edge, dict):
                        continue
                    node = edge.get("node")
                    if not isinstance(node, dict):
                        continue
                    u = node.get("username")
                    if not isinstance(u, str) or not u.strip():
                        continue
                    u = u.strip()
                    if seen_conn and seen_contains(seen_conn, u):
                        continue
                    out_f.write(u + "\n")
                    out_f.flush()
                    batch_new += 1
                    new_written += 1
                    if seen_conn:
                        seen_insert(seen_conn, u, "photographers_all_userSearch")
                    if args.max_users and new_written >= args.max_users:
                        break

                # 无 seen-db：连续「本页新写入为 0」视为拿不到新数据；有 seen-db 时仅当 edges 为空才算停滞，
                # 避免「整页均在库里」误触发。
                stall_hit = len(edges) == 0 or (
                    seen_conn is None and batch_new == 0
                )
                if stall_hit:
                    no_progress_streak += 1
                    if (
                        args.max_no_progress_pages > 0
                        and no_progress_streak >= args.max_no_progress_pages
                    ):
                        print(
                            f"停止: 已连续 {no_progress_streak} 页判定无进展（"
                            f"--max-no-progress-pages={args.max_no_progress_pages}）；"
                            "接口返回空页或（未使用 seen-db 时）连续无新用户名",
                            flush=True,
                        )
                        break
                else:
                    no_progress_streak = 0

                pages += 1
                pinfo = us.get("pageInfo") or {}
                if not pinfo.get("hasNextPage"):
                    print("已到达末页（hasNextPage=false）", flush=True)
                    break
                cursor = pinfo.get("endCursor")
                if not cursor:
                    break
                after_cursor = cursor
                if args.sleep > 0:
                    time.sleep(args.sleep)

                if pages % 50 == 0:
                    print(
                        f"进度: {pages} 页, 本轮累计新写入 {new_written} 行 "
                        f"(本页新 {batch_new})",
                        flush=True,
                    )

    except requests.HTTPError as e:
        print(f"HTTP 错误: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"失败: {e}", file=sys.stderr)
        return 1
    finally:
        if seen_conn:
            seen_conn.close()

    print(
        f"完成: {pages} 页, 新写入 {new_written} 个用户名 -> {out_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
