#!/usr/bin/env python3
"""用真实 Chrome（Selenium）打开 YAML 中的地址列表，下滑加载至「无新内容」条件满足，再保存 HTML。

滚动结束条件见 YAML ``scroll.stall_strategy``：默认 ``long_retry``（**未接近文档底部**时只按 ``pause_sec`` 连续下滑；**已到底**且 metric 不涨时，连续 ``long_wait_cycles`` 次各 ``long_wait_sec`` 后检测 metric，仍不涨则结束）。
或 ``quick``（短周期连续无增长即停）。另受 ``scroll.max_rounds`` 约束（``<= 0`` 表示不限制）；退出后写入 ``page_source``（最终快照）。
可选 ``scroll.checkpoint_every_rounds``（默认 100，设为 0 关闭）：每累计 N 次下滑将当前 DOM 写入同一 checkpoint 文件（覆盖），长时间滚动时也能周期性落盘；写入采用临时文件再替换，降低落盘过程中被中断导致半截文件的概率。
可选 ``scroll.checkpoint_extract: true``：每次 checkpoint 落盘后立刻按 ``scroll.extract_config`` 从该 HTML 提取 uid 并合并写入清单（与单独跑 ``extract_500px_usernames_from_saved_html.py`` 相同逻辑），即使随后 Chrome tab crash，已提取的用户名也已追加到 ``output`` 文件。

默认配置: config/save_500px_scroll_html.yaml

依赖（Debian/Ubuntu 推荐用 apt，不经 pip 写系统 site-packages）::

  sudo apt install python3-selenium python3-yaml chromium-chromedriver

若必须用 pip，而系统提示 ``externally-managed-environment``（PEP 668），可选用其一::

  pip3 install --user --break-system-packages selenium pyyaml   # 自担风险
  pipx install ...   # 由 pipx 单独维护环境，非系统 Python

Selenium 4 默认走 **Selenium Manager** 自动拉取 chromedriver；若报 ``NoSuchDriverException``，
请安装系统 chromedriver 并确保与 Chrome **主版本号**一致，例如::

  sudo apt install chromium-chromedriver   # 或 google-chrome 官方仓库里的 chromedriver
  # 脚本会依次使用: YAML ``chromedriver_binary`` → 环境变量 CHROMEDRIVER_PATH → PATH → /usr/bin/chromedriver

登录态（二选一，推荐 Cookie 文件，改动小）::

  **cookies_file** — Netscape 格式 ``cookies.txt``（与 ``fetch_500px_photographers_all.py --cookies`` 相同）。
  用 Chrome 登录 500px 后导出 Cookie（见仓库内导出说明或 DevTools Application → Cookies 导出），
  在 YAML 里填写路径；脚本先打开 ``https://500px.com/`` 再 ``add_cookie``，之后照常访问各 URL。

  **user_data_dir** — 指向本机 Chrome/Chromium 的 **用户数据目录**（如 ``~/.config/google-chrome``），
  可选 ``profile_directory: Default``。这样会直接使用你日常浏览器里已登录的会话；
  **必须先完全退出**正在使用该目录的 Chrome，否则启动会失败或锁 profile。

  **cookies_after_inject_refresh** — 注入后是否 ``refresh()`` 落地页（默认 true，便于 SPA 识别会话）。

  **page_load_strategy** — ``normal`` / ``eager`` / ``none``。500px 等 SPA 在 ``normal`` 下可能长时间等不到
  ``document.complete``，导致 ``driver.get()`` 卡在「打开: …」之后不进入滚动；默认 ``eager``（DOM 就绪即返回）。

  **script_timeout_sec** — ``execute_script``（含「是否到底」的 DOM 遍历）超时秒数；大页面默认 180，避免 ``TimeoutException: script timeout``。

无图形服务器: 将配置 ``browser.headed: false`` 或执行前 ``export AUTO_HEADLESS=1``。

进程退出时（正常结束、异常、Ctrl+C、SIGTERM）会尽力 ``quit()`` 浏览器并释放 chromedriver。
"""

from __future__ import annotations

import argparse
import atexit
import os
import re
import shutil
import signal
import sys
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
except ImportError as e:
    print("请先安装 selenium: pip install selenium", file=sys.stderr)
    raise SystemExit(2) from e


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _atomic_write_text(path: Path, text: str, *, encoding: str = "utf-8") -> None:
    """先写入同目录临时文件再 ``os.replace``，避免落盘过程中进程被杀留下半截 HTML。"""
    path = path.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp)
    try:
        with os.fdopen(fd, "w", encoding=encoding) as f:
            f.write(text)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            tmp_path.unlink()
        except OSError:
            pass
        raise


def _load_yaml(path: Path) -> Dict[str, Any]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(obj, dict):
        raise ValueError("YAML 顶层须为 mapping")
    return obj


def _metric_photo_links(driver: webdriver.Chrome) -> int:
    els = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/photo/"]')
    return len(els)


_METRICS = {"photo_links": _metric_photo_links}

# 供 atexit / 信号处理 / finally 共用，确保只释放一次
_driver_box: List[Optional["webdriver.Chrome"]] = [None]


def _dispose_chrome() -> None:
    """关闭浏览器并结束 chromedriver 子进程；可安全多次调用。"""
    d = _driver_box[0]
    _driver_box[0] = None
    if d is None:
        return
    try:
        d.quit()
    except BaseException:
        try:
            d.close()
        except BaseException:
            pass


def _on_signal(signum: int, frame: Any) -> None:
    _dispose_chrome()
    # 恢复默认行为后再递送，便于 shell 得到正确退出码
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


atexit.register(_dispose_chrome)


def _pick_headed(cfg: Dict[str, Any]) -> bool:
    b = cfg.get("browser") if isinstance(cfg.get("browser"), dict) else {}
    headed = bool(b.get("headed", True))
    if os.environ.get("AUTO_HEADLESS", "").strip() in ("1", "true", "yes"):
        return False
    if headed and not os.environ.get("DISPLAY"):
        print("提示: 未检测到 DISPLAY，自动改用无头模式（headless）", file=sys.stderr)
        return False
    return headed


def _resolve_chromedriver_path(b: Dict[str, Any]) -> Optional[str]:
    """返回 chromedriver 可执行文件路径；找不到则 None（交给 Selenium Manager）。"""
    p = str(b.get("chromedriver_binary") or "").strip()
    if p:
        pp = Path(p).expanduser()
        if pp.is_file():
            return str(pp.resolve())
        print(f"警告: chromedriver_binary 不是文件: {p}，将尝试自动探测", file=sys.stderr)
    env = os.environ.get("CHROMEDRIVER_PATH", "").strip()
    if env:
        ep = Path(env).expanduser()
        if ep.is_file():
            return str(ep.resolve())
    w = shutil.which("chromedriver")
    if w:
        return w
    for cand in (
        "/usr/bin/chromedriver",
        "/usr/lib/chromium-browser/chromedriver",
        "/snap/bin/chromium.chromedriver",
    ):
        if Path(cand).is_file():
            return cand
    return None


def _resolve_repo_path(root: Path, p: str) -> Path:
    pp = Path(p).expanduser()
    return pp if pp.is_absolute() else (root / pp).resolve()


_DEFAULT_EXTRACT_MERGE_CFG = "config/extract_500px_usernames_from_saved_html.yaml"
_extract_merge_mod: Optional[Any] = None


def _lazy_merge_extract_mod(repo_root: Path) -> Any:
    """懒加载 ``extract_500px_usernames_from_saved_html``（供 checkpoint 后合并 uid）。"""
    global _extract_merge_mod
    if _extract_merge_mod is not None:
        return _extract_merge_mod
    import importlib.util

    mp = repo_root / "scripts" / "extract_500px_usernames_from_saved_html.py"
    spec = importlib.util.spec_from_file_location("_px500_merge_extract", mp)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载模块: {mp}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _extract_merge_mod = mod
    return mod


def _netscape_rows_for_selenium(cookies_path: Path) -> List[Dict[str, Any]]:
    """解析 Netscape cookies.txt 中与 500px 相关的条目，供 Selenium add_cookie。"""
    out: List[Dict[str, Any]] = []
    now = int(time.time())
    for raw in cookies_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 7:
            continue
        domain, _flag, path, secure, expiry, name, value = fields[:7]
        if "500px.com" not in domain:
            continue
        dom = domain.strip()
        try:
            exp_int = int(expiry)
        except ValueError:
            exp_int = 0
        if exp_int and exp_int < now:
            continue
        secure_bool = str(secure).upper() == "TRUE"
        c: Dict[str, Any] = {
            "name": name,
            "value": value,
            "domain": dom,
            "path": path or "/",
            "secure": secure_bool,
        }
        if exp_int:
            c["expiry"] = exp_int
        out.append(c)
    return out


def _inject_cookies_from_file(
    driver: webdriver.Chrome,
    cookies_path: Path,
    landing: str,
    *,
    refresh_after: bool,
) -> int:
    driver.get(landing)
    time.sleep(1.0)
    rows = _netscape_rows_for_selenium(cookies_path)
    ok = 0
    for c in rows:
        base = {
            "name": c["name"],
            "value": c["value"],
            "domain": c["domain"],
            "path": c["path"],
            "secure": bool(c["secure"]),
        }
        if "expiry" in c:
            base["expiry"] = int(c["expiry"])
        added = False
        try:
            driver.add_cookie(base)
            added = True
        except BaseException:
            dom = str(c["domain"]).lstrip(".")
            alt = {**base, "domain": f".{dom}" if not str(c["domain"]).startswith(".") else c["domain"]}
            try:
                driver.add_cookie(alt)
                added = True
            except BaseException as e:
                print(f"跳过 cookie {c.get('name')!r}: {e}", file=sys.stderr)
        if added:
            ok += 1
    print(f"已注入 Cookie {ok}/{len(rows)} 条（来源 {cookies_path}）", flush=True)
    if refresh_after and ok > 0:
        driver.refresh()
        time.sleep(2.0)
    return ok


def _apply_page_load_strategy(opts: ChromeOptions, raw: str) -> None:
    s = (raw or "eager").strip().lower()
    if s not in ("normal", "eager", "none"):
        print(
            f"警告: 未知 browser.page_load_strategy={raw!r}，改用 eager（可选: normal, eager, none）",
            flush=True,
        )
        s = "eager"
    opts.page_load_strategy = s
    print(f"pageLoadStrategy={s}", flush=True)


def _build_driver(cfg: Dict[str, Any], *, headed: bool) -> webdriver.Chrome:
    b = cfg.get("browser") if isinstance(cfg.get("browser"), dict) else {}
    opts = ChromeOptions()
    _apply_page_load_strategy(opts, str(b.get("page_load_strategy") or "eager"))
    w = int(b.get("window_width", 1400))
    h = int(b.get("window_height", 900))
    opts.add_argument(f"--window-size={w},{h}")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-US,en")
    if not headed:
        opts.add_argument("--headless=new")
    cb = str(b.get("chrome_binary") or "").strip()
    if cb:
        opts.binary_location = cb

    ud = str(b.get("user_data_dir") or "").strip()
    if ud:
        udp = Path(ud).expanduser().resolve()
        if not udp.is_dir():
            raise SystemExit(f"user_data_dir 不是目录: {udp}")
        opts.add_argument(f"--user-data-dir={udp}")
        prof = str(b.get("profile_directory") or "Default").strip()
        if prof:
            opts.add_argument(f"--profile-directory={prof}")
        print(f"使用 Chrome 用户数据目录: {udp}  profile={prof}", flush=True)

    # 降低自动化特征（不保证绕过检测）
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    cd = _resolve_chromedriver_path(b)
    if cd:
        print(f"使用 chromedriver: {cd}", flush=True)
        service = ChromeService(executable_path=cd)
    else:
        print("未探测到系统 chromedriver，尝试 Selenium Manager 自动获取", flush=True)
        service = ChromeService()
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(120)
    st_script = float(b.get("script_timeout_sec", 180))
    if st_script > 0:
        driver.set_script_timeout(int(st_script))
        print(f"execute_script 超时: {int(st_script)}s", flush=True)
    return driver


def _sanitize_slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^a-z0-9._-]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "page"


def _normalize_urls(entries: Any) -> List[Tuple[str, str]]:
    out: List[Tuple[str, str]] = []
    if not isinstance(entries, list):
        return out
    for i, row in enumerate(entries):
        if isinstance(row, str) and row.strip():
            url = row.strip()
            slug = _sanitize_slug(re.sub(r"^https?://[^/]+", "", url).replace("/", "_")[:80])
            out.append((url, slug))
        elif isinstance(row, dict) and row.get("url"):
            url = str(row["url"]).strip()
            slug = str(row.get("slug") or "").strip()
            if not slug:
                slug = _sanitize_slug(f"u{i}_{url}")
            else:
                slug = _sanitize_slug(slug)
            out.append((url, slug))
    return out


def _parse_max_rounds(raw: Any) -> Optional[int]:
    """正整数 = 最多下滑次数；``<= 0`` 为不限制。``None`` / 非法值 视为未指定，默认 400。"""
    if raw is None:
        return 400
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return 400
    if v <= 0:
        return None
    return v


def _format_checkpoint_filename(template: str, *, slug: str) -> str:
    t = (template or "{slug}_checkpoint.html").strip() or "{slug}_checkpoint.html"
    return t.replace("{slug}", slug)


def _near_effective_scroll_bottom(
    driver: webdriver.Chrome,
    *,
    epsilon_px: int,
    inner_scroll_min_overflow_px: int,
    inner_scroll_max_nodes: int,
) -> bool:
    """是否已接近「有效」滚动底部：先看 window/document，再检查内部 overflow 可滚区域。

    旧实现对 ``*`` 全表扫描，在 500px 超长列表上会触发 ``execute_script`` 默认超时。现改为
    自 ``body`` 起 **深度优先遍历子树**，且 ``getComputedStyle`` 仅在 ``scrollHeight`` 明显大于
    ``clientHeight`` 时调用；总遍历节点数上限 ``inner_scroll_max_nodes``，超出则视为**未到底**
    （宁可继续滑，也不长等）。
    """
    eps = max(1, int(epsilon_px))
    min_ov = max(0, int(inner_scroll_min_overflow_px))
    max_nodes = max(50, int(inner_scroll_max_nodes))
    try:
        return bool(
            driver.execute_script(
                """
                var eps = arguments[0], minOv = arguments[1], maxNodes = arguments[2];
                var st0 = window.pageYOffset || document.documentElement.scrollTop || 0;
                var sh = Math.max(
                    document.body.scrollHeight || 0,
                    document.documentElement.scrollHeight || 0
                );
                var wh = window.innerHeight || document.documentElement.clientHeight || 0;
                if ((st0 + wh) < (sh - eps)) {
                    return false;
                }
                var budget = maxNodes;
                function walk(el) {
                    if (!el || el.nodeType !== 1) {
                        return true;
                    }
                    if (budget <= 0) {
                        return false;
                    }
                    budget--;
                    var diff = (el.scrollHeight || 0) - (el.clientHeight || 0);
                    if (diff >= minOv) {
                        var cs = window.getComputedStyle(el);
                        var oy = cs.overflowY;
                        if (oy === 'auto' || oy === 'scroll' || oy === 'overlay') {
                            if ((el.scrollTop + el.clientHeight) < (el.scrollHeight - eps)) {
                                return false;
                            }
                        }
                    }
                    var c = el.firstElementChild;
                    while (c) {
                        if (!walk(c)) {
                            return false;
                        }
                        c = c.nextElementSibling;
                    }
                    return true;
                }
                var body = document.body;
                if (!body) {
                    return true;
                }
                return walk(body);
                """,
                eps,
                min_ov,
                max_nodes,
            )
        )
    except BaseException:
        return False


def _scroll_until_stall(
    driver: webdriver.Chrome,
    *,
    metric_fn,
    pixels: int,
    pause_sec: float,
    max_rounds: Optional[int],
    stall_strategy: str,
    stall_threshold: int,
    long_wait_sec: float,
    long_wait_cycles: int,
    bottom_epsilon_px: int,
    long_wait_only_when_at_bottom: bool,
    inner_scroll_min_overflow_px: int,
    inner_scroll_max_nodes: int,
    checkpoint_path: Optional[Path],
    checkpoint_every_rounds: int,
    on_checkpoint_saved: Optional[Callable[[Path], None]] = None,
) -> Tuple[int, int]:
    """返回 (最终 metric 值, 实际执行的滚动次数）。

    ``max_rounds`` 为 ``None`` 时不限制次数（仅由下方 stall 条件退出；若页面无限增长则可能长时间运行）。

    若 ``checkpoint_every_rounds > 0`` 且提供 ``checkpoint_path``：每累计该次数次下滑后写入该路径（覆盖），
    日志前缀 ``[checkpoint]`` 区分首次写入与覆盖写入。可选 ``on_checkpoint_saved(path)``：在每次成功落盘后立即调用
    （例如把 HTML 中的 uid 合并进清单，避免 tab crash 后只剩 checkpoint 文件而提取结果未持久化）。

    stall_strategy:
      - ``long_retry``（默认）: 以**有效滚动是否接近底部**为准（document + 内部 ``overflow`` 可滚区域）。
        **未到底**：只按 ``pause_sec`` 间隔继续 ``scrollBy``，不因 metric 暂不涨而长等。
        **已到底**且本次下滑后 metric 未涨：连续 ``long_wait_cycles`` 次，每次先 ``long_wait_sec`` 再读 metric；
        任一次 metric 变大则回到下滑；若 ``long_wait_cycles`` 次后仍不涨则结束。
      - ``quick``: 连续 ``stall_threshold`` 次短等待后无增长即退出。
    """
    last = metric_fn(driver)
    rounds = 0
    mode = (stall_strategy or "long_retry").strip().lower()
    if mode not in ("long_retry", "quick"):
        mode = "long_retry"

    ck_writes = [0]
    ck_every = max(0, int(checkpoint_every_rounds))

    def _checkpoint_if_due() -> None:
        if ck_every <= 0 or checkpoint_path is None:
            return
        if rounds <= 0 or rounds % ck_every != 0:
            return
        _atomic_write_text(checkpoint_path, driver.page_source, encoding="utf-8")
        ck_writes[0] += 1
        n = ck_writes[0]
        if n == 1:
            print(
                f"[checkpoint] 首次写入 HTML: {checkpoint_path}（累计下滑 {rounds} 次）",
                flush=True,
            )
        else:
            print(
                f"[checkpoint] 覆盖写入 HTML: {checkpoint_path}（累计下滑 {rounds} 次，第 {n} 次落盘）",
                flush=True,
            )
        if on_checkpoint_saved is not None:
            try:
                on_checkpoint_saved(checkpoint_path.resolve())
            except BaseException as e:
                print(
                    f"[checkpoint] 警告: checkpoint 后回调失败（不影响继续滚动）: {e}",
                    flush=True,
                )

    def _under_cap() -> bool:
        return max_rounds is None or rounds < max_rounds

    if mode == "quick":
        stall = 0
        while _under_cap():
            driver.execute_script(f"window.scrollBy(0, {int(pixels)});")
            time.sleep(float(pause_sec))
            rounds += 1
            _checkpoint_if_due()
            cur = metric_fn(driver)
            if cur > last:
                last = cur
                stall = 0
            else:
                stall += 1
                if stall >= int(stall_threshold):
                    break
        if max_rounds is not None and rounds >= max_rounds:
            print(f"提示: 已达 scroll.max_rounds={max_rounds}，停止滚动", flush=True)
        return last, rounds

    # long_retry
    lw = float(long_wait_sec)
    lc = max(1, int(long_wait_cycles))
    legacy_fails = 0
    while _under_cap():
        driver.execute_script(f"window.scrollBy(0, {int(pixels)});")
        time.sleep(float(pause_sec))
        rounds += 1
        _checkpoint_if_due()
        cur = metric_fn(driver)
        if cur > last:
            last = cur
            legacy_fails = 0
            continue

        if long_wait_only_when_at_bottom:
            if not _near_effective_scroll_bottom(
                driver,
                epsilon_px=bottom_epsilon_px,
                inner_scroll_min_overflow_px=inner_scroll_min_overflow_px,
                inner_scroll_max_nodes=inner_scroll_max_nodes,
            ):
                continue

            print(
                f"提示: 已接近页底且 metric 未增（{cur}），"
                f"将最多 {lc} 次各等待 {lw:g}s 后检测 metric…",
                flush=True,
            )
            recovered = False
            for attempt in range(1, lc + 1):
                time.sleep(lw)
                chk = metric_fn(driver)
                if chk > last:
                    print(
                        f"提示: 第 {attempt}/{lc} 次等待后 metric 增长 {last}→{chk}，继续下滑",
                        flush=True,
                    )
                    last = chk
                    recovered = True
                    break
                print(f"提示: 第 {attempt}/{lc} 次等待后 metric 仍为 {chk}", flush=True)
            if recovered:
                continue
            print(
                f"提示: 已连续 {lc} 次各等待 {lw:g}s 后 metric 仍无增长，保存 HTML",
                flush=True,
            )
            break

        # legacy：不区分是否到底，长等后再滚一次
        print(
            f"提示: 下滑后 metric 未增加（当前 {cur}），长等待 {lw:g}s 后再滚一次…",
            flush=True,
        )
        time.sleep(lw)
        if max_rounds is not None and rounds >= max_rounds:
            break
        driver.execute_script(f"window.scrollBy(0, {int(pixels)});")
        rounds += 1
        _checkpoint_if_due()
        time.sleep(float(pause_sec))
        cur2 = metric_fn(driver)
        if cur2 > last:
            last = cur2
            legacy_fails = 0
            continue
        legacy_fails += 1
        print(
            f"提示: 长等待后仍无新内容（连续 {legacy_fails}/{lc} 次），metric={cur2}",
            flush=True,
        )
        if legacy_fails >= lc:
            print(
                f"提示: 已连续 {lc} 次「等{lw:g}s 再滚」仍无增长，视为到底并保存 HTML",
                flush=True,
            )
            legacy_fails = 0
            break

    if max_rounds is not None and rounds >= max_rounds:
        print(f"提示: 已达 scroll.max_rounds={max_rounds}，停止滚动", flush=True)
    return last, rounds


def main() -> int:
    root = _repo_root()
    ap = argparse.ArgumentParser(description="Selenium 下滑到底并保存 500px 等页面 HTML")
    ap.add_argument(
        "--config",
        type=Path,
        default=root / "config" / "save_500px_scroll_html.yaml",
        help="YAML 配置路径",
    )
    args = ap.parse_args()
    cfg_path = args.config if args.config.is_absolute() else (root / args.config).resolve()
    if not cfg_path.is_file():
        print(f"找不到配置: {cfg_path}", file=sys.stderr)
        return 2

    cfg = _load_yaml(cfg_path)
    out_cfg = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    out_dir = root / str(out_cfg.get("dir", "output/html")).strip()
    use_ts = bool(out_cfg.get("filename_timestamp", True))

    sc = cfg.get("scroll") if isinstance(cfg.get("scroll"), dict) else {}
    pixels = int(sc.get("pixels", 900))
    pause_sec = float(sc.get("pause_sec", 2.5))
    stall_threshold = int(sc.get("stall_threshold", 3))
    stall_strategy = str(sc.get("stall_strategy", "long_retry")).strip().lower()
    if stall_strategy not in ("long_retry", "quick"):
        print(
            f"警告: 未知 scroll.stall_strategy={stall_strategy!r}，改用 long_retry",
            flush=True,
        )
        stall_strategy = "long_retry"
    long_wait_sec = float(sc.get("long_wait_sec", 30))
    long_wait_cycles = int(sc.get("long_wait_cycles", 3))
    max_rounds = _parse_max_rounds(sc.get("max_rounds", 400))
    bottom_epsilon_px = int(sc.get("bottom_epsilon_px", 100))
    inner_scroll_min_overflow_px = int(sc.get("inner_scroll_min_overflow_px", 120))
    inner_scroll_max_nodes = int(sc.get("inner_scroll_max_nodes", 8000))
    inner_scroll_max_nodes = max(500, min(100_000, inner_scroll_max_nodes))
    long_wait_only_when_at_bottom = bool(sc.get("long_wait_only_when_at_bottom", True))
    try:
        checkpoint_every = int(sc.get("checkpoint_every_rounds", 100))
    except (TypeError, ValueError):
        checkpoint_every = 0
    if checkpoint_every < 0:
        checkpoint_every = 0
    checkpoint_tpl = str(sc.get("checkpoint_file", "{slug}_checkpoint.html")).strip()
    ck_run_extract = bool(sc.get("checkpoint_extract", False))
    extract_cfg_rel = str(sc.get("extract_config") or _DEFAULT_EXTRACT_MERGE_CFG).strip()
    extract_cfg_for_ck = (root / extract_cfg_rel).resolve()
    if ck_run_extract and not extract_cfg_for_ck.is_file():
        print(
            f"警告: scroll.checkpoint_extract 已开启但找不到 extract_config: {extract_cfg_for_ck}，"
            "将不在 checkpoint 后合并 uid",
            flush=True,
        )
        ck_run_extract = False
    if max_rounds is None:
        print(
            "提示: scroll.max_rounds<=0 为不限制下滑次数，仅由「无新内容」逻辑结束；"
            "若 metric 持续增长则可能运行很久，可用 Ctrl+C 中断。",
            flush=True,
        )
    metric_name = str(sc.get("metric", "photo_links")).strip()
    metric_fn = _METRICS.get(metric_name)
    if metric_fn is None:
        print(f"未知 scroll.metric: {metric_name!r}，可选: {list(_METRICS)}", file=sys.stderr)
        return 2

    urls = _normalize_urls(cfg.get("urls"))
    if not urls:
        print("YAML urls 为空", file=sys.stderr)
        return 2

    headed = _pick_headed(cfg)
    b = cfg.get("browser") if isinstance(cfg.get("browser"), dict) else {}
    page_load_wait = float(b.get("page_load_wait_sec", 5))
    ud = str(b.get("user_data_dir") or "").strip()
    cf_raw = str(b.get("cookies_file") or "").strip()
    if ud and cf_raw:
        print(
            "提示: 已设置 user_data_dir，将忽略 cookies_file（profile 内已有登录态）",
            flush=True,
        )
        cf_raw = ""
    cookies_path: Optional[Path] = None
    if cf_raw:
        cookies_path = _resolve_repo_path(root, cf_raw)
        if not cookies_path.is_file():
            print(f"找不到 cookies_file: {cookies_path}", file=sys.stderr)
            return 2
    landing = str(b.get("cookies_landing_url") or "https://500px.com/").strip()
    if not landing.startswith("http"):
        landing = "https://500px.com/"
    refresh_after_cookies = bool(b.get("cookies_after_inject_refresh", True))

    if not ud and not cookies_path:
        print(
            "提示: 未配置 browser.cookies_file 也未配置 browser.user_data_dir，"
            "500px 将以匿名会话打开（通常显示未登录 / 弹窗）。"
            "要登录请在 YAML 中设置其一：导出 Netscape cookies 到 cookies_file，"
            "或填写本机 Chrome 的 user_data_dir（须先退出日常 Chrome）。",
            flush=True,
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"输出目录: {out_dir}  headed={headed}", flush=True)

    old_int = signal.signal(signal.SIGINT, _on_signal)
    old_term = None
    if hasattr(signal, "SIGTERM"):
        old_term = signal.signal(signal.SIGTERM, _on_signal)

    driver: Optional[webdriver.Chrome] = None
    try:
        driver = _build_driver(cfg, headed=headed)
        _driver_box[0] = driver
        if cookies_path is not None:
            _inject_cookies_from_file(
                driver,
                cookies_path,
                landing,
                refresh_after=refresh_after_cookies,
            )
        for url, slug in urls:
            print(f"打开: {url}", flush=True)
            driver.get(url)
            print("driver.get 已返回，等待页面稳定…", flush=True)
            time.sleep(page_load_wait)
            before = metric_fn(driver)
            ck_path: Optional[Path] = None
            if checkpoint_every > 0:
                ck_path = (out_dir / _format_checkpoint_filename(checkpoint_tpl, slug=slug)).resolve()
                print(
                    f"提示: 已启用滚动 checkpoint：每 {checkpoint_every} 次下滑写入 {ck_path}（覆盖同一文件）",
                    flush=True,
                )
                if ck_run_extract:
                    print(
                        f"提示: 每次 checkpoint 后将按 {extract_cfg_for_ck} 合并 uid 到清单",
                        flush=True,
                    )
            on_ck: Optional[Callable[[Path], None]] = None
            if ck_run_extract and ck_path is not None:
                _ec = extract_cfg_for_ck
                _rt = root

                def _on_ck_saved(p: Path) -> None:
                    mod = _lazy_merge_extract_mod(_rt)
                    ret = mod.merge_usernames_from_html_files(_rt, _ec, [p])
                    if ret != 0:
                        print(
                            f"[checkpoint] 警告: 合并 uid 返回码 {ret}",
                            flush=True,
                        )

                on_ck = _on_ck_saved
            final, rounds = _scroll_until_stall(
                driver,
                metric_fn=metric_fn,
                pixels=pixels,
                pause_sec=pause_sec,
                max_rounds=max_rounds,
                stall_strategy=stall_strategy,
                stall_threshold=stall_threshold,
                long_wait_sec=long_wait_sec,
                long_wait_cycles=long_wait_cycles,
                bottom_epsilon_px=bottom_epsilon_px,
                long_wait_only_when_at_bottom=long_wait_only_when_at_bottom,
                inner_scroll_min_overflow_px=inner_scroll_min_overflow_px,
                inner_scroll_max_nodes=inner_scroll_max_nodes,
                checkpoint_path=ck_path,
                checkpoint_every_rounds=checkpoint_every,
                on_checkpoint_saved=on_ck,
            )
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"{slug}_{ts}.html" if use_ts else f"{slug}.html"
            dest = out_dir / fname
            html = driver.page_source
            _atomic_write_text(dest, html, encoding="utf-8")
            print(
                f"[final] 已保存（结束滚动·最终快照） {dest}  photo_links: 初始≈{before} → 最终≈{final}  滚动轮数={rounds}"
                + ("  (max_rounds 无上限)" if max_rounds is None else ""),
                flush=True,
            )
    finally:
        _dispose_chrome()
        signal.signal(signal.SIGINT, old_int)
        if old_term is not None:
            signal.signal(signal.SIGTERM, old_term)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
