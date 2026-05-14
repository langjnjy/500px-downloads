#!/usr/bin/env python3
"""从 ``save_500px_scroll_html.py`` 保存的 500px 页面 HTML 中提取每张图对应的上传者 uid（username）。

页面里上传者链接形如::

  <a class="Elements__UploaderWrapper-..." href="/steven_leftjoke">

``/photo/<id>/<slug>`` 中的 slug 是作品标题 slug，**不是** uid。

配置::

  默认读取 ``config/extract_500px_usernames_from_saved_html.yaml``（可用 ``--config`` 指定）。

  命令行传入的 HTML 路径（若有）**覆盖** YAML 里的 ``inputs``；
  ``-o`` / ``--append-only`` 若给出则**覆盖** YAML 中对应项。

用法::

  python3 scripts/extract_500px_usernames_from_saved_html.py
  python3 scripts/extract_500px_usernames_from_saved_html.py --config path/to.yaml
  python3 scripts/extract_500px_usernames_from_saved_html.py output/html/foo.html -o out.txt
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set

import yaml

# 500px 单段路径里常见非用户页（避免误匹配其它 href="/xxx"）
_RESERVED = frozenset(
    {
        "popular",
        "fresh",
        "editors",
        "upcoming",
        "explore",
        "forYou",
        "photo_stories",
        "photographers",
        "quests",
        "login",
        "signup",
        "search",
        "settings",
        "notifications",
        "licensing",
        "about",
        "apps",
        "blog",
        "help",
        "developers",
        "legal",
        "privacy",
        "tos",
        "downloadApp",
        "upgrade",
        "getApp",
        "favicon.ico",
        "p",
        "q",
        "c",
        "u",
        "t",
        "i",
        "m",
        "v",
        "x",
        "y",
        "z",
        "en",
        "jp",
        "cn",
        "undefined",
        "null",
        "true",
        "false",
    }
)

_RE_UPLOADER_HREF = re.compile(
    r'Elements__UploaderWrapper[^>]{0,400}?href="/([^"/]+)"',
    re.IGNORECASE,
)
_RE_UPLOADER_HREF_ABS = re.compile(
    r'Elements__UploaderWrapper[^>]{0,400}?href="https://500px\.com/([^"/]+)"',
    re.IGNORECASE,
)

DEFAULT_CONFIG_RELPATH = "config/extract_500px_usernames_from_saved_html.yaml"


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_yaml(path: Path) -> Dict[str, Any]:
    obj = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(obj, dict):
        raise ValueError("YAML 顶层须为 mapping")
    return obj


def _valid_username(s: str) -> bool:
    s = s.strip()
    if len(s) < 2 or len(s) > 80:
        return False
    if s in _RESERVED:
        return False
    if s.startswith(".") or "/" in s:
        return False
    if not re.match(r"^[a-zA-Z0-9_.-]+$", s):
        return False
    return True


def extract_usernames_from_html(html: str) -> Set[str]:
    out: Set[str] = set()
    for rx in (_RE_UPLOADER_HREF, _RE_UPLOADER_HREF_ABS):
        for m in rx.finditer(html):
            u = m.group(1).strip()
            if _valid_username(u):
                out.add(u)
    return out


def _read_existing_usernames(path: Path) -> Set[str]:
    if not path.is_file():
        return set()
    s: Set[str] = set()
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if line and not line.startswith("#"):
            s.add(line)
    return s


def _resolve_repo_path(root: Path, p: str) -> Path:
    pp = Path(p).expanduser()
    return pp if pp.is_absolute() else (root / pp).resolve()


def merge_usernames_from_html_files(
    root: Path,
    extract_cfg: Path,
    html_files: Sequence[Path],
    *,
    output_path: Optional[Path] = None,
    append_only: Optional[bool] = None,
) -> int:
    """按 YAML 中的 ``output`` / ``append_only``（可被参数覆盖），从给定 HTML 提取用户名并合并写入。

    供 ``save_500px_scroll_html.py`` 在每次 checkpoint 后调用。返回 ``0`` 表示逻辑成功，``2`` 表示配置或输入无效。
    """
    if not extract_cfg.is_file():
        print(f"找不到 extract 配置: {extract_cfg}", file=sys.stderr)
        return 2
    cfg = _load_yaml(extract_cfg)
    files = [p.resolve() for p in html_files if p.is_file()]
    if not files:
        print("merge_usernames_from_html_files: 无有效 HTML 文件", file=sys.stderr)
        return 2
    out_rel = str(cfg.get("output") or "output/500px_photographers_all_usernames.txt").strip()
    out_path = (
        output_path.expanduser().resolve()
        if output_path is not None
        else _resolve_repo_path(root, out_rel)
    )
    use_append = bool(cfg.get("append_only", False)) if append_only is None else append_only
    found: Set[str] = set()
    for fp in files:
        html = fp.read_text(encoding="utf-8", errors="replace")
        n = extract_usernames_from_html(html)
        print(f"{fp.name}: 提取到 {len(n)} 个 uid（去重后）", flush=True)
        found |= n
    existing = _read_existing_usernames(out_path)
    if use_append:
        new_only = sorted(found - existing)
        if new_only:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open("a", encoding="utf-8") as fh:
                for u in new_only:
                    fh.write(u + "\n")
        print(
            f"追加 {len(new_only)} 行 → {out_path}（原有 {len(existing)}，HTML 中共 {len(found)}）",
            flush=True,
        )
        return 0
    merged = existing | found
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(sorted(merged)) + "\n", encoding="utf-8")
    print(
        f"已写入 {out_path} 共 {len(merged)} 行（原 {len(existing)} + HTML 新 {len(found)}，并集去重后排序）",
        flush=True,
    )
    return 0


def _expand_inputs(paths: Iterable[str], root: Path) -> List[Path]:
    out: List[Path] = []
    for p in paths:
        if not isinstance(p, str) or not str(p).strip():
            continue
        pp = _resolve_repo_path(root, str(p).strip())
        sp = str(pp)
        if "*" in sp or "?" in sp:
            parent = pp.parent
            for g in sorted(parent.glob(pp.name)):
                if g.is_file():
                    out.append(g)
        else:
            if pp.is_file():
                out.append(pp)
            else:
                print(f"跳过（非文件）: {pp}", file=sys.stderr)
    return out


def main() -> int:
    root = _repo_root()
    ap = argparse.ArgumentParser(description="从保存的 500px HTML 提取上传者 username 并写入清单")
    ap.add_argument(
        "--config",
        type=Path,
        default=root / DEFAULT_CONFIG_RELPATH,
        help=f"YAML 配置（默认 {DEFAULT_CONFIG_RELPATH}）",
    )
    ap.add_argument(
        "html_files",
        nargs="*",
        help="覆盖 YAML 中的 inputs；留空则完全使用 YAML",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="覆盖 YAML 中的 output",
    )
    ap.add_argument(
        "--append-only",
        action="store_true",
        help="覆盖 YAML：仅追加新用户名",
    )
    args = ap.parse_args()

    cfg_path = args.config
    if not cfg_path.is_absolute():
        cfg_path = (root / cfg_path).resolve()

    cfg: Dict[str, Any] = {}
    if cfg_path.is_file():
        cfg = _load_yaml(cfg_path)
        print(f"配置: {cfg_path}", flush=True)
    elif args.html_files or args.output is not None:
        print(f"提示: 未找到配置文件 {cfg_path}，仅使用命令行参数", flush=True)
    else:
        print(f"找不到配置且无 HTML 参数: {cfg_path}", file=sys.stderr)
        return 2

    raw_inputs = list(args.html_files) if args.html_files else None
    if raw_inputs is None:
        inp = cfg.get("inputs")
        if inp is None:
            inp = cfg.get("html_files")
        if not isinstance(inp, list):
            inp = []
        raw_inputs = [str(x).strip() for x in inp if str(x).strip()]

    if not raw_inputs:
        print("inputs 为空：请在 YAML 中配置 inputs，或在命令行传入 HTML 路径", file=sys.stderr)
        return 2

    files = _expand_inputs(raw_inputs, root)
    if not files:
        print("没有可读的 HTML 文件", file=sys.stderr)
        return 2

    return merge_usernames_from_html_files(
        root,
        cfg_path,
        files,
        output_path=args.output,
        append_only=True if args.append_only else None,
    )


if __name__ == "__main__":
    raise SystemExit(main())
