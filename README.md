# 500px-downloads

从 **500px** 站点拉取摄影师名录与用户发现结果（GraphQL `userSearch` 等），输出文本列表供后续下载工具使用。仓库内含可选的 **go-downloader** 子目录（Go HTTP 下载器，配置可与其它流水线对齐）。

> **合规提示**：请自行遵守 500px 服务条款、许可与速率限制；本仓库仅为技术示例。

## 目录结构

```
500px-downloads/
├── config/
│   ├── 500px_cookies.txt                      # Netscape Cookie（勿提交到 git；可用 export 脚本生成）
│   ├── 500px_usersearch_filter_ids.yaml       # 过滤维度 id 与探测统计
│   ├── 500px_usersearch_filter_batches.yaml   # 批量任务定义（自行维护）
│   ├── 500px_usersearch_filter_batches.example.yaml
│   └── discover_500px_users_gallery_dl.yaml   # gallery-dl 用户发现脚本默认配置
├── scripts/
│   ├── fetch_500px_photographers_all.py       # photographers/all 对应 userSearch 分页
│   ├── run_500px_photographer_filter_batches.py  # 多组 filter 批量调用上一脚本
│   ├── discover_500px_users_gallery_dl.py     # gallery-dl 拉用户作品 JSON 抠用户名
│   ├── gallery_dl_500px_graphql_vendor.py     # GraphQL 片段（与 gallery-dl 同源 vendored）
│   └── export_chrome_cookies_500px.py         # Chrome → Netscape Cookie
├── output/                                    # 默认被 .gitignore 忽略
│   └── 500px_photographers_all_usernames.txt  # 示例产出路径
└── go-downloader/                             # 可选：通用 HTTP 下载 CLI
```

## 依赖

| 组件 | 用途 |
|------|------|
| Python 3.10+ | 脚本运行时 |
| `requests`、`PyYAML` | HTTP 与 YAML 配置 |
| `browser-cookie3` | （可选）Chrome Cookie 导出脚本 |

安装：

```bash
pip install -r requirements.txt
```

从仓库根目录运行脚本（脚本内以仓库根解析 `config/`、`output/`）。

## 常用命令

```bash
# 导出 Cookie（需本机 Chrome 已登录 500px）
python3 scripts/export_chrome_cookies_500px.py -o config/500px_cookies.txt

# 抓取 photographers/all 用户名录（示例）
python3 scripts/fetch_500px_photographers_all.py \
  --cookies config/500px_cookies.txt \
  -o output/500px_photographers_all_usernames.txt

# 按 YAML 批量多组 filter 追加写入同一输出
python3 scripts/run_500px_photographer_filter_batches.py \
  --batches-yaml config/500px_usersearch_filter_batches.yaml
```

## go-downloader

见 `go-downloader/README.md`。在项目根执行 `go build -o download ./cmd/download`（路径以该目录内说明为准）。
