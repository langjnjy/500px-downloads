# go-downloader

从 URL 列表读入，HTTP 并发下载到 `media_dir_template` 指定目录，并按 UTC 日写 metadata JSONL。默认在未传 `-config` 时，若存在 `go-downloader/config/download-500px.yaml` 则优先使用（本仓库 500px）；否则回退到 `download-http.yaml`（与 `scripts/crawl_unsplash.py` 的 `output/media`、`output/metadata/{date}.metadata.jsonl` 对齐）。另有 `config/download-unsplash.yaml`、`config/download-500px.yaml` 示例。

## 构建与运行（与 go-discover / go-extractor 一致）

**在模块目录内构建：**
```bash
cd go-downloader
go build -C . -o ../go-downloader/download ./cmd/download
# 或：go build -o download ./cmd/download（二进制在 go-downloader/download）
```

**从仓库根目录构建：**
```bash
go build -C go-downloader -o go-downloader/download ./cmd/download
```

**运行（在仓库根目录下，无 `-config` 时先试 500px 配置）：**
```bash
./go-downloader/download
nohup ./go-downloader/download > /dev/null 2>&1 &
./go-downloader/download -config go-downloader/config/download-http.yaml
./go-downloader/download -config go-downloader/config/download-unsplash.yaml
./go-downloader/download -config go-downloader/config/download-500px.yaml
```
