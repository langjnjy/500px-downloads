# go-downloader

从 URL 列表读入，HTTP 并发下载到 `media_dir_template` 指定目录，并按 UTC 日写 metadata JSONL。默认 `config/download-http.yaml` 已与 `scripts/crawl_unsplash.py` 的 `output/media`、`output/metadata/{date}.metadata.jsonl` 对齐（S3 媒体 key：`photo-download/<UTC日>/<文件>`，`aws-s3` profile、桶 `nobodies-ai-data`，不设 `endpoint_url`）；另有等价示例 `config/download-unsplash.yaml`。

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

**运行（在仓库根目录下，默认已使用 `go-downloader/config/download-http.yaml`，一般无需 `-config`）：**
```bash
./go-downloader/download
nohup ./go-downloader/download > /dev/null 2>&1 &
# 显式指定配置时：
./go-downloader/download -config go-downloader/config/download-http.yaml
# 与 download-http 等价的 project_root 示例（在 go-downloader 子目录内运行时可选用）：
./go-downloader/download -config go-downloader/config/download-unsplash.yaml
```
