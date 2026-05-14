# go-uploader（500px-downloads）

从本地 `output/media` 扫描并上传到 AWS S3（`s3://nobodies-ai-data/500px-downloads/media/`）。  
本批上传成功后，按 basename 将 `output/metadata/*.metadata.jsonl` 中对应行的 `image_key` 更新为 `500px-downloads/media/<文件名>`。

## 构建

在仓库根目录：

```bash
go build -C go-uploader -o go-uploader/uploader ./cmd/uploader
```

在 `go-uploader` 目录内：

```bash
cd go-uploader && go build -o uploader ./cmd/uploader
```

## 运行

```bash
./go-uploader/uploader -config go-uploader/config/upload-500px.yaml
```

日志目录：`output/logs/upload/`（与 main 中 `teeLogToFile` 一致）。
