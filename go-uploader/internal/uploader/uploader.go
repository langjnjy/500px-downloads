package uploader

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/500px-downloads/go-uploader/internal/config"
	"github.com/500px-downloads/go-uploader/internal/metadatasync"
)

// Uploader 上传器
type Uploader struct {
	cfg              *config.Config
	s3Client         *s3.S3
	stats            *Stats
	batchBasenames   map[string]struct{}
	batchBasenamesMu sync.Mutex
}

// Record 上传记录
type Record struct {
	ImageKey  string
	Category  string
	LocalPath string
}

// Stats 统计信息
type Stats struct {
	Scanned     int64
	Uploaded    int64
	Failed      int64
	Deleted     int64
	mu          sync.RWMutex
	perCategory map[string]*CategoryStats
}

// CategoryStats 分类统计
type CategoryStats struct {
	Scanned  int64
	Uploaded int64
	Failed   int64
	Deleted  int64
}

// NewUploader 创建新的上传器
func NewUploader(cfg *config.Config) (*Uploader, error) {
	awsCfg := aws.Config{
		Region:      aws.String(cfg.Wasabi.RegionName),
		Credentials: credentials.NewSharedCredentials("", cfg.Wasabi.Profile),
	}
	if strings.TrimSpace(cfg.Wasabi.EndpointURL) != "" {
		awsCfg.Endpoint = aws.String(cfg.Wasabi.EndpointURL)
		awsCfg.S3ForcePathStyle = aws.Bool(true)
	}
	sess, err := session.NewSessionWithOptions(session.Options{
		Config:  awsCfg,
		Profile: cfg.Wasabi.Profile,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 AWS session 失败: %w", err)
	}
	httpClient := &http.Client{
		Timeout: time.Duration(maxInt(cfg.Wasabi.SDKReadTimeoutSec, 0)) * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          cfg.Wasabi.MaxPoolConnections,
			MaxIdleConnsPerHost:   cfg.Wasabi.MaxPoolConnections,
			ResponseHeaderTimeout: time.Duration(maxInt(cfg.Wasabi.SDKReadTimeoutSec, 0)) * time.Second,
			TLSHandshakeTimeout:   time.Duration(maxInt(cfg.Wasabi.SDKConnectTimeoutSec, 0)) * time.Second,
		},
	}
	s3Client := s3.New(sess, &aws.Config{
		HTTPClient:                    httpClient,
		S3DisableContentMD5Validation: aws.Bool(true),
	})

	return &Uploader{
		cfg:      cfg,
		s3Client: s3Client,
		stats: &Stats{
			perCategory: make(map[string]*CategoryStats),
		},
	}, nil
}

// BeginRewriteBatch 开始一批上传：若配置了 metadata_image_key_prefix，则跟踪本批成功上传的文件名以便后续重写 JSONL。
func (u *Uploader) BeginRewriteBatch() {
	if strings.TrimSpace(u.cfg.MetadataImageKeyPrefix) == "" {
		return
	}
	u.batchBasenamesMu.Lock()
	u.batchBasenames = make(map[string]struct{})
	u.batchBasenamesMu.Unlock()
}

// FinishRewriteBatch 将本批已上传文件的 basename 对应行在 metadata_dir 的 *.metadata.jsonl 中 image_key 写为 prefix/basename。
func (u *Uploader) FinishRewriteBatch() error {
	prefix := strings.TrimSpace(u.cfg.MetadataImageKeyPrefix)
	if prefix == "" {
		return nil
	}
	u.batchBasenamesMu.Lock()
	m := u.batchBasenames
	u.batchBasenames = nil
	u.batchBasenamesMu.Unlock()
	if len(m) == 0 {
		return nil
	}
	return metadatasync.RewriteImageKeysForBasenames(u.cfg.MetadataDir, prefix, m)
}

// UploadRecord 上传单个记录
func (u *Uploader) UploadRecord(ctx context.Context, record Record, localFilePath string) error {
	s3Key := record.ImageKey

	// 检查本地文件是否存在
	if _, err := os.Stat(localFilePath); os.IsNotExist(err) {
		return nil
	}

	u.incScanned(record.Category)

	// 检查是否 dry-run
	if u.cfg.Upload.DryRun {
		return nil
	}

	// 上传（与 scripts/download.py 的 S3Client.put_file 对齐：aws s3 cp）
	err := u.uploadWithRetry(ctx, localFilePath, s3Key)
	if err != nil {
		u.incFailed(record.Category)
		fmt.Printf("[upload-failed] category=%s local=%s s3=%s err=%v\n", record.Category, localFilePath, s3Key, err)
		return fmt.Errorf("上传失败: %w", err)
	}

	u.incUploaded(record.Category)

	u.batchBasenamesMu.Lock()
	if u.batchBasenames != nil {
		u.batchBasenames[filepath.Base(localFilePath)] = struct{}{}
	}
	u.batchBasenamesMu.Unlock()

	// 上传成功后删除本地文件
	if u.cfg.Wasabi.DeleteAfterUpload {
		if err := os.Remove(localFilePath); err == nil {
			u.incDeleted(record.Category)
		} else {
			fmt.Printf("[delete-failed] category=%s local=%s err=%v\n", record.Category, localFilePath, err)
		}
	}

	return nil
}

// uploadWithRetry 带重试的上传（与 scripts/download.py S3Client.put_file 对齐）
func (u *Uploader) uploadWithRetry(ctx context.Context, localFilePath, s3Key string) error {
	attempts := 1 + u.cfg.Wasabi.UploadRetries
	if attempts < 1 {
		attempts = 1
	}

	base := u.cfg.Wasabi.UploadRetryBaseSec
	capSec := u.cfg.Wasabi.UploadRetryMaxSec
	lastErr := ""

	for attempt := 0; attempt < attempts; attempt++ {
		file, err := os.Open(localFilePath)
		if err != nil {
			lastErr = err.Error()
		} else {
			_, err = u.s3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
				Bucket: aws.String(u.cfg.Wasabi.Bucket),
				Key:    aws.String(s3Key),
				Body:   file,
			})
			_ = file.Close()
		}
		if err == nil && lastErr == "" {
			return nil
		}
		if err != nil {
			lastErr = err.Error()
		}
		if attempt < attempts-1 {
			delay := math.Min(capSec, base*math.Pow(2, float64(attempt))) + rand.Float64()*0.15
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(delay * float64(time.Second))):
			}
		}
	}
	return fmt.Errorf("%s", lastErr)
}

// PrintStats 打印统计信息
func (u *Uploader) PrintStats() {
	u.stats.mu.RLock()
	defer u.stats.mu.RUnlock()

	totalScanned := atomic.LoadInt64(&u.stats.Scanned)
	totalUploaded := atomic.LoadInt64(&u.stats.Uploaded)
	totalFailed := atomic.LoadInt64(&u.stats.Failed)
	totalDeleted := atomic.LoadInt64(&u.stats.Deleted)

	fmt.Printf("=== 上传统计 ===\n")
	fmt.Printf("总扫描: %d\n", totalScanned)
	fmt.Printf("总上传: %d\n", totalUploaded)
	fmt.Printf("总失败: %d\n", totalFailed)
	fmt.Printf("总删除: %d\n", totalDeleted)

	if len(u.stats.perCategory) > 0 {
		fmt.Printf("\n分类统计:\n")
		for category, stats := range u.stats.perCategory {
			fmt.Printf("  %s: 扫描=%d 上传=%d 失败=%d 删除=%d\n",
				category,
				atomic.LoadInt64(&stats.Scanned),
				atomic.LoadInt64(&stats.Uploaded),
				atomic.LoadInt64(&stats.Failed),
				atomic.LoadInt64(&stats.Deleted))
		}
	}
	fmt.Printf("\n")
}

// 统计方法
func (u *Uploader) incScanned(category string) {
	atomic.AddInt64(&u.stats.Scanned, 1)
	u.getCategoryStats(category).incScanned()
}

func (u *Uploader) incUploaded(category string) {
	atomic.AddInt64(&u.stats.Uploaded, 1)
	u.getCategoryStats(category).incUploaded()
}

func (u *Uploader) incFailed(category string) {
	atomic.AddInt64(&u.stats.Failed, 1)
	u.getCategoryStats(category).incFailed()
}

func (u *Uploader) incDeleted(category string) {
	atomic.AddInt64(&u.stats.Deleted, 1)
	u.getCategoryStats(category).incDeleted()
}

func (u *Uploader) getCategoryStats(category string) *CategoryStats {
	u.stats.mu.Lock()
	defer u.stats.mu.Unlock()

	if stats, ok := u.stats.perCategory[category]; ok {
		return stats
	}

	stats := &CategoryStats{}
	u.stats.perCategory[category] = stats
	return stats
}

func (cs *CategoryStats) incScanned() {
	atomic.AddInt64(&cs.Scanned, 1)
}

func (cs *CategoryStats) incUploaded() {
	atomic.AddInt64(&cs.Uploaded, 1)
}

func (cs *CategoryStats) incFailed() {
	atomic.AddInt64(&cs.Failed, 1)
}

func (cs *CategoryStats) incDeleted() {
	atomic.AddInt64(&cs.Deleted, 1)
}

func maxInt(v, floor int) int {
	if v < floor {
		return floor
	}
	return v
}
