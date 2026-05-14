package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config 主配置结构
type Config struct {
	OutputDir              string       `yaml:"output_dir"`
	DownloadsDir           string       `yaml:"downloads_dir"`
	PipelineRootDir        string       `yaml:"pipeline_root_dir"`
	MetadataDir            string       `yaml:"metadata_dir"`
	MetadataImageKeyPrefix string       `yaml:"metadata_image_key_prefix"` // 非空时：本批成功上传后，将 *.metadata.jsonl 中对应 basename 的 image_key 重写为该前缀 + "/" + basename
	Wasabi                 WasabiConfig `yaml:"wasabi"`
	Upload                 UploadConfig `yaml:"upload"`
	projectRoot            string       // 加载时解析，用于发现 category
	ProjectRoot            string       `yaml:"-"` // 项目根目录（与 projectRoot 一致，供 main 写日志等）
}

// WasabiConfig Wasabi S3 配置
type WasabiConfig struct {
	Enabled              bool    `yaml:"enabled"`
	Profile              string  `yaml:"profile"`
	EndpointURL          string  `yaml:"endpoint_url"`
	Bucket               string  `yaml:"bucket"`
	RegionName           string  `yaml:"region_name"`
	KeyPrefix            string  `yaml:"key_prefix"`
	UploadWorkers        int     `yaml:"upload_workers"`
	MaxQueueSize         int     `yaml:"max_queue_size"`
	MaxPoolConnections   int     `yaml:"max_pool_connections"`
	DeleteAfterUpload    bool    `yaml:"delete_after_upload"`
	UploadRetries        int     `yaml:"upload_retries"`
	UploadRetryBaseSec   float64 `yaml:"upload_retry_base_sec"`
	UploadRetryMaxSec    float64 `yaml:"upload_retry_max_sec"`
	SDKConnectTimeoutSec int     `yaml:"sdk_connect_timeout_sec"`
	SDKReadTimeoutSec    int     `yaml:"sdk_read_timeout_sec"`

	// 兼容旧字段
	MaxRetryAttempts     int `yaml:"max_retry_attempts"`
	RetryBaseSleepMs     int `yaml:"retry_base_sleep_ms"`
	RetryMaxSleepMs      int `yaml:"retry_max_sleep_ms"`
	CLIConnectTimeoutSec int `yaml:"cli_connect_timeout_sec"`
	CLIReadTimeoutSec    int `yaml:"cli_read_timeout_sec"`
}

// UploadConfig 上传行为配置
type UploadConfig struct {
	DryRun             bool     `yaml:"dry_run"`
	// ScanRoot 非空时：扁平扫描该目录（如 output/media），忽略 category / discover_categories；S3 key 为 key_prefix/相对路径
	ScanRoot           string   `yaml:"scan_root"`
	Category           string   `yaml:"category"`            // 单 category 时使用
	Categories         []string `yaml:"categories"`          // 多 category 时使用；非空则共享 worker 池
	DiscoverCategories bool     `yaml:"discover_categories"` // true 时从 output/downloads/ 子目录发现 category
	IncludeCategories  []string `yaml:"include_categories"`  // uploader 独立 include
	ExcludeCategories  []string `yaml:"exclude_categories"`  // uploader 独立 exclude
	Loop               bool     `yaml:"loop"`                // 持续轮询不退出
	UntilEmpty         bool     `yaml:"until_empty"`         // 多轮扫描上传，本轮扫到 0 个文件则退出
	PollSeconds        int      `yaml:"poll_seconds"`
	IdlePollSeconds    int      `yaml:"idle_poll_seconds"`
	ReportSeconds      int      `yaml:"report_seconds"`
}

// LoadConfig 从文件加载配置
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 解析路径（相对于配置文件所在目录的父目录，即项目根目录）
	absConfigPath, _ := filepath.Abs(configPath)
	absConfigDir := filepath.Dir(absConfigPath)
	projectRoot := filepath.Dir(absConfigDir)
	if filepath.Base(absConfigDir) == "config" && filepath.Base(projectRoot) == "go-uploader" {
		projectRoot = filepath.Dir(projectRoot)
	}
	cfg.projectRoot = projectRoot
	cfg.ProjectRoot = projectRoot

	// 解析路径
	if !filepath.IsAbs(cfg.OutputDir) {
		if strings.HasPrefix(cfg.OutputDir, "../") {
			cfg.OutputDir = filepath.Join(projectRoot, strings.TrimPrefix(cfg.OutputDir, "../"))
		} else {
			cfg.OutputDir = filepath.Join(absConfigDir, cfg.OutputDir)
		}
	}
	if !filepath.IsAbs(cfg.MetadataDir) {
		if strings.HasPrefix(cfg.MetadataDir, "../") {
			cfg.MetadataDir = filepath.Join(projectRoot, strings.TrimPrefix(cfg.MetadataDir, "../"))
		} else {
			cfg.MetadataDir = filepath.Join(absConfigDir, cfg.MetadataDir)
		}
	}
	if strings.TrimSpace(cfg.PipelineRootDir) == "" {
		cfg.PipelineRootDir = "pipeline"
	}

	// scan_root：绝对路径直接使用；否则相对于项目根目录
	if strings.TrimSpace(cfg.Upload.ScanRoot) != "" {
		sr := strings.TrimSpace(cfg.Upload.ScanRoot)
		if filepath.IsAbs(sr) {
			cfg.Upload.ScanRoot = filepath.Clean(sr)
		} else {
			cfg.Upload.ScanRoot = filepath.Clean(filepath.Join(projectRoot, sr))
		}
		cfg.Upload.DiscoverCategories = false
	}

	// 并发默认值：S3 上传为 I/O 密集，显著高于核数；封顶减轻内存与连接开销
	if cfg.Wasabi.UploadWorkers <= 0 {
		n := runtime.NumCPU() * 48
		if n > 512 {
			n = 512
		}
		if n < 16 {
			n = 16
		}
		cfg.Wasabi.UploadWorkers = n
	}
	if cfg.Wasabi.MaxQueueSize <= 0 {
		cfg.Wasabi.MaxQueueSize = cfg.Wasabi.UploadWorkers * 8
	}
	if cfg.Wasabi.MaxPoolConnections <= 0 {
		cfg.Wasabi.MaxPoolConnections = cfg.Wasabi.UploadWorkers
	}
	if cfg.Wasabi.KeyPrefix == "" {
		cfg.Wasabi.KeyPrefix = "500px-downloads/media"
	}
	if strings.TrimSpace(cfg.Wasabi.RegionName) == "" {
		cfg.Wasabi.RegionName = "us-west-2"
	}
	// 与 scripts/download.py 的 s3 配置语义对齐
	if cfg.Wasabi.UploadRetries == 0 && cfg.Wasabi.MaxRetryAttempts > 0 {
		// 旧字段是总尝试次数；新字段是额外重试次数
		if cfg.Wasabi.MaxRetryAttempts > 1 {
			cfg.Wasabi.UploadRetries = cfg.Wasabi.MaxRetryAttempts - 1
		}
	}
	if cfg.Wasabi.UploadRetryBaseSec == 0 && cfg.Wasabi.RetryBaseSleepMs > 0 {
		cfg.Wasabi.UploadRetryBaseSec = float64(cfg.Wasabi.RetryBaseSleepMs) / 1000.0
	}
	if cfg.Wasabi.UploadRetryMaxSec == 0 && cfg.Wasabi.RetryMaxSleepMs > 0 {
		cfg.Wasabi.UploadRetryMaxSec = float64(cfg.Wasabi.RetryMaxSleepMs) / 1000.0
	}
	if cfg.Wasabi.UploadRetries < 0 {
		cfg.Wasabi.UploadRetries = 0
	}
	if cfg.Wasabi.UploadRetryBaseSec <= 0 {
		cfg.Wasabi.UploadRetryBaseSec = 0.5
	}
	if cfg.Wasabi.UploadRetryMaxSec <= 0 {
		cfg.Wasabi.UploadRetryMaxSec = 60
	}
	if cfg.Wasabi.SDKConnectTimeoutSec <= 0 && cfg.Wasabi.CLIConnectTimeoutSec > 0 {
		cfg.Wasabi.SDKConnectTimeoutSec = cfg.Wasabi.CLIConnectTimeoutSec
	}
	if cfg.Wasabi.SDKReadTimeoutSec <= 0 && cfg.Wasabi.CLIReadTimeoutSec > 0 {
		cfg.Wasabi.SDKReadTimeoutSec = cfg.Wasabi.CLIReadTimeoutSec
	}
	if cfg.Wasabi.SDKConnectTimeoutSec <= 0 {
		cfg.Wasabi.SDKConnectTimeoutSec = 60
	}
	if cfg.Wasabi.SDKReadTimeoutSec <= 0 {
		cfg.Wasabi.SDKReadTimeoutSec = 300
	}
	if cfg.Upload.PollSeconds == 0 {
		cfg.Upload.PollSeconds = 30
	}
	if cfg.Upload.IdlePollSeconds == 0 {
		cfg.Upload.IdlePollSeconds = 600
	}
	if cfg.Upload.ReportSeconds == 0 {
		cfg.Upload.ReportSeconds = 600
	}
	// 默认多 category：未指定 scan_root、未指定 category/categories 且未显式关闭 discover 时，从 output/pipeline/ 发现
	if cfg.Upload.ScanRoot == "" && cfg.Upload.Category == "" && len(cfg.Upload.Categories) == 0 && !cfg.Upload.DiscoverCategories {
		cfg.Upload.DiscoverCategories = true
	}

	return &cfg, nil
}

// PollInterval 返回轮询间隔
func (c *Config) PollInterval() time.Duration {
	return time.Duration(c.Upload.PollSeconds) * time.Second
}

// IdlePollInterval 返回空闲轮询间隔
func (c *Config) IdlePollInterval() time.Duration {
	return time.Duration(c.Upload.IdlePollSeconds) * time.Second
}

// ReportInterval 返回统计报告间隔
func (c *Config) ReportInterval() time.Duration {
	return time.Duration(c.Upload.ReportSeconds) * time.Second
}

// UploadCategories 返回本次要上传的 category 列表（与 discover.yaml 的 category 一致，用于本地目录与 S3 路径）。多 category 时共享 worker 池。
func (c *Config) UploadCategories() ([]string, bool) {
	if strings.TrimSpace(c.Upload.ScanRoot) != "" {
		return []string{"media"}, true
	}
	if c.Upload.DiscoverCategories {
		downloadsRoot := filepath.Join(c.OutputDir, c.PipelineRootDir)
		entries, err := os.ReadDir(downloadsRoot)
		if err != nil {
			return nil, false
		}
		var list []string
		for _, e := range entries {
			if e.IsDir() && e.Name() != "" && e.Name()[0] != '.' {
				list = append(list, e.Name())
			}
		}
		if len(c.Upload.IncludeCategories) > 0 || len(c.Upload.ExcludeCategories) > 0 {
			m := map[string]bool{}
			for _, x := range list {
				m[x] = true
			}
			exclude := map[string]bool{}
			for _, x := range c.Upload.ExcludeCategories {
				x = strings.TrimSpace(x)
				if x != "" {
					exclude[x] = true
				}
			}
			out := make([]string, 0, len(list))
			if len(c.Upload.IncludeCategories) > 0 {
				for _, x := range c.Upload.IncludeCategories {
					x = strings.TrimSpace(x)
					if x != "" && m[x] && !exclude[x] {
						out = append(out, x)
					}
				}
			} else {
				for _, x := range list {
					if !exclude[x] {
						out = append(out, x)
					}
				}
			}
			list = out
		}
		return list, len(list) > 0
	}
	if len(c.Upload.Categories) > 0 {
		return c.Upload.Categories, true
	}
	if c.Upload.Category != "" {
		return []string{c.Upload.Category}, true
	}
	return nil, false
}
