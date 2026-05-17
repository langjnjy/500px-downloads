package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config 下载配置（legacy 扁平字段 + pipeline 合并结果）
type Config struct {
	Input                     string             `yaml:"input"`
	URLsPathTemplate          string             `yaml:"urls_path_template"`
	FilteredOut               string             `yaml:"filtered_out"`
	OutputDir                 string             `yaml:"output_dir"`
	MediaDirTemplate          string             `yaml:"media_dir_template"`
	MetadataDir               string             `yaml:"metadata_dir"`
	Category                  string             `yaml:"category"`
	ExtractDir                string             `yaml:"extract_dir"`
	ProjectRoot               string             `yaml:"-"`
	Workers                   int                `yaml:"workers"`
	Retries                   int                `yaml:"retries"`
	Timeout                   int                `yaml:"timeout"`
	ProgressInterval          int                `yaml:"progress_interval"`
	MetadataLinesPerFile      int                `yaml:"metadata_lines_per_file"`
	FailedFileName            string             `yaml:"failed_file_name"`
	DiskGlobFallback          bool               `yaml:"disk_glob_fallback"`
	ImageKeyPrefix            string             `yaml:"image_key_prefix"`
	ImageKeyStyle             string             `yaml:"image_key_style"`
	InputMode                 string             `yaml:"input_mode"`
	ExtractMetadataPathTemplate string           `yaml:"extract_metadata_path_template"`
	ExtractMetadataInputs       []string         `yaml:"-"`
	MediaUpscaleDirTemplate   string             `yaml:"media_upscale_dir_template"`
	MediaUpscaleDir           string             `yaml:"-"`
	ResolutionMinShort        int                `yaml:"resolution_min_short"`
	ResolutionMinLong         int                `yaml:"resolution_min_long"`
	ExtractMetadataResolutionPolicy string       `yaml:"extract_metadata_resolution_policy"`
	UpscalePython             string             `yaml:"upscale_python"`
	UpscaleScriptTemplate     string             `yaml:"upscale_script"`
	UpscaleScript             string             `yaml:"-"`
	UpscaleWorkers            int                `yaml:"upscale_workers"`
	SeenDB                    SeenDBConfig       `yaml:"seen_db"`
	CheckpointInterval        int                `yaml:"checkpoint_interval"`
	MinSidePixels             int                `yaml:"min_side_pixels"`
	URLsDeltaDirTemplate      string             `yaml:"urls_delta_dir_template"`
	MetadataDailyPathTemplate string             `yaml:"metadata_daily_path_template"`
	MetadataGlobPathTemplate  string             `yaml:"metadata_glob_path_template"`
	CheckpointPathTemplate    string             `yaml:"checkpoint_path_template"`
	FailedURLsPathTemplate    string             `yaml:"failed_urls_path_template"`
	SourceS3                  SourceS3Config     `yaml:"source_s3"`
	IncludeCategories         []string           `yaml:"include_categories"`
	ExcludeCategories         []string           `yaml:"exclude_categories"`
	SkipSuffixes              []string           `yaml:"skip_suffixes"`
	UseUserAgents             bool               `yaml:"use_user_agents"`
	HTTPUserAgent             string             `yaml:"http_user_agent"`
	HTTPPoolMaxsize           int                `yaml:"http_pool_maxsize"`
	ProxiesYAML               string             `yaml:"proxies_yaml"`
	UseProxy                  bool               `yaml:"use_proxy"`
	MetadataFlushEvery        int                `yaml:"metadata_flush_every"`
	MetadataFlushIntervalSec  int                `yaml:"metadata_flush_interval_sec"`
	Loop                      bool               `yaml:"loop"`
	LoopIdlePollSeconds       int                `yaml:"loop_idle_poll_seconds"`
	RetryFailed               bool               `yaml:"retry_failed"`
	MetadataBloomEnable       bool               `yaml:"metadata_bloom_enable"`
	MetadataBloomBits         int64              `yaml:"metadata_bloom_bits"`
	MetadataBloomHashes       int                `yaml:"metadata_bloom_hashes"`
	MetadataBloomFlushSec     int                `yaml:"metadata_bloom_flush_sec"`
	MetadataBloomPathTemplate string             `yaml:"metadata_bloom_path_template"`
	MetadataSync              MetadataSyncConfig `yaml:"metadata_sync"`
}

// SeenDBConfig seen DB 配置
type SeenDBConfig struct {
	Enable bool   `yaml:"enable"`
	Path   string `yaml:"path"`
}

// SourceS3Config 与 go-downloader 各 download-*.yaml 中的 source_s3 一致
type SourceS3Config struct {
	Enabled                  bool   `yaml:"enabled"`
	Profile                  string `yaml:"profile"`
	EndpointURL              string `yaml:"endpoint_url"`
	RegionName               string `yaml:"region_name"`
	Bucket                   string `yaml:"bucket"`
	KeyPrefix                string `yaml:"key_prefix"`
	PullRetry                int    `yaml:"pull_retry"`
	VerifyHead               bool   `yaml:"verify_head"`
	PullSchedulerEnable      bool   `yaml:"pull_scheduler_enable"`
	PullTimeUTC              string `yaml:"pull_time_utc"`
	PullSchedulerIntervalSec int    `yaml:"pull_scheduler_interval_sec"`
}

type MetadataSyncConfig struct {
	Enabled           bool   `yaml:"enabled"`
	TimeUTC           string `yaml:"time_utc"`
	IntervalSec       int    `yaml:"interval_sec"`
	Retry             int    `yaml:"retry"`
	KeyPrefix         string `yaml:"key_prefix"`
	ExtractConfigPath string `yaml:"extract_config_path"`
}

// pipelineRootYAML 与 go-downloader 各 download-*.yaml 顶层结构一致
type pipelineRootYAML struct {
	ProjectRoot                string             `yaml:"project_root"`
	ImageKeyPrefix             string             `yaml:"image_key_prefix"`
	ImageKeyStyle              string             `yaml:"image_key_style"`
	InputMode                  string             `yaml:"input_mode"`
	ExtractMetadataPathTemplate string            `yaml:"extract_metadata_path_template"`
	MediaUpscaleDirTemplate    string             `yaml:"media_upscale_dir_template"`
	ResolutionMinShort         int                `yaml:"resolution_min_short"`
	ResolutionMinLong          int                `yaml:"resolution_min_long"`
	ExtractMetadataResolutionPolicy string        `yaml:"extract_metadata_resolution_policy"`
	UpscalePython              string             `yaml:"upscale_python"`
	UpscaleScript              string             `yaml:"upscale_script"`
	UpscaleWorkers             int                `yaml:"upscale_workers"`
	IncludeCategories          []string           `yaml:"include_categories"`
	ExcludeCategories          []string           `yaml:"exclude_categories"`
	URLsPathTemplate           string             `yaml:"urls_path_template"`
	URLsDeltaDirTemplate       string             `yaml:"urls_delta_dir_template"`
	PipelineRoot               string             `yaml:"pipeline_root"`
	MediaDirTemplate           string             `yaml:"media_dir_template"`
	MetadataDailyPathTemplate  string             `yaml:"metadata_daily_path_template"`
	MetadataGlobPathTemplate   string             `yaml:"metadata_glob_path_template"`
	MetadataBloomEnable        bool               `yaml:"metadata_bloom_enable"`
	MetadataBloomBits          int64              `yaml:"metadata_bloom_bits"`
	MetadataBloomHashes        int                `yaml:"metadata_bloom_hashes"`
	MetadataBloomFlushSec      int                `yaml:"metadata_bloom_flush_sec"`
	MetadataBloomPathTemplate  string             `yaml:"metadata_bloom_path_template"`
	MetadataSeenDBEnable       bool               `yaml:"metadata_seen_db_enable"`
	MetadataSeenDBPathTemplate string             `yaml:"metadata_seen_db_path_template"`
	Download                   downloadBlockYAML  `yaml:"download"`
	SourceS3                   SourceS3Config     `yaml:"source_s3"`
	MetadataSync               MetadataSyncConfig `yaml:"metadata_sync"`
	S3                         s3YAML             `yaml:"s3"`
}

type downloadBlockYAML struct {
	Workers                  int      `yaml:"workers"`
	Retries                  int      `yaml:"retries"`
	TimeoutSec               int      `yaml:"timeout_sec"`
	SkipSuffixes             []string `yaml:"skip_suffixes"`
	UseUserAgents            bool     `yaml:"use_user_agents"`
	HTTPUserAgent            string   `yaml:"http_user_agent"`
	HTTPPoolMaxsize          int      `yaml:"http_pool_maxsize"`
	ProxiesYAML              string   `yaml:"proxies_yaml"`
	UseProxy                 bool     `yaml:"use_proxy"`
	ProgressIntervalSec      int      `yaml:"progress_interval_sec"`
	DiskGlobFallback         bool     `yaml:"disk_glob_fallback"`
	MetadataFlushEvery       int      `yaml:"metadata_flush_every"`
	MetadataFlushIntervalSec int      `yaml:"metadata_flush_interval_sec"`
	Loop                     bool     `yaml:"loop"`
	LoopIdlePollSeconds      int      `yaml:"loop_idle_poll_seconds"`
	RetryFailed              bool     `yaml:"retry_failed"`
	CheckpointIntervalLines  int      `yaml:"checkpoint_interval_lines"`
	CheckpointPathTemplate   string   `yaml:"checkpoint_path_template"`
	FailedURLsPathTemplate   string   `yaml:"failed_urls_path_template"`
}

type s3YAML struct {
	Enabled   bool   `yaml:"enabled"`
	KeyPrefix string `yaml:"key_prefix"`
}

// LoadConfig 加载配置文件：支持 go-downloader 的 pipelineRootYAML 结构，以及旧版扁平 legacy
func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	projectRoot := resolveProjectRoot(configPath)

	var root pipelineRootYAML
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	if isPipelineYAML(&root) {
		cfg, err := mergePipelineYAML(&root, projectRoot)
		if err != nil {
			return nil, err
		}
		applyDefaults(cfg)
		return cfg, nil
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
	cfg.ProjectRoot = projectRoot

	applyDefaults(&cfg)

	if cfg.ExtractDir != "" {
		cfg.ExtractDir = resolvePath(cfg.ExtractDir, projectRoot, "")
		return &cfg, nil
	}

	cfg.Input = resolvePath(cfg.Input, projectRoot, cfg.Category)
	cfg.OutputDir = resolvePath(cfg.OutputDir, projectRoot, cfg.Category)
	cfg.MetadataDir = resolvePath(cfg.MetadataDir, projectRoot, cfg.Category)
	if cfg.FilteredOut != "" {
		cfg.FilteredOut = resolvePath(cfg.FilteredOut, projectRoot, cfg.Category)
	}
	if cfg.SeenDB.Path != "" {
		cfg.SeenDB.Path = resolvePath(cfg.SeenDB.Path, projectRoot, cfg.Category)
	}

	return &cfg, nil
}

func resolveProjectRoot(configPath string) string {
	absConfigPath, _ := filepath.Abs(configPath)
	absConfigDir := filepath.Dir(absConfigPath)
	projectRoot := filepath.Dir(absConfigDir)
	if filepath.Base(absConfigDir) == "config" && filepath.Base(projectRoot) == "go-downloader" {
		projectRoot = filepath.Dir(projectRoot)
	}
	return projectRoot
}

func isPipelineYAML(r *pipelineRootYAML) bool {
	return strings.TrimSpace(r.URLsPathTemplate) != "" || r.Download.Workers > 0
}

func mergePipelineYAML(root *pipelineRootYAML, projectRoot string) (*Config, error) {
	cfg := &Config{ProjectRoot: projectRoot}
	cfg.IncludeCategories = root.IncludeCategories
	cfg.ExcludeCategories = root.ExcludeCategories

	cat := ""
	if len(root.IncludeCategories) > 0 {
		cat = strings.TrimSpace(root.IncludeCategories[0])
	}
	cfg.Category = cat
	cfg.ExtractDir = ""
	cfg.FilteredOut = ""

	cfg.InputMode = strings.TrimSpace(root.InputMode)
	cfg.ExtractMetadataPathTemplate = strings.TrimSpace(root.ExtractMetadataPathTemplate)
	cfg.MediaUpscaleDirTemplate = strings.TrimSpace(root.MediaUpscaleDirTemplate)
	cfg.ResolutionMinShort = root.ResolutionMinShort
	cfg.ResolutionMinLong = root.ResolutionMinLong
	cfg.ExtractMetadataResolutionPolicy = strings.TrimSpace(root.ExtractMetadataResolutionPolicy)
	cfg.UpscalePython = strings.TrimSpace(root.UpscalePython)
	cfg.UpscaleWorkers = root.UpscaleWorkers
	upsTpl := strings.TrimSpace(root.UpscaleScript)
	cfg.UpscaleScriptTemplate = upsTpl
	if upsTpl != "" {
		cfg.UpscaleScript = resolvePath(upsTpl, projectRoot, cat)
	} else {
		cfg.UpscaleScript = ""
	}
	if strings.TrimSpace(cfg.MediaUpscaleDirTemplate) != "" {
		cfg.MediaUpscaleDir = resolvePath(cfg.MediaUpscaleDirTemplate, projectRoot, cat)
	}

	if strings.EqualFold(cfg.InputMode, "extract_metadata") {
		metaTpl := cfg.ExtractMetadataPathTemplate
		if metaTpl != "" {
			p := resolvePath(metaTpl, projectRoot, cat)
			cfg.ExtractMetadataInputs = []string{p}
			cfg.Input = p
		} else {
			files, err := DiscoverExtractMetadataFiles(projectRoot)
			if err != nil {
				return nil, err
			}
			if len(files) > 0 {
				cfg.ExtractMetadataInputs = files
				cfg.Input = files[0]
			}
		}
	} else {
		cfg.Input = resolvePath(root.URLsPathTemplate, projectRoot, cat)
	}
	cfg.URLsPathTemplate = root.URLsPathTemplate
	cfg.OutputDir = resolvePath(root.MediaDirTemplate, projectRoot, cat)
	cfg.MediaDirTemplate = root.MediaDirTemplate
	cfg.MetadataDir = resolvePath("output/pipeline/{category}/download/metadata", projectRoot, cat)

	if strings.TrimSpace(root.URLsDeltaDirTemplate) != "" {
		cfg.URLsDeltaDirTemplate = root.URLsDeltaDirTemplate
	}
	if strings.TrimSpace(root.MetadataDailyPathTemplate) != "" {
		cfg.MetadataDailyPathTemplate = root.MetadataDailyPathTemplate
	}
	if strings.TrimSpace(root.MetadataGlobPathTemplate) != "" {
		cfg.MetadataGlobPathTemplate = root.MetadataGlobPathTemplate
	}

	d := root.Download
	cfg.Workers = d.Workers
	cfg.Retries = d.Retries
	cfg.Timeout = d.TimeoutSec
	cfg.ProgressInterval = d.ProgressIntervalSec
	cfg.DiskGlobFallback = d.DiskGlobFallback
	cfg.CheckpointInterval = d.CheckpointIntervalLines
	cfg.CheckpointPathTemplate = d.CheckpointPathTemplate
	cfg.FailedURLsPathTemplate = d.FailedURLsPathTemplate
	cfg.SkipSuffixes = d.SkipSuffixes
	cfg.UseUserAgents = d.UseUserAgents
	cfg.HTTPUserAgent = d.HTTPUserAgent
	cfg.HTTPPoolMaxsize = d.HTTPPoolMaxsize
	cfg.ProxiesYAML = d.ProxiesYAML
	cfg.UseProxy = d.UseProxy
	cfg.MetadataFlushEvery = d.MetadataFlushEvery
	cfg.MetadataFlushIntervalSec = d.MetadataFlushIntervalSec
	cfg.Loop = d.Loop
	cfg.LoopIdlePollSeconds = d.LoopIdlePollSeconds
	cfg.RetryFailed = d.RetryFailed

	cfg.MetadataLinesPerFile = 1000000
	cfg.FailedFileName = "failed_urls.txt"

	cfg.SeenDB.Enable = root.MetadataSeenDBEnable
	if strings.TrimSpace(root.MetadataSeenDBPathTemplate) != "" {
		cfg.SeenDB.Path = resolvePath(root.MetadataSeenDBPathTemplate, projectRoot, cat)
	}

	cfg.ImageKeyPrefix = strings.TrimSpace(root.ImageKeyPrefix)
	if cfg.ImageKeyPrefix == "" {
		cfg.ImageKeyPrefix = strings.TrimSpace(root.S3.KeyPrefix)
	}
	cfg.ImageKeyStyle = strings.TrimSpace(root.ImageKeyStyle)
	cfg.SourceS3 = root.SourceS3
	cfg.MetadataSync = root.MetadataSync

	cfg.MinSidePixels = 0

	cfg.MetadataBloomEnable = root.MetadataBloomEnable
	cfg.MetadataBloomBits = root.MetadataBloomBits
	cfg.MetadataBloomHashes = root.MetadataBloomHashes
	cfg.MetadataBloomFlushSec = root.MetadataBloomFlushSec
	if strings.TrimSpace(root.MetadataBloomPathTemplate) != "" {
		cfg.MetadataBloomPathTemplate = root.MetadataBloomPathTemplate
	}

	// 模板默认值（与 download-http.yaml 一致）
	if strings.TrimSpace(cfg.URLsDeltaDirTemplate) == "" {
		cfg.URLsDeltaDirTemplate = "output/pipeline/{category}/download/urls"
	}
	if strings.TrimSpace(cfg.MetadataDailyPathTemplate) == "" {
		cfg.MetadataDailyPathTemplate = "output/pipeline/{category}/download/metadata/{date}/metadata.jsonl"
	}
	if strings.TrimSpace(cfg.MetadataGlobPathTemplate) == "" {
		cfg.MetadataGlobPathTemplate = "output/pipeline/{category}/download/metadata/glob/metadata.jsonl"
	}
	if strings.TrimSpace(cfg.CheckpointPathTemplate) == "" {
		cfg.CheckpointPathTemplate = "output/pipeline/{category}/download/checkpoints/download.ckpt"
	}
	if strings.TrimSpace(cfg.FailedURLsPathTemplate) == "" {
		cfg.FailedURLsPathTemplate = "output/pipeline/{category}/download/failed/failed_urls.txt"
	}

	if cfg.IsExtractMetadataInput() {
		if _, err := cfg.NormalizedExtractMetadataResolutionPolicy(); err != nil {
			return nil, err
		}
	}

	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Workers == 0 {
		cfg.Workers = 40
	}
	if cfg.Retries == 0 {
		cfg.Retries = 2
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 300
	}
	if cfg.ProgressInterval == 0 {
		cfg.ProgressInterval = 600
	}
	if cfg.MetadataLinesPerFile == 0 {
		cfg.MetadataLinesPerFile = 1000000
	}
	if cfg.FailedFileName == "" {
		cfg.FailedFileName = "failed_urls.txt"
	}
	if cfg.ImageKeyPrefix == "" {
		cfg.ImageKeyPrefix = "photo-download"
	}
	if cfg.CheckpointInterval == 0 {
		cfg.CheckpointInterval = 100000
	} else if cfg.CheckpointInterval < 0 {
		cfg.CheckpointInterval = 0
	} else if cfg.CheckpointInterval < 1000 && cfg.CheckpointInterval > 0 {
		cfg.CheckpointInterval = 10000
	}
	if strings.TrimSpace(cfg.URLsDeltaDirTemplate) == "" {
		cfg.URLsDeltaDirTemplate = "output/pipeline/{category}/download/urls"
	}
	if strings.TrimSpace(cfg.MetadataDailyPathTemplate) == "" {
		cfg.MetadataDailyPathTemplate = "output/pipeline/{category}/download/metadata/{date}/metadata.jsonl"
	}
	if strings.TrimSpace(cfg.MetadataGlobPathTemplate) == "" {
		cfg.MetadataGlobPathTemplate = "output/pipeline/{category}/download/metadata/glob/metadata.jsonl"
	}
	if strings.TrimSpace(cfg.CheckpointPathTemplate) == "" {
		cfg.CheckpointPathTemplate = "output/pipeline/{category}/download/checkpoints/download.ckpt"
	}
	if strings.TrimSpace(cfg.FailedURLsPathTemplate) == "" {
		cfg.FailedURLsPathTemplate = "output/pipeline/{category}/download/failed/failed_urls.txt"
	}
	if cfg.SourceS3.PullRetry <= 0 {
		cfg.SourceS3.PullRetry = 3
	}
	if cfg.SourceS3.PullSchedulerIntervalSec <= 0 {
		cfg.SourceS3.PullSchedulerIntervalSec = 120
	}
	if cfg.MetadataSync.Retry <= 0 {
		cfg.MetadataSync.Retry = 3
	}
	// 0 = 不启用周期 ticker，仅进程启动时与每日 time_utc 定点上传；>0 为额外周期（秒，最少 60）
	if cfg.MetadataSync.IntervalSec < 0 {
		cfg.MetadataSync.IntervalSec = 0
	}
	if strings.TrimSpace(cfg.MetadataSync.TimeUTC) == "" {
		cfg.MetadataSync.TimeUTC = "00:05"
	}
	if strings.TrimSpace(cfg.MetadataSync.KeyPrefix) == "" {
		cfg.MetadataSync.KeyPrefix = "metadata/photo-download"
	}
	if len(cfg.SkipSuffixes) == 0 {
		cfg.SkipSuffixes = []string{".m3u8", ".mp4"}
	}
	if cfg.HTTPPoolMaxsize <= 0 {
		cfg.HTTPPoolMaxsize = 0
	}
	if cfg.MetadataFlushEvery <= 0 {
		cfg.MetadataFlushEvery = 1000
	}
	if cfg.MetadataFlushIntervalSec <= 0 {
		cfg.MetadataFlushIntervalSec = 2
	}
	if cfg.LoopIdlePollSeconds <= 0 {
		cfg.LoopIdlePollSeconds = 5
	}
	if cfg.MetadataBloomBits <= 0 {
		cfg.MetadataBloomBits = 1600000000
	}
	if cfg.MetadataBloomHashes <= 0 {
		cfg.MetadataBloomHashes = 7
	}
	if cfg.MetadataBloomFlushSec <= 0 {
		cfg.MetadataBloomFlushSec = 60
	}
	if strings.TrimSpace(cfg.MetadataBloomPathTemplate) == "" {
		cfg.MetadataBloomPathTemplate = "output/pipeline/{category}/download/metadata/seen/metadata.bloom"
	}
	if cfg.ResolutionMinShort <= 0 {
		cfg.ResolutionMinShort = 1080
	}
	if cfg.ResolutionMinLong <= 0 {
		cfg.ResolutionMinLong = 2000
	}
	if strings.TrimSpace(cfg.UpscalePython) == "" {
		cfg.UpscalePython = "python3"
	}
	if cfg.UpscaleWorkers <= 0 {
		cfg.UpscaleWorkers = 4
	}
	if strings.TrimSpace(cfg.UpscaleScript) == "" && strings.TrimSpace(cfg.UpscaleScriptTemplate) != "" {
		cfg.UpscaleScript = resolvePath(cfg.UpscaleScriptTemplate, cfg.ProjectRoot, cfg.Category)
	}
}

// IsExtractMetadataInput 从 extract_metadata_*.jsonl 读入（含 image_url / resolution）。
func (c *Config) IsExtractMetadataInput() bool {
	if c == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(c.InputMode), "extract_metadata")
}

// NormalizedExtractMetadataResolutionPolicy 返回 metadata_large、metadata_small 或 full。
func (c *Config) NormalizedExtractMetadataResolutionPolicy() (string, error) {
	if c == nil {
		return "", fmt.Errorf("config 为 nil")
	}
	p := strings.TrimSpace(c.ExtractMetadataResolutionPolicy)
	if p == "" {
		return "full", nil
	}
	switch strings.ToLower(p) {
	case "metadata_large":
		return "metadata_large", nil
	case "metadata_small":
		return "metadata_small", nil
	case "full":
		return "full", nil
	default:
		return "", fmt.Errorf("extract_metadata_resolution_policy 无效: %q，应为 metadata_large、metadata_small 或 full", p)
	}
}

func (c *Config) IsExtractMetadataLargeBatch() bool {
	v, err := c.NormalizedExtractMetadataResolutionPolicy()
	return err == nil && v == "metadata_large"
}

func (c *Config) IsExtractMetadataSmallBatch() bool {
	v, err := c.NormalizedExtractMetadataResolutionPolicy()
	return err == nil && v == "metadata_small"
}

func (c *Config) IsExtractMetadataFullBatch() bool {
	v, err := c.NormalizedExtractMetadataResolutionPolicy()
	return err == nil && v == "full"
}

// resolvePath 解析路径，支持模板变量 {category}、{category_plural}（均替换为 category）
func resolvePath(path, projectRoot, category string) string {
	if filepath.IsAbs(path) {
		return path
	}

	path = strings.ReplaceAll(path, "{category}", category)
	path = strings.ReplaceAll(path, "{category_plural}", category)

	if strings.HasPrefix(path, "../") {
		return filepath.Join(projectRoot, strings.TrimPrefix(path, "../"))
	}

	return filepath.Join(projectRoot, path)
}

func resolvePathWithDate(path, projectRoot, category, date string) string {
	if filepath.IsAbs(path) {
		return path
	}
	path = strings.ReplaceAll(path, "{category}", category)
	path = strings.ReplaceAll(path, "{category_plural}", category)
	path = strings.ReplaceAll(path, "{date}", date)
	if strings.HasPrefix(path, "../") {
		return filepath.Join(projectRoot, strings.TrimPrefix(path, "../"))
	}
	return filepath.Join(projectRoot, path)
}

func (c *Config) ExpandPath(tpl, category string) string {
	return resolvePath(tpl, c.ProjectRoot, category)
}

func (c *Config) ExpandPathWithDate(tpl, category, date string) string {
	return resolvePathWithDate(tpl, c.ProjectRoot, category, date)
}

// CategorySelected 判断某 category 是否应参与多 category 运行（extract_dir 模式）。
func (c *Config) CategorySelected(cat string) bool {
	cat = strings.TrimSpace(cat)
	if cat == "" {
		return false
	}
	if len(c.IncludeCategories) > 0 {
		ok := false
		for _, x := range c.IncludeCategories {
			if strings.TrimSpace(x) == cat {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	for _, x := range c.ExcludeCategories {
		if strings.TrimSpace(x) == cat {
			return false
		}
	}
	return true
}
