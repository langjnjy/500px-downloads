package proxy

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// yamlRoot 与仓库 config/proxies.yaml 及 Python prepare_and_test_proxies 一致。
type yamlRoot struct {
	Proxies []struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"proxies"`
}

// LoadFromYAML 解析代理列表；按 host 去重（与 Python 脚本一致）。返回可用于 http.Transport.Proxy 的 *url.URL。
func LoadFromYAML(path string) ([]*url.URL, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read proxies yaml: %w", err)
	}
	var root yamlRoot
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse proxies yaml: %w", err)
	}
	seen := make(map[string]struct{})
	var out []*url.URL
	for _, p := range root.Proxies {
		host := strings.TrimSpace(p.Host)
		if host == "" || p.Port <= 0 {
			continue
		}
		if _, ok := seen[host]; ok {
			continue
		}
		seen[host] = struct{}{}
		u := &url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("%s:%d", host, p.Port),
			User:   url.UserPassword(strings.TrimSpace(p.Username), strings.TrimSpace(p.Password)),
		}
		out = append(out, u)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no valid proxies in %s", path)
	}
	return out, nil
}
