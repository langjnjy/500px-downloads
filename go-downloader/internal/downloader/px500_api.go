package downloader

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
)

var px500BatchPhotosURL = "https://api.500px.com/v1/photos"

var cdnMaxEdgeRe = regexp.MustCompile(`m%3D(\d+)`)

// Refresh500pxCDN4096 与 gallery-dl _extend 一致：batch API + image_size=4096，返回 m=4096 的 CDN URL。
func Refresh500pxCDN4096(client *http.Client, photoID string) (string, error) {
	photoID = strings.TrimSpace(photoID)
	if photoID == "" {
		return "", fmt.Errorf("empty photo id")
	}
	if client == nil {
		client = http.DefaultClient
	}
	reqURL := px500BatchPhotosURL + "?image_size=4096&ids=" + photoID
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://500px.com")
	req.Header.Set("Referer", "https://500px.com/photo/"+photoID)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("500px API HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var payload struct {
		Photos map[string]struct {
			Images []struct {
				Size int    `json:"size"`
				URL  string `json:"url"`
			} `json:"images"`
		} `json:"photos"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	photo, ok := payload.Photos[photoID]
	if !ok {
		return "", fmt.Errorf("photo %s not in API response", photoID)
	}
	best := pick4096PhotoCDNURL(photo.Images)
	if best == "" {
		return "", fmt.Errorf("photo %s: no m=4096 CDN url", photoID)
	}
	return best, nil
}

func pick4096PhotoCDNURL(images []struct {
	Size int    `json:"size"`
	URL  string `json:"url"`
}) string {
	bestURL := ""
	bestRank := -1
	for _, im := range images {
		u := strings.TrimSpace(im.URL)
		if u == "" || !strings.Contains(u, "/photo/") {
			continue
		}
		rank := im.Size
		if m := cdnMaxEdgeRe.FindStringSubmatch(u); len(m) >= 2 {
			if edge := atoiDefault(m[1], 0); edge > rank {
				rank = edge
			}
		}
		if rank > bestRank {
			bestRank = rank
			bestURL = u
		}
	}
	return bestURL
}

func atoiDefault(s string, def int) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	if n == 0 {
		return def
	}
	return n
}

func apiErrRetryable(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "HTTP 429") ||
		strings.Contains(s, "Too Many Requests") ||
		strings.Contains(s, "HTTP 502") ||
		strings.Contains(s, "HTTP 503") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "timeout")
}

func appendUniqueURL(urls []string, u string) []string {
	u = strings.TrimSpace(u)
	if u == "" {
		return urls
	}
	for _, x := range urls {
		if x == u {
			return urls
		}
	}
	return append(urls, u)
}
