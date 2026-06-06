// Worker is a reference Go implementation of the crawlerv3 worker protocol.
//
// Loop: reserve → fetch each URL → push result (multipart) or fail.
// HTML responses are scanned for <a href> and <base href> to populate
// discovered_links sent back to the registry.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/html"

	cli "github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "worker",
		Usage: "crawlerv3 reference worker",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "registry", Required: true, Sources: cli.EnvVars("REGISTRY")},
			&cli.StringFlag{Name: "pat", Required: true, Sources: cli.EnvVars("PAT")},
			&cli.IntFlag{Name: "batch", Value: 10},
			&cli.IntFlag{Name: "concurrency", Value: 4,
				Usage: "max parallel fetches per reserved batch"},
			&cli.DurationFlag{Name: "idle-sleep", Value: 3 * time.Second},
			&cli.DurationFlag{Name: "fetch-timeout", Value: 30 * time.Second},
			&cli.StringFlag{Name: "user-agent", Value: "crawlerv3-worker/0.1"},
		},
		Action: runLoop,
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

type reserveResp struct {
	Jobs []job `json:"jobs"`
}

type job struct {
	JobID        string `json:"job_id"`
	URL          string `json:"url"`
	CanonicalURL string `json:"canonical_url"`
	Depth        int    `json:"depth"`
	AttemptCount int    `json:"attempt_count"`
	LeaseToken   string `json:"lease_token"`
	LeaseExpires int64  `json:"lease_expires_at"`
	MaxBodyBytes int64  `json:"max_body_bytes"`
}

type discoveredLink struct {
	URL      string `json:"url"`
	Anchor   string `json:"anchor"`
	Rel      string `json:"rel"`
	NewDepth int    `json:"new_depth"`
}

type resultMeta struct {
	LeaseToken      string           `json:"lease_token"`
	HTTPStatus      int              `json:"http_status"`
	ContentType     string           `json:"content_type"`
	ContentSHA256   string           `json:"content_sha256"`
	Size            int64            `json:"size"`
	DiscoveredLinks []discoveredLink `json:"discovered_links"`
}

func runLoop(ctx context.Context, cmd *cli.Command) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		fmt.Println("worker: stopping")
		os.Exit(0)
	}()

	registry := strings.TrimRight(cmd.String("registry"), "/")
	pat := cmd.String("pat")
	batch := cmd.Int("batch")
	conc := cmd.Int("concurrency")
	if conc < 1 {
		conc = 1
	}
	idle := cmd.Duration("idle-sleep")
	fetchTO := cmd.Duration("fetch-timeout")
	ua := cmd.String("user-agent")

	httpc := &http.Client{Timeout: fetchTO}
	apic := &http.Client{Timeout: 30 * time.Second}

	sem := make(chan struct{}, conc)

	for {
		jobs, err := reserve(ctx, apic, registry, pat, batch)
		if err != nil {
			fmt.Fprintln(os.Stderr, "reserve:", err)
			time.Sleep(idle)
			continue
		}
		if len(jobs) == 0 {
			time.Sleep(idle)
			continue
		}
		var wg sync.WaitGroup
		for _, j := range jobs {
			wg.Add(1)
			sem <- struct{}{}
			go func(j job) {
				defer wg.Done()
				defer func() { <-sem }()
				handleJob(ctx, httpc, apic, registry, pat, ua, j)
			}(j)
		}
		wg.Wait()
	}
}

func reserve(ctx context.Context, c *http.Client, registry, pat string, batch int) ([]job, error) {
	body, _ := json.Marshal(map[string]any{"batch": batch})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/jobs/reserve", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("reserve: status=%d body=%s", resp.StatusCode, string(b))
	}
	var rr reserveResp
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, err
	}
	return rr.Jobs, nil
}

func handleJob(ctx context.Context, httpc, apic *http.Client, registry, pat, ua string, j job) {
	fmt.Printf("worker: fetch %s\n", j.URL)
	req, err := http.NewRequestWithContext(ctx, "GET", j.URL, nil)
	if err != nil {
		_ = postFail(ctx, apic, registry, pat, j.LeaseToken, 0, "bad_url", err.Error(), false)
		return
	}
	req.Header.Set("User-Agent", ua)
	resp, err := httpc.Do(req)
	if err != nil {
		_ = postFail(ctx, apic, registry, pat, j.LeaseToken, 0, "fetch_error", err.Error(), true)
		return
	}
	defer resp.Body.Close()

	limit := j.MaxBodyBytes
	if limit <= 0 {
		limit = 200 << 20
	}
	limited := io.LimitReader(resp.Body, limit+1)
	buf := bytes.NewBuffer(make([]byte, 0, 64*1024))
	h := sha256.New()
	mw := io.MultiWriter(buf, h)
	n, err := io.Copy(mw, limited)
	if err != nil {
		_ = postFail(ctx, apic, registry, pat, j.LeaseToken, resp.StatusCode, "read_error", err.Error(), true)
		return
	}
	if n > limit {
		_ = postFail(ctx, apic, registry, pat, j.LeaseToken, resp.StatusCode, "too_large", fmt.Sprintf("body > %d bytes", limit), false)
		return
	}

	ct := resp.Header.Get("Content-Type")
	links := []discoveredLink{}
	if strings.HasPrefix(strings.ToLower(ct), "text/html") {
		links = extractLinks(j.CanonicalURL, j.Depth, buf.Bytes())
	}
	meta := resultMeta{
		LeaseToken:      j.LeaseToken,
		HTTPStatus:      resp.StatusCode,
		ContentType:     ct,
		ContentSHA256:   hex.EncodeToString(h.Sum(nil)),
		Size:            n,
		DiscoveredLinks: links,
	}
	if err := postResult(ctx, apic, registry, pat, meta, buf.Bytes()); err != nil {
		fmt.Fprintln(os.Stderr, "post result:", err)
	}
}

func postResult(ctx context.Context, c *http.Client, registry, pat string, meta resultMeta, blob []byte) error {
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	if err := mw.WriteField("meta", string(mustJSON(meta))); err != nil {
		return err
	}
	fw, err := mw.CreateFormFile("blob", "body.bin")
	if err != nil {
		return err
	}
	if _, err := fw.Write(blob); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/jobs/result", body)
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("result: status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func postFail(ctx context.Context, c *http.Client, registry, pat, token string, httpStatus int, code, msg string, retryable bool) error {
	body, _ := json.Marshal(map[string]any{
		"lease_token":   token,
		"http_status":   httpStatus,
		"error_code":    code,
		"error_message": msg,
		"retryable":     retryable,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/jobs/fail", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// extractLinks pulls <a href>, honoring <base href>. baseURL is the canonical URL of
// the fetched page; relative refs resolve against base href if present, else baseURL.
func extractLinks(pageURL string, depth int, body []byte) []discoveredLink {
	base, err := url.Parse(pageURL)
	if err != nil {
		return nil
	}
	z := html.NewTokenizer(bytes.NewReader(body))
	var out []discoveredLink
	for {
		tt := z.Next()
		if tt == html.ErrorToken {
			return out
		}
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}
		name, hasAttr := z.TagName()
		tag := string(name)
		switch tag {
		case "base":
			if !hasAttr {
				continue
			}
			for {
				k, v, more := z.TagAttr()
				if string(k) == "href" {
					if u, err := base.Parse(string(v)); err == nil {
						base = u
					}
				}
				if !more {
					break
				}
			}
		case "a":
			if !hasAttr {
				continue
			}
			var href, anchor, rel string
			for {
				k, v, more := z.TagAttr()
				switch string(k) {
				case "href":
					href = string(v)
				case "rel":
					rel = string(v)
				}
				if !more {
					break
				}
			}
			anchor = "" // collecting inner text would need more parsing — skip in v1
			if href == "" {
				continue
			}
			u, err := base.Parse(href)
			if err != nil {
				continue
			}
			if u.Scheme != "http" && u.Scheme != "https" {
				continue
			}
			out = append(out, discoveredLink{
				URL: u.String(), Anchor: anchor, Rel: rel, NewDepth: depth + 1,
			})
		}
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
