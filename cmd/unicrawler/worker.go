package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	cli "github.com/urfave/cli/v3"
)

// --- registry wire DTOs (mirror internal/infra/http/jobs.go) --------------

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

// --- top-level loop -------------------------------------------------------

type workerCfg struct {
	registry    string
	pat         string
	batch       int
	conc        int
	idle        time.Duration
	pageLoadTO  time.Duration
	scriptTO    time.Duration
	sidecarDir  string
	webdriver   string
	browser     string
	cfg         *Config
	pool        *BrowserPool
	apic        *http.Client
}

func runRunCmd(ctx context.Context, cmd *cli.Command) error {
	path := cmd.Args().First()
	if path == "" {
		return fmt.Errorf("usage: unicrawler run <config.yaml>")
	}
	c, err := LoadConfig(path)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		slog.Info("stopping (signal)")
		cancel()
	}()

	wc := workerCfg{
		registry:   strings.TrimRight(cmd.String("registry"), "/"),
		pat:        cmd.String("pat"),
		batch:      cmd.Int("batch"),
		conc:       cmd.Int("concurrency"),
		idle:       cmd.Duration("idle-sleep"),
		pageLoadTO: cmd.Duration("page-load-timeout"),
		scriptTO:   cmd.Duration("script-timeout"),
		sidecarDir: cmd.String("sidecar-dir"),
		webdriver:  cmd.String("webdriver"),
		browser:    cmd.String("browser"),
		cfg:        c,
	}
	if wc.conc < 1 {
		wc.conc = 1
	}
	if wc.batch < wc.conc {
		wc.batch = wc.conc
	}
	if err := os.MkdirAll(wc.sidecarDir, 0o755); err != nil {
		return fmt.Errorf("sidecar dir: %w", err)
	}
	wc.apic = &http.Client{Timeout: 30 * time.Second}

	pool, err := NewBrowserPool(wc.webdriver, wc.browser, wc.conc, wc.pageLoadTO, wc.scriptTO)
	if err != nil {
		return fmt.Errorf("open browser pool: %w", err)
	}
	defer pool.Close()
	wc.pool = pool

	slog.Info("unicrawler started",
		"site", c.Name, "registry", wc.registry, "batch", wc.batch,
		"concurrency", wc.conc, "webdriver", wc.webdriver)

	jobs := make(chan job, wc.conc)
	var wg sync.WaitGroup
	for i := 0; i < wc.conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				handleJob(ctx, wc, j)
			}
		}()
	}

	for {
		if ctx.Err() != nil {
			break
		}
		js, err := reserveJobs(ctx, wc)
		if err != nil {
			slog.Error("reserve", "err", err)
			sleep(ctx, wc.idle)
			continue
		}
		if len(js) == 0 {
			slog.Debug("reserve empty, idling", "sleep", wc.idle.String())
			sleep(ctx, wc.idle)
			continue
		}
		slog.Info("batch reserved", "n", len(js))
		for _, j := range js {
			select {
			case jobs <- j:
			case <-ctx.Done():
			}
		}
	}
	close(jobs)
	wg.Wait()
	return nil
}

func reserveJobs(ctx context.Context, c workerCfg) ([]job, error) {
	body, _ := json.Marshal(map[string]any{"batch": c.batch})
	req, _ := http.NewRequestWithContext(ctx, "POST", c.registry+"/v1/jobs/reserve", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+c.pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.apic.Do(req)
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

// --- per-job orchestration ------------------------------------------------

func handleJob(ctx context.Context, c workerCfg, j job) {
	start := time.Now()
	pt := c.cfg.MatchPageType(j.URL)
	ptName := "(none)"
	if pt != nil {
		ptName = pt.Name
	}
	slog.Info("job start", "job_id", j.JobID, "url", j.URL, "page_type", ptName, "depth", j.Depth)
	defer func() {
		slog.Debug("job done", "job_id", j.JobID, "dur_ms", time.Since(start).Milliseconds())
	}()

	b := c.pool.Checkout()
	defer c.pool.Return(b)

	if err := b.WD.Get(j.URL); err != nil {
		_ = postFail(ctx, c, j.LeaseToken, 0, "navigation_error", err.Error(), true)
		return
	}

	// Page 1 HTML — this is what gets uploaded as the blob.
	page1HTML, err := b.WD.PageSource()
	if err != nil {
		_ = postFail(ctx, c, j.LeaseToken, 0, "page_source", err.Error(), true)
		return
	}

	seen := make(map[string]struct{})
	var allLinks []discoveredLink
	fields := map[string]any{}

	if pt != nil {
		// Page 1: extract links + fields.
		allLinks = append(allLinks, extractLinks(b.WD, j.URL, pt.Extract.Links, seen)...)
		mergeMap(fields, extractFields(b.WD, pt.Extract.Fields))

		// Paginate: each subsequent page yields more links + a fields-merged dict.
		// (Per-page fields are merged shallowly; later pages overwrite same-key
		// scalar fields, and list-typed fields are concatenated.)
		err := paginate(ctx, b.WD, j.URL, pt.Pagination, func(html string) error {
			more := extractLinks(b.WD, j.URL, pt.Extract.Links, seen)
			allLinks = append(allLinks, more...)
			mergeFields(fields, extractFields(b.WD, pt.Extract.Fields))
			return nil
		})
		if err != nil {
			slog.Warn("paginate", "job_id", j.JobID, "err", err)
			// Post what we have anyway.
		}
	}

	// Sidecar: write extracted fields when non-empty.
	if len(fields) > 0 {
		if err := writeSidecar(c.sidecarDir, j.URL, fields); err != nil {
			slog.Warn("sidecar write", "job_id", j.JobID, "err", err)
		}
	}

	sum := sha256.Sum256([]byte(page1HTML))
	meta := resultMeta{
		LeaseToken:      j.LeaseToken,
		HTTPStatus:      200, // Selenium hides HTTP status; assume 200 if Get succeeded.
		ContentType:     "text/html; charset=utf-8",
		ContentSHA256:   hex.EncodeToString(sum[:]),
		Size:            int64(len(page1HTML)),
		DiscoveredLinks: allLinks,
	}
	if err := postResult(ctx, c, meta, []byte(page1HTML)); err != nil {
		slog.Error("post result", "job_id", j.JobID, "err", err)
		return
	}
	slog.Info("job ok",
		"job_id", j.JobID, "page_type", ptName,
		"bytes", len(page1HTML), "links", len(allLinks), "fields", len(fields))
}

// mergeMap shallow-copies src into dst.
func mergeMap(dst, src map[string]any) {
	for k, v := range src {
		dst[k] = v
	}
}

// mergeFields merges per-page extraction output. List-typed fields are
// concatenated (so paginated `text_list` results accumulate); scalar fields
// are left untouched once set.
func mergeFields(dst, src map[string]any) {
	for k, v := range src {
		cur, ok := dst[k]
		if !ok {
			dst[k] = v
			continue
		}
		switch curV := cur.(type) {
		case []string:
			if newV, ok := v.([]string); ok {
				dst[k] = append(curV, newV...)
			}
		case []map[string]string:
			if newV, ok := v.([]map[string]string); ok {
				dst[k] = append(curV, newV...)
			}
		}
		// scalars: keep first
	}
}

// writeSidecar persists fields as <sidecar-dir>/<sha256(url)>.json.
func writeSidecar(dir, urlStr string, fields map[string]any) error {
	sum := sha256.Sum256([]byte(urlStr))
	name := hex.EncodeToString(sum[:]) + ".json"
	path := filepath.Join(dir, name)
	payload := map[string]any{
		"url":    urlStr,
		"fields": fields,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// --- registry I/O ---------------------------------------------------------

func postResult(ctx context.Context, c workerCfg, meta resultMeta, blob []byte) error {
	buf := &bytes.Buffer{}
	mw := multipart.NewWriter(buf)
	mb, _ := json.Marshal(meta)
	if err := mw.WriteField("meta", string(mb)); err != nil {
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
	req, _ := http.NewRequestWithContext(ctx, "POST", c.registry+"/v1/jobs/result", buf)
	req.Header.Set("Authorization", "Bearer "+c.pat)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.apic.Do(req)
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

func postFail(ctx context.Context, c workerCfg, token string, httpStatus int, code, msg string, retryable bool) error {
	body, _ := json.Marshal(map[string]any{
		"lease_token":   token,
		"http_status":   httpStatus,
		"error_code":    code,
		"error_message": msg,
		"retryable":     retryable,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", c.registry+"/v1/jobs/fail", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+c.pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.apic.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
