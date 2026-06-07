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
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	cli "github.com/urfave/cli/v3"
)

// --- registry DTOs (must match internal/infra/http/jobs.go on the wire) ---

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

func runWorker(ctx context.Context, cmd *cli.Command) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		slog.Info("stopping (signal)")
		cancel()
	}()

	cfg := workerCfg{
		registry:  strings.TrimRight(cmd.String("registry"), "/"),
		pat:       cmd.String("pat"),
		batch:     cmd.Int("batch"),
		conc:      cmd.Int("concurrency"),
		idle:      cmd.Duration("idle-sleep"),
		fetchTO:   cmd.Duration("fetch-timeout"),
		pageDelay: cmd.Duration("page-delay"),
		ua:        cmd.String("user-agent"),
	}
	if cfg.conc < 1 {
		cfg.conc = 1
	}
	if cfg.batch < cfg.conc {
		cfg.batch = cfg.conc
	}
	cfg.httpc = &http.Client{Timeout: cfg.fetchTO}
	cfg.apic = &http.Client{Timeout: 30 * time.Second}

	slog.Info("litekoworker started",
		"registry", cfg.registry, "batch", cfg.batch, "concurrency", cfg.conc,
		"fetch_timeout", cfg.fetchTO.String(), "page_delay", cfg.pageDelay.String())

	jobs := make(chan job, cfg.conc)
	var wg sync.WaitGroup
	for i := 0; i < cfg.conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				handleJob(ctx, cfg, j)
			}
		}()
	}

	for {
		if ctx.Err() != nil {
			break
		}
		js, err := reserveJobs(ctx, cfg)
		if err != nil {
			slog.Error("reserve", "err", err)
			sleep(ctx, cfg.idle)
			continue
		}
		if len(js) == 0 {
			slog.Debug("reserve empty, idling", "sleep", cfg.idle.String())
			sleep(ctx, cfg.idle)
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

type workerCfg struct {
	registry  string
	pat       string
	batch     int
	conc      int
	idle      time.Duration
	fetchTO   time.Duration
	pageDelay time.Duration
	ua        string
	httpc     *http.Client
	apic      *http.Client
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

// --- per-job dispatch -----------------------------------------------------

func handleJob(ctx context.Context, c workerCfg, j job) {
	start := time.Now()
	slog.Info("job start", "job_id", j.JobID, "url", j.URL, "depth", j.Depth, "attempt", j.AttemptCount)
	defer func() {
		slog.Debug("job done", "job_id", j.JobID, "dur_ms", time.Since(start).Milliseconds())
	}()

	body, status, ct, err := fetchGET(ctx, c, j.URL)
	if err != nil {
		_ = postFail(ctx, c, j.LeaseToken, 0, "fetch_error", err.Error(), true)
		return
	}

	var links []discoveredLink
	if isListing(j.URL) {
		links, err = walkListing(ctx, c, j.URL, body)
		if err != nil {
			// Pagination failures don't lose page-1 data: post what we have and continue.
			slog.Warn("paginate", "job_id", j.JobID, "err", err)
		}
	}

	sum := sha256.Sum256(body)
	meta := resultMeta{
		LeaseToken:      j.LeaseToken,
		HTTPStatus:      status,
		ContentType:     ct,
		ContentSHA256:   hex.EncodeToString(sum[:]),
		Size:            int64(len(body)),
		DiscoveredLinks: links,
	}
	if err := postResult(ctx, c, meta, body); err != nil {
		slog.Error("post result", "job_id", j.JobID, "err", err)
		return
	}
	slog.Info("job ok", "job_id", j.JobID, "status", status, "bytes", len(body), "ct", ct, "links", len(links))
}

// isListing returns true when u is a paieska.aspx search URL. Everything else
// (tekstas.aspx?id=..., etc.) is treated as a plain detail GET.
func isListing(u string) bool {
	return strings.Contains(u, "paieska.aspx")
}

// walkListing parses page 1, accumulates its case detail URLs, then walks the
// remaining RadDataPager pages via __doPostBack POSTs. Returns all detail
// URLs found across every page (deduped, depth+1).
func walkListing(ctx context.Context, c workerCfg, listingURL string, page1 []byte) ([]discoveredLink, error) {
	lp, err := parseListing(page1)
	if err != nil {
		return nil, fmt.Errorf("parse page 1: %w", err)
	}
	seen := make(map[string]struct{})
	out := collect(lp.Cases, seen)

	if lp.Total <= resultsPerPage {
		return out, nil
	}
	if lp.ViewState == "" || lp.ViewStateGen == "" {
		return out, fmt.Errorf("missing viewstate on page 1 (total=%d)", lp.Total)
	}

	viewState := lp.ViewState
	viewStateGen := lp.ViewStateGen
	extraPages := lp.Total / resultsPerPage // matches scrape.ts: i <= count/50

	for i := 1; i <= extraPages; i++ {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if c.pageDelay > 0 {
			select {
			case <-time.After(c.pageDelay):
			case <-ctx.Done():
				return out, ctx.Err()
			}
		}

		form := url.Values{}
		form.Set("__EVENTTARGET",
			"ctl00$ContentPlaceHolder1$listRez$RadDataPager1$ctl00$ctl"+pageButton(i))
		form.Set("__VIEWSTATE", viewState)
		form.Set("__VIEWSTATEGENERATOR", viewStateGen)

		body, err := postForm(ctx, c, listingURL, form)
		if err != nil {
			return out, fmt.Errorf("page %d: %w", i+1, err)
		}
		next, err := parseListing(body)
		if err != nil {
			return out, fmt.Errorf("parse page %d: %w", i+1, err)
		}
		out = append(out, collect(next.Cases, seen)...)

		if next.ViewState != "" {
			viewState = next.ViewState
		}
		if next.ViewStateGen != "" {
			viewStateGen = next.ViewStateGen
		}
	}
	return out, nil
}

// collect turns listingCases into discoveredLinks, resolving relative hrefs
// against BaseURL and skipping any URL already in seen. Detail pages live at
// depth+1 from a listing seed (depth 0).
func collect(cases []listingCase, seen map[string]struct{}) []discoveredLink {
	out := make([]discoveredLink, 0, len(cases))
	for _, cs := range cases {
		full := BaseURL + cs.Href
		if _, dup := seen[full]; dup {
			continue
		}
		seen[full] = struct{}{}
		out = append(out, discoveredLink{
			URL:      full,
			Anchor:   cs.Anchor,
			NewDepth: 1,
		})
	}
	return out
}

// --- HTTP helpers ---------------------------------------------------------

func fetchGET(ctx context.Context, c workerCfg, u string) ([]byte, int, string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, 0, "", err
	}
	req.Header.Set("User-Agent", c.ua)
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return body, resp.StatusCode, resp.Header.Get("Content-Type"),
			fmt.Errorf("status %d", resp.StatusCode)
	}
	return body, resp.StatusCode, resp.Header.Get("Content-Type"), nil
}

// postForm POSTs an x-www-form-urlencoded body to u and returns the response
// bytes. Used to drive WebForms __doPostBack pagination.
func postForm(ctx context.Context, c workerCfg, u string, form url.Values) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", u, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", c.ua)
	resp, err := c.httpc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return body, fmt.Errorf("status %d", resp.StatusCode)
	}
	return body, nil
}

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
