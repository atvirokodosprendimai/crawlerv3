// agent is the unified worker for crawlerv3.
//
// One process; capabilities chosen via --enable. Each enabled kind runs in
// its own goroutine, reserving from the matching registry endpoint and
// dispatching to an internal handler:
//
//   --enable crawl                          → /v1/jobs/* + HTTP fetch
//   --enable pdf_ocr                        → /v1/tasks/* with kind=pdf_ocr  + extract-cmd
//   --enable docx_to_pdf                    → /v1/tasks/* with kind=docx_to_pdf + extract-cmd
//   --enable html_strip                     → /v1/tasks/* with kind=html_strip   + extract-cmd
//
// Embed is intentionally NOT supported here (model API + vector store client
// belongs in a dedicated embed worker the operator builds).
//
// Server enforces concurrency + capabilities; agent just respects whatever
// the reserve responses contain.
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
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/html"

	cli "github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/logx"
)

func main() {
	cmd := &cli.Command{
		Name:  "agent",
		Usage: "unified crawlerv3 worker (crawl + task kinds)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "registry", Required: true, Sources: cli.EnvVars("REGISTRY")},
			&cli.StringFlag{Name: "pat", Required: true, Sources: cli.EnvVars("PAT")},
			&cli.StringSliceFlag{Name: "enable", Required: true, Usage: "kinds: crawl, pdf_ocr, docx_to_pdf, html_strip"},
			&cli.IntFlag{Name: "batch", Value: 4},
			&cli.DurationFlag{Name: "idle-sleep", Value: 5 * time.Second},
			&cli.DurationFlag{Name: "fetch-timeout", Value: 30 * time.Second},
			&cli.StringFlag{Name: "user-agent", Value: "crawlerv3-agent/0.1"},
			// per-kind extract command, e.g. --extract-cmd.pdf_ocr "tesseract {input} -"
			&cli.StringMapFlag{Name: "extract-cmd", Usage: "per-kind shell command (key=value)"},
			&cli.StringMapFlag{Name: "output-glob", Usage: "per-kind blob-mode glob"},
			&cli.StringMapFlag{Name: "output-content-type", Usage: "per-kind output MIME"},
			&cli.StringMapFlag{Name: "next-processor", Usage: "per-kind chain processor"},
			&cli.StringMapFlag{Name: "mode", Usage: "per-kind mode: text|blob (default text)"},
			&cli.DurationFlag{Name: "exec-timeout", Value: 5 * time.Minute},

			// embed kind backend (slice 11). HTTP only — operators wanting a
			// shell-out embed backend should run the dedicated embedworker bin.
			&cli.StringFlag{Name: "embed-url", Sources: cli.EnvVars("EMBED_URL"),
				Usage: "Ollama-style /api/embeddings server URL for --enable embed"},
			&cli.StringFlag{Name: "embed-model", Value: "nomic-embed-text"},
			&cli.StringFlag{Name: "embed-api-key", Sources: cli.EnvVars("EMBED_API_KEY")},
			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL"),
				Usage: "debug | info | warn | error"},
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			logx.Init("agent", c.String("log-level"))
			return ctx, nil
		},
		Action: run,
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("agent exit", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd *cli.Command) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-stop
		slog.Info("stopping")
		cancel()
	}()

	registry := strings.TrimRight(cmd.String("registry"), "/")
	pat := cmd.String("pat")
	enable := cmd.StringSlice("enable")
	batch := cmd.Int("batch")
	idle := cmd.Duration("idle-sleep")
	fetchTO := cmd.Duration("fetch-timeout")
	ua := cmd.String("user-agent")
	execTO := cmd.Duration("exec-timeout")
	extractCmds := cmd.StringMap("extract-cmd")
	outputGlobs := cmd.StringMap("output-glob")
	outputCTs := cmd.StringMap("output-content-type")
	nextProcs := cmd.StringMap("next-processor")
	modes := cmd.StringMap("mode")

	c := &http.Client{Timeout: 60 * time.Second}
	fetchC := &http.Client{Timeout: fetchTO}

	embedURL := strings.TrimRight(cmd.String("embed-url"), "/")
	embedModel := cmd.String("embed-model")
	embedAPIKey := cmd.String("embed-api-key")

	var wg sync.WaitGroup
	for _, kind := range enable {
		kind := kind
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch kind {
			case "crawl":
				crawlLoop(ctx, c, fetchC, registry, pat, batch, idle, ua)
				return
			case "embed":
				if embedURL == "" {
					slog.Error("--enable embed needs --embed-url")
					return
				}
				embedLoop(ctx, c, registry, pat, batch, idle, embedURL, embedModel, embedAPIKey, execTO)
				return
			}
			cfg := taskKindCfg{
				Kind:        kind,
				ExtractCmd:  extractCmds[kind],
				OutputGlob:  outputGlobs[kind],
				OutputCT:    outputCTs[kind],
				NextProc:    nextProcs[kind],
				Mode:        mapDefault(modes, kind, "text"),
				ExecTimeout: execTO,
			}
			if cfg.ExtractCmd == "" {
				slog.Error("kind missing --extract-cmd", "kind", kind)
				return
			}
			taskLoop(ctx, c, registry, pat, batch, idle, cfg)
		}()
	}
	wg.Wait()
	return nil
}

// --- embed loop -----------------------------------------------------------

type embedChunk struct {
	ChunkID      string `json:"chunk_id"`
	Text         string `json:"text"`
	Collection   string `json:"collection"`
	LeaseToken   string `json:"lease_token"`
	LeaseExpires int64  `json:"lease_expires_at"`
}

func embedLoop(ctx context.Context, c *http.Client, registry, pat string, batch int, idle time.Duration, embedURL, model, apiKey string, execTO time.Duration) {
	httpc := &http.Client{Timeout: execTO}
	for {
		if ctx.Err() != nil {
			return
		}
		chunks, err := embedReserve(ctx, c, registry, pat, batch)
		if err != nil {
			slog.Error("embed reserve", "err", err)
			sleep(ctx, idle)
			continue
		}
		if len(chunks) == 0 {
			slog.Debug("embed reserve empty, idling")
			sleep(ctx, idle)
			continue
		}
		slog.Info("embed batch reserved", "n", len(chunks))
		results := make([]map[string]any, 0, len(chunks))
		for _, ch := range chunks {
			vec, err := embedHTTP(ctx, httpc, embedURL, model, apiKey, ch.Text)
			if err != nil {
				results = append(results, map[string]any{
					"chunk_id": ch.ChunkID, "lease_token": ch.LeaseToken,
					"failed": true, "reason": err.Error(),
				})
				continue
			}
			results = append(results, map[string]any{
				"chunk_id": ch.ChunkID, "lease_token": ch.LeaseToken,
				"vector": vec,
			})
		}
		if err := embedPostResults(ctx, c, registry, pat, results); err != nil {
			slog.Error("embed post", "err", err)
		} else {
			slog.Info("embed batch done", "n", len(results))
		}
	}
}

func embedReserve(ctx context.Context, c *http.Client, registry, pat string, batch int) ([]embedChunk, error) {
	body, _ := json.Marshal(map[string]any{"batch": batch})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/embed/reserve", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
	}
	var rr struct {
		Chunks []embedChunk `json:"chunks"`
	}
	if err := json.Unmarshal(raw, &rr); err != nil {
		return nil, err
	}
	return rr.Chunks, nil
}

func embedHTTP(ctx context.Context, c *http.Client, url, model, apiKey, text string) ([]float32, error) {
	body, _ := json.Marshal(map[string]any{"model": model, "prompt": text})
	req, _ := http.NewRequestWithContext(ctx, "POST", url+"/api/embeddings", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("embed status=%d body=%s", resp.StatusCode, string(raw))
	}
	var out struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if len(out.Embedding) == 0 {
		return nil, fmt.Errorf("embed: empty vector")
	}
	return out.Embedding, nil
}

func embedPostResults(ctx context.Context, c *http.Client, registry, pat string, results []map[string]any) error {
	body, _ := json.Marshal(map[string]any{"results": results})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/embed/result", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return nil
}

func mapDefault(m map[string]string, k, d string) string {
	if v, ok := m[k]; ok && v != "" {
		return v
	}
	return d
}

// --- crawl loop -----------------------------------------------------------

type crawlJob struct {
	JobID        string `json:"job_id"`
	URL          string `json:"url"`
	CanonicalURL string `json:"canonical_url"`
	Depth        int    `json:"depth"`
	AttemptCount int    `json:"attempt_count"`
	LeaseToken   string `json:"lease_token"`
	LeaseExpires int64  `json:"lease_expires_at"`
	MaxBodyBytes int64  `json:"max_body_bytes"`
}

func crawlLoop(ctx context.Context, c, fetchC *http.Client, registry, pat string, batch int, idle time.Duration, ua string) {
	for {
		if ctx.Err() != nil {
			return
		}
		jobs, err := crawlReserve(ctx, c, registry, pat, batch)
		if err != nil {
			slog.Error("crawl reserve", "err", err)
			sleep(ctx, idle)
			continue
		}
		if len(jobs) == 0 {
			slog.Debug("crawl reserve empty, idling")
			sleep(ctx, idle)
			continue
		}
		slog.Info("crawl batch reserved", "n", len(jobs))
		for _, j := range jobs {
			crawlOne(ctx, fetchC, c, registry, pat, ua, j)
		}
	}
}

func crawlReserve(ctx context.Context, c *http.Client, registry, pat string, batch int) ([]crawlJob, error) {
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
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	var rr struct {
		Jobs []crawlJob `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, err
	}
	return rr.Jobs, nil
}

func crawlOne(ctx context.Context, fetchC, c *http.Client, registry, pat, ua string, j crawlJob) {
	slog.Info("crawl fetch", "job_id", j.JobID, "url", j.URL, "depth", j.Depth)
	req, err := http.NewRequestWithContext(ctx, "GET", j.URL, nil)
	if err != nil {
		crawlFail(ctx, c, registry, pat, j, "bad_url", err.Error(), false)
		return
	}
	req.Header.Set("User-Agent", ua)
	resp, err := fetchC.Do(req)
	if err != nil {
		crawlFail(ctx, c, registry, pat, j, "fetch_error", err.Error(), true)
		return
	}
	defer resp.Body.Close()
	limit := j.MaxBodyBytes
	if limit <= 0 {
		limit = 200 << 20
	}
	limited := io.LimitReader(resp.Body, limit+1)
	buf := &bytes.Buffer{}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(buf, h), limited)
	if err != nil {
		crawlFail(ctx, c, registry, pat, j, "read_error", err.Error(), true)
		return
	}
	if n > limit {
		crawlFail(ctx, c, registry, pat, j, "too_large", fmt.Sprintf("body > %d", limit), false)
		return
	}
	ct := resp.Header.Get("Content-Type")
	var links []map[string]any
	if strings.HasPrefix(strings.ToLower(ct), "text/html") {
		links = extractLinks(j.CanonicalURL, j.Depth, buf.Bytes())
	}
	meta := map[string]any{
		"lease_token":      j.LeaseToken,
		"http_status":      resp.StatusCode,
		"content_type":     ct,
		"content_sha256":   hex.EncodeToString(h.Sum(nil)),
		"size":             n,
		"discovered_links": links,
	}
	if err := crawlPostResult(ctx, c, registry, pat, meta, buf.Bytes()); err != nil {
		slog.Error("crawl post", "job_id", j.JobID, "err", err)
		return
	}
	slog.Info("crawl ok", "job_id", j.JobID, "status", resp.StatusCode, "bytes", n, "links", len(links))
}

func crawlPostResult(ctx context.Context, c *http.Client, registry, pat string, meta map[string]any, blob []byte) error {
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	mb, _ := json.Marshal(meta)
	_ = mw.WriteField("meta", string(mb))
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
	resp.Body.Close()
	return nil
}

func crawlFail(ctx context.Context, c *http.Client, registry, pat string, j crawlJob, code, msg string, retryable bool) {
	body, _ := json.Marshal(map[string]any{
		"lease_token":   j.LeaseToken,
		"http_status":   0,
		"error_code":    code,
		"error_message": msg,
		"retryable":     retryable,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/jobs/fail", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	if resp, err := c.Do(req); err == nil {
		resp.Body.Close()
	}
}

func extractLinks(pageURL string, depth int, body []byte) []map[string]any {
	base, err := url.Parse(pageURL)
	if err != nil {
		return nil
	}
	z := html.NewTokenizer(bytes.NewReader(body))
	var out []map[string]any
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
			var href, rel string
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
			if href == "" {
				continue
			}
			u, err := base.Parse(href)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
				continue
			}
			out = append(out, map[string]any{
				"url": u.String(), "anchor": "", "rel": rel, "new_depth": depth + 1,
			})
		}
	}
}

// --- task loop ------------------------------------------------------------

type taskKindCfg struct {
	Kind        string
	Mode        string // text | blob
	ExtractCmd  string
	OutputGlob  string
	OutputCT    string
	NextProc    string
	ExecTimeout time.Duration
}

type taskItem struct {
	TaskID          int64  `json:"task_id"`
	Processor       string `json:"processor"`
	LakeObjectID    int64  `json:"lake_object_id"`
	BlobURL         string `json:"blob_url"`
	BlobContentType string `json:"blob_content_type"`
	BlobSizeBytes   int64  `json:"blob_size_bytes"`
	AttemptCount    int    `json:"attempt_count"`
	LeaseToken      string `json:"lease_token"`
	LeaseExpiresAt  int64  `json:"lease_expires_at"`
}

func taskLoop(ctx context.Context, c *http.Client, registry, pat string, batch int, idle time.Duration, cfg taskKindCfg) {
	for {
		if ctx.Err() != nil {
			return
		}
		ts, err := taskReserve(ctx, c, registry, pat, []string{cfg.Kind}, batch)
		if err != nil {
			slog.Error("task reserve", "kind", cfg.Kind, "err", err)
			sleep(ctx, idle)
			continue
		}
		if len(ts) == 0 {
			slog.Debug("task reserve empty, idling", "kind", cfg.Kind)
			sleep(ctx, idle)
			continue
		}
		slog.Info("task batch reserved", "kind", cfg.Kind, "n", len(ts))
		for _, t := range ts {
			taskOne(ctx, c, registry, pat, t, cfg)
		}
	}
}

func taskReserve(ctx context.Context, c *http.Client, registry, pat string, kinds []string, batch int) ([]taskItem, error) {
	body, _ := json.Marshal(map[string]any{"kinds": kinds, "batch": batch})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/reserve", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	var rr struct {
		Tasks []taskItem `json:"tasks"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, err
	}
	return rr.Tasks, nil
}

func taskOne(ctx context.Context, c *http.Client, registry, pat string, t taskItem, cfg taskKindCfg) {
	slog.Info("task start", "kind", cfg.Kind, "task_id", t.TaskID, "blob", t.BlobURL, "size", t.BlobSizeBytes)
	scratch, err := os.MkdirTemp("", "agent-task-*")
	if err != nil {
		taskFail(ctx, c, registry, pat, t, "scratch_mkdir", err.Error(), true)
		return
	}
	defer os.RemoveAll(scratch)
	input := filepath.Join(scratch, "input"+extFor(t.BlobContentType))
	if err := downloadBlob(ctx, c, registry, pat, t.BlobURL, input); err != nil {
		taskFail(ctx, c, registry, pat, t, "download", err.Error(), true)
		return
	}
	outdir := filepath.Join(scratch, "out")
	_ = os.MkdirAll(outdir, 0o755)

	cmdStr := strings.NewReplacer("{input}", input, "{outdir}", outdir).Replace(cfg.ExtractCmd)
	execCtx, cancel := context.WithTimeout(ctx, cfg.ExecTimeout)
	defer cancel()
	c2 := exec.CommandContext(execCtx, "sh", "-c", cmdStr)
	var stdout, stderr bytes.Buffer
	c2.Stdout = &stdout
	c2.Stderr = &stderr
	if err := c2.Run(); err != nil {
		taskFail(ctx, c, registry, pat, t, "extract_cmd",
			fmt.Sprintf("%v: %s", err, stderr.String()), true)
		return
	}

	switch cfg.Mode {
	case "blob":
		glob := strings.ReplaceAll(cfg.OutputGlob, "{outdir}", outdir)
		if glob == "" {
			glob = filepath.Join(outdir, "*")
		}
		matches, _ := filepath.Glob(glob)
		if len(matches) == 0 {
			taskFail(ctx, c, registry, pat, t, "no_output", "no file matching "+glob, false)
			return
		}
		if err := taskPostBlob(ctx, c, registry, pat, t, matches[0], cfg.OutputCT, cfg.NextProc); err != nil {
			slog.Error("task post blob", "task_id", t.TaskID, "err", err)
		} else {
			slog.Info("task ok", "kind", cfg.Kind, "task_id", t.TaskID, "mode", "blob", "output", matches[0])
		}
	default: // text
		if err := taskPostText(ctx, c, registry, pat, t, stdout.String()); err != nil {
			slog.Error("task post text", "task_id", t.TaskID, "err", err)
		} else {
			slog.Info("task ok", "kind", cfg.Kind, "task_id", t.TaskID, "mode", "text", "text_bytes", stdout.Len())
		}
	}
}

func downloadBlob(ctx context.Context, c *http.Client, registry, pat, blobURL, dst string) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", registry+blobURL, nil)
	req.Header.Set("Authorization", "Bearer "+pat)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func taskPostText(ctx context.Context, c *http.Client, registry, pat string, t taskItem, text string) error {
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	meta := map[string]any{
		"task_id":        t.TaskID,
		"lease_token":    t.LeaseToken,
		"extracted_text": text,
	}
	mb, _ := json.Marshal(meta)
	_ = mw.WriteField("meta", string(mb))
	if err := mw.Close(); err != nil {
		return err
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/result", body)
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func taskPostBlob(ctx context.Context, c *http.Client, registry, pat string, t taskItem, path, ct, next string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	buf := &bytes.Buffer{}
	if _, err := io.Copy(io.MultiWriter(h, buf), f); err != nil {
		return err
	}
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	meta := map[string]any{
		"task_id":               t.TaskID,
		"lease_token":           t.LeaseToken,
		"output_content_type":   ct,
		"output_content_sha256": hex.EncodeToString(h.Sum(nil)),
		"next_processor":        next,
	}
	mb, _ := json.Marshal(meta)
	_ = mw.WriteField("meta", string(mb))
	fw, _ := mw.CreateFormFile("blob", filepath.Base(path))
	_, _ = fw.Write(buf.Bytes())
	if err := mw.Close(); err != nil {
		return err
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/result", body)
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func taskFail(ctx context.Context, c *http.Client, registry, pat string, t taskItem, code, msg string, retryable bool) {
	body, _ := json.Marshal(map[string]any{
		"task_id":       t.TaskID,
		"lease_token":   t.LeaseToken,
		"error_code":    code,
		"error_message": msg,
		"retryable":     retryable,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/fail", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	if resp, err := c.Do(req); err == nil {
		resp.Body.Close()
	}
}

// --- shared helpers -------------------------------------------------------

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func extFor(ct string) string {
	ct = strings.ToLower(strings.TrimSpace(ct))
	if i := strings.Index(ct, ";"); i > 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case "application/pdf":
		return ".pdf"
	case "text/html":
		return ".html"
	case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
		return ".docx"
	}
	return ".bin"
}
