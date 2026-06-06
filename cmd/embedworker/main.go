// embedworker is the external embedding worker for crawlerv3.
//
// Loop:
//  1. Reserve N chunks from the registry (POST /v1/embed/reserve).
//  2. Split into sub-batches of size --embed-batch and dispatch them
//     round-robin across a fleet of Ollama hosts (POST /api/embed with
//     {"model","input":[...]}). Big sub-batches keep GPUs loaded.
//  3. Post vectors back (POST /v1/embed/result). Registry upserts into Qdrant.
//
// No local persistence. The registry is the source of truth.
//
// Fleet config (at least one required):
//
//	--embed-url http://h1:11434 --embed-url http://h2:11434 ...   (repeatable)
//	--embed-urls-file fleet.txt                                   (one URL per line)
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	cli "github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/logx"
)

func main() {
	cmd := &cli.Command{
		Name:  "embedworker",
		Usage: "load-balanced embedding worker for crawlerv3",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "registry", Required: true, Sources: cli.EnvVars("REGISTRY")},
			&cli.StringFlag{Name: "pat", Required: true, Sources: cli.EnvVars("PAT")},

			&cli.IntFlag{Name: "batch", Value: 1024,
				Usage: "chunks per /v1/embed/reserve call"},
			&cli.IntFlag{Name: "embed-batch", Value: 64,
				Usage: "chunks per Ollama /api/embed call (sub-batch size)"},
			&cli.IntFlag{Name: "max-concurrent", Value: 0,
				Usage: "in-flight Ollama calls; 0 = len(embed-url)"},
			&cli.DurationFlag{Name: "idle-sleep", Value: 5 * time.Second},
			&cli.DurationFlag{Name: "max-runtime", Value: 0,
				Usage: "exit after this duration; 0 = run forever"},

			&cli.StringSliceFlag{Name: "embed-url", Sources: cli.EnvVars("EMBED_URL"),
				Usage: "Ollama base URL; repeat for fleet"},
			&cli.StringFlag{Name: "embed-urls-file", Sources: cli.EnvVars("EMBED_URLS_FILE"),
				Usage: "file with one Ollama base URL per line; merged with --embed-url"},
			&cli.StringFlag{Name: "embed-model", Value: "nomic-embed-text", Sources: cli.EnvVars("EMBED_MODEL")},
			&cli.StringFlag{Name: "embed-api-key", Sources: cli.EnvVars("EMBED_API_KEY")},
			&cli.DurationFlag{Name: "embed-timeout", Value: 120 * time.Second},

			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL"),
				Usage: "debug | info | warn | error"},
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			logx.Init("embedworker", c.String("log-level"))
			return ctx, nil
		},
		Action: run,
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("embedworker exit", "err", err)
		os.Exit(1)
	}
}

type embedChunk struct {
	ChunkID      string `json:"chunk_id"`
	DocumentID   int64  `json:"document_id"`
	ChunkIndex   int    `json:"chunk_index"`
	Text         string `json:"text"`
	TokenCount   int    `json:"token_count"`
	Collection   string `json:"collection"`
	LeaseToken   string `json:"lease_token"`
	LeaseExpires int64  `json:"lease_expires_at"`
}

type reserveResp struct {
	Chunks []embedChunk `json:"chunks"`
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

	urls, err := loadURLs(cmd.StringSlice("embed-url"), cmd.String("embed-urls-file"))
	if err != nil {
		return err
	}
	if len(urls) == 0 {
		return errors.New("embedworker: at least one --embed-url or --embed-urls-file required")
	}

	maxConc := cmd.Int("max-concurrent")
	if maxConc <= 0 {
		maxConc = len(urls)
	}

	lb := &fleet{
		urls:   urls,
		model:  cmd.String("embed-model"),
		apiKey: cmd.String("embed-api-key"),
		http:   &http.Client{Timeout: cmd.Duration("embed-timeout")},
	}

	registry := strings.TrimRight(cmd.String("registry"), "/")
	pat := cmd.String("pat")
	batch := cmd.Int("batch")
	embedBatch := cmd.Int("embed-batch")
	if embedBatch <= 0 {
		embedBatch = 64
	}
	idle := cmd.Duration("idle-sleep")
	deadline := time.Time{}
	if d := cmd.Duration("max-runtime"); d > 0 {
		deadline = time.Now().Add(d)
	}

	api := &http.Client{Timeout: 60 * time.Second}
	slog.Info("embedworker started",
		"registry", registry,
		"fleet", len(urls),
		"batch", batch,
		"embed_batch", embedBatch,
		"max_concurrent", maxConc,
		"model", lb.model)

	for {
		if ctx.Err() != nil {
			return nil
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			slog.Info("max-runtime reached")
			return nil
		}
		chunks, err := reserveBatch(ctx, api, registry, pat, batch)
		if err != nil {
			slog.Error("reserve", "err", err)
			sleep(ctx, idle)
			continue
		}
		if len(chunks) == 0 {
			slog.Debug("reserve empty, idling")
			sleep(ctx, idle)
			continue
		}
		start := time.Now()
		slog.Info("batch reserved", "n", len(chunks))

		results := embedFleet(ctx, lb, chunks, embedBatch, maxConc)
		if err := postResults(ctx, api, registry, pat, results); err != nil {
			slog.Error("post", "err", err)
			continue
		}
		slog.Info("batch done", "n", len(results), "elapsed", time.Since(start))
	}
}

// ---- fleet ---------------------------------------------------------------

type fleet struct {
	urls   []string
	model  string
	apiKey string
	http   *http.Client
	count  uint64
}

func (f *fleet) pick() string {
	n := atomic.AddUint64(&f.count, 1)
	return f.urls[(n-1)%uint64(len(f.urls))]
}

type embedReq struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embedResp struct {
	Embeddings [][]float32 `json:"embeddings"`
}

func (f *fleet) embed(ctx context.Context, texts []string) ([][]float32, error) {
	body, _ := json.Marshal(embedReq{Model: f.model, Input: texts})
	url := f.pick() + "/api/embed"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if f.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+f.apiKey)
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s status=%d body=%s", url, resp.StatusCode, snip(raw, 200))
	}
	var out embedResp
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s decode: %w", url, err)
	}
	return out.Embeddings, nil
}

// embedFleet fans sub-batches across the fleet. On batch failure or
// length mismatch, retries each text alone so one poisoned chunk doesn't
// kill the whole sub-batch (vectors lost otherwise -> registry lease expires).
func embedFleet(ctx context.Context, lb *fleet, chunks []embedChunk, subBatch, maxConc int) []map[string]any {
	results := make([]map[string]any, len(chunks))
	sem := make(chan struct{}, maxConc)
	var wg sync.WaitGroup

	for i := 0; i < len(chunks); i += subBatch {
		end := i + subBatch
		if end > len(chunks) {
			end = len(chunks)
		}
		sub := chunks[i:end]
		offset := i

		sem <- struct{}{}
		wg.Add(1)
		go func(off int, sub []embedChunk) {
			defer wg.Done()
			defer func() { <-sem }()

			texts := make([]string, len(sub))
			for j, c := range sub {
				texts[j] = c.Text
			}

			vecs, err := lb.embed(ctx, texts)
			if err == nil && len(vecs) == len(sub) {
				for j, v := range vecs {
					results[off+j] = map[string]any{
						"chunk_id":    sub[j].ChunkID,
						"lease_token": sub[j].LeaseToken,
						"vector":      v,
					}
				}
				return
			}
			if err != nil {
				slog.Warn("sub-batch failed, retrying singly", "n", len(sub), "err", err)
			} else {
				slog.Warn("sub-batch length mismatch, retrying singly", "want", len(sub), "got", len(vecs))
			}

			for j, c := range sub {
				if ctx.Err() != nil {
					return
				}
				sv, serr := lb.embed(ctx, []string{c.Text})
				if serr != nil || len(sv) == 0 || len(sv[0]) == 0 {
					reason := "empty embedding"
					if serr != nil {
						reason = serr.Error()
					}
					results[off+j] = map[string]any{
						"chunk_id":    c.ChunkID,
						"lease_token": c.LeaseToken,
						"failed":      true,
						"reason":      reason,
					}
					continue
				}
				results[off+j] = map[string]any{
					"chunk_id":    c.ChunkID,
					"lease_token": c.LeaseToken,
					"vector":      sv[0],
				}
			}
		}(offset, sub)
	}
	wg.Wait()
	return results
}

// ---- url loading ---------------------------------------------------------

func loadURLs(flagURLs []string, file string) ([]string, error) {
	seen := make(map[string]bool)
	out := make([]string, 0, len(flagURLs))
	add := func(u string) {
		u = strings.TrimSpace(strings.TrimRight(u, "/"))
		if u == "" || strings.HasPrefix(u, "#") || seen[u] {
			return
		}
		seen[u] = true
		out = append(out, u)
	}
	for _, u := range flagURLs {
		add(u)
	}
	if file == "" {
		return out, nil
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", file, err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		add(sc.Text())
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read %s: %w", file, err)
	}
	return out, nil
}

// ---- registry plumbing ---------------------------------------------------

func reserveBatch(ctx context.Context, c *http.Client, registry, pat string, batch int) ([]embedChunk, error) {
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
		return nil, fmt.Errorf("reserve status=%d body=%s", resp.StatusCode, snip(raw, 200))
	}
	var rr reserveResp
	if err := json.Unmarshal(raw, &rr); err != nil {
		return nil, err
	}
	return rr.Chunks, nil
}

func postResults(ctx context.Context, c *http.Client, registry, pat string, results []map[string]any) error {
	body, _ := json.Marshal(map[string]any{"results": results})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/embed/result", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("result status=%d body=%s", resp.StatusCode, snip(raw, 200))
	}
	slog.Info("result posted", "resp", snip(raw, 200))
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

func snip(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
