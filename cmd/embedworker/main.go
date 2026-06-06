// embedworker is the reference external embedding worker for crawlerv3.
//
// Loop: reserve a batch of pending chunks → embed each text → post the vector
// back. Registry handles Qdrant upsert + collection bookkeeping.
//
// Two backends out of the box:
//
//	--embed-url http://localhost:11434  (Ollama-style POST /api/embeddings)
//	--extract-cmd "python3 /opt/embed.py"  (writes JSON {"embedding":[...]} to stdout, text on stdin)
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	cli "github.com/urfave/cli/v3"
)

func main() {
	cmd := &cli.Command{
		Name:  "embedworker",
		Usage: "external embedding worker for crawlerv3",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "registry", Required: true, Sources: cli.EnvVars("REGISTRY")},
			&cli.StringFlag{Name: "pat", Required: true, Sources: cli.EnvVars("PAT")},
			&cli.IntFlag{Name: "batch", Value: 64},
			&cli.DurationFlag{Name: "idle-sleep", Value: 5 * time.Second},
			&cli.DurationFlag{Name: "max-runtime", Value: 0, Usage: "exit after this duration; 0 = run forever"},

			// HTTP backend (Ollama-style /api/embeddings)
			&cli.StringFlag{Name: "embed-url", Sources: cli.EnvVars("EMBED_URL"),
				Usage: "Ollama-style server URL; mutually exclusive with --extract-cmd"},
			&cli.StringFlag{Name: "embed-model", Value: "nomic-embed-text"},
			&cli.StringFlag{Name: "embed-api-key", Sources: cli.EnvVars("EMBED_API_KEY")},

			// Shell-out backend
			&cli.StringFlag{Name: "extract-cmd", Sources: cli.EnvVars("EXTRACT_CMD"),
				Usage: "shell command; receives chunk text on stdin, must emit {\"embedding\":[...]} on stdout"},
			&cli.DurationFlag{Name: "exec-timeout", Value: 60 * time.Second},
		},
		Action: run,
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "embedworker:", err)
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

type embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

func run(ctx context.Context, cmd *cli.Command) error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-stop
		fmt.Println("embedworker: stopping")
		cancel()
	}()

	embedURL := cmd.String("embed-url")
	extractCmd := cmd.String("extract-cmd")
	if embedURL == "" && extractCmd == "" {
		return fmt.Errorf("embedworker: one of --embed-url or --extract-cmd required")
	}
	if embedURL != "" && extractCmd != "" {
		return fmt.Errorf("embedworker: --embed-url and --extract-cmd are mutually exclusive")
	}

	var e embedder
	if embedURL != "" {
		e = &httpEmbedder{
			URL:    strings.TrimRight(embedURL, "/"),
			Model:  cmd.String("embed-model"),
			APIKey: cmd.String("embed-api-key"),
			HTTP:   &http.Client{Timeout: cmd.Duration("exec-timeout")},
		}
	} else {
		e = &cmdEmbedder{Cmd: extractCmd, Timeout: cmd.Duration("exec-timeout")}
	}

	registry := strings.TrimRight(cmd.String("registry"), "/")
	pat := cmd.String("pat")
	batch := cmd.Int("batch")
	idle := cmd.Duration("idle-sleep")
	deadline := time.Time{}
	if d := cmd.Duration("max-runtime"); d > 0 {
		deadline = time.Now().Add(d)
	}

	api := &http.Client{Timeout: 60 * time.Second}
	for {
		if ctx.Err() != nil {
			return nil
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			fmt.Println("embedworker: max-runtime reached")
			return nil
		}
		chunks, err := reserveBatch(ctx, api, registry, pat, batch)
		if err != nil {
			fmt.Fprintln(os.Stderr, "reserve:", err)
			sleep(ctx, idle)
			continue
		}
		if len(chunks) == 0 {
			sleep(ctx, idle)
			continue
		}
		fmt.Printf("embedworker: leased %d chunks\n", len(chunks))
		results := make([]map[string]any, 0, len(chunks))
		for _, c := range chunks {
			vec, err := e.Embed(ctx, c.Text)
			if err != nil {
				results = append(results, map[string]any{
					"chunk_id": c.ChunkID, "lease_token": c.LeaseToken,
					"failed": true, "reason": err.Error(),
				})
				continue
			}
			results = append(results, map[string]any{
				"chunk_id": c.ChunkID, "lease_token": c.LeaseToken,
				"vector": vec,
			})
		}
		if err := postResults(ctx, api, registry, pat, results); err != nil {
			fmt.Fprintln(os.Stderr, "post:", err)
		}
	}
}

// ---- backends ------------------------------------------------------------

type httpEmbedder struct {
	URL, Model, APIKey string
	HTTP               *http.Client
}

func (h *httpEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	body, _ := json.Marshal(map[string]any{"model": h.Model, "prompt": text})
	req, _ := http.NewRequestWithContext(ctx, "POST", h.URL+"/api/embeddings", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if h.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.APIKey)
	}
	resp, err := h.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("embed-url status=%d body=%s", resp.StatusCode, string(raw))
	}
	var out struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("embed-url decode: %w", err)
	}
	if len(out.Embedding) == 0 {
		return nil, fmt.Errorf("embed-url returned empty vector")
	}
	return out.Embedding, nil
}

type cmdEmbedder struct {
	Cmd     string
	Timeout time.Duration
}

func (c *cmdEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	execCtx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	cmd := exec.CommandContext(execCtx, "sh", "-c", c.Cmd)
	cmd.Stdin = strings.NewReader(text)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("extract-cmd %v: %s", err, stderr.String())
	}
	// stdout may contain extra log lines; find the JSON line with "embedding".
	line := jsonLineWith(stdout.Bytes(), []byte("embedding"))
	if len(line) == 0 {
		return nil, fmt.Errorf("extract-cmd: no JSON line with 'embedding'")
	}
	var out struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(line, &out); err != nil {
		return nil, fmt.Errorf("extract-cmd decode: %w", err)
	}
	if len(out.Embedding) == 0 {
		return nil, fmt.Errorf("extract-cmd returned empty vector")
	}
	return out.Embedding, nil
}

func jsonLineWith(out, needle []byte) []byte {
	sc := bufio.NewScanner(bytes.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 1<<24)
	for sc.Scan() {
		b := sc.Bytes()
		if bytes.Contains(b, needle) && len(b) > 0 && b[0] == '{' {
			return append([]byte(nil), b...)
		}
	}
	return nil
}

// ---- HTTP plumbing -------------------------------------------------------

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
		return nil, fmt.Errorf("reserve status=%d body=%s", resp.StatusCode, string(raw))
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
		return fmt.Errorf("result status=%d body=%s", resp.StatusCode, string(raw))
	}
	fmt.Printf("embedworker: result -> %s\n", string(raw))
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
