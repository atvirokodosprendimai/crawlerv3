// Package quickwit is a small HTTP client for the subset of the Quickwit REST
// API the registry needs: ingest one document, search by query string.
//
// Quickwit REST conventions used here:
//   POST {BaseURL}/api/v1/{index}/ingest     NDJSON body, one doc per line
//   POST {BaseURL}/api/v1/{index}/search     {query, max_hits}
//
// Empty BaseURL means the client is disabled; all methods become no-ops or
// return an "fts: quickwit not configured" error on read paths.
package quickwit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Config tunes the client.
type Config struct {
	BaseURL string        // empty disables the client
	APIKey  string        // optional bearer
	Timeout time.Duration // default 30s
}

// Client talks to one Quickwit cluster.
type Client struct {
	cfg  Config
	http *http.Client
}

// New constructs a Client. Empty BaseURL = disabled.
func New(cfg Config) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &Client{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}}
}

// Enabled returns true when a BaseURL is configured.
func (c *Client) Enabled() bool { return c.cfg.BaseURL != "" }

// Doc is one document to index. Free-form keys map to Quickwit's doc mapping.
type Doc map[string]any

// Ingest pushes a single document into the named index as NDJSON.
// Idempotent on the caller's chosen primary key (Quickwit upsert semantics
// depend on the doc-mapping config).
func (c *Client) Ingest(ctx context.Context, index string, doc Doc) error {
	if !c.Enabled() || len(doc) == 0 {
		return nil
	}
	line, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	body := append(line, '\n')
	url := fmt.Sprintf("%s/api/v1/%s/ingest", c.cfg.BaseURL, index)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("quickwit ingest %q: %w", index, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("quickwit ingest %q status=%d body=%s", index, resp.StatusCode, string(raw))
	}
	return nil
}

// Hit is one search result (the subset the registry forwards back).
type Hit struct {
	Score float64        `json:"_score"`
	Doc   map[string]any `json:"-"`
}

// Search runs a query-string search and returns up to limit hits.
func (c *Client) Search(ctx context.Context, index, query string, limit int) ([]Hit, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("quickwit: disabled")
	}
	if limit <= 0 {
		limit = 10
	}
	body, _ := json.Marshal(map[string]any{
		"query":    query,
		"max_hits": limit,
	})
	url := fmt.Sprintf("%s/api/v1/%s/search", c.cfg.BaseURL, index)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("quickwit search %q: %w", index, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("quickwit search %q status=%d body=%s", index, resp.StatusCode, string(raw))
	}
	var envelope struct {
		Hits []map[string]any `json:"hits"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("quickwit search decode: %w", err)
	}
	out := make([]Hit, 0, len(envelope.Hits))
	for _, h := range envelope.Hits {
		var score float64
		if s, ok := h["_score"].(float64); ok {
			score = s
		}
		out = append(out, Hit{Score: score, Doc: h})
	}
	return out, nil
}
