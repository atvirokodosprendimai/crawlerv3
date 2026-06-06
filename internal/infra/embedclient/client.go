// Package embedclient is an optional Ollama-style HTTP client used to embed
// search queries server-side (so callers can POST /v1/search with query_text
// instead of a precomputed vector).
//
// Wire-compatible with the Ollama /api/embeddings endpoint:
//
//	POST /api/embeddings  { "model": "...", "prompt": "..." }
//	→ { "embedding": [..floats..] }
//
// Most local embedding servers (Ollama, llama.cpp server, LocalAI) speak this
// shape. If the deployed server uses a different protocol, swap or extend
// this client.
package embedclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Config controls the client.
type Config struct {
	BaseURL string // e.g. "http://localhost:11434" — empty = client disabled
	Model   string // default "nomic-embed-text"
	APIKey  string // optional Bearer token (LocalAI, OpenAI-style)
	Timeout time.Duration
}

// Client embeds a single query text into a vector.
type Client struct {
	cfg  Config
	http *http.Client
}

// New constructs a Client.
func New(cfg Config) *Client {
	if cfg.Model == "" {
		cfg.Model = "nomic-embed-text"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &Client{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}}
}

// Enabled is true when a BaseURL is configured.
func (c *Client) Enabled() bool { return c.cfg.BaseURL != "" }

// Embed runs the Ollama-style request and returns the vector.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("embed client: no BaseURL configured")
	}
	body, _ := json.Marshal(map[string]any{
		"model":  c.cfg.Model,
		"prompt": text,
	})
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.BaseURL+"/api/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embed call: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("embed: status=%d body=%s", resp.StatusCode, string(raw))
	}
	var out struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("embed: decode: %w", err)
	}
	if len(out.Embedding) == 0 {
		return nil, fmt.Errorf("embed: empty vector")
	}
	return out.Embedding, nil
}
