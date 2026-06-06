// Package stanza is a tiny HTTP client for an external text-rewrite service
// (typically a Python Stanza + AI-model pipeline behind a simple HTTP endpoint).
//
// Contract: POST {BaseURL}{Path} with JSON {"text": "<input>"} returns
// {"text": "<rewritten>"}. The service is treated as opaque — the registry
// only knows it takes text and returns text.
//
// Use Enabled() to check whether a base URL was configured. A disabled client
// is a no-op: callers receive the input string back unchanged.
package stanza

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
	Path    string        // default "/rewrite"
	APIKey  string        // optional bearer
	Timeout time.Duration // default 30s
}

// Client talks to one Stanza rewrite service.
type Client struct {
	cfg  Config
	http *http.Client
}

// New constructs a Client. Empty BaseURL = disabled (no-op).
func New(cfg Config) *Client {
	if cfg.Path == "" {
		cfg.Path = "/rewrite"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &Client{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}}
}

// Enabled returns true when a BaseURL is configured.
func (c *Client) Enabled() bool { return c.cfg.BaseURL != "" }

// Rewrite POSTs text to the rewrite endpoint and returns the mutated text.
// When the client is disabled, returns text unchanged.
func (c *Client) Rewrite(ctx context.Context, text string) (string, error) {
	if !c.Enabled() || text == "" {
		return text, nil
	}
	body, _ := json.Marshal(map[string]string{"text": text})
	req, err := http.NewRequestWithContext(ctx, "POST", c.cfg.BaseURL+c.cfg.Path, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("stanza: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("stanza status=%d body=%s", resp.StatusCode, string(raw))
	}
	var out struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", fmt.Errorf("stanza decode: %w", err)
	}
	if out.Text == "" {
		return text, nil
	}
	return out.Text, nil
}
