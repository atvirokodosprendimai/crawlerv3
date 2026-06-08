// Package qdrant is a small HTTP client for the subset of the Qdrant REST API
// the registry needs: ensure collection, upsert point, search.
package qdrant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// Config tunes the client.
type Config struct {
	BaseURL  string // e.g. "http://localhost:6333" — empty = client disabled
	APIKey   string // optional, set as `api-key` header
	Shards   int    // shard_number on collection create; default 9
	Distance string // Cosine | Dot | Euclid; default Cosine
	Timeout  time.Duration
}

// Client talks to one Qdrant cluster.
type Client struct {
	cfg    Config
	http   *http.Client
	ensure sync.Map // collection_name → struct{} once created
}

// New builds a Client. Pass an empty BaseURL to construct a disabled (no-op)
// client; callers must check Enabled() before any push/search.
func New(cfg Config) *Client {
	if cfg.Shards <= 0 {
		cfg.Shards = 9
	}
	if cfg.Distance == "" {
		cfg.Distance = "Cosine"
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &Client{cfg: cfg, http: &http.Client{Timeout: cfg.Timeout}}
}

// Enabled is true when a BaseURL is configured.
func (c *Client) Enabled() bool { return c.cfg.BaseURL != "" }

// EnsureCollection creates the collection if absent. dim is the vector size;
// passed lazily on the first observed embed. Idempotent + cached.
func (c *Client) EnsureCollection(ctx context.Context, name string, dim int) error {
	if !c.Enabled() {
		return nil
	}
	if _, ok := c.ensure.Load(name); ok {
		return nil
	}
	// HEAD-style check: GET /collections/{name}; 200 = exists.
	if exists, err := c.collectionExists(ctx, name); err != nil {
		return err
	} else if exists {
		c.ensure.Store(name, struct{}{})
		return nil
	}
	// Create with shards.
	body := map[string]any{
		"vectors":            map[string]any{"size": dim, "distance": c.cfg.Distance},
		"shard_number":       c.cfg.Shards,
		"replication_factor": 1,
	}
	if _, _, err := c.do(ctx, "PUT", "/collections/"+name, body); err != nil {
		return fmt.Errorf("qdrant ensure %q: %w", name, err)
	}
	c.ensure.Store(name, struct{}{})
	return nil
}

// Point is one row to upsert.
type Point struct {
	ID      string         `json:"id"`
	Vector  []float32      `json:"vector"`
	Payload map[string]any `json:"payload,omitempty"`
}

// Upsert pushes one or more points (idempotent on ID).
func (c *Client) Upsert(ctx context.Context, collection string, points []Point) error {
	if !c.Enabled() || len(points) == 0 {
		return nil
	}
	body := map[string]any{"points": points}
	_, _, err := c.do(ctx, "PUT", "/collections/"+collection+"/points?wait=true", body)
	if err != nil {
		return fmt.Errorf("qdrant upsert %q: %w", collection, err)
	}
	return nil
}

// DeletePoints removes points by id. Used by the rechunk path to drop
// vectors that pointed at chunks the operator just replaced.
//
// Behavior contract:
//   - Disabled client (no qdrant URL) → no-op, returns nil.
//   - Empty ids → no-op, returns nil.
//   - Qdrant returns 404 for a missing collection — treated as a no-op
//     rather than an error, because rechunk runs against arbitrary
//     collection names and only some have a Qdrant counterpart wired up.
//   - Any other non-2xx is surfaced as an error.
func (c *Client) DeletePoints(ctx context.Context, collection string, ids []string) error {
	if !c.Enabled() || len(ids) == 0 {
		return nil
	}
	body := map[string]any{"points": ids}
	status, _, err := c.do(ctx, "POST", "/collections/"+collection+"/points/delete?wait=true", body)
	if err == nil {
		return nil
	}
	if status == 404 {
		// Collection doesn't exist on this Qdrant — nothing to clean up.
		return nil
	}
	return fmt.Errorf("qdrant delete %q: %w", collection, err)
}

// SearchHit is one search result.
type SearchHit struct {
	ID      string         `json:"id"`
	Score   float32        `json:"score"`
	Payload map[string]any `json:"payload"`
}

// Search runs a vector query against the collection.
func (c *Client) Search(ctx context.Context, collection string, vector []float32, limit int, filter map[string]any) ([]SearchHit, error) {
	if !c.Enabled() {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	body := map[string]any{
		"vector":       vector,
		"limit":        limit,
		"with_payload": true,
		"with_vector":  false,
	}
	if len(filter) > 0 {
		body["filter"] = filter
	}
	_, raw, err := c.do(ctx, "POST", "/collections/"+collection+"/points/search", body)
	if err != nil {
		return nil, fmt.Errorf("qdrant search %q: %w", collection, err)
	}
	var resp struct {
		Result []SearchHit `json:"result"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("qdrant search decode: %w", err)
	}
	return resp.Result, nil
}

// collectionExists returns true on HTTP 200. Other Qdrant deployments may
// return 4xx for "missing" — treat any non-2xx as missing and only surface
// network / transport errors.
func (c *Client) collectionExists(ctx context.Context, name string) (bool, error) {
	status, _, err := c.do(ctx, "GET", "/collections/"+name, nil)
	if status == 0 {
		return false, err // transport failure
	}
	if status == 200 {
		return true, nil
	}
	return false, nil
}

// do issues an HTTP request. Returns the HTTP status (0 = transport failure),
// the raw body, and a non-nil error for any non-2xx response.
func (c *Client) do(ctx context.Context, method, path string, body any) (int, []byte, error) {
	var rb io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rb = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.BaseURL+path, rb)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.cfg.APIKey != "" {
		req.Header.Set("api-key", c.cfg.APIKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, raw, nil
	}
	return resp.StatusCode, raw, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(raw))
}
