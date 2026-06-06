// Package workerid models the human-facing worker identity (PAT holder).
// Named "workerid" to avoid clashing with go-routine "worker" concepts.
package workerid

import "time"

// Capability is a named permission grantable to a Worker.
// Only endpoint-gated capabilities live here — these gate fixed registry routes
// and the HTTP handlers reference them by literal string.
// Processor kinds (pdf_ocr, html_strip, etc.) and tenant tags (e.g. vvtat)
// are NOT endpoint-gated; they flow through tasks.go which matches any string
// against worker capabilities. Discover those by querying the workers table.
type Capability struct {
	Name        string
	Group       string // "reserve" | "read"
	Description string
}

// EndpointGatedCapabilities lists capabilities hardcoded into registry HTTP handlers.
func EndpointGatedCapabilities() []Capability {
	return []Capability{
		{Name: "crawl", Group: "reserve", Description: "POST /v1/jobs/reserve — fetch crawl frontier jobs"},
		{Name: "embed", Group: "reserve", Description: "embed endpoint — reserve embedding work"},
		{Name: "lake_read", Group: "read", Description: "read raw lake objects"},
		{Name: "extracted_read", Group: "read", Description: "read extracted content"},
		{Name: "chunks_read", Group: "read", Description: "read document chunks"},
	}
}

// Worker is a PAT-bound external participant.
type Worker struct {
	ID              int64
	PATHash         []byte
	Label           string
	IPLast          string
	ReputationScore int
	BannedAt        *time.Time
	CreatedAt       time.Time
	Capabilities    []string
	MaxConcurrent   int
	LastSeenAt      *time.Time
}

// IsBanned is true when BannedAt is set.
func (w Worker) IsBanned() bool { return w.BannedAt != nil }

// Can returns true when the worker is allowed to handle the given kind.
// An empty Capabilities list grants any kind (backward compat with legacy rows).
func (w Worker) Can(kind string) bool {
	if len(w.Capabilities) == 0 {
		return true
	}
	for _, c := range w.Capabilities {
		if c == kind {
			return true
		}
	}
	return false
}
