// Package frontier models the URL crawl queue.
package frontier

import "time"

// Status of a frontier row.
type Status string

const (
	StatusQueued Status = "queued"
	StatusLeased Status = "leased"
	StatusDone   Status = "done"
	StatusFailed Status = "failed"
	StatusDead   Status = "dead"
)

// Job is a unit of crawl work handed to a worker.
type Job struct {
	URLHash       []byte
	URL           string
	CanonicalURL  string
	DomainID      int64
	Depth         int
	Priority      int
	AttemptCount  int
	MaxAttempts   int
	ParentURLHash []byte
}

// Lease describes a worker's hold on a Job.
type Lease struct {
	JobURLHash []byte
	WorkerID   int64
	Token      string
	ExpiresAt  time.Time
}

// ReserveRequest carries reservation parameters.
type ReserveRequest struct {
	WorkerID     int64
	Batch        int
	Capabilities []string
}

// Result is the worker's completion payload.
type Result struct {
	HTTPStatus      int
	ContentType     string
	ContentSHA256   []byte
	FileSize        int64
	StorageBackend  string
	StorageKey      string
	DiscoveredLinks []DiscoveredLink
}

// DiscoveredLink is a href observed on the fetched page.
type DiscoveredLink struct {
	URL      string
	Anchor   string
	Rel      string
	NewDepth int
}

// Failure describes a non-success outcome.
type Failure struct {
	HTTPStatus   int
	ErrorCode    string
	ErrorMessage string
	Retryable    bool
}
