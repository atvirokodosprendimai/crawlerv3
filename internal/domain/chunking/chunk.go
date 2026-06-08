// Package chunking models text chunks ready for embedding.
package chunking

import (
	"context"
	"time"
)

// EmbedStatus is the chunk's life-cycle for the embed worker queue.
type EmbedStatus string

const (
	EmbedPending EmbedStatus = "pending"
	EmbedLeased  EmbedStatus = "leased"
	EmbedDone    EmbedStatus = "done"
	EmbedFailed  EmbedStatus = "failed"
)

// Chunk is a single text segment from an extracted document.
type Chunk struct {
	ID          string // UUID
	DocumentID  int64
	ChunkIndex  int
	Text        string
	TokenCount  int
	VectorID    string
	EmbedStatus EmbedStatus
	Collection  string // vector-store collection hint, joined from extracted_documents at reserve time
}

// Lease for an embed worker batch.
type Lease struct {
	ChunkID   string
	Token     string
	ExpiresAt time.Time
}

// LeasedChunk pairs a Chunk with its issued Lease.
type LeasedChunk struct {
	Chunk Chunk
	Lease Lease
}

// Result is the embed worker's per-chunk payload.
type Result struct {
	ChunkID    string
	VectorID   string
	LeaseToken string
}

// Context is a fully-resolved chunk view used at embed-result time to build
// a Qdrant point payload. Joined from document_chunks + extracted_documents
// + lake_objects + crawl_frontier in one read.
type Context struct {
	ChunkID      string
	Text         string
	ChunkIndex   int
	DocumentID   int64
	Collection   string
	LakeObjectID int64
	CanonicalURL string
}

// Repository is the persistence port.
type Repository interface {
	InsertMany(ctx context.Context, chunks []Chunk) error
	ReserveBatch(
		ctx context.Context,
		workerID int64,
		batch int,
		leaseTTL time.Duration,
		signLease func(chunkUUID string, expires time.Time) (string, []byte),
	) ([]LeasedChunk, error)
	MarkEmbedded(ctx context.Context, chunkID, vectorID string, leaseToken []byte) error
	MarkEmbedFailed(ctx context.Context, chunkID string, leaseToken []byte, reason string) error
	SweepExpired(ctx context.Context, now time.Time) (int64, error)
	CountPending(ctx context.Context) (int64, error)
	ListSince(ctx context.Context, embedStatus EmbedStatus, sinceCreatedAt time.Time, limit int) ([]Chunk, error)
	GetContext(ctx context.Context, chunkID string) (*Context, error)
	RequeueByFilter(ctx context.Context, f RequeueFilter) (int64, error)
	StatusCounts(ctx context.Context) (map[string]int64, error)

	// ReplaceByDocument deletes every chunk attached to documentID and
	// inserts the given fresh slice in one WriteTX. Returns the old chunk
	// IDs so the caller can drive a downstream Qdrant point-delete after
	// the DB commit lands. Used by the rechunk operator command.
	ReplaceByDocument(ctx context.Context, documentID int64, fresh []Chunk) (oldIDs []string, err error)
}

// RequeueFilter selects which chunks to flip back to pending.
// All fields are AND-ed; zero-value means "no constraint".
type RequeueFilter struct {
	Status     EmbedStatus // "" = any non-done
	WorkerID   int64       // 0 = any
	DocumentID int64       // 0 = any
}
