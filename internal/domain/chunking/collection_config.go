package chunking

import (
	"context"
	"errors"
)

// CollectionConfig is the per-collection chunker sizing. Lives in its own
// table (collections) so multiple domains pointing at the same collection
// share a config, and the table can grow over time (search re-ranking,
// hybrid weights, vector dim, …) without re-shaping the per-domain rows.
type CollectionConfig struct {
	Name         string
	ChunkTokens  int
	OverlapPrev  int
	OverlapNext  int
	Tokenizer    string
}

// CollectionConfigRepo is the persistence port for the collections table.
//
// Get returns ErrCollectionNotFound when no row matches — callers treat that
// as "use registry defaults" rather than as an error.
type CollectionConfigRepo interface {
	Get(ctx context.Context, name string) (*CollectionConfig, error)
	Upsert(ctx context.Context, cfg CollectionConfig) error
	List(ctx context.Context) ([]CollectionConfig, error)
	Delete(ctx context.Context, name string) error
}

// ErrCollectionNotFound signals a missing row. Get returns it; callers fall
// back to registry defaults.
var ErrCollectionNotFound = errors.New("collection config not found")
