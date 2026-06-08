package app

import (
	"context"
	"errors"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/chunker"
)

// CollectionConfigResolver maps a collection name to a chunker.Config. The
// resolver hands back the registry defaults when the collections table has
// no row for the name, so an empty table behaves as "use defaults for every
// collection" — the operator only adds rows to deviate.
//
// The default tokenizer is the one registered at startup (cl100k_base unless
// overridden). When a row specifies a tokenizer different from the default,
// the resolver currently still uses the default — wiring a tokenizer
// registry for per-row overrides is a follow-up; the field is stored for
// forward compatibility and surfaced via list-collections.
type CollectionConfigResolver struct {
	Repo     chunking.CollectionConfigRepo
	Defaults chunker.Config // registry-wide defaults; Tok must be set
}

// NewCollectionConfigResolver wires the resolver to a repo + defaults.
func NewCollectionConfigResolver(repo chunking.CollectionConfigRepo, defaults chunker.Config) *CollectionConfigResolver {
	return &CollectionConfigResolver{Repo: repo, Defaults: defaults}
}

// ResolveConfig returns the chunker config for a collection name and a flag
// telling the caller whether the config came from the collections table
// (true) or the registry defaults (false). On any lookup error other than
// ErrCollectionNotFound, returns defaults with the error so the caller can
// log without blocking ingest.
func (r *CollectionConfigResolver) ResolveConfig(ctx context.Context, name string) (chunker.Config, bool, error) {
	if r == nil || r.Repo == nil || name == "" {
		return r.defaults(), false, nil
	}
	cfg, err := r.Repo.Get(ctx, name)
	if errors.Is(err, chunking.ErrCollectionNotFound) {
		return r.defaults(), false, nil
	}
	if err != nil {
		return r.defaults(), false, err
	}
	out := r.defaults()
	if cfg.ChunkTokens > 0 {
		out.ChunkTokens = cfg.ChunkTokens
	}
	if cfg.OverlapPrev >= 0 {
		out.OverlapPrev = cfg.OverlapPrev
	}
	if cfg.OverlapNext >= 0 {
		out.OverlapNext = cfg.OverlapNext
	}
	return out, true, nil
}

func (r *CollectionConfigResolver) defaults() chunker.Config {
	if r == nil {
		return chunker.Defaults()
	}
	return r.Defaults
}
