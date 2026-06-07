package app

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/extraction"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/chunker"
)

// RechunkSvc rebuilds document_chunks for every document in a collection,
// honoring the collection's current per-row config (or registry defaults
// when no row is set). Designed for the operator's repair surface — not a
// hot path.
//
// Per-document transaction shape: the chunk repo's ReplaceByDocument deletes
// the existing chunks and inserts the fresh slice in one WriteTX, so a
// registry crash mid-run leaves documents in either fully-old or fully-new
// state, never a mix.
//
// Qdrant cleanup is deferred to Phase 5: this service returns the list of
// old chunk IDs per document so a follow-up can delete the corresponding
// points without recomputing the join.
type RechunkSvc struct {
	Extractions extraction.Repository
	Chunks      chunking.Repository
	Resolver    *CollectionConfigResolver
	Defaults    chunker.Config // tokenizer-bearing registry default
}

// NewRechunkSvc wires the service.
func NewRechunkSvc(e extraction.Repository, c chunking.Repository, r *CollectionConfigResolver, defaults chunker.Config) *RechunkSvc {
	return &RechunkSvc{Extractions: e, Chunks: c, Resolver: r, Defaults: defaults}
}

// RechunkOpts narrows the work set.
type RechunkOpts struct {
	SinceDocID int64 // 0 = start from the beginning
	Limit      int   // 0 = no limit
	DryRun     bool  // report only, no writes
}

// RechunkDoc is one per-document line item.
type RechunkDoc struct {
	DocumentID int64
	OldCount   int
	NewCount   int
	OldChunkIDs []string // empty in DryRun
	Err        error
}

// RechunkReport is the aggregate return.
type RechunkReport struct {
	Collection   string
	Config       chunker.Config
	FromTable    bool
	Documents    []RechunkDoc
	TotalOld     int64
	TotalNew     int64
	Errors       int
	DryRun       bool
}

// Rechunk drives the rebuild for one collection name.
//
// The collection name "-" matches documents whose Collection field is empty
// (the default-collection bucket); any other non-empty string matches by
// equality. The empty string is rejected so the operator must opt in.
func (s *RechunkSvc) Rechunk(ctx context.Context, collection string, opts RechunkOpts) (*RechunkReport, error) {
	if s == nil || s.Extractions == nil || s.Chunks == nil {
		return nil, fmt.Errorf("rechunk: service not wired")
	}
	if collection == "" {
		return nil, fmt.Errorf("rechunk: --collection is required (use '-' for the default bucket)")
	}
	queryColl := collection
	if collection == "-" {
		queryColl = ""
	}

	cfg := s.Defaults
	fromTable := false
	if s.Resolver != nil && queryColl != "" {
		resolved, hit, err := s.Resolver.ResolveConfig(ctx, queryColl)
		if err != nil {
			slog.WarnContext(ctx, "rechunk: resolver lookup", "collection", collection, "err", err)
		} else {
			resolved.Tok = s.Defaults.Tok
			cfg = resolved
			fromTable = hit
		}
	}

	rep := &RechunkReport{
		Collection: collection,
		Config:     cfg,
		FromTable:  fromTable,
		DryRun:     opts.DryRun,
	}

	since := opts.SinceDocID
	for {
		batch := opts.Limit
		const pageSize = 100
		fetch := pageSize
		if batch > 0 && batch < pageSize {
			fetch = batch
		}
		docs, err := s.Extractions.ListByCollection(ctx, queryColl, since, fetch)
		if err != nil {
			return rep, fmt.Errorf("rechunk: list docs: %w", err)
		}
		if len(docs) == 0 {
			break
		}
		for _, d := range docs {
			item := RechunkDoc{DocumentID: d.ID}
			fresh := chunker.Split(d.Text, cfg)
			item.NewCount = len(fresh)

			if opts.DryRun {
				// Probe old count via a no-op replace path is overkill —
				// surface as zero in dry-run; operator gets the new count
				// and decides whether to commit.
				rep.Documents = append(rep.Documents, item)
				rep.TotalNew += int64(item.NewCount)
				since = d.ID
				continue
			}

			cks := toChunks(d.ID, fresh)
			oldIDs, err := s.Chunks.ReplaceByDocument(ctx, d.ID, cks)
			if err != nil {
				item.Err = err
				rep.Errors++
			}
			item.OldCount = len(oldIDs)
			item.OldChunkIDs = oldIDs
			rep.TotalOld += int64(item.OldCount)
			rep.TotalNew += int64(item.NewCount)
			rep.Documents = append(rep.Documents, item)
			since = d.ID
		}
		if opts.Limit > 0 {
			opts.Limit -= len(docs)
			if opts.Limit <= 0 {
				break
			}
		}
		if len(docs) < fetch {
			break
		}
	}
	return rep, nil
}

func toChunks(docID int64, pieces []chunker.Chunk) []chunking.Chunk {
	out := make([]chunking.Chunk, len(pieces))
	for i, p := range pieces {
		out[i] = chunking.Chunk{
			ID:          newUUID(),
			DocumentID:  docID,
			ChunkIndex:  p.Index,
			Text:        p.Text,
			TokenCount:  p.TokenCount,
			EmbedStatus: chunking.EmbedPending,
		}
	}
	return out
}
