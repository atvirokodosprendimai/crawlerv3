// fts.go — full-text search via Quickwit, with an optional Stanza rewrite step
// applied to both ingest text and query strings.
//
// FTSSvc is wired into both the indexing pipeline (Pipeline.execHTML /
// execPDF / execTextPassthrough, and TaskSvc.AcceptText) and the read API
// (/v1/search/fts), so the same Stanza pipeline mutates writes and reads
// symmetrically.

package app

import (
	"context"
	"errors"
	"log/slog"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/quickwit"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/stanza"
)

// FTSSvc is the registry-side facade for full-text indexing + search.
type FTSSvc struct {
	Stanza   *stanza.Client
	Quickwit *quickwit.Client
	Index    string // default Quickwit index name when caller omits one
}

// NewFTSSvc constructs an FTSSvc. Either client may be disabled — if Quickwit
// is disabled the service itself is effectively a no-op (Enabled() returns
// false).
func NewFTSSvc(s *stanza.Client, q *quickwit.Client, defaultIndex string) *FTSSvc {
	return &FTSSvc{Stanza: s, Quickwit: q, Index: defaultIndex}
}

// Enabled returns true when Quickwit is configured. Stanza is independent;
// the service still works without it (passthrough text).
func (f *FTSSvc) Enabled() bool { return f != nil && f.Quickwit != nil && f.Quickwit.Enabled() }

// OnExtracted is called by the pipeline whenever extracted_documents gains a
// new row. It runs the text through Stanza (if enabled) and pushes the result
// to Quickwit. Failure is logged but never propagated — FTS indexing is a
// best-effort sink, not a blocker for the main pipeline.
func (f *FTSSvc) OnExtracted(ctx context.Context, documentID, lakeObjectID int64, collection, text string) {
	if !f.Enabled() || text == "" {
		return
	}
	rewritten, err := f.Stanza.Rewrite(ctx, text)
	if err != nil {
		slog.Warn("fts: stanza rewrite failed; falling back to raw text",
			"document_id", documentID, "err", err)
		rewritten = text
	}
	doc := quickwit.Doc{
		"document_id":    documentID,
		"lake_object_id": lakeObjectID,
		"collection":     collection,
		"text":           rewritten,
	}
	if err := f.Quickwit.Ingest(ctx, f.Index, doc); err != nil {
		slog.Warn("fts: quickwit ingest failed",
			"document_id", documentID, "index", f.Index, "err", err)
	}
}

// SearchByText runs query → (optional Stanza rewrite) → Quickwit. The index
// argument is optional; empty falls back to FTSSvc.Index.
func (f *FTSSvc) SearchByText(ctx context.Context, index, query string, limit int) ([]quickwit.Hit, error) {
	if !f.Enabled() {
		return nil, errors.New("fts: quickwit not configured")
	}
	if index == "" {
		index = f.Index
	}
	if index == "" {
		return nil, errors.New("fts: index required (set --quickwit-index or pass index in request)")
	}
	rewritten, err := f.Stanza.Rewrite(ctx, query)
	if err != nil {
		slog.Warn("fts: stanza rewrite failed for query; using raw query", "err", err)
		rewritten = query
	}
	return f.Quickwit.Search(ctx, index, rewritten, limit)
}
