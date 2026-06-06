package app

import (
	"context"
	"errors"
	"strconv"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/embedclient"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/qdrant"
)

// SearchSvc is the read-side facade for vector retrieval.
//
// It wraps a Qdrant client (required) and an optional EmbedClient that turns
// query_text into a vector when callers don't precompute one.
type SearchSvc struct {
	Qdrant *qdrant.Client
	Embed  *embedclient.Client
}

// NewSearchSvc constructs a SearchSvc.
func NewSearchSvc(q *qdrant.Client, e *embedclient.Client) *SearchSvc {
	return &SearchSvc{Qdrant: q, Embed: e}
}

// SearchHit is one returned vector match.
type SearchHit struct {
	ChunkID      string
	LakeObjectID int64
	DocumentID   int64
	ChunkIndex   int
	Score        float32
	Text         string
	URL          string
	Collection   string
}

// SearchByVector runs a raw vector query against the named collection.
func (s *SearchSvc) SearchByVector(ctx context.Context, collection string, vector []float32, limit int, filter map[string]any) ([]SearchHit, error) {
	if s.Qdrant == nil || !s.Qdrant.Enabled() {
		return nil, errors.New("search: qdrant not configured")
	}
	if collection == "" {
		return nil, errors.New("search: collection required")
	}
	if len(vector) == 0 {
		return nil, errors.New("search: empty query vector")
	}
	raw, err := s.Qdrant.Search(ctx, collection, vector, limit, filter)
	if err != nil {
		return nil, err
	}
	return mapHits(raw, collection), nil
}

// SearchByText embeds the query via the optional EmbedClient and then
// delegates to SearchByVector.
func (s *SearchSvc) SearchByText(ctx context.Context, collection, text string, limit int, filter map[string]any) ([]SearchHit, error) {
	if s.Embed == nil || !s.Embed.Enabled() {
		return nil, errors.New("search: query_text requires --embed-url (none configured)")
	}
	vec, err := s.Embed.Embed(ctx, text)
	if err != nil {
		return nil, err
	}
	return s.SearchByVector(ctx, collection, vec, limit, filter)
}

func mapHits(hits []qdrant.SearchHit, collection string) []SearchHit {
	out := make([]SearchHit, 0, len(hits))
	for _, h := range hits {
		out = append(out, SearchHit{
			ChunkID:      h.ID,
			Score:        h.Score,
			Collection:   collection,
			LakeObjectID: numToInt64(h.Payload["lake_object_id"]),
			DocumentID:   numToInt64(h.Payload["document_id"]),
			ChunkIndex:   int(numToInt64(h.Payload["chunk_index"])),
			Text:         toStr(h.Payload["text"]),
			URL:          toStr(h.Payload["url"]),
		})
	}
	return out
}

func numToInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	case int:
		return int64(x)
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}

func toStr(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
