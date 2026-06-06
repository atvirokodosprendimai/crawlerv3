package http

import (
	"encoding/json"
	"net/http"

	"github.com/atvirokodosprendimai/crawlerv3/internal/app"
)

// SearchHandler routes POST /v1/search.
type SearchHandler struct {
	Svc *app.SearchSvc
}

// NewSearchHandler constructs the handler.
func NewSearchHandler(svc *app.SearchSvc) *SearchHandler { return &SearchHandler{Svc: svc} }

// searchReq is the body shape.
type searchReq struct {
	Collection  string         `json:"collection"`
	QueryVector []float32      `json:"query_vector,omitempty"`
	QueryText   string         `json:"query_text,omitempty"`
	Limit       int            `json:"limit,omitempty"`
	Filter      map[string]any `json:"filter,omitempty"`
}

// searchHitDTO mirrors app.SearchHit for the wire.
type searchHitDTO struct {
	ChunkID      string  `json:"chunk_id"`
	LakeObjectID int64   `json:"lake_object_id"`
	DocumentID   int64   `json:"document_id"`
	ChunkIndex   int     `json:"chunk_index"`
	Score        float32 `json:"score"`
	Text         string  `json:"text"`
	URL          string  `json:"url"`
	Collection   string  `json:"collection"`
}

// Search handles POST /v1/search.
func (h *SearchHandler) Search(w http.ResponseWriter, r *http.Request) {
	wk, ok := WorkerFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "no_worker", "")
		return
	}
	if !wk.Can("search") {
		writeError(w, http.StatusForbidden, "capability_denied", "worker not allowed to search")
		return
	}
	var req searchReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	if req.Collection == "" {
		writeError(w, http.StatusBadRequest, "missing_collection", "collection required")
		return
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}
	var hits []app.SearchHit
	var err error
	switch {
	case len(req.QueryVector) > 0:
		hits, err = h.Svc.SearchByVector(r.Context(), req.Collection, req.QueryVector, req.Limit, req.Filter)
	case req.QueryText != "":
		hits, err = h.Svc.SearchByText(r.Context(), req.Collection, req.QueryText, req.Limit, req.Filter)
	default:
		writeError(w, http.StatusBadRequest, "missing_query", "query_vector or query_text required")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "search_failed", err.Error())
		return
	}
	items := make([]searchHitDTO, 0, len(hits))
	for _, h := range hits {
		items = append(items, searchHitDTO{
			ChunkID: h.ChunkID, LakeObjectID: h.LakeObjectID,
			DocumentID: h.DocumentID, ChunkIndex: h.ChunkIndex,
			Score: h.Score, Text: h.Text, URL: h.URL,
			Collection: h.Collection,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}
