package http

import (
	"encoding/json"
	"net/http"

	"github.com/atvirokodosprendimai/crawlerv3/internal/app"
)

// FTSHandler routes POST /v1/search/fts.
//
// Body: {"query": "...", "index": "optional", "limit": 10}
// The query is optionally rewritten via Stanza before being executed against
// Quickwit. Symmetric with the ingest-side Stanza rewrite — same model,
// applied to both writes and reads.
type FTSHandler struct {
	Svc *app.FTSSvc
}

// NewFTSHandler constructs the handler.
func NewFTSHandler(svc *app.FTSSvc) *FTSHandler { return &FTSHandler{Svc: svc} }

type ftsReq struct {
	Query string `json:"query"`
	Index string `json:"index,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type ftsHitDTO struct {
	Score float64        `json:"score"`
	Doc   map[string]any `json:"doc"`
}

// Search handles POST /v1/search/fts.
func (h *FTSHandler) Search(w http.ResponseWriter, r *http.Request) {
	wk, ok := WorkerFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "no_worker", "")
		return
	}
	if !wk.Can("search") {
		writeError(w, http.StatusForbidden, "capability_denied", "worker not allowed to search")
		return
	}
	var req ftsReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	if req.Query == "" {
		writeError(w, http.StatusBadRequest, "missing_query", "query required")
		return
	}
	if req.Limit <= 0 {
		req.Limit = 10
	}
	hits, err := h.Svc.SearchByText(r.Context(), req.Index, req.Query, req.Limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "fts_failed", err.Error())
		return
	}
	items := make([]ftsHitDTO, 0, len(hits))
	for _, h := range hits {
		items = append(items, ftsHitDTO{Score: h.Score, Doc: h.Doc})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}
