package http

import (
	"encoding/json"
	"net/http"

	"github.com/atvirokodosprendimai/crawlerv3/internal/app"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
)

// embedReserveReq is the POST body for /v1/embed/reserve.
type embedReserveReq struct {
	Batch int `json:"batch"`
}

// embedChunkDTO is the per-chunk payload returned to embed workers.
type embedChunkDTO struct {
	ChunkID      string `json:"chunk_id"`
	DocumentID   int64  `json:"document_id"`
	ChunkIndex   int    `json:"chunk_index"`
	Text         string `json:"text"`
	TokenCount   int    `json:"token_count"`
	Collection   string `json:"collection"` // per-domain vector-store hint; "" = embed worker's default
	LeaseToken   string `json:"lease_token"`
	LeaseExpires int64  `json:"lease_expires_at"`
}

type embedReserveResp struct {
	Chunks []embedChunkDTO `json:"chunks"`
}

// embedResultItem is one chunk's embed result.
//
// Two acceptance modes:
//   - Vector mode (preferred, slice 10+): worker sends the raw vector []. Server
//     auto-creates the Qdrant collection if needed and upserts the point.
//   - Legacy mode: worker sends an opaque vector_id (worker handled the vector
//     store itself). Server just records the ID.
type embedResultItem struct {
	ChunkID    string    `json:"chunk_id"`
	Vector     []float32 `json:"vector,omitempty"`
	VectorID   string    `json:"vector_id,omitempty"`
	LeaseToken string    `json:"lease_token"`
	Failed     bool      `json:"failed,omitempty"`
	Reason     string    `json:"reason,omitempty"`
}

type embedResultReq struct {
	Results []embedResultItem `json:"results"`
}

// EmbedHandler routes embed HTTP calls.
type EmbedHandler struct {
	Svc     *app.EmbedSvc
	Workers workerid.Repository
}

// NewEmbedHandler constructs the handler.
func NewEmbedHandler(svc *app.EmbedSvc, workers workerid.Repository) *EmbedHandler {
	return &EmbedHandler{Svc: svc, Workers: workers}
}

// Reserve handles POST /v1/embed/reserve.
func (h *EmbedHandler) Reserve(w http.ResponseWriter, r *http.Request) {
	wk, ok := WorkerFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "no_worker", "")
		return
	}
	if !wk.Can("embed") {
		writeError(w, http.StatusForbidden, "capability_denied", "worker not allowed to embed")
		return
	}
	var req embedReserveReq
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_json", err.Error())
			return
		}
	}
	if req.Batch <= 0 {
		req.Batch = 1000
	}
	effBatch, err := effectiveBatch(r.Context(), h.Workers, wk, req.Batch)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cap_check", err.Error())
		return
	}
	if effBatch == 0 {
		writeJSON(w, http.StatusOK, embedReserveResp{Chunks: []embedChunkDTO{}})
		return
	}
	leased, err := h.Svc.Reserve(r.Context(), wk.ID, effBatch)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "reserve_failed", err.Error())
		return
	}
	out := embedReserveResp{Chunks: make([]embedChunkDTO, 0, len(leased))}
	for _, lc := range leased {
		out.Chunks = append(out.Chunks, embedChunkDTO{
			ChunkID:      lc.Chunk.ID,
			DocumentID:   lc.Chunk.DocumentID,
			ChunkIndex:   lc.Chunk.ChunkIndex,
			Text:         lc.Chunk.Text,
			TokenCount:   lc.Chunk.TokenCount,
			Collection:   lc.Chunk.Collection,
			LeaseToken:   lc.Lease.Token,
			LeaseExpires: lc.Lease.ExpiresAt.Unix(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// Result handles POST /v1/embed/result.
func (h *EmbedHandler) Result(w http.ResponseWriter, r *http.Request) {
	var req embedResultReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	var ok, failed int
	var firstErr string
	for _, it := range req.Results {
		if it.Failed {
			if err := h.Svc.AcceptFailure(r.Context(), it.ChunkID, it.LeaseToken, it.Reason); err == nil {
				failed++
			}
			continue
		}
		var err error
		switch {
		case len(it.Vector) > 0:
			err = h.Svc.AcceptVectorResult(r.Context(), it.ChunkID, it.LeaseToken, it.Vector)
		case it.VectorID != "":
			err = h.Svc.AcceptResult(r.Context(), it.ChunkID, it.VectorID, it.LeaseToken)
		default:
			err = errMissingVector
		}
		if err == nil {
			ok++
		} else if firstErr == "" {
			firstErr = err.Error()
		}
	}
	resp := map[string]any{"accepted": ok, "failed_recorded": failed}
	if firstErr != "" {
		resp["first_error"] = firstErr
	}
	writeJSON(w, http.StatusOK, resp)
}

var errMissingVector = jsonErr("embed result: vector or vector_id required")

type jsonErr string

func (e jsonErr) Error() string { return string(e) }
