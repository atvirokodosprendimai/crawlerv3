package http

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/extraction"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
)

// ReadsHandler exposes data-lake read endpoints for sink workers
// (Qdrant indexer, Quickwit FTS, SQL warehouse, etc.).
type ReadsHandler struct {
	Lake        lake.Repository
	Extractions extraction.Repository
	Chunks      chunking.Repository
}

// NewReadsHandler constructs the handler.
func NewReadsHandler(l lake.Repository, e extraction.Repository, c chunking.Repository) *ReadsHandler {
	return &ReadsHandler{Lake: l, Extractions: e, Chunks: c}
}

// LakeList handles GET /v1/lake?since_id=&limit=&backend=&content_type_prefix=
func (h *ReadsHandler) LakeList(w http.ResponseWriter, r *http.Request) {
	wk, _ := WorkerFromCtx(r.Context())
	if wk == nil || !wk.Can("lake_read") {
		writeError(w, http.StatusForbidden, "capability_denied", "lake_read required")
		return
	}
	opts := lake.ListOpts{
		SinceID:           parseInt64(r.URL.Query().Get("since_id"), 0),
		Limit:             parseInt(r.URL.Query().Get("limit"), 100),
		Backend:           r.URL.Query().Get("backend"),
		ContentTypePrefix: r.URL.Query().Get("content_type_prefix"),
	}
	objs, err := h.Lake.ListSince(r.Context(), opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(objs))
	for _, o := range objs {
		out = append(out, map[string]any{
			"id":              o.ID,
			"url_hash":        hex.EncodeToString(o.URLHash),
			"content_type":    o.ContentType,
			"content_sha256":  hex.EncodeToString(o.ContentSHA256),
			"file_size_bytes": o.FileSize,
			"storage_backend": o.StorageBackend,
			"storage_key":     o.StorageKey,
			"archived_at":     o.ArchivedAt.Unix(),
			"blob_url":        "/v1/blobs/" + strconv.FormatInt(o.ID, 10),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "count": len(out)})
}

// ExtractedList handles GET /v1/extracted?since_id=&limit=
func (h *ReadsHandler) ExtractedList(w http.ResponseWriter, r *http.Request) {
	wk, _ := WorkerFromCtx(r.Context())
	if wk == nil || !wk.Can("extracted_read") {
		writeError(w, http.StatusForbidden, "capability_denied", "extracted_read required")
		return
	}
	since := parseInt64(r.URL.Query().Get("since_id"), 0)
	limit := parseInt(r.URL.Query().Get("limit"), 100)
	docs, err := h.Extractions.ListSince(r.Context(), since, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(docs))
	for _, d := range docs {
		preview := d.Text
		if len(preview) > 500 {
			preview = preview[:500]
		}
		out = append(out, map[string]any{
			"id":                    d.ID,
			"source_lake_object_id": d.SourceLakeObjectID,
			"language":              d.Language,
			"page_count":            d.PageCount,
			"collection":            d.Collection,
			"extracted_at":          d.ExtractedAt.Unix(),
			"text_size_bytes":       len(d.Text),
			"text_preview":          preview,
			"text_url":              "/v1/extracted/" + strconv.FormatInt(d.ID, 10) + "/text",
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "count": len(out)})
}

// ExtractedText handles GET /v1/extracted/{id}/text — returns plain text body.
func (h *ReadsHandler) ExtractedText(w http.ResponseWriter, r *http.Request) {
	wk, _ := WorkerFromCtx(r.Context())
	if wk == nil || !wk.Can("extracted_read") {
		writeError(w, http.StatusForbidden, "capability_denied", "extracted_read required")
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_id", err.Error())
		return
	}
	doc, err := h.Extractions.GetByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "lookup_failed", err.Error())
		return
	}
	if doc == nil {
		writeError(w, http.StatusNotFound, "not_found", "extracted document not found")
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(doc.Text))
}

// ChunksList handles GET /v1/chunks?embed_status=&since=<unix>&limit=
func (h *ReadsHandler) ChunksList(w http.ResponseWriter, r *http.Request) {
	wk, _ := WorkerFromCtx(r.Context())
	if wk == nil || !wk.Can("chunks_read") {
		writeError(w, http.StatusForbidden, "capability_denied", "chunks_read required")
		return
	}
	status := chunking.EmbedStatus(r.URL.Query().Get("embed_status"))
	sinceUnix := parseInt64(r.URL.Query().Get("since"), 0)
	limit := parseInt(r.URL.Query().Get("limit"), 100)
	since := time.Unix(sinceUnix, 0).UTC()
	chunks, err := h.Chunks.ListSince(r.Context(), status, since, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "list_failed", err.Error())
		return
	}
	out := make([]map[string]any, 0, len(chunks))
	for _, c := range chunks {
		out = append(out, map[string]any{
			"id":           c.ID,
			"document_id":  c.DocumentID,
			"chunk_index":  c.ChunkIndex,
			"text":         c.Text,
			"token_count":  c.TokenCount,
			"vector_id":    c.VectorID,
			"embed_status": string(c.EmbedStatus),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": out, "count": len(out)})
}

// --- helpers --------------------------------------------------------------

func parseInt(s string, d int) int {
	if s == "" {
		return d
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return d
	}
	return n
}

func parseInt64(s string, d int64) int64 {
	if s == "" {
		return d
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return d
	}
	return n
}
