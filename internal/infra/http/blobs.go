package http

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
)

// BlobsHandler serves raw blob bytes to authenticated workers.
type BlobsHandler struct {
	Lake  lake.Repository
	Blobs lake.BlobStore
}

// NewBlobsHandler wires a BlobsHandler.
func NewBlobsHandler(l lake.Repository, b lake.BlobStore) *BlobsHandler {
	return &BlobsHandler{Lake: l, Blobs: b}
}

// Get handles GET /v1/blobs/{id} — streams the lake_object body.
//
// Note: this BlobStore handle serves only blobs whose storage_backend matches
// the active backend. Migrating to a heterogeneous deployment (mixed local+s3
// rows) requires the handler to dispatch by row.StorageBackend; for slice 6
// we assume single-backend at read time.
func (h *BlobsHandler) Get(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_id", err.Error())
		return
	}
	o, err := h.Lake.GetByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "lookup_failed", err.Error())
		return
	}
	if o == nil {
		writeError(w, http.StatusNotFound, "not_found", "lake object not found")
		return
	}
	if o.StorageBackend != h.Blobs.Backend() {
		writeError(w, http.StatusConflict, "backend_mismatch",
			"row stored on "+o.StorageBackend+", active backend is "+h.Blobs.Backend())
		return
	}
	rc, _, err := h.Blobs.Get(r.Context(), o.StorageKey)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "blob_read", err.Error())
		return
	}
	defer rc.Close()

	if o.ContentType != "" {
		w.Header().Set("Content-Type", o.ContentType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	if o.FileSize > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(o.FileSize, 10))
	}
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, rc); err != nil && !errors.Is(err, io.EOF) {
		// Connection dropped mid-stream; nothing we can do beyond logging server-side.
		_ = err
	}
}
