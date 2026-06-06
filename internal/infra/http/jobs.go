package http

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/atvirokodosprendimai/crawlerv3/internal/app"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/frontier"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
)

// reserveReq is POST /v1/jobs/reserve body.
type reserveReq struct {
	Batch        int      `json:"batch"`
	Capabilities []string `json:"capabilities"`
}

// reserveJobDTO is the per-job payload returned to workers.
type reserveJobDTO struct {
	JobID         string `json:"job_id"`         // hex(url_hash) for opaque routing
	URL           string `json:"url"`
	CanonicalURL  string `json:"canonical_url"`
	Depth         int    `json:"depth"`
	AttemptCount  int    `json:"attempt_count"`
	LeaseToken    string `json:"lease_token"`
	LeaseExpires  int64  `json:"lease_expires_at"`
	MaxBodyBytes  int64  `json:"max_body_bytes"`
}

// reserveResp wraps the leased batch.
type reserveResp struct {
	Jobs []reserveJobDTO `json:"jobs"`
}

// resultMeta is the JSON part of the multipart result upload.
type resultMeta struct {
	LeaseToken      string             `json:"lease_token"`
	HTTPStatus      int                `json:"http_status"`
	ContentType     string             `json:"content_type"`
	ContentSHA256   string             `json:"content_sha256"` // hex
	Size            int64              `json:"size"`
	DiscoveredLinks []discoveredLinkIn `json:"discovered_links"`
}

type discoveredLinkIn struct {
	URL      string `json:"url"`
	Anchor   string `json:"anchor"`
	Rel      string `json:"rel"`
	NewDepth int    `json:"new_depth"`
}

type failReq struct {
	LeaseToken   string `json:"lease_token"`
	HTTPStatus   int    `json:"http_status"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	Retryable    bool   `json:"retryable"`
}

type heartbeatReq struct {
	LeaseToken string `json:"lease_token"`
}

// JobsHandler wires HTTP endpoints to the Service.
type JobsHandler struct {
	Svc          *app.Service
	Workers      workerid.Repository
	MaxBodyBytes int64
}

// NewJobsHandler constructs the handler.
func NewJobsHandler(svc *app.Service, workers workerid.Repository, maxBody int64) *JobsHandler {
	return &JobsHandler{Svc: svc, Workers: workers, MaxBodyBytes: maxBody}
}

// Reserve handles POST /v1/jobs/reserve.
func (h *JobsHandler) Reserve(w http.ResponseWriter, r *http.Request) {
	wk, ok := WorkerFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "no_worker", "")
		return
	}
	if !wk.Can("crawl") {
		writeError(w, http.StatusForbidden, "capability_denied", "worker not allowed to crawl")
		return
	}
	var req reserveReq
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_json", err.Error())
			return
		}
	}
	if req.Batch <= 0 {
		req.Batch = h.Svc.Cfg.DefaultBatch
	}
	effBatch, err := effectiveBatch(r.Context(), h.Workers, wk, req.Batch)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cap_check", err.Error())
		return
	}
	if effBatch == 0 {
		writeJSON(w, http.StatusOK, reserveResp{Jobs: []reserveJobDTO{}})
		return
	}
	// Use the server-stored capabilities, not the (potentially spoofed) request body.
	leased, err := h.Svc.ReserveJobs(r.Context(), frontier.ReserveRequest{
		WorkerID: wk.ID, Batch: effBatch, Capabilities: wk.Capabilities,
	})
	if err != nil {
		slog.ErrorContext(r.Context(), "jobs reserve", "worker_id", wk.ID, "err", err)
		writeError(w, http.StatusInternalServerError, "reserve_failed", err.Error())
		return
	}
	slog.InfoContext(r.Context(), "jobs reserved", "worker_id", wk.ID, "requested", req.Batch, "granted", len(leased))
	out := reserveResp{Jobs: make([]reserveJobDTO, 0, len(leased))}
	for _, lj := range leased {
		out.Jobs = append(out.Jobs, reserveJobDTO{
			JobID:        hexLower(lj.Job.URLHash),
			URL:          lj.Job.URL,
			CanonicalURL: lj.Job.CanonicalURL,
			Depth:        lj.Job.Depth,
			AttemptCount: lj.Job.AttemptCount,
			LeaseToken:   lj.Lease.Token,
			LeaseExpires: lj.Lease.ExpiresAt.Unix(),
			MaxBodyBytes: h.MaxBodyBytes,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// Heartbeat handles POST /v1/jobs/heartbeat.
func (h *JobsHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	var req heartbeatReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	exp, err := h.Svc.Heartbeat(r.Context(), req.LeaseToken)
	if err != nil {
		writeError(w, http.StatusConflict, "heartbeat_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease_expires_at": exp.Unix()})
}

// Result handles POST /v1/jobs/result.
//
// Multipart fields:
//   - meta: JSON resultMeta
//   - blob: raw bytes
func (h *JobsHandler) Result(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.MaxBodyBytes+1<<20) // body + slack for headers
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "bad_multipart", err.Error())
		return
	}
	// Parts larger than 1MB are spooled to /tmp/multipart-* — net/http does
	// not auto-clean. Remove on handler return.
	defer func() {
		if r.MultipartForm != nil {
			_ = r.MultipartForm.RemoveAll()
		}
	}()
	metaStr := r.FormValue("meta")
	if metaStr == "" {
		writeError(w, http.StatusBadRequest, "missing_meta", "meta field required")
		return
	}
	var meta resultMeta
	if err := json.NewDecoder(strings.NewReader(metaStr)).Decode(&meta); err != nil {
		writeError(w, http.StatusBadRequest, "bad_meta", err.Error())
		return
	}
	file, _, err := r.FormFile("blob")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing_blob", err.Error())
		return
	}
	defer file.Close()

	var claimed []byte
	if meta.ContentSHA256 != "" {
		claimed, err = hexDecode(meta.ContentSHA256)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_sha", err.Error())
			return
		}
	}
	links := make([]frontier.DiscoveredLink, 0, len(meta.DiscoveredLinks))
	for _, l := range meta.DiscoveredLinks {
		links = append(links, frontier.DiscoveredLink{
			URL: l.URL, Anchor: l.Anchor, Rel: l.Rel, NewDepth: l.NewDepth,
		})
	}
	id, err := h.Svc.AcceptResult(r.Context(), app.ResultIngest{
		LeaseToken:      meta.LeaseToken,
		HTTPStatus:      meta.HTTPStatus,
		ContentType:     meta.ContentType,
		Body:            io.LimitReader(file, h.MaxBodyBytes),
		BodySize:        meta.Size,
		ClaimedSHA256:   claimed,
		DiscoveredLinks: links,
	})
	if err != nil {
		slog.WarnContext(r.Context(), "jobs result rejected", "err", err)
		writeError(w, http.StatusConflict, "result_failed", err.Error())
		return
	}
	slog.InfoContext(r.Context(), "jobs result accepted",
		"lake_object_id", id, "http_status", meta.HTTPStatus,
		"ct", meta.ContentType, "size", meta.Size, "links", len(links))
	writeJSON(w, http.StatusOK, map[string]any{"lake_object_id": id, "accepted": true})
}

// Fail handles POST /v1/jobs/fail.
func (h *JobsHandler) Fail(w http.ResponseWriter, r *http.Request) {
	var req failReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	err := h.Svc.AcceptFailure(r.Context(), req.LeaseToken, frontier.Failure{
		HTTPStatus: req.HTTPStatus, ErrorCode: req.ErrorCode,
		ErrorMessage: req.ErrorMessage, Retryable: req.Retryable,
	})
	if err != nil {
		slog.WarnContext(r.Context(), "jobs fail rejected", "err", err)
		writeError(w, http.StatusConflict, "fail_failed", err.Error())
		return
	}
	slog.InfoContext(r.Context(), "jobs failure recorded",
		"http_status", req.HTTPStatus, "code", req.ErrorCode, "retryable", req.Retryable)
	writeJSON(w, http.StatusOK, map[string]any{"recorded": true})
}

// Me handles GET /v1/workers/me.
func (h *JobsHandler) Me(w http.ResponseWriter, r *http.Request) {
	wk, ok := WorkerFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "no_worker", "")
		return
	}
	held, _ := h.Workers.CountHeldLeases(r.Context(), wk.ID)
	writeJSON(w, http.StatusOK, map[string]any{
		"id":             wk.ID,
		"label":          wk.Label,
		"reputation":     wk.ReputationScore,
		"banned":         wk.IsBanned(),
		"capabilities":   wk.Capabilities,
		"max_concurrent": wk.MaxConcurrent,
		"held":           held,
	})
}

// --- helpers --------------------------------------------------------------

const hexd = "0123456789abcdef"

func hexLower(b []byte) string {
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexd[v>>4]
		out[i*2+1] = hexd[v&0x0f]
	}
	return string(out)
}

func hexDecode(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, errors.New("odd hex length")
	}
	out := make([]byte, len(s)/2)
	for i := 0; i < len(out); i++ {
		hi, ok1 := hexNyb(s[i*2])
		lo, ok2 := hexNyb(s[i*2+1])
		if !ok1 || !ok2 {
			return nil, errors.New("bad hex char")
		}
		out[i] = hi<<4 | lo
	}
	return out, nil
}

func hexNyb(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	}
	return 0, false
}

// keep base64 imported in case future endpoints need it
var _ = base64.RawURLEncoding
var _ = strconv.Itoa
