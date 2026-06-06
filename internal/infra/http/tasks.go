package http

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/atvirokodosprendimai/crawlerv3/internal/app"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/processing"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
)

// taskReserveReq is the POST body for /v1/tasks/reserve.
type taskReserveReq struct {
	Kinds []string `json:"kinds"`
	Batch int      `json:"batch"`
}

// taskDTO is the per-task payload returned to workers.
type taskDTO struct {
	TaskID          int64  `json:"task_id"`
	Processor       string `json:"processor"`
	LakeObjectID    int64  `json:"lake_object_id"`
	BlobURL         string `json:"blob_url"`
	BlobContentType string `json:"blob_content_type"`
	BlobSizeBytes   int64  `json:"blob_size_bytes"`
	AttemptCount    int    `json:"attempt_count"`
	LeaseToken      string `json:"lease_token"`
	LeaseExpiresAt  int64  `json:"lease_expires_at"`
}

type taskReserveResp struct {
	Tasks []taskDTO `json:"tasks"`
}

type taskHeartbeatReq struct {
	TaskID     int64  `json:"task_id"`
	LeaseToken string `json:"lease_token"`
}

type taskFailReq struct {
	TaskID       int64  `json:"task_id"`
	LeaseToken   string `json:"lease_token"`
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
	Retryable    bool   `json:"retryable"`
}

// taskResultMeta is the JSON part of POST /v1/tasks/result multipart.
type taskResultMeta struct {
	TaskID            int64  `json:"task_id"`
	LeaseToken        string `json:"lease_token"`
	ExtractedText     string `json:"extracted_text,omitempty"`
	Language          string `json:"language,omitempty"`
	PageCount         int    `json:"page_count,omitempty"`
	OutputContentType string `json:"output_content_type,omitempty"`
	OutputSHA256      string `json:"output_content_sha256,omitempty"` // hex
	NextProcessor     string `json:"next_processor,omitempty"`
}

// TasksHandler routes /v1/tasks/* HTTP calls.
type TasksHandler struct {
	Svc     *app.TaskSvc
	Workers workerid.Repository
}

// NewTasksHandler constructs the handler.
func NewTasksHandler(svc *app.TaskSvc, workers workerid.Repository) *TasksHandler {
	return &TasksHandler{Svc: svc, Workers: workers}
}

// Reserve handles POST /v1/tasks/reserve.
func (h *TasksHandler) Reserve(w http.ResponseWriter, r *http.Request) {
	wk, ok := WorkerFromCtx(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "no_worker", "")
		return
	}
	var req taskReserveReq
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "bad_json", err.Error())
			return
		}
	}
	// Enforce capability per requested kind.
	for _, k := range req.Kinds {
		if !wk.Can(k) {
			writeError(w, http.StatusForbidden, "capability_denied", "worker lacks capability "+k)
			return
		}
	}
	kinds := make([]processing.Processor, 0, len(req.Kinds))
	for _, k := range req.Kinds {
		kinds = append(kinds, processing.Processor(k))
	}
	if req.Batch <= 0 {
		req.Batch = 4
	}
	effBatch, err := effectiveBatch(r.Context(), h.Workers, wk, req.Batch)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cap_check", err.Error())
		return
	}
	if effBatch == 0 {
		writeJSON(w, http.StatusOK, taskReserveResp{Tasks: []taskDTO{}})
		return
	}
	leased, err := h.Svc.Reserve(r.Context(), wk.ID, kinds, effBatch)
	if err != nil {
		slog.ErrorContext(r.Context(), "tasks reserve", "worker_id", wk.ID, "err", err)
		writeError(w, http.StatusInternalServerError, "reserve_failed", err.Error())
		return
	}
	slog.InfoContext(r.Context(), "tasks reserved",
		"worker_id", wk.ID, "kinds", req.Kinds, "requested", req.Batch, "granted", len(leased))
	out := taskReserveResp{Tasks: make([]taskDTO, 0, len(leased))}
	for _, lt := range leased {
		out.Tasks = append(out.Tasks, taskDTO{
			TaskID:          lt.Job.ID,
			Processor:       string(lt.Job.Processor),
			LakeObjectID:    lt.Job.LakeObjectID,
			BlobURL:         blobURL(lt.Job.LakeObjectID),
			BlobContentType: lt.BlobContentType,
			BlobSizeBytes:   lt.BlobSizeBytes,
			AttemptCount:    lt.Job.AttemptCount,
			LeaseToken:      lt.LeaseToken,
			LeaseExpiresAt:  lt.LeaseExpiresAt.Unix(),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// Heartbeat handles POST /v1/tasks/heartbeat.
func (h *TasksHandler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	var req taskHeartbeatReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	exp, err := h.Svc.Heartbeat(r.Context(), req.TaskID, req.LeaseToken)
	if err != nil {
		writeError(w, http.StatusConflict, "heartbeat_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease_expires_at": exp.Unix()})
}

// Result handles POST /v1/tasks/result.
//
// multipart form:
//   - meta: JSON taskResultMeta
//   - blob: optional output bytes (e.g. docx_to_pdf produces a PDF)
func (h *TasksHandler) Result(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 200<<20+1<<20)
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "bad_multipart", err.Error())
		return
	}
	metaStr := r.FormValue("meta")
	if metaStr == "" {
		writeError(w, http.StatusBadRequest, "missing_meta", "meta field required")
		return
	}
	var meta taskResultMeta
	if err := json.NewDecoder(strings.NewReader(metaStr)).Decode(&meta); err != nil {
		writeError(w, http.StatusBadRequest, "bad_meta", err.Error())
		return
	}

	hasBlob := false
	var blobReader io.Reader
	if f, _, err := r.FormFile("blob"); err == nil {
		hasBlob = true
		blobReader = f
		defer f.Close()
	} else if !errors.Is(err, http.ErrMissingFile) {
		writeError(w, http.StatusBadRequest, "blob_error", err.Error())
		return
	}

	if hasBlob {
		var sha []byte
		if meta.OutputSHA256 != "" {
			b, herr := hexDecode(meta.OutputSHA256)
			if herr != nil {
				writeError(w, http.StatusBadRequest, "bad_sha", herr.Error())
				return
			}
			sha = b
		}
		id, err := h.Svc.AcceptBlob(r.Context(), app.BlobResult{
			TaskID:            meta.TaskID,
			LeaseToken:        meta.LeaseToken,
			OutputContentType: meta.OutputContentType,
			OutputBody:        blobReader,
			OutputSHA256:      sha,
			NextProcessor:     processing.Processor(meta.NextProcessor),
		})
		if err != nil {
			slog.WarnContext(r.Context(), "tasks blob result rejected", "task_id", meta.TaskID, "err", err)
			writeError(w, http.StatusConflict, "result_failed", err.Error())
			return
		}
		slog.InfoContext(r.Context(), "tasks blob result accepted",
			"task_id", meta.TaskID, "output_lake_object_id", id,
			"next_processor", meta.NextProcessor)
		writeJSON(w, http.StatusOK, map[string]any{"output_lake_object_id": id, "accepted": true})
		return
	}

	if err := h.Svc.AcceptText(r.Context(), app.TextResult{
		TaskID:     meta.TaskID,
		LeaseToken: meta.LeaseToken,
		Text:       meta.ExtractedText,
		Language:   meta.Language,
		PageCount:  meta.PageCount,
	}); err != nil {
		slog.WarnContext(r.Context(), "tasks text result rejected", "task_id", meta.TaskID, "err", err)
		writeError(w, http.StatusConflict, "result_failed", err.Error())
		return
	}
	slog.InfoContext(r.Context(), "tasks text result accepted",
		"task_id", meta.TaskID, "text_bytes", len(meta.ExtractedText),
		"language", meta.Language, "page_count", meta.PageCount)
	writeJSON(w, http.StatusOK, map[string]any{"accepted": true})
}

// Fail handles POST /v1/tasks/fail.
func (h *TasksHandler) Fail(w http.ResponseWriter, r *http.Request) {
	var req taskFailReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_json", err.Error())
		return
	}
	msg := req.ErrorMessage
	if req.ErrorCode != "" {
		msg = req.ErrorCode + ": " + msg
	}
	if err := h.Svc.AcceptFailure(r.Context(), req.TaskID, req.LeaseToken, msg, req.Retryable); err != nil {
		slog.WarnContext(r.Context(), "tasks fail rejected", "task_id", req.TaskID, "err", err)
		writeError(w, http.StatusConflict, "fail_failed", err.Error())
		return
	}
	slog.InfoContext(r.Context(), "tasks failure recorded",
		"task_id", req.TaskID, "code", req.ErrorCode, "retryable", req.Retryable)
	writeJSON(w, http.StatusOK, map[string]any{"recorded": true})
}

func blobURL(lakeObjectID int64) string {
	return "/v1/blobs/" + itoa64(lakeObjectID)
}

// itoa64 avoids strconv import explosion in this file's tight footprint.
func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
