package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/extraction"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/processing"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/triggers"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/lease"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/chunker"
)

// TaskSvc orchestrates external-worker reservation, result acceptance, and
// downstream chaining (e.g. docx→pdf produces a new lake_objects row + a
// follow-up pdf_ocr task).
type TaskSvc struct {
	Cfg         Config
	Processing  processing.Repository
	Lake        lake.Repository
	Blobs       lake.BlobStore
	Extractions extraction.Repository
	Chunks      chunkInserter
	Lease       *lease.Signer
	ChunkCfg    chunker.Config
	Dispatcher  *TriggerDispatcher  // optional; fires EvtBlobProduced on AcceptBlob
	Resolver    *CollectionResolver // optional; per-domain embed_collection lookup
}

// chunkInserter is the slice of chunking.Repository TaskSvc needs.
// Declared locally to avoid an import cycle with the chunking domain when
// downstream packages compose facades.
type chunkInserter interface {
	InsertMany(ctx context.Context, chunks []chunkRow) error
}

// chunkRow mirrors chunking.Chunk; TaskSvc converts to it before passing to the
// adapter so the adapter remains decoupled.
type chunkRow struct {
	ID         string
	DocumentID int64
	ChunkIndex int
	Text       string
	TokenCount int
}

// NewTaskSvc wires a TaskSvc with the necessary ports.
func NewTaskSvc(cfg Config, p processing.Repository, l lake.Repository, b lake.BlobStore, e extraction.Repository, s *lease.Signer) *TaskSvc {
	return &TaskSvc{Cfg: cfg, Processing: p, Lake: l, Blobs: b, Extractions: e, Lease: s, ChunkCfg: chunker.Defaults()}
}

// AttachChunkSink installs the chunk persistence adapter (kept separate from
// the constructor to avoid cycle issues across packages).
func (t *TaskSvc) AttachChunkSink(sink chunkInserter) { t.Chunks = sink }

// SetDispatcher attaches the trigger dispatcher; AcceptBlob will then fire
// EvtBlobProduced so triggers can chain further processing automatically.
func (t *TaskSvc) SetDispatcher(d *TriggerDispatcher) { t.Dispatcher = d }

// SetResolver wires the per-domain collection resolver.
func (t *TaskSvc) SetResolver(r *CollectionResolver) { t.Resolver = r }

// Reserve leases up to batch tasks of the given kinds.
func (t *TaskSvc) Reserve(ctx context.Context, workerID int64, kinds []processing.Processor, batch int) ([]processing.LeasedTask, error) {
	if batch <= 0 {
		batch = 4
	}
	sign := func(taskID int64, expires time.Time) (string, []byte) {
		return t.Lease.SignTask(taskID, workerID, expires)
	}
	return t.Processing.ReserveBatch(ctx, workerID, kinds, batch, t.Cfg.LeaseTTL, sign)
}

// Heartbeat extends a task lease.
func (t *TaskSvc) Heartbeat(ctx context.Context, taskID int64, token string) (time.Time, error) {
	if _, _, err := t.Lease.VerifyTask(token, taskID); err != nil {
		return time.Time{}, fmt.Errorf("heartbeat: %w", err)
	}
	raw, err := lease.Raw(token)
	if err != nil {
		return time.Time{}, err
	}
	return t.Processing.Heartbeat(ctx, taskID, raw, t.Cfg.HeartbeatExtend)
}

// TextResult is the worker's result for processors that extract text directly
// (e.g. pdf_ocr).
type TextResult struct {
	TaskID     int64
	LeaseToken string
	Text       string
	Language   string
	PageCount  int
}

// AcceptText writes extracted_documents + chunks, then marks the task done.
func (t *TaskSvc) AcceptText(ctx context.Context, in TextResult) error {
	if _, _, err := t.Lease.VerifyTask(in.LeaseToken, in.TaskID); err != nil {
		return fmt.Errorf("task result: %w", err)
	}
	raw, err := lease.Raw(in.LeaseToken)
	if err != nil {
		return err
	}
	// Look up source lake_object via the task row.
	job, err := t.fetchTask(ctx, in.TaskID)
	if err != nil {
		return err
	}
	collection := ""
	if t.Resolver != nil {
		collection = t.Resolver.ResolveForLakeObject(ctx, job.LakeObjectID)
	}
	docID, err := t.Extractions.Upsert(ctx, extraction.Document{
		SourceLakeObjectID: job.LakeObjectID,
		Text:               in.Text,
		Language:           in.Language,
		PageCount:          in.PageCount,
		Collection:         collection,
	})
	if err != nil {
		return fmt.Errorf("extracted upsert: %w", err)
	}
	if t.Chunks != nil && in.Text != "" {
		pieces := chunker.Split(in.Text, t.ChunkCfg)
		rows := make([]chunkRow, 0, len(pieces))
		for _, c := range pieces {
			rows = append(rows, chunkRow{
				ID:         newUUID(),
				DocumentID: docID,
				ChunkIndex: c.Index,
				Text:       c.Text,
				TokenCount: c.WordCount,
			})
		}
		if err := t.Chunks.InsertMany(ctx, rows); err != nil {
			return fmt.Errorf("chunks insert: %w", err)
		}
	}
	return t.Processing.Complete(ctx, in.TaskID, raw, nil)
}

// BlobResult is the worker's result for processors that produce a new blob
// (e.g. docx_to_pdf produces a PDF). The new blob is stored as a fresh
// lake_objects row, set as output_lake_object_id, and a downstream
// processing_jobs row is enqueued (e.g. pdf_ocr on the new PDF).
type BlobResult struct {
	TaskID            int64
	LeaseToken        string
	OutputContentType string
	OutputBody        io.Reader
	OutputSHA256      []byte
	NextProcessor     processing.Processor // optional follow-up
}

// AcceptBlob stores the output blob, links it on the source task row, and
// enqueues the optional next processor.
func (t *TaskSvc) AcceptBlob(ctx context.Context, in BlobResult) (newLakeID int64, err error) {
	if _, _, err := t.Lease.VerifyTask(in.LeaseToken, in.TaskID); err != nil {
		return 0, fmt.Errorf("task result: %w", err)
	}
	raw, err := lease.Raw(in.LeaseToken)
	if err != nil {
		return 0, err
	}
	job, err := t.fetchTask(ctx, in.TaskID)
	if err != nil {
		return 0, err
	}
	// Reuse the source URL hash for the output blob (provenance: same crawl).
	src, err := t.Lake.GetByID(ctx, job.LakeObjectID)
	if err != nil {
		return 0, err
	}
	if src == nil {
		return 0, errors.New("task result: source lake object missing")
	}
	key := storageKey(src.URLHash, in.OutputContentType)
	stat, err := t.Blobs.Put(ctx, key, in.OutputBody, lake.PutMeta{
		ContentType: in.OutputContentType, SHA256: in.OutputSHA256,
	})
	if err != nil {
		return 0, fmt.Errorf("blob put: %w", err)
	}
	newID, err := t.Lake.Insert(ctx, lake.Object{
		URLHash:        src.URLHash,
		StorageBackend: t.Blobs.Backend(),
		StorageKey:     key,
		ContentType:    in.OutputContentType,
		ContentSHA256:  stat.SHA256,
		FileSize:       stat.Size,
	})
	if err != nil {
		return 0, fmt.Errorf("lake insert: %w", err)
	}
	if err := t.Processing.Complete(ctx, in.TaskID, raw, &newID); err != nil {
		return 0, err
	}
	if in.NextProcessor != "" {
		if _, err := t.Processing.Enqueue(ctx, newID, in.NextProcessor); err != nil {
			return newID, fmt.Errorf("enqueue next: %w", err)
		}
	}
	if t.Dispatcher != nil {
		t.Dispatcher.Fire(ctx, triggers.EvtBlobProduced, EventPayload{
			LakeObjectID:    newID,
			ContentType:     in.OutputContentType,
			SourceProcessor: string(job.Processor),
		})
	}
	return newID, nil
}

// AcceptFailure records a task failure with retry/backoff handling.
func (t *TaskSvc) AcceptFailure(ctx context.Context, taskID int64, token, errMsg string, retryable bool) error {
	if _, _, err := t.Lease.VerifyTask(token, taskID); err != nil {
		return fmt.Errorf("task fail: %w", err)
	}
	raw, err := lease.Raw(token)
	if err != nil {
		return err
	}
	return t.Processing.Fail(ctx, taskID, raw, errMsg, retryable)
}

// SweepExpired re-queues stuck task leases.
func (t *TaskSvc) SweepExpired(ctx context.Context) (int64, error) {
	return t.Processing.SweepExpired(ctx, time.Now().UTC())
}

// fetchTask is a tiny convenience that re-reads a task by ID so AcceptText /
// AcceptBlob can locate the source lake object.
func (t *TaskSvc) fetchTask(ctx context.Context, taskID int64) (*processing.Job, error) {
	// Use ClaimNext's dialect? Cheaper: a side-door read. The processing repo
	// doesn't have a "GetByID" method yet; for slice 6 we use a small accessor.
	type getter interface {
		GetByID(ctx context.Context, id int64) (*processing.Job, error)
	}
	if g, ok := t.Processing.(getter); ok {
		return g.GetByID(ctx, taskID)
	}
	return nil, errors.New("processing repo missing GetByID")
}
