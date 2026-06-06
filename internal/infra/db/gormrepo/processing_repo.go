package gormrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/processing"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// ProcessingRepo implements processing.Repository.
type ProcessingRepo struct{ DB *rwdb.DB }

// NewProcessingRepo wires a ProcessingRepo to the rwdb pools.
func NewProcessingRepo(db *rwdb.DB) *ProcessingRepo { return &ProcessingRepo{DB: db} }

// Enqueue inserts a queued processing job for a lake object + processor.
func (r *ProcessingRepo) Enqueue(ctx context.Context, lakeID int64, p processing.Processor) (int64, error) {
	m := ProcessingJob{
		LakeObjectID: lakeID,
		Processor:    string(p),
		Status:       string(processing.StatusQueued),
		MaxAttempts:  3,
		CreatedAt:    time.Now().UTC(),
	}
	if err := r.DB.W.WithContext(ctx).Create(&m).Error; err != nil {
		return 0, err
	}
	return m.ID, nil
}

// ClaimNext atomically picks one queued job for processor p and marks it running.
func (r *ProcessingRepo) ClaimNext(ctx context.Context, p processing.Processor) (*processing.Job, error) {
	var out *processing.Job
	err := r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		var m ProcessingJob
		err := tx.Where("status = ? AND processor = ?", string(processing.StatusQueued), string(p)).
			Order("id ASC").First(&m).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		res := tx.Exec(`UPDATE processing_jobs
   SET status = ?, started_at = ?, attempt_count = attempt_count + 1
 WHERE id = ? AND status = ?`,
			string(processing.StatusRunning), now, m.ID, string(processing.StatusQueued))
		if res.Error != nil {
			return res.Error
		}
		if res.RowsAffected == 0 {
			return nil // raced; try again next tick
		}
		out = &processing.Job{
			ID:           m.ID,
			LakeObjectID: m.LakeObjectID,
			Processor:    processing.Processor(m.Processor),
			Status:       processing.StatusRunning,
			AttemptCount: m.AttemptCount + 1,
			MaxAttempts:  m.MaxAttempts,
			StartedAt:    &now,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// MarkDone records a successful completion.
func (r *ProcessingRepo) MarkDone(ctx context.Context, id int64, outputLakeID *int64) error {
	now := time.Now().UTC()
	updates := map[string]interface{}{
		"status":                string(processing.StatusDone),
		"finished_at":           now,
		"output_lake_object_id": outputLakeID,
	}
	return r.DB.W.WithContext(ctx).Model(&ProcessingJob{}).Where("id = ?", id).Updates(updates).Error
}

// MarkFailed records a failure, demotes back to queued if attempts remain.
func (r *ProcessingRepo) MarkFailed(ctx context.Context, id int64, errMsg string) error {
	return r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		var m ProcessingJob
		if err := tx.First(&m, "id = ?", id).Error; err != nil {
			return err
		}
		newStatus := string(processing.StatusFailed)
		if m.AttemptCount < m.MaxAttempts {
			newStatus = string(processing.StatusQueued)
		}
		now := time.Now().UTC()
		em := errMsg
		updates := map[string]interface{}{
			"status":      newStatus,
			"last_error":  &em,
			"finished_at": &now,
		}
		return tx.Model(&ProcessingJob{}).Where("id = ?", id).Updates(updates).Error
	})
}

// MarkSkipped records intentional non-execution (e.g. unsupported MIME).
func (r *ProcessingRepo) MarkSkipped(ctx context.Context, id int64, reason string) error {
	now := time.Now().UTC()
	em := reason
	updates := map[string]interface{}{
		"status":      string(processing.StatusSkipped),
		"last_error":  &em,
		"finished_at": now,
	}
	return r.DB.W.WithContext(ctx).Model(&ProcessingJob{}).Where("id = ?", id).Updates(updates).Error
}

// GetByID returns a processing job by primary key, or nil.
func (r *ProcessingRepo) GetByID(ctx context.Context, id int64) (*processing.Job, error) {
	var m ProcessingJob
	err := r.DB.R.WithContext(ctx).Where("id = ?", id).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &processing.Job{
		ID:                 m.ID,
		LakeObjectID:       m.LakeObjectID,
		Processor:          processing.Processor(m.Processor),
		Status:             processing.Status(m.Status),
		AttemptCount:       m.AttemptCount,
		MaxAttempts:        m.MaxAttempts,
		StartedAt:          m.StartedAt,
		FinishedAt:         m.FinishedAt,
		OutputLakeObjectID: m.OutputLakeObjectID,
	}, nil
}

// CountQueued returns total queued processing jobs.
func (r *ProcessingRepo) CountQueued(ctx context.Context) (int64, error) {
	var n int64
	err := r.DB.R.WithContext(ctx).Model(&ProcessingJob{}).
		Where("status = ?", string(processing.StatusQueued)).Count(&n).Error
	return n, err
}

// BulkEnqueueByLakeIDs inserts queued jobs for an explicit list of lake objects.
func (r *ProcessingRepo) BulkEnqueueByLakeIDs(ctx context.Context, p processing.Processor, lakeIDs []int64) (int64, error) {
	if len(lakeIDs) == 0 {
		return 0, nil
	}
	rows := make([]ProcessingJob, 0, len(lakeIDs))
	now := time.Now().UTC()
	for _, id := range lakeIDs {
		rows = append(rows, ProcessingJob{
			LakeObjectID: id,
			Processor:    string(p),
			Status:       string(processing.StatusQueued),
			MaxAttempts:  3,
			CreatedAt:    now,
		})
	}
	res := r.DB.W.WithContext(ctx).
		Session(&gorm.Session{CreateBatchSize: 500}).
		Create(&rows)
	return res.RowsAffected, res.Error
}

// BulkEnqueueByContentType creates queued jobs for every lake_objects row whose
// content_type starts with the given prefix and which has no row in
// processing_jobs for the same (processor, lake_object_id). Pages via sinceID.
func (r *ProcessingRepo) BulkEnqueueByContentType(ctx context.Context, p processing.Processor, contentTypePrefix string, sinceID int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	type row struct {
		ID int64 `gorm:"column:id"`
	}
	var ids []row
	if err := r.DB.R.WithContext(ctx).Raw(`
SELECT lo.id AS id FROM lake_objects lo
 WHERE lo.id > ?
   AND lo.content_type LIKE ?
   AND NOT EXISTS (
       SELECT 1 FROM processing_jobs pj
        WHERE pj.lake_object_id = lo.id AND pj.processor = ?
   )
 ORDER BY lo.id ASC LIMIT ?`,
		sinceID, contentTypePrefix+"%", string(p), limit).Scan(&ids).Error; err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	plain := make([]int64, 0, len(ids))
	for _, r := range ids {
		plain = append(plain, r.ID)
	}
	return r.BulkEnqueueByLakeIDs(ctx, p, plain)
}

// ReserveBatch leases up to batch queued tasks for external workers.
// Tasks whose processor is not in kinds (or kinds is empty for "any") are skipped.
func (r *ProcessingRepo) ReserveBatch(
	ctx context.Context,
	workerID int64,
	kinds []processing.Processor,
	batch int,
	leaseTTL time.Duration,
	signLease func(taskID int64, expires time.Time) (string, []byte),
) ([]processing.LeasedTask, error) {
	if batch <= 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	expires := now.Add(leaseTTL)

	kindStrs := make([]string, 0, len(kinds))
	for _, k := range kinds {
		kindStrs = append(kindStrs, string(k))
	}

	var leased []processing.LeasedTask
	err := r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		q := tx.Where("status = ?", string(processing.StatusQueued))
		if len(kindStrs) > 0 {
			q = q.Where("processor IN ?", kindStrs)
		}
		var picks []ProcessingJob
		if err := q.Order("id ASC").Limit(batch).Find(&picks).Error; err != nil {
			return fmt.Errorf("reserve tasks: pick: %w", err)
		}
		if len(picks) == 0 {
			return nil
		}
		for _, p := range picks {
			tok, raw := signLease(p.ID, expires)
			res := tx.Exec(`
UPDATE processing_jobs
   SET status = ?, started_at = ?, attempt_count = attempt_count + 1,
       lease_token = ?, leased_by_worker_id = ?, lease_expires_at = ?
 WHERE id = ? AND status = ?`,
				string(processing.StatusRunning), now,
				raw, workerID, expires,
				p.ID, string(processing.StatusQueued))
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				continue
			}
			// Read source blob metadata so we can give the worker a useful hint.
			var lo LakeObject
			if err := tx.Where("id = ?", p.LakeObjectID).First(&lo).Error; err != nil {
				return fmt.Errorf("reserve tasks: refetch lake: %w", err)
			}
			ct := ""
			if lo.ContentType != nil {
				ct = *lo.ContentType
			}
			leased = append(leased, processing.LeasedTask{
				Job: processing.Job{
					ID:           p.ID,
					LakeObjectID: p.LakeObjectID,
					Processor:    processing.Processor(p.Processor),
					Status:       processing.StatusRunning,
					AttemptCount: p.AttemptCount + 1,
					MaxAttempts:  p.MaxAttempts,
					StartedAt:    &now,
				},
				LeaseToken:      tok,
				LeaseExpiresAt:  expires,
				BlobContentType: ct,
				BlobSizeBytes:   lo.FileSizeBytes,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return leased, nil
}

// Heartbeat extends the lease on a task.
func (r *ProcessingRepo) Heartbeat(ctx context.Context, taskID int64, leaseToken []byte, extend time.Duration) (time.Time, error) {
	newExpiry := time.Now().UTC().Add(extend)
	res := r.DB.W.WithContext(ctx).Exec(`
UPDATE processing_jobs
   SET lease_expires_at = ?
 WHERE id = ? AND status = ? AND lease_token = ?`,
		newExpiry, taskID, string(processing.StatusRunning), leaseToken)
	if res.Error != nil {
		return time.Time{}, res.Error
	}
	if res.RowsAffected == 0 {
		return time.Time{}, errors.New("heartbeat: lease not held")
	}
	return newExpiry, nil
}

// Complete marks a task done and clears the lease.
func (r *ProcessingRepo) Complete(ctx context.Context, taskID int64, leaseToken []byte, outputLakeID *int64) error {
	now := time.Now().UTC()
	res := r.DB.W.WithContext(ctx).Exec(`
UPDATE processing_jobs
   SET status = ?, finished_at = ?,
       output_lake_object_id = ?,
       lease_token = NULL, leased_by_worker_id = NULL, lease_expires_at = NULL
 WHERE id = ? AND status = ? AND lease_token = ?`,
		string(processing.StatusDone), now, outputLakeID,
		taskID, string(processing.StatusRunning), leaseToken)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("complete: lease not held")
	}
	return nil
}

// Fail records a failure; re-queues if attempts remain and retryable, else marks failed.
func (r *ProcessingRepo) Fail(ctx context.Context, taskID int64, leaseToken []byte, errMsg string, retryable bool) error {
	return r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		var m ProcessingJob
		if err := tx.Where("id = ?", taskID).First(&m).Error; err != nil {
			return err
		}
		if len(m.LeaseToken) == 0 || !equalBytes(m.LeaseToken, leaseToken) {
			return errors.New("fail: lease not held")
		}
		newStatus := string(processing.StatusFailed)
		if retryable && m.AttemptCount < m.MaxAttempts {
			newStatus = string(processing.StatusQueued)
		}
		now := time.Now().UTC()
		em := errMsg
		updates := map[string]interface{}{
			"status":              newStatus,
			"last_error":          &em,
			"finished_at":         &now,
			"lease_token":         nil,
			"leased_by_worker_id": nil,
			"lease_expires_at":    nil,
		}
		return tx.Model(&ProcessingJob{}).Where("id = ?", taskID).Updates(updates).Error
	})
}

// RequeueByFilter flips matching processing_jobs rows back to 'queued' and
// clears their lease columns. All fields AND-ed. Empty Status means no
// status constraint; the CLI is responsible for requiring at least one
// filter to avoid mass-requeue accidents.
func (r *ProcessingRepo) RequeueByFilter(ctx context.Context, f processing.TaskRequeueFilter) (int64, error) {
	q := r.DB.W.WithContext(ctx).Model(&ProcessingJob{})
	if f.Status != "" {
		q = q.Where("status = ?", string(f.Status))
	}
	if f.WorkerID > 0 {
		q = q.Where("leased_by_worker_id = ?", f.WorkerID)
	}
	if f.Processor != "" {
		q = q.Where("processor = ?", string(f.Processor))
	}
	res := q.Updates(map[string]interface{}{
		"status":              string(processing.StatusQueued),
		"lease_token":         nil,
		"leased_by_worker_id": nil,
		"lease_expires_at":    nil,
	})
	return res.RowsAffected, res.Error
}

// StatusCounts returns a {processor → {status → count}} histogram.
func (r *ProcessingRepo) StatusCounts(ctx context.Context) (map[string]map[string]int64, error) {
	type row struct {
		Processor string `gorm:"column:p"`
		Status    string `gorm:"column:s"`
		N         int64  `gorm:"column:n"`
	}
	var rows []row
	if err := r.DB.R.WithContext(ctx).Raw(`
SELECT processor AS p, status AS s, COUNT(*) AS n FROM processing_jobs GROUP BY processor, status`).Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]map[string]int64)
	for _, r := range rows {
		if _, ok := out[r.Processor]; !ok {
			out[r.Processor] = make(map[string]int64)
		}
		out[r.Processor][r.Status] = r.N
	}
	return out, nil
}

// SweepExpired re-queues running tasks whose lease expired.
func (r *ProcessingRepo) SweepExpired(ctx context.Context, now time.Time) (int64, error) {
	res := r.DB.W.WithContext(ctx).Exec(`
UPDATE processing_jobs
   SET status = ?, lease_token = NULL, leased_by_worker_id = NULL, lease_expires_at = NULL
 WHERE status = ? AND lease_expires_at < ?`,
		string(processing.StatusQueued), string(processing.StatusRunning), now)
	return res.RowsAffected, res.Error
}
