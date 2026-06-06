package gormrepo

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// WorkerRepo implements workerid.Repository on top of rwdb.DB.
type WorkerRepo struct{ DB *rwdb.DB }

// NewWorkerRepo wires a WorkerRepo to the rwdb pools.
func NewWorkerRepo(db *rwdb.DB) *WorkerRepo { return &WorkerRepo{DB: db} }

// Create inserts a worker row.
func (r *WorkerRepo) Create(ctx context.Context, w workerid.Worker) (int64, error) {
	m := Worker{
		PATHash:         w.PATHash,
		Label:           w.Label,
		ReputationScore: w.ReputationScore,
		MaxConcurrent:   w.MaxConcurrent,
		CreatedAt:       time.Now().UTC(),
	}
	if m.ReputationScore == 0 {
		m.ReputationScore = 100
	}
	if m.MaxConcurrent == 0 {
		m.MaxConcurrent = 4
	}
	if len(w.Capabilities) > 0 {
		raw := encodeCaps(w.Capabilities)
		m.Capabilities = &raw
	}
	if err := r.DB.W.WithContext(ctx).Create(&m).Error; err != nil {
		return 0, err
	}
	return m.ID, nil
}

// FindByPATHash returns the worker matching the PAT hash, or nil if not found.
func (r *WorkerRepo) FindByPATHash(ctx context.Context, patHash []byte) (*workerid.Worker, error) {
	var m Worker
	err := r.DB.R.WithContext(ctx).Where("pat_hash = ?", patHash).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return mapWorker(&m), nil
}

// FindByID returns the worker by primary key, or nil.
func (r *WorkerRepo) FindByID(ctx context.Context, id int64) (*workerid.Worker, error) {
	var m Worker
	err := r.DB.R.WithContext(ctx).Where("id = ?", id).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return mapWorker(&m), nil
}

// TouchIP records the most recent IP seen for this worker and updates last_seen_at.
func (r *WorkerRepo) TouchIP(ctx context.Context, id int64, ip string) error {
	now := time.Now().UTC()
	return r.DB.W.WithContext(ctx).Model(&Worker{}).Where("id = ?", id).
		Updates(map[string]interface{}{"ip_last": ip, "last_seen_at": now}).Error
}

// List returns all workers.
func (r *WorkerRepo) List(ctx context.Context) ([]workerid.Worker, error) {
	var ms []Worker
	if err := r.DB.R.WithContext(ctx).Order("id ASC").Find(&ms).Error; err != nil {
		return nil, err
	}
	out := make([]workerid.Worker, 0, len(ms))
	for i := range ms {
		out = append(out, *mapWorker(&ms[i]))
	}
	return out, nil
}

// UpdateCapabilities replaces the capabilities array.
func (r *WorkerRepo) UpdateCapabilities(ctx context.Context, id int64, caps []string) error {
	raw := encodeCaps(caps)
	return r.DB.W.WithContext(ctx).Model(&Worker{}).Where("id = ?", id).
		Update("capabilities", raw).Error
}

// UpdateMaxConcurrent sets the per-worker global concurrency cap.
func (r *WorkerRepo) UpdateMaxConcurrent(ctx context.Context, id int64, n int) error {
	if n < 0 {
		n = 0
	}
	return r.DB.W.WithContext(ctx).Model(&Worker{}).Where("id = ?", id).
		Update("max_concurrent", n).Error
}

// Ban sets banned_at to now.
func (r *WorkerRepo) Ban(ctx context.Context, id int64) error {
	now := time.Now().UTC()
	return r.DB.W.WithContext(ctx).Model(&Worker{}).Where("id = ?", id).
		Update("banned_at", now).Error
}

// Unban clears banned_at.
func (r *WorkerRepo) Unban(ctx context.Context, id int64) error {
	return r.DB.W.WithContext(ctx).Model(&Worker{}).Where("id = ?", id).
		Update("banned_at", nil).Error
}

// CountHeldLeases sums active leases this worker holds across all three queues.
func (r *WorkerRepo) CountHeldLeases(ctx context.Context, id int64) (int64, error) {
	type row struct {
		Held int64 `gorm:"column:held"`
	}
	var rr row
	err := r.DB.R.WithContext(ctx).Raw(`
SELECT
  (SELECT COUNT(*) FROM crawl_frontier  WHERE status='leased'  AND leased_by_worker_id = ?)
+ (SELECT COUNT(*) FROM processing_jobs WHERE status='running' AND leased_by_worker_id = ?)
+ (SELECT COUNT(*) FROM document_chunks WHERE embed_status='leased' AND leased_by_worker_id = ?)
AS held`, id, id, id).Scan(&rr).Error
	return rr.Held, err
}

func encodeCaps(caps []string) string {
	if len(caps) == 0 {
		return "[]"
	}
	b, _ := json.Marshal(caps)
	return string(b)
}

func decodeCaps(s *string) []string {
	if s == nil || *s == "" {
		return nil
	}
	var out []string
	_ = json.Unmarshal([]byte(*s), &out)
	return out
}

func mapWorker(m *Worker) *workerid.Worker {
	w := workerid.Worker{
		ID:              m.ID,
		PATHash:         m.PATHash,
		Label:           m.Label,
		ReputationScore: m.ReputationScore,
		BannedAt:        m.BannedAt,
		CreatedAt:       m.CreatedAt,
		MaxConcurrent:   m.MaxConcurrent,
		LastSeenAt:      m.LastSeenAt,
		Capabilities:    decodeCaps(m.Capabilities),
	}
	if m.IPLast != nil {
		w.IPLast = *m.IPLast
	}
	return &w
}
