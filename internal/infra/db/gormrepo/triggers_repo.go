package gormrepo

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/triggers"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// PipelineTrigger row.
type PipelineTrigger struct {
	ID          int64     `gorm:"primaryKey;column:id"`
	WhenEvent   string    `gorm:"column:when_event;index:idx_triggers_event"`
	WhenFilter  *string   `gorm:"column:when_filter"`
	EnqueueKind string    `gorm:"column:enqueue_kind"`
	Enabled     bool      `gorm:"column:enabled"`
	CreatedAt   time.Time `gorm:"column:created_at"`
}

func (PipelineTrigger) TableName() string { return "pipeline_triggers" }

// TriggersRepo implements triggers.Repository.
type TriggersRepo struct{ DB *rwdb.DB }

// NewTriggersRepo wires a TriggersRepo to rwdb.
func NewTriggersRepo(db *rwdb.DB) *TriggersRepo { return &TriggersRepo{DB: db} }

// Create inserts a trigger row.
func (r *TriggersRepo) Create(ctx context.Context, t triggers.Trigger) (int64, error) {
	m := PipelineTrigger{
		WhenEvent:   string(t.WhenEvent),
		EnqueueKind: t.EnqueueKind,
		Enabled:     true,
		CreatedAt:   time.Now().UTC(),
	}
	if t.WhenFilter != "" {
		s := t.WhenFilter
		m.WhenFilter = &s
	}
	if err := r.DB.W.WithContext(ctx).Create(&m).Error; err != nil {
		return 0, err
	}
	return m.ID, nil
}

// List returns all triggers.
func (r *TriggersRepo) List(ctx context.Context) ([]triggers.Trigger, error) {
	var ms []PipelineTrigger
	if err := r.DB.R.WithContext(ctx).Order("id ASC").Find(&ms).Error; err != nil {
		return nil, err
	}
	return mapTriggers(ms), nil
}

// ListByEvent returns enabled triggers matching the event name.
func (r *TriggersRepo) ListByEvent(ctx context.Context, e triggers.Event) ([]triggers.Trigger, error) {
	var ms []PipelineTrigger
	if err := r.DB.R.WithContext(ctx).
		Where("when_event = ? AND enabled = ?", string(e), true).
		Order("id ASC").Find(&ms).Error; err != nil {
		return nil, err
	}
	return mapTriggers(ms), nil
}

// SetEnabled toggles a trigger.
func (r *TriggersRepo) SetEnabled(ctx context.Context, id int64, enabled bool) error {
	return r.DB.W.WithContext(ctx).Model(&PipelineTrigger{}).Where("id = ?", id).
		Update("enabled", enabled).Error
}

// Delete removes a trigger.
func (r *TriggersRepo) Delete(ctx context.Context, id int64) error {
	return r.DB.W.WithContext(ctx).Delete(&PipelineTrigger{}, id).Error
}

func mapTriggers(ms []PipelineTrigger) []triggers.Trigger {
	out := make([]triggers.Trigger, 0, len(ms))
	for _, m := range ms {
		t := triggers.Trigger{
			ID:          m.ID,
			WhenEvent:   triggers.Event(m.WhenEvent),
			EnqueueKind: m.EnqueueKind,
			Enabled:     m.Enabled,
			CreatedAt:   m.CreatedAt,
		}
		if m.WhenFilter != nil {
			t.WhenFilter = *m.WhenFilter
		}
		out = append(out, t)
	}
	return out
}

// keep gorm import used in case future query helpers need it
var _ = gorm.ErrRecordNotFound
