package gormrepo

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/processing"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// Capability row.
type Capability struct {
	Name        string    `gorm:"primaryKey;column:name"`
	Description *string   `gorm:"column:description"`
	Internal    bool      `gorm:"column:internal"`
	CreatedAt   time.Time `gorm:"column:created_at"`
}

func (Capability) TableName() string { return "capabilities" }

// CapabilityRepo implements processing.CapabilityRepo on rwdb.
type CapabilityRepo struct{ DB *rwdb.DB }

// NewCapabilityRepo wires a CapabilityRepo to rwdb.
func NewCapabilityRepo(db *rwdb.DB) *CapabilityRepo { return &CapabilityRepo{DB: db} }

// List returns every capability row ordered by name.
func (r *CapabilityRepo) List(ctx context.Context) ([]processing.Capability, error) {
	var ms []Capability
	if err := r.DB.R.WithContext(ctx).Order("name ASC").Find(&ms).Error; err != nil {
		return nil, err
	}
	out := make([]processing.Capability, 0, len(ms))
	for i := range ms {
		out = append(out, mapCapability(&ms[i]))
	}
	return out, nil
}

// Get returns the capability row by name, or nil if absent.
func (r *CapabilityRepo) Get(ctx context.Context, name string) (*processing.Capability, error) {
	var m Capability
	err := r.DB.R.WithContext(ctx).Where("name = ?", name).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	c := mapCapability(&m)
	return &c, nil
}

// Upsert inserts or updates a capability row by name.
func (r *CapabilityRepo) Upsert(ctx context.Context, c processing.Capability) error {
	m := Capability{Name: c.Name, Internal: c.Internal, CreatedAt: time.Now().UTC()}
	if c.Description != "" {
		s := c.Description
		m.Description = &s
	}
	return r.DB.W.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "name"}},
		DoUpdates: clause.AssignmentColumns([]string{"description", "internal"}),
	}).Create(&m).Error
}

// Delete removes a capability row by name.
func (r *CapabilityRepo) Delete(ctx context.Context, name string) error {
	return r.DB.W.WithContext(ctx).Where("name = ?", name).Delete(&Capability{}).Error
}

func mapCapability(m *Capability) processing.Capability {
	c := processing.Capability{
		Name:      m.Name,
		Internal:  m.Internal,
		CreatedAt: m.CreatedAt,
	}
	if m.Description != nil {
		c.Description = *m.Description
	}
	return c
}
