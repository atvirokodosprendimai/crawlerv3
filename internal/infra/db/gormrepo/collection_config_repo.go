package gormrepo

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// CollectionConfigModel maps the collections table to gorm.
type CollectionConfigModel struct {
	Name         string    `gorm:"column:name;primaryKey"`
	ChunkTokens  int       `gorm:"column:chunk_tokens"`
	OverlapPrev  int       `gorm:"column:overlap_prev"`
	OverlapNext  int       `gorm:"column:overlap_next"`
	Tokenizer    string    `gorm:"column:tokenizer"`
	CreatedAt    time.Time `gorm:"column:created_at"`
	UpdatedAt    time.Time `gorm:"column:updated_at"`
}

func (CollectionConfigModel) TableName() string { return "collections" }

// CollectionConfigRepo implements chunking.CollectionConfigRepo on rwdb.DB.
type CollectionConfigRepo struct{ DB *rwdb.DB }

// NewCollectionConfigRepo wires the repo to the rwdb pools.
func NewCollectionConfigRepo(db *rwdb.DB) *CollectionConfigRepo { return &CollectionConfigRepo{DB: db} }

// Get returns the row for a collection name, or chunking.ErrCollectionNotFound
// when no row matches. Read pool.
func (r *CollectionConfigRepo) Get(ctx context.Context, name string) (*chunking.CollectionConfig, error) {
	var m CollectionConfigModel
	err := r.DB.R.WithContext(ctx).Where("name = ?", name).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, chunking.ErrCollectionNotFound
	}
	if err != nil {
		return nil, err
	}
	return mapCollectionConfig(&m), nil
}

// Upsert inserts or updates. Operator surface; touches updated_at.
func (r *CollectionConfigRepo) Upsert(ctx context.Context, cfg chunking.CollectionConfig) error {
	now := time.Now().UTC()
	m := CollectionConfigModel{
		Name:         cfg.Name,
		ChunkTokens:  cfg.ChunkTokens,
		OverlapPrev:  cfg.OverlapPrev,
		OverlapNext:  cfg.OverlapNext,
		Tokenizer:    cfg.Tokenizer,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	return r.DB.W.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "name"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"chunk_tokens", "overlap_prev", "overlap_next", "tokenizer", "updated_at",
			}),
		}).
		Create(&m).Error
}

// List returns every row in name order. Read pool.
func (r *CollectionConfigRepo) List(ctx context.Context) ([]chunking.CollectionConfig, error) {
	var rows []CollectionConfigModel
	if err := r.DB.R.WithContext(ctx).Order("name ASC").Find(&rows).Error; err != nil {
		return nil, err
	}
	out := make([]chunking.CollectionConfig, len(rows))
	for i := range rows {
		out[i] = *mapCollectionConfig(&rows[i])
	}
	return out, nil
}

// Delete removes a row. No-op if missing.
func (r *CollectionConfigRepo) Delete(ctx context.Context, name string) error {
	return r.DB.W.WithContext(ctx).
		Where("name = ?", name).
		Delete(&CollectionConfigModel{}).Error
}

func mapCollectionConfig(m *CollectionConfigModel) *chunking.CollectionConfig {
	return &chunking.CollectionConfig{
		Name:         m.Name,
		ChunkTokens:  m.ChunkTokens,
		OverlapPrev:  m.OverlapPrev,
		OverlapNext:  m.OverlapNext,
		Tokenizer:    m.Tokenizer,
	}
}
