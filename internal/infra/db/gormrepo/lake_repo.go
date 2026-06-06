package gormrepo

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// LakeRepo implements lake.Repository on rwdb.DB.
type LakeRepo struct{ DB *rwdb.DB }

// NewLakeRepo wires a LakeRepo to the rwdb pools.
func NewLakeRepo(db *rwdb.DB) *LakeRepo { return &LakeRepo{DB: db} }

// Insert writes a lake_objects row.
func (r *LakeRepo) Insert(ctx context.Context, o lake.Object) (int64, error) {
	m := LakeObject{
		URLHash:        o.URLHash,
		StorageBackend: o.StorageBackend,
		StorageKey:     o.StorageKey,
		ContentSHA256:  o.ContentSHA256,
		FileSizeBytes:  o.FileSize,
		ArchivedAt:     time.Now().UTC(),
	}
	if o.ContentType != "" {
		ct := o.ContentType
		m.ContentType = &ct
	}
	if o.MigratedFrom != "" {
		mf := o.MigratedFrom
		m.MigratedFrom = &mf
	}
	if err := r.DB.W.WithContext(ctx).Create(&m).Error; err != nil {
		return 0, err
	}
	return m.ID, nil
}

// FindBySHA reads from the read pool.
func (r *LakeRepo) FindBySHA(ctx context.Context, sha []byte) (*lake.Object, error) {
	var m LakeObject
	err := r.DB.R.WithContext(ctx).Where("content_sha256 = ?", sha).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	o := lake.Object{
		ID:             m.ID,
		URLHash:        m.URLHash,
		StorageBackend: m.StorageBackend,
		StorageKey:     m.StorageKey,
		ContentSHA256:  m.ContentSHA256,
		FileSize:       m.FileSizeBytes,
		ArchivedAt:     m.ArchivedAt,
	}
	if m.ContentType != nil {
		o.ContentType = *m.ContentType
	}
	if m.MigratedFrom != nil {
		o.MigratedFrom = *m.MigratedFrom
	}
	return &o, nil
}

// GetByID returns a single lake object by primary key, or nil.
func (r *LakeRepo) GetByID(ctx context.Context, id int64) (*lake.Object, error) {
	var m LakeObject
	err := r.DB.R.WithContext(ctx).Where("id = ?", id).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	o := lake.Object{
		ID:             m.ID,
		URLHash:        m.URLHash,
		StorageBackend: m.StorageBackend,
		StorageKey:     m.StorageKey,
		ContentSHA256:  m.ContentSHA256,
		FileSize:       m.FileSizeBytes,
		ArchivedAt:     m.ArchivedAt,
	}
	if m.ContentType != nil {
		o.ContentType = *m.ContentType
	}
	if m.MigratedFrom != nil {
		o.MigratedFrom = *m.MigratedFrom
	}
	return &o, nil
}

// ListByBackend pages through objects on a given storage backend after afterID.
func (r *LakeRepo) ListByBackend(ctx context.Context, backend string, limit int, afterID int64) ([]lake.Object, error) {
	if limit <= 0 {
		limit = 100
	}
	var ms []LakeObject
	if err := r.DB.R.WithContext(ctx).
		Where("storage_backend = ? AND id > ?", backend, afterID).
		Order("id ASC").Limit(limit).Find(&ms).Error; err != nil {
		return nil, err
	}
	out := make([]lake.Object, 0, len(ms))
	for _, m := range ms {
		o := lake.Object{
			ID:             m.ID,
			URLHash:        m.URLHash,
			StorageBackend: m.StorageBackend,
			StorageKey:     m.StorageKey,
			ContentSHA256:  m.ContentSHA256,
			FileSize:       m.FileSizeBytes,
			ArchivedAt:     m.ArchivedAt,
		}
		if m.ContentType != nil {
			o.ContentType = *m.ContentType
		}
		if m.MigratedFrom != nil {
			o.MigratedFrom = *m.MigratedFrom
		}
		out = append(out, o)
	}
	return out, nil
}

// ListSince pages through lake_objects with optional backend + content-type filters.
func (r *LakeRepo) ListSince(ctx context.Context, opts lake.ListOpts) ([]lake.Object, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	q := r.DB.R.WithContext(ctx).Model(&LakeObject{}).Where("id > ?", opts.SinceID)
	if opts.Backend != "" {
		q = q.Where("storage_backend = ?", opts.Backend)
	}
	if opts.ContentTypePrefix != "" {
		q = q.Where("content_type LIKE ?", opts.ContentTypePrefix+"%")
	}
	var ms []LakeObject
	if err := q.Order("id ASC").Limit(opts.Limit).Find(&ms).Error; err != nil {
		return nil, err
	}
	out := make([]lake.Object, 0, len(ms))
	for _, m := range ms {
		o := lake.Object{
			ID:             m.ID,
			URLHash:        m.URLHash,
			StorageBackend: m.StorageBackend,
			StorageKey:     m.StorageKey,
			ContentSHA256:  m.ContentSHA256,
			FileSize:       m.FileSizeBytes,
			ArchivedAt:     m.ArchivedAt,
		}
		if m.ContentType != nil {
			o.ContentType = *m.ContentType
		}
		if m.MigratedFrom != nil {
			o.MigratedFrom = *m.MigratedFrom
		}
		out = append(out, o)
	}
	return out, nil
}

// UpdateStorage moves a row to a new backend+key (write).
func (r *LakeRepo) UpdateStorage(ctx context.Context, id int64, backend, key, migratedFrom string) error {
	updates := map[string]interface{}{
		"storage_backend": backend,
		"storage_key":     key,
		"migrated_from":   migratedFrom,
	}
	return r.DB.W.WithContext(ctx).Model(&LakeObject{}).Where("id = ?", id).Updates(updates).Error
}
