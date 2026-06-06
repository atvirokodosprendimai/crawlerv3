package gormrepo

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/extraction"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// ExtractionRepo implements extraction.Repository.
type ExtractionRepo struct{ DB *rwdb.DB }

// NewExtractionRepo wires an ExtractionRepo to rwdb.
func NewExtractionRepo(db *rwdb.DB) *ExtractionRepo { return &ExtractionRepo{DB: db} }

// Upsert writes or replaces the extracted text for a lake object.
func (r *ExtractionRepo) Upsert(ctx context.Context, d extraction.Document) (int64, error) {
	var id int64
	err := r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		var m ExtractedDocument
		err := tx.Where("source_lake_object_id = ?", d.SourceLakeObjectID).First(&m).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			m = ExtractedDocument{
				SourceLakeObjectID: d.SourceLakeObjectID,
				Text:               d.Text,
				ExtractedAt:        time.Now().UTC(),
			}
			if d.Language != "" {
				lg := d.Language
				m.Language = &lg
			}
			if d.PageCount > 0 {
				pc := d.PageCount
				m.PageCount = &pc
			}
			if d.Collection != "" {
				col := d.Collection
				m.Collection = &col
			}
			if err := tx.Create(&m).Error; err != nil {
				return err
			}
			id = m.ID
			return nil
		}
		if err != nil {
			return err
		}
		updates := map[string]interface{}{"text": d.Text, "extracted_at": time.Now().UTC()}
		if d.Language != "" {
			updates["language"] = d.Language
		}
		if d.PageCount > 0 {
			updates["page_count"] = d.PageCount
		}
		if d.Collection != "" {
			updates["collection"] = d.Collection
		}
		if err := tx.Model(&ExtractedDocument{}).Where("id = ?", m.ID).Updates(updates).Error; err != nil {
			return err
		}
		id = m.ID
		return nil
	})
	return id, err
}

// GetByID returns an extracted document by primary key, or nil.
func (r *ExtractionRepo) GetByID(ctx context.Context, id int64) (*extraction.Document, error) {
	var m ExtractedDocument
	err := r.DB.R.WithContext(ctx).Where("id = ?", id).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return mapExtracted(&m), nil
}

// ListSince pages through extracted_documents by id.
func (r *ExtractionRepo) ListSince(ctx context.Context, sinceID int64, limit int) ([]extraction.Document, error) {
	if limit <= 0 {
		limit = 100
	}
	var ms []ExtractedDocument
	if err := r.DB.R.WithContext(ctx).Where("id > ?", sinceID).
		Order("id ASC").Limit(limit).Find(&ms).Error; err != nil {
		return nil, err
	}
	out := make([]extraction.Document, 0, len(ms))
	for _, m := range ms {
		out = append(out, *mapExtracted(&m))
	}
	return out, nil
}

func mapExtracted(m *ExtractedDocument) *extraction.Document {
	d := extraction.Document{
		ID:                 m.ID,
		SourceLakeObjectID: m.SourceLakeObjectID,
		Text:               m.Text,
		ExtractedAt:        m.ExtractedAt,
	}
	if m.Language != nil {
		d.Language = *m.Language
	}
	if m.PageCount != nil {
		d.PageCount = *m.PageCount
	}
	if m.Collection != nil {
		d.Collection = *m.Collection
	}
	return &d
}

// GetBySource returns the extracted doc for a lake object, or nil.
func (r *ExtractionRepo) GetBySource(ctx context.Context, sourceLakeObjectID int64) (*extraction.Document, error) {
	var m ExtractedDocument
	err := r.DB.R.WithContext(ctx).Where("source_lake_object_id = ?", sourceLakeObjectID).First(&m).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	d := extraction.Document{
		ID:                 m.ID,
		SourceLakeObjectID: m.SourceLakeObjectID,
		Text:               m.Text,
		ExtractedAt:        m.ExtractedAt,
	}
	if m.Language != nil {
		d.Language = *m.Language
	}
	if m.PageCount != nil {
		d.PageCount = *m.PageCount
	}
	return &d, nil
}
