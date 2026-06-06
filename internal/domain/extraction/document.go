// Package extraction holds extracted plain-text documents.
package extraction

import (
	"context"
	"time"
)

// Document is the cleaned text derived from a lake object.
type Document struct {
	ID                 int64
	SourceLakeObjectID int64
	Text               string
	Language           string
	PageCount          int
	Collection         string // vector-store collection hint (per-domain)
	ExtractedAt        time.Time
}

// Repository is the persistence port.
type Repository interface {
	Upsert(ctx context.Context, d Document) (int64, error)
	GetBySource(ctx context.Context, sourceLakeObjectID int64) (*Document, error)
	GetByID(ctx context.Context, id int64) (*Document, error)
	ListSince(ctx context.Context, sinceID int64, limit int) ([]Document, error)
}
