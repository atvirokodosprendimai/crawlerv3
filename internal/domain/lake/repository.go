package lake

import "context"

// ListOpts are query knobs for ListSince.
type ListOpts struct {
	SinceID           int64
	Limit             int
	Backend           string // empty = any
	ContentTypePrefix string // empty = any
}

// Repository is the persistence port for lake objects (index, not blob).
type Repository interface {
	Insert(ctx context.Context, o Object) (id int64, err error)
	FindBySHA(ctx context.Context, sha []byte) (*Object, error)
	GetByID(ctx context.Context, id int64) (*Object, error)
	ListByBackend(ctx context.Context, backend string, limit int, afterID int64) ([]Object, error)
	ListSince(ctx context.Context, opts ListOpts) ([]Object, error)
	UpdateStorage(ctx context.Context, id int64, backend, key, migratedFrom string) error
}
