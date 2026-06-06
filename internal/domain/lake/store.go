package lake

import (
	"context"
	"io"
	"time"
)

// Stat is metadata about a stored blob.
type Stat struct {
	Size        int64
	ContentType string
	SHA256      []byte
	ModTime     time.Time
}

// PutMeta carries hints into a Put call.
type PutMeta struct {
	ContentType string
	SHA256      []byte
}

// BlobStore is the port for raw bytes. Implementations: local FS, S3, ...
type BlobStore interface {
	Backend() string
	Put(ctx context.Context, key string, r io.Reader, m PutMeta) (Stat, error)
	Get(ctx context.Context, key string) (io.ReadCloser, Stat, error)
	Stat(ctx context.Context, key string) (Stat, error)
	Delete(ctx context.Context, key string) error
}
