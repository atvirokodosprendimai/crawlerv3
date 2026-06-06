package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
)

// Mover migrates blobs between two BlobStores and rewrites their lake_objects rows.
type Mover struct {
	Lake lake.Repository
	Src  lake.BlobStore
	Dst  lake.BlobStore
	// DeleteSrc removes the source blob after a successful copy + DB update.
	// Default false — operators usually want a manual grace period first.
	DeleteSrc bool
	// BatchSize is the page size for ListByBackend pagination.
	BatchSize int
}

// Stats summarizes a migration run.
type Stats struct {
	Scanned int64
	Copied  int64
	Skipped int64
	Errors  int64
}

// Run migrates objects whose storage_backend == Src.Backend() to Dst.
// It is idempotent: rows already on Dst are skipped.
func (m *Mover) Run(ctx context.Context) (Stats, error) {
	if m.Src == nil || m.Dst == nil || m.Lake == nil {
		return Stats{}, errors.New("mover: Lake, Src, Dst all required")
	}
	if m.Src.Backend() == m.Dst.Backend() {
		return Stats{}, fmt.Errorf("mover: src and dst backends are both %q", m.Src.Backend())
	}
	if m.BatchSize <= 0 {
		m.BatchSize = 100
	}
	var stats Stats
	var afterID int64
	for {
		objs, err := m.Lake.ListByBackend(ctx, m.Src.Backend(), m.BatchSize, afterID)
		if err != nil {
			return stats, fmt.Errorf("mover: list: %w", err)
		}
		if len(objs) == 0 {
			return stats, nil
		}
		for _, o := range objs {
			stats.Scanned++
			afterID = o.ID
			if err := m.moveOne(ctx, o); err != nil {
				stats.Errors++
				fmt.Printf("mover: object %d: %v\n", o.ID, err)
				continue
			}
			stats.Copied++
		}
	}
}

func (m *Mover) moveOne(ctx context.Context, o lake.Object) error {
	rc, _, err := m.Src.Get(ctx, o.StorageKey)
	if err != nil {
		return fmt.Errorf("get src: %w", err)
	}
	defer rc.Close()

	stat, err := m.Dst.Put(ctx, o.StorageKey, rc, lake.PutMeta{
		ContentType: o.ContentType,
		SHA256:      o.ContentSHA256,
	})
	if err != nil {
		return fmt.Errorf("put dst: %w", err)
	}
	if stat.Size != o.FileSize {
		return fmt.Errorf("size mismatch src=%d dst=%d", o.FileSize, stat.Size)
	}
	migratedFrom := fmt.Sprintf("%s:%s", m.Src.Backend(), o.StorageKey)
	if err := m.Lake.UpdateStorage(ctx, o.ID, m.Dst.Backend(), o.StorageKey, migratedFrom); err != nil {
		return fmt.Errorf("update row: %w", err)
	}
	if m.DeleteSrc {
		if err := m.Src.Delete(ctx, o.StorageKey); err != nil {
			fmt.Printf("mover: delete src %s: %v\n", o.StorageKey, err)
		}
	}
	return nil
}
