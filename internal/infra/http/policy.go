package http

import (
	"context"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
)

// effectiveBatch reduces requestedBatch by the worker's remaining concurrency
// headroom. Returns 0 when the worker is saturated. An empty Capabilities list
// on the worker is treated as "any kind allowed" (see Worker.Can).
func effectiveBatch(ctx context.Context, repo workerid.Repository, w *workerid.Worker, requested int) (int, error) {
	if requested <= 0 {
		requested = 1
	}
	if w.MaxConcurrent <= 0 {
		return requested, nil // 0 → unlimited
	}
	held, err := repo.CountHeldLeases(ctx, w.ID)
	if err != nil {
		return 0, err
	}
	rem := w.MaxConcurrent - int(held)
	if rem < 0 {
		rem = 0
	}
	if rem > requested {
		rem = requested
	}
	return rem, nil
}
