package processing

import (
	"context"
	"time"
)

// Repository is the persistence port for processing_jobs.
type Repository interface {
	Enqueue(ctx context.Context, lakeID int64, p Processor) (int64, error)
	BulkEnqueueByLakeIDs(ctx context.Context, p Processor, lakeIDs []int64) (int64, error)
	BulkEnqueueByContentType(ctx context.Context, p Processor, contentType string, sinceID int64, limit int) (int64, error)
	ClaimNext(ctx context.Context, p Processor) (*Job, error)
	GetByID(ctx context.Context, id int64) (*Job, error)
	MarkDone(ctx context.Context, id int64, outputLakeID *int64) error
	MarkFailed(ctx context.Context, id int64, errMsg string) error
	MarkSkipped(ctx context.Context, id int64, reason string) error
	CountQueued(ctx context.Context) (int64, error)

	// External-worker lease pattern.
	ReserveBatch(
		ctx context.Context,
		workerID int64,
		kinds []Processor,
		batch int,
		leaseTTL time.Duration,
		signLease func(taskID int64, expires time.Time) (string, []byte),
	) ([]LeasedTask, error)
	Heartbeat(ctx context.Context, taskID int64, leaseToken []byte, extend time.Duration) (time.Time, error)
	Complete(ctx context.Context, taskID int64, leaseToken []byte, outputLakeID *int64) error
	Fail(ctx context.Context, taskID int64, leaseToken []byte, errMsg string, retryable bool) error
	SweepExpired(ctx context.Context, now time.Time) (int64, error)
	RequeueByFilter(ctx context.Context, f TaskRequeueFilter) (int64, error)
	StatusCounts(ctx context.Context) (map[string]map[string]int64, error)
}

// TaskRequeueFilter selects processing_jobs rows to flip back to queued.
// Fields are AND-ed; zero-value means "no constraint".
type TaskRequeueFilter struct {
	Status    Status    // "" = any non-done
	WorkerID  int64     // 0 = any
	Processor Processor // "" = any
}

// LeasedTask is a Job + lease metadata + a hint of where the source blob lives.
type LeasedTask struct {
	Job             Job
	LeaseToken      string
	LeaseExpiresAt  time.Time
	BlobContentType string
	BlobSizeBytes   int64
}
