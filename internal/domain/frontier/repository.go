package frontier

import (
	"context"
	"time"
)

// Repository is the persistence port for the frontier domain.
type Repository interface {
	// Enqueue inserts a URL if its hash is not present. Returns true if inserted.
	Enqueue(ctx context.Context, j Job) (inserted bool, err error)

	// EnqueueMany inserts a batch of URLs in a single transaction. Returns the
	// count actually inserted (duplicates by url_hash are silently skipped) and
	// the first error if any. Used for discovered_links intake where holding
	// the write lock once is far cheaper than N implicit transactions.
	EnqueueMany(ctx context.Context, jobs []Job) (inserted int64, err error)

	// Reserve leases up to batch jobs for the given worker. Respects per-domain
	// crawl_delay_ms and skips dead/leased rows. The capabilities slice gates
	// per-domain required_capability filtering: empty = match any, otherwise
	// only domains with a matching (or empty) required_capability are eligible.
	Reserve(ctx context.Context, workerID int64, capabilities []string, batch int, leaseTTL time.Duration, leaseToken func(urlHash []byte, expires time.Time) (string, []byte)) ([]LeasedJob, error)

	// Heartbeat extends the lease if the token matches.
	Heartbeat(ctx context.Context, urlHash []byte, leaseToken []byte, extend time.Duration) (newExpiry time.Time, err error)

	// Complete marks the row done after a successful result.
	Complete(ctx context.Context, urlHash []byte, leaseToken []byte, httpStatus int) error

	// Fail marks attempt failure; sets next_retry_at or status='dead'.
	Fail(ctx context.Context, urlHash []byte, leaseToken []byte, f Failure, backoff time.Duration) error

	// SweepExpired re-queues leased rows whose lease_expires_at < now.
	SweepExpired(ctx context.Context, now time.Time) (int64, error)

	// LookupCanonical returns the row by canonical url hash (used to dedup discovered links).
	LookupCanonical(ctx context.Context, urlHash []byte) (exists bool, err error)

	// DomainIDByURLHash returns the domain_id for the given frontier row.
	// Used to resolve per-domain settings (e.g. embed_collection) from a lake_object.
	DomainIDByURLHash(ctx context.Context, urlHash []byte) (int64, error)

	// RequeueByFilter flips matching rows back to 'queued', clearing leases.
	// Operator surface — not used during normal worker flow.
	RequeueByFilter(ctx context.Context, f RequeueFilter) (int64, error)

	// StatusCounts returns a {status → count} histogram for the queue.
	StatusCounts(ctx context.Context) (map[string]int64, error)
}

// RequeueFilter selects crawl_frontier rows to flip back to 'queued'.
// Fields are AND-ed; zero-value means "no constraint".
type RequeueFilter struct {
	Status   Status // "" = any non-done
	WorkerID int64  // 0 = any
	DomainID int64  // 0 = any
}

// LeasedJob is a Job plus the issued Lease.
type LeasedJob struct {
	Job   Job
	Lease Lease
}
