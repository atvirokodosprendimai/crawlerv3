package gormrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/frontier"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// FrontierRepo implements frontier.Repository and frontier.DomainRepo on rwdb.
type FrontierRepo struct {
	DB *rwdb.DB
}

// NewFrontierRepo wires a FrontierRepo to the rwdb pools.
func NewFrontierRepo(db *rwdb.DB) *FrontierRepo {
	return &FrontierRepo{DB: db}
}

// --- DomainRepo -----------------------------------------------------------

// UpsertByHost inserts or returns an existing domain row by host.
func (r *FrontierRepo) UpsertByHost(ctx context.Context, host, scheme string, crawlDelayMS int) (frontier.DomainRow, error) {
	var existing Domain
	err := r.DB.R.WithContext(ctx).Where("host = ?", host).First(&existing).Error
	if err == nil {
		return *mapDomain(&existing), nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return frontier.DomainRow{}, err
	}
	d := Domain{Host: host, Scheme: scheme, IsActive: true, CrawlDelayMS: crawlDelayMS, ParallelFetches: 1, CreatedAt: time.Now().UTC()}
	if err := r.DB.W.WithContext(ctx).Create(&d).Error; err != nil {
		return frontier.DomainRow{}, err
	}
	return *mapDomain(&d), nil
}

// FindByHost returns the domain row or nil if absent.
func (r *FrontierRepo) FindByHost(ctx context.Context, host string) (*frontier.DomainRow, error) {
	var d Domain
	err := r.DB.R.WithContext(ctx).Where("host = ?", host).First(&d).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return mapDomain(&d), nil
}

// List returns every domain row.
func (r *FrontierRepo) List(ctx context.Context) ([]frontier.DomainRow, error) {
	var ds []Domain
	if err := r.DB.R.WithContext(ctx).Order("id ASC").Find(&ds).Error; err != nil {
		return nil, err
	}
	out := make([]frontier.DomainRow, 0, len(ds))
	for i := range ds {
		out = append(out, *mapDomain(&ds[i]))
	}
	return out, nil
}

// GetByID returns a domain by primary key, or nil if absent.
func (r *FrontierRepo) GetByID(ctx context.Context, id int64) (*frontier.DomainRow, error) {
	var d Domain
	err := r.DB.R.WithContext(ctx).Where("id = ?", id).First(&d).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return mapDomain(&d), nil
}

func mapDomain(d *Domain) *frontier.DomainRow {
	row := &frontier.DomainRow{
		ID: d.ID, Host: d.Host, Scheme: d.Scheme,
		IsActive: d.IsActive, CrawlDelayMS: d.CrawlDelayMS,
		ParallelFetches: d.ParallelFetches,
	}
	if d.EmbedCollection != nil {
		row.EmbedCollection = *d.EmbedCollection
	}
	if d.RequiredCapability != nil {
		row.RequiredCapability = *d.RequiredCapability
	}
	return row
}

// UpdateCrawlDelay sets the per-host politeness delay.
func (r *FrontierRepo) UpdateCrawlDelay(ctx context.Context, host string, ms int) error {
	if ms < 0 {
		ms = 0
	}
	res := r.DB.W.WithContext(ctx).Model(&Domain{}).Where("host = ?", host).
		Update("crawl_delay_ms", ms)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("domain not found")
	}
	return nil
}

// UpdateParallelFetches caps how many URLs a single reserve call may pull
// from this domain. 1 = strict serial (the default, preserves classic
// politeness). >1 = let a cooperative host serve multiple URLs concurrently
// per reserve; crawl_delay_ms still gates the gap between successive reserves.
func (r *FrontierRepo) UpdateParallelFetches(ctx context.Context, host string, n int) error {
	if n < 1 {
		n = 1
	}
	res := r.DB.W.WithContext(ctx).Model(&Domain{}).Where("host = ?", host).
		Update("parallel_fetches", n)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("domain not found")
	}
	return nil
}

// UpdateScheme switches a domain's default scheme (http/https).
func (r *FrontierRepo) UpdateScheme(ctx context.Context, host, scheme string) error {
	res := r.DB.W.WithContext(ctx).Model(&Domain{}).Where("host = ?", host).
		Update("scheme", scheme)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("domain not found")
	}
	return nil
}

// UpdateRequiredCapability sets the per-domain worker-binding hint. Workers
// must have this capability to reserve URLs of this domain. Empty string
// clears the binding (any crawl-capable worker can then reserve).
func (r *FrontierRepo) UpdateRequiredCapability(ctx context.Context, host, capability string) error {
	var val interface{}
	if capability == "" {
		val = nil
	} else {
		val = capability
	}
	res := r.DB.W.WithContext(ctx).Model(&Domain{}).Where("host = ?", host).
		Update("required_capability", val)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("domain not found")
	}
	return nil
}

// UpdateEmbedCollection sets the per-domain vector-store collection hint.
// Empty string clears the override (chunks fall back to host name).
func (r *FrontierRepo) UpdateEmbedCollection(ctx context.Context, host, collection string) error {
	var val interface{}
	if collection == "" {
		val = nil
	} else {
		val = collection
	}
	res := r.DB.W.WithContext(ctx).Model(&Domain{}).Where("host = ?", host).
		Update("embed_collection", val)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("domain not found")
	}
	return nil
}

// DomainIDByURLHash returns the domain_id stored on a crawl_frontier row.
func (r *FrontierRepo) DomainIDByURLHash(ctx context.Context, urlHash []byte) (int64, error) {
	var m Frontier
	err := r.DB.R.WithContext(ctx).Select("domain_id").Where("url_hash = ?", urlHash).First(&m).Error
	if err != nil {
		return 0, err
	}
	return m.DomainID, nil
}

// SetActive toggles whether a domain participates in reserves and discovery.
func (r *FrontierRepo) SetActive(ctx context.Context, host string, active bool) error {
	res := r.DB.W.WithContext(ctx).Model(&Domain{}).Where("host = ?", host).
		Update("is_active", active)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("domain not found")
	}
	return nil
}

// --- Repository -----------------------------------------------------------

// Enqueue inserts a frontier row idempotently keyed by url_hash.
func (r *FrontierRepo) Enqueue(ctx context.Context, j frontier.Job) (bool, error) {
	m := Frontier{
		URLHash:       j.URLHash,
		URL:           j.URL,
		CanonicalURL:  j.CanonicalURL,
		DomainID:      j.DomainID,
		Depth:         j.Depth,
		Priority:      j.Priority,
		Status:        string(frontier.StatusQueued),
		MaxAttempts:   j.MaxAttempts,
		ScheduledFor:  time.Now().UTC(),
		DiscoveredAt:  time.Now().UTC(),
		ParentURLHash: j.ParentURLHash,
	}
	if m.MaxAttempts == 0 {
		m.MaxAttempts = 5
	}
	res := r.DB.W.WithContext(ctx).Where("url_hash = ?", j.URLHash).
		Attrs(m).FirstOrCreate(&Frontier{})
	if res.Error != nil {
		return false, res.Error
	}
	return res.RowsAffected == 1, nil
}

// EnqueueMany inserts up to len(jobs) frontier rows in a single WriteTX,
// skipping duplicates keyed by url_hash. Holding the SQLite writer lock once
// per batch avoids the SQLITE_BUSY_SNAPSHOT (517) contention that loses URLs
// when callers loop one-Enqueue-per-URL under concurrent result POSTs.
func (r *FrontierRepo) EnqueueMany(ctx context.Context, jobs []frontier.Job) (int64, error) {
	if len(jobs) == 0 {
		return 0, nil
	}
	now := time.Now().UTC()
	var inserted int64
	err := r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		for _, j := range jobs {
			m := Frontier{
				URLHash:       j.URLHash,
				URL:           j.URL,
				CanonicalURL:  j.CanonicalURL,
				DomainID:      j.DomainID,
				Depth:         j.Depth,
				Priority:      j.Priority,
				Status:        string(frontier.StatusQueued),
				MaxAttempts:   j.MaxAttempts,
				ScheduledFor:  now,
				DiscoveredAt:  now,
				ParentURLHash: j.ParentURLHash,
			}
			if m.MaxAttempts == 0 {
				m.MaxAttempts = 5
			}
			res := tx.Where("url_hash = ?", j.URLHash).Attrs(m).FirstOrCreate(&Frontier{})
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 1 {
				inserted++
			}
		}
		return nil
	})
	return inserted, err
}

// LookupCanonical returns whether a row with this url_hash exists.
func (r *FrontierRepo) LookupCanonical(ctx context.Context, urlHash []byte) (bool, error) {
	var n int64
	if err := r.DB.R.WithContext(ctx).Model(&Frontier{}).Where("url_hash = ?", urlHash).Count(&n).Error; err != nil {
		return false, err
	}
	return n > 0, nil
}

// Reserve picks up to batch eligible jobs, one per domain, and leases them.
// All work runs in a single WriteTX for cross-engine safety.
func (r *FrontierRepo) Reserve(
	ctx context.Context,
	workerID int64,
	capabilities []string,
	batch int,
	leaseTTL time.Duration,
	signLease func(urlHash []byte, expires time.Time) (string, []byte),
) ([]frontier.LeasedJob, error) {
	if batch <= 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	expires := now.Add(leaseTTL)

	var leased []frontier.LeasedJob

	politenessExpr := politenessSQL(r.DB.Driver)

	// Build the optional capability filter. Empty capabilities = "any domain"
	// (backward-compat with slice 1-11 workers without explicit caps).
	capFilter := ""
	args := []interface{}{now, now}
	if len(capabilities) > 0 {
		capFilter = ` AND (d.required_capability IS NULL OR d.required_capability = '' OR d.required_capability IN ?)`
		args = append(args, capabilities)
	}
	args = append(args, batch)

	err := r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		// rn <= pf lets cooperative hosts (parallel_fetches > 1) ship multiple
		// URLs per reserve call. Default parallel_fetches=1 preserves classic
		// one-at-a-time politeness. crawl_delay_ms still gates the gap between
		// successive reserves of the same domain via politenessExpr.
		pickSQL := fmt.Sprintf(`
SELECT url_hash, domain_id FROM (
  SELECT cf.url_hash, cf.domain_id, d.parallel_fetches AS pf,
         ROW_NUMBER() OVER (PARTITION BY cf.domain_id ORDER BY cf.priority DESC, cf.scheduled_for ASC) AS rn
  FROM crawl_frontier cf
  JOIN domains d ON d.id = cf.domain_id
  WHERE cf.status = 'queued'
    AND cf.scheduled_for <= ?
    AND (cf.next_retry_at IS NULL OR cf.next_retry_at <= ?)
    AND d.is_active = 1
    AND %s%s
) ranked WHERE rn <= pf LIMIT ?`, politenessExpr, capFilter)

		type pickRow struct {
			URLHash  []byte `gorm:"column:url_hash"`
			DomainID int64  `gorm:"column:domain_id"`
		}
		var picks []pickRow
		if err := tx.Raw(pickSQL, args...).Scan(&picks).Error; err != nil {
			return fmt.Errorf("reserve: pick: %w", err)
		}
		if len(picks) == 0 {
			return nil
		}

		touched := make(map[int64]bool, len(picks))
		for _, p := range picks {
			tok, raw := signLease(p.URLHash, expires)
			res := tx.Exec(`
UPDATE crawl_frontier
   SET status = 'leased',
       leased_by_worker_id = ?,
       lease_token = ?,
       lease_expires_at = ?,
       attempt_count = attempt_count + 1
 WHERE url_hash = ? AND status = 'queued'`,
				workerID, raw, expires, p.URLHash)
			if res.Error != nil {
				return fmt.Errorf("reserve: lease update: %w", res.Error)
			}
			if res.RowsAffected == 0 {
				continue
			}
			if !touched[p.DomainID] {
				if err := tx.Exec(`UPDATE domains SET last_request_at = ? WHERE id = ?`, now, p.DomainID).Error; err != nil {
					return fmt.Errorf("reserve: touch domain: %w", err)
				}
				touched[p.DomainID] = true
			}
			var m Frontier
			if err := tx.Where("url_hash = ?", p.URLHash).First(&m).Error; err != nil {
				return fmt.Errorf("reserve: refetch: %w", err)
			}
			leased = append(leased, frontier.LeasedJob{
				Job: frontier.Job{
					URLHash:       m.URLHash,
					URL:           m.URL,
					CanonicalURL:  m.CanonicalURL,
					DomainID:      m.DomainID,
					Depth:         m.Depth,
					Priority:      m.Priority,
					AttemptCount:  m.AttemptCount,
					MaxAttempts:   m.MaxAttempts,
					ParentURLHash: m.ParentURLHash,
				},
				Lease: frontier.Lease{
					JobURLHash: m.URLHash,
					WorkerID:   workerID,
					Token:      tok,
					ExpiresAt:  expires,
				},
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return leased, nil
}

// Heartbeat extends the lease if the token matches.
func (r *FrontierRepo) Heartbeat(ctx context.Context, urlHash, leaseToken []byte, extend time.Duration) (time.Time, error) {
	newExpiry := time.Now().UTC().Add(extend)
	res := r.DB.W.WithContext(ctx).Exec(`
UPDATE crawl_frontier
   SET lease_expires_at = ?
 WHERE url_hash = ? AND status = 'leased' AND lease_token = ?`,
		newExpiry, urlHash, leaseToken)
	if res.Error != nil {
		return time.Time{}, res.Error
	}
	if res.RowsAffected == 0 {
		return time.Time{}, errors.New("heartbeat: lease not held")
	}
	return newExpiry, nil
}

// Complete marks the row done.
func (r *FrontierRepo) Complete(ctx context.Context, urlHash, leaseToken []byte, httpStatus int) error {
	now := time.Now().UTC()
	res := r.DB.W.WithContext(ctx).Exec(`
UPDATE crawl_frontier
   SET status = 'done',
       http_status = ?,
       completed_at = ?,
       lease_token = NULL,
       lease_expires_at = NULL,
       leased_by_worker_id = NULL
 WHERE url_hash = ? AND status = 'leased' AND lease_token = ?`,
		httpStatus, now, urlHash, leaseToken)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("complete: lease not held")
	}
	return nil
}

// Fail records a failure inside a write transaction (read+update).
func (r *FrontierRepo) Fail(ctx context.Context, urlHash, leaseToken []byte, f frontier.Failure, backoff time.Duration) error {
	return r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		var m Frontier
		if err := tx.Where("url_hash = ?", urlHash).First(&m).Error; err != nil {
			return err
		}
		if len(m.LeaseToken) == 0 || !equalBytes(m.LeaseToken, leaseToken) {
			return errors.New("fail: lease not held")
		}
		newStatus := string(frontier.StatusQueued)
		var nextRetry *time.Time
		if !f.Retryable || m.AttemptCount >= m.MaxAttempts {
			newStatus = string(frontier.StatusDead)
		} else {
			t := time.Now().UTC().Add(backoff)
			nextRetry = &t
		}
		hs := f.HTTPStatus
		ec := f.ErrorCode
		em := f.ErrorMessage
		updates := map[string]interface{}{
			"status":              newStatus,
			"http_status":         &hs,
			"error_code":          &ec,
			"error_message":       &em,
			"next_retry_at":       nextRetry,
			"lease_token":         nil,
			"lease_expires_at":    nil,
			"leased_by_worker_id": nil,
		}
		return tx.Model(&Frontier{}).Where("url_hash = ?", urlHash).Updates(updates).Error
	})
}

// RequeueByFilter flips matching crawl_frontier rows back to 'queued' and
// clears their lease columns. All fields AND-ed. Empty Status means no
// status constraint; the CLI is responsible for requiring at least one
// filter to avoid mass-requeue accidents.
func (r *FrontierRepo) RequeueByFilter(ctx context.Context, f frontier.RequeueFilter) (int64, error) {
	q := r.DB.W.WithContext(ctx).Model(&Frontier{})
	if f.Status != "" {
		q = q.Where("status = ?", string(f.Status))
	}
	if f.WorkerID > 0 {
		q = q.Where("leased_by_worker_id = ?", f.WorkerID)
	}
	if f.DomainID > 0 {
		q = q.Where("domain_id = ?", f.DomainID)
	}
	res := q.Updates(map[string]interface{}{
		"status":              string(frontier.StatusQueued),
		"lease_token":         nil,
		"leased_by_worker_id": nil,
		"lease_expires_at":    nil,
		"next_retry_at":       nil,
	})
	return res.RowsAffected, res.Error
}

// StatusCounts returns a {status → count} histogram of the frontier.
func (r *FrontierRepo) StatusCounts(ctx context.Context) (map[string]int64, error) {
	type row struct {
		Status string `gorm:"column:s"`
		N      int64  `gorm:"column:n"`
	}
	var rows []row
	if err := r.DB.R.WithContext(ctx).Raw(`
SELECT status AS s, COUNT(*) AS n FROM crawl_frontier GROUP BY status`).Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		out[r.Status] = r.N
	}
	return out, nil
}

// SweepExpired re-queues rows whose lease expired.
func (r *FrontierRepo) SweepExpired(ctx context.Context, now time.Time) (int64, error) {
	res := r.DB.W.WithContext(ctx).Exec(`
UPDATE crawl_frontier
   SET status = 'queued',
       lease_token = NULL,
       lease_expires_at = NULL,
       leased_by_worker_id = NULL
 WHERE status = 'leased' AND lease_expires_at < ?`, now)
	return res.RowsAffected, res.Error
}

// politenessSQL returns the dialect-specific elapsed-ms expression.
func politenessSQL(d rwdb.Driver) string {
	switch d {
	case rwdb.DriverPostgres:
		return "(d.last_request_at IS NULL OR EXTRACT(EPOCH FROM (NOW() - d.last_request_at))*1000 >= d.crawl_delay_ms)"
	case rwdb.DriverMySQL:
		return "(d.last_request_at IS NULL OR TIMESTAMPDIFF(MICROSECOND, d.last_request_at, NOW())/1000 >= d.crawl_delay_ms)"
	default: // sqlite
		return "(d.last_request_at IS NULL OR (strftime('%s','now') - strftime('%s', d.last_request_at)) * 1000 >= d.crawl_delay_ms)"
	}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var v byte
	for i := range a {
		v |= a[i] ^ b[i]
	}
	return v == 0
}
