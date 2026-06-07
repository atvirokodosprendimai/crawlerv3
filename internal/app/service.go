// Package app holds use-case orchestrators wiring domain ports together.
package app

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/frontier"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/triggers"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/lease"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/urls"
)

// Config holds tunables shared by all use cases.
type Config struct {
	LeaseTTL         time.Duration // default 10m (overridable via registry --lease-ttl / LEASE_TTL)
	HeartbeatExtend  time.Duration // default 60s (overridable via registry --heartbeat-extend / HEARTBEAT_EXTEND)
	DefaultBatch     int           // default 10
	DefaultBackoff   time.Duration // base for exponential backoff, default 30s
	MaxBackoff       time.Duration // cap, default 24h
	DefaultBackend   string        // "local" | "s3"
	AllowAutoDomains bool          // when false, discovered links to unseeded hosts are dropped
	MaxDepth         int           // 0 = unlimited; otherwise drop discovered links with new_depth > MaxDepth
}

// Defaults returns a populated Config with sane defaults.
// Crawl scope is intentionally conservative: the crawler stays on seeded hosts
// only. Operators that want unrestricted recursion can set AllowAutoDomains=true.
func Defaults() Config {
	return Config{
		LeaseTTL:         600 * time.Second,
		HeartbeatExtend:  60 * time.Second,
		DefaultBatch:     10,
		DefaultBackoff:   30 * time.Second,
		MaxBackoff:       24 * time.Hour,
		DefaultBackend:   "local",
		AllowAutoDomains: false,
		MaxDepth:         0,
	}
}

// Service is the application facade for crawl orchestration.
type Service struct {
	Cfg        Config
	Frontier   frontier.Repository
	Domains    frontier.DomainRepo
	Lake       lake.Repository
	Blobs      lake.BlobStore
	Workers    workerid.Repository
	Lease      *lease.Signer
	Pipeline   *Pipeline          // optional; runs internal processors (html_strip)
	Dispatcher *TriggerDispatcher // optional; replaces hardcoded routing
}

// New builds a Service. Pipeline is optional and may be wired separately via SetPipeline.
func New(cfg Config, f frontier.Repository, d frontier.DomainRepo, l lake.Repository, b lake.BlobStore, w workerid.Repository, s *lease.Signer) *Service {
	return &Service{Cfg: cfg, Frontier: f, Domains: d, Lake: l, Blobs: b, Workers: w, Lease: s}
}

// SetPipeline attaches a Pipeline so AcceptResult enqueues per-MIME processors.
func (s *Service) SetPipeline(p *Pipeline) { s.Pipeline = p }

// SetDispatcher attaches a TriggerDispatcher; when set, dispatcher drives
// routing instead of the legacy Pipeline.EnqueueFor path.
func (s *Service) SetDispatcher(d *TriggerDispatcher) { s.Dispatcher = d }

// ReserveJobs leases up to req.Batch jobs for the worker.
//
// req.Capabilities is the worker's server-stored capability set (NOT
// client-supplied) — the caller is expected to pass wk.Capabilities from the
// PAT-authenticated context. The slice gates per-domain required_capability
// filtering in the underlying SQL.
func (s *Service) ReserveJobs(ctx context.Context, req frontier.ReserveRequest) ([]frontier.LeasedJob, error) {
	if req.Batch <= 0 {
		req.Batch = s.Cfg.DefaultBatch
	}
	sign := func(urlHash []byte, expires time.Time) (string, []byte) {
		return s.Lease.Sign(urlHash, req.WorkerID, expires)
	}
	return s.Frontier.Reserve(ctx, req.WorkerID, req.Capabilities, req.Batch, s.Cfg.LeaseTTL, sign)
}

// Heartbeat extends the lease for a single job.
func (s *Service) Heartbeat(ctx context.Context, token string) (time.Time, error) {
	urlHash, _, _, err := s.Lease.Verify(token)
	if err != nil {
		return time.Time{}, fmt.Errorf("heartbeat: %w", err)
	}
	raw, _ := lease.Raw(token)
	return s.Frontier.Heartbeat(ctx, urlHash, raw, s.Cfg.HeartbeatExtend)
}

// ResultIngest is the worker's success payload.
type ResultIngest struct {
	LeaseToken      string
	HTTPStatus      int
	ContentType     string
	Body            io.Reader
	BodySize        int64
	ClaimedSHA256   []byte
	DiscoveredLinks []frontier.DiscoveredLink
}

// AcceptResult stores the blob, indexes it, marks the frontier row done,
// and enqueues discovered links.
func (s *Service) AcceptResult(ctx context.Context, in ResultIngest) (lakeID int64, err error) {
	urlHash, _, _, err := s.Lease.Verify(in.LeaseToken)
	if err != nil {
		return 0, fmt.Errorf("result: %w", err)
	}
	raw, _ := lease.Raw(in.LeaseToken)

	// Stream blob to the configured store while computing sha256.
	if len(in.ClaimedSHA256) > 0 && len(in.ClaimedSHA256) != sha256.Size {
		return 0, errors.New("result: bad claimed sha")
	}
	key := storageKey(urlHash, in.ContentType)
	stat, err := s.Blobs.Put(ctx, key, in.Body, lake.PutMeta{ContentType: in.ContentType, SHA256: in.ClaimedSHA256})
	if err != nil {
		return 0, fmt.Errorf("result: blob put: %w", err)
	}

	id, err := s.Lake.Insert(ctx, lake.Object{
		URLHash:        urlHash,
		StorageBackend: s.Blobs.Backend(),
		StorageKey:     key,
		ContentType:    in.ContentType,
		ContentSHA256:  stat.SHA256,
		FileSize:       stat.Size,
	})
	if err != nil {
		return 0, fmt.Errorf("result: lake insert: %w", err)
	}

	if err := s.Frontier.Complete(ctx, urlHash, raw, in.HTTPStatus); err != nil {
		return 0, fmt.Errorf("result: complete: %w", err)
	}

	// Routing: declarative triggers fire processing jobs for matching events.
	if s.Dispatcher != nil {
		s.Dispatcher.Fire(ctx, triggers.EvtLakeObjectInserted, EventPayload{
			LakeObjectID: id, ContentType: in.ContentType,
		})
	}

	// Enqueue discovered links. The listing row is already marked done — we do
	// not fail the result if a few links can't be saved, but we no longer drop
	// the error on the floor: it goes to the request log so a regression in the
	// enqueue path stops being silent.
	if n, err := s.enqueueDiscovered(ctx, urlHash, in.DiscoveredLinks); err != nil {
		slog.WarnContext(ctx, "discovered_links partial",
			"err", err, "received", len(in.DiscoveredLinks), "inserted", n)
	}
	return id, nil
}

// AcceptFailure records a non-success outcome with retry/backoff handling.
func (s *Service) AcceptFailure(ctx context.Context, token string, f frontier.Failure) error {
	urlHash, _, _, err := s.Lease.Verify(token)
	if err != nil {
		return fmt.Errorf("fail: %w", err)
	}
	raw, _ := lease.Raw(token)
	backoff := s.Cfg.DefaultBackoff
	return s.Frontier.Fail(ctx, urlHash, raw, f, backoff)
}

// SweepExpiredLeases re-queues stuck jobs. Call periodically.
func (s *Service) SweepExpiredLeases(ctx context.Context) (int64, error) {
	return s.Frontier.SweepExpired(ctx, time.Now().UTC())
}

// enqueueDiscovered canonicalizes hrefs and writes them to the frontier in a
// single batch. Returns the count actually inserted (duplicates by url_hash
// are silently skipped) and the first persistence error.
//
// Default scope rules:
//   - Drop links whose host is not in the domains table (Cfg.AllowAutoDomains=false).
//   - Drop links whose domain row has is_active=false.
//   - Drop links whose new_depth exceeds Cfg.MaxDepth (when MaxDepth > 0).
//
// Set Cfg.AllowAutoDomains=true to fall back to the legacy "auto-add any host
// the crawler discovers" behavior (rarely desired in production).
//
// Batching: every accepted link becomes one Job; the whole slice is handed to
// frontier.EnqueueMany so the SQLite writer lock is acquired once. The earlier
// per-URL loop racked up N implicit transactions and lost rows to
// SQLITE_BUSY_SNAPSHOT (517) under concurrent result POSTs.
func (s *Service) enqueueDiscovered(ctx context.Context, parentHash []byte, links []frontier.DiscoveredLink) (int64, error) {
	jobs := make([]frontier.Job, 0, len(links))
	for _, l := range links {
		if s.Cfg.MaxDepth > 0 && l.NewDepth > s.Cfg.MaxDepth {
			continue
		}
		canon, err := urls.Canonical(l.URL)
		if err != nil {
			continue
		}
		u, err := url.Parse(canon)
		if err != nil || u.Host == "" {
			continue
		}
		dom, err := s.Domains.FindByHost(ctx, u.Host)
		if err != nil {
			continue
		}
		if dom == nil {
			if !s.Cfg.AllowAutoDomains {
				continue // external link, scope-locked
			}
			created, err := s.Domains.UpsertByHost(ctx, u.Host, u.Scheme, 1000)
			if err != nil {
				continue
			}
			dom = &created
		}
		if !dom.IsActive {
			continue
		}
		jobs = append(jobs, frontier.Job{
			URLHash:       urls.Hash(canon),
			URL:           l.URL,
			CanonicalURL:  canon,
			DomainID:      dom.ID,
			Depth:         l.NewDepth,
			Priority:      0,
			MaxAttempts:   5,
			ParentURLHash: parentHash,
		})
	}
	return s.Frontier.EnqueueMany(ctx, jobs)
}

// storageKey lays out blobs as: <hashPrefix>/<urlHash>.<ext>
// hashPrefix shards across 256 dirs to avoid huge directories.
func storageKey(urlHash []byte, contentType string) string {
	hexBuf := make([]byte, 2)
	const hexd = "0123456789abcdef"
	hexBuf[0] = hexd[urlHash[0]>>4]
	hexBuf[1] = hexd[urlHash[0]&0x0f]
	full := make([]byte, len(urlHash)*2)
	for i, b := range urlHash {
		full[i*2] = hexd[b>>4]
		full[i*2+1] = hexd[b&0x0f]
	}
	ext := extFor(contentType)
	return path.Join(string(hexBuf), string(full)+ext)
}

func extFor(ct string) string {
	ct = strings.ToLower(strings.TrimSpace(ct))
	if i := strings.Index(ct, ";"); i > 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case "text/html":
		return ".html"
	case "application/pdf":
		return ".pdf"
	case "application/json":
		return ".json"
	case "application/xml", "text/xml":
		return ".xml"
	case "text/plain":
		return ".txt"
	}
	return ".bin"
}
