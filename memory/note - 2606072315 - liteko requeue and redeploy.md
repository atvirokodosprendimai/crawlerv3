---
title: liteko — requeue and redeploy after enqueue-contention fix
created: 2026-06-07 23:15
fix_commit: 4de0ef8
plan: [[plan - 2606072235 - litekoworker pagination - close the 22M gap]]
---

## Root cause (settled)

`internal/app/service.go` had `_, _ = s.Frontier.Enqueue(ctx, ...)` inside a per-URL loop in `enqueueDiscovered`. Each call was its own implicit gorm transaction on the SQLite writer pool (size 1). Under litekoworker's `--batch 32 --concurrency 32`, concurrent result POSTs racked up hundreds of writer transactions, hit `SQLITE_BUSY_SNAPSHOT (517)`, and the error was discarded — silently dropping URLs.

Distribution from prod (`ubuntu@dc-head:crawler.db`):
- 7,827 listings done, 0 failed.
- 128,504 detail URLs total — 16.4 emitted per listing on average.
- For 2024-03-15 (real `Total=198`): only **11 of 50** page-1 case UUIDs reached the frontier; the other 39 are absent everywhere in `crawl_frontier`.
- 4,095 listings emit 1–20 detail URLs (high contention), 353 emit 51+ (low contention) — classic write-lock-victim distribution.

## Fix (commit `4de0ef8`)

1. New port method `frontier.Repository.EnqueueMany([]Job) (int64, error)`.
2. Implementation in `gormrepo.FrontierRepo.EnqueueMany` wraps the whole batch in a single `WriteTX` — one writer-lock acquisition for the whole result POST.
3. `Service.enqueueDiscovered` returns `(inserted, err)` and the caller (`AcceptResult`) emits `slog.Warn` if `inserted < len(received)` or `err != nil`.

## Deploy

Build on the prod host and replace the registry binary. The registry must restart for the change to take effect.

```bash
ssh ubuntu@dc-head
cd /home/ubuntu     # or wherever the source lives
git pull
go build -o registry.new ./cmd/registry
# Stop the running registry (the worker will get 5xx from reserve — it's fine,
# litekoworker re-tries on next idle-sleep)
sudo systemctl stop registry || pkill registry || screen -S registry -X quit
mv registry registry.bak
mv registry.new registry
./registry serve --addr :80 ... &   # or systemctl start
```

## Requeue the 7,827 broken listings

Listings have `lake_objects` rows; re-fetch will hit the `UNIQUE(content_sha256, url_hash)` constraint on insert. Delete those rows first, then flip listings back to `queued`.

```sql
BEGIN;

-- 1. drop lake objects for the broken listings (page-1 HTML is re-fetchable)
DELETE FROM lake_objects
WHERE url_hash IN (
  SELECT url_hash
  FROM crawl_frontier
  WHERE domain_id = 2
    AND canonical_url LIKE '%paieska.aspx%'
    AND status = 'done'
);

-- 2. requeue
UPDATE crawl_frontier
SET status              = 'queued',
    leased_by_worker_id = NULL,
    lease_token         = NULL,
    lease_expires_at    = NULL,
    completed_at        = NULL,
    attempt_count       = 0,
    next_retry_at       = NULL,
    http_status         = NULL,
    error_code          = NULL
WHERE domain_id = 2
  AND canonical_url LIKE '%paieska.aspx%'
  AND status = 'done';

-- 3. verify (expect: queued=7827, done=0 for listings)
SELECT
  CASE WHEN canonical_url LIKE '%paieska.aspx%' THEN 'listing' ELSE 'detail' END AS kind,
  status,
  COUNT(*) AS n
FROM crawl_frontier
WHERE domain_id = 2
GROUP BY kind, status
ORDER BY kind, status;

COMMIT;
```

Run with:

```bash
ssh ubuntu@dc-head 'sqlite3 crawler.db < requeue.sql'
```

## Re-launch litekoworker

Keep concurrency conservative — even with the batched enqueue, 32 parallel result POSTs each holding the writer for a few ms can still queue up. Start at 8, observe.

```bash
./litekoworker run \
  --registry http://localhost \
  --pat $PAT \
  --batch 8 \
  --concurrency 8 \
  --idle-sleep 1s
```

If reserves stop reporting `database is locked (517)` after a few minutes of running, raise concurrency. The detail-fetch phase (which is the long tail) can run wider — those don't generate discovered_links.

## Verification gate

After the requeue + redrain completes, on prod:

```sql
SELECT
  done_listings,
  detail_urls,
  CAST(detail_urls AS REAL) / NULLIF(done_listings, 0) AS per_listing_avg
FROM (
  SELECT
    (SELECT COUNT(*) FROM crawl_frontier WHERE domain_id=2 AND canonical_url LIKE '%paieska.aspx%' AND status='done') AS done_listings,
    (SELECT COUNT(*) FROM crawl_frontier WHERE domain_id=2 AND canonical_url LIKE '%tekstas.aspx%') AS detail_urls
);
```

Acceptance: `per_listing_avg` should rise from 16.4 toward ~200–400 (matches real per-day case volume). Total detail URLs should reach ~1.5–2.2M when all 7,827 listings are redrained.

If the average is still ≤50, the page-1-only signature, then pagination is actually broken under load (which contradicts my postback-driver evidence, but should be re-checked then). If average is 200–400, we're done.

## Open questions still

- 2,220 listings emitted **zero** discovered_links in the original run. After requeue, do they all emit normally? If a subset persistently emits zero, that's a separate bug.
- The reserve path also showed `database is locked (517)` in worker logs. Reserve already uses `WriteTX`, but the error returns 500 to the worker rather than retrying internally. Worth a follow-up: small backoff-retry on `SQLITE_BUSY_*` inside `Reserve`.
