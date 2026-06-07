---
tldr: per-site worker for liteko.teismai.lt — seeds per-day search URLs and drives ASP.NET WebForms __doPostBack pagination over a plain HTTP client, emitting case-detail URLs as discovered_links
category: core
---

# litekoworker

## Target

`cmd/litekoworker/` — a single-host worker bound to `liteko.teismai.lt`. Two subcommands: `seed` (direct DB enqueue of search URLs) and `run` (registry-backed reserve/result loop with WebForms pagination inside one job).

## Behaviour

- A `seed` invocation accepts a date window `[from, to]` and produces one frontier job per calendar day in the range, idempotently — re-running with an overlapping window adds only the new days.
- The `liteko.teismai.lt` domain row is upserted as a side effect of seeding, so a fresh registry DB can be made Liteko-ready in one command.
- A `run` invocation reserves jobs from the registry, fetches each URL, and posts a result whose blob is page 1's bytes and whose `discovered_links` cover every case across every result page for that day.
- A single listing job, regardless of how many cases the day contains, surfaces as exactly one frontier→lake row plus N discovered detail URLs — not N listing rows.
- Detail URLs (case-text pages) are handled as ordinary GETs with no pagination, with no discovered_links emitted from them.
- Pagination failure mid-walk is non-fatal: page-1 bytes and whatever cases were collected up to the failure are still posted as a successful result; the operator sees the warning in logs but the day's first 50 cases are never lost to a transient pager error.
- A network or HTTP error on the initial GET fails the job as retryable, returning it to the queue under the standard sweeper rules.
- Concurrency is bounded per process: a reserved batch is drained by a fixed pool of goroutines; idle reserves back off by a configurable sleep.
- Graceful shutdown on SIGINT/SIGTERM drains in-flight jobs before exit.

## Design

### Site-specific worker, not a generic capability

Liteko's listing protocol — a `RadDataPager` driven by `__doPostBack` POSTs that echo `__VIEWSTATE` / `__VIEWSTATEGENERATOR` back — cannot be expressed as "one URL, one GET", which is the assumption baked into the reference `worker`. Rather than teach the generic worker about WebForms, the decision is to ship a dedicated binary that owns the protocol. The site URL shape, the `RadDataPager` ctl-suffix sequence, and the result-page size are encoded as constants {>> `litekoHost`, `BaseURL`, `resultsPerPage`, `caseRowLabel` in listing.go/seed.go <<} because they describe one site, not a family.

### Day-sliced seeding to dodge the result cap

Liteko silently truncates a search whose result count exceeds an internal cap, so a multi-year query loses the tail. The seed strategy is to issue one URL per calendar day with `nuo=YYYY-MM-DD 00:00:00&iki=YYYY-MM-DD 23:59:59`, keeping each listing well under the cap. Idempotence on re-seed comes from the frontier's URL-hash uniqueness {>> `frepo.Enqueue` returns ok=false on duplicate; counted as `dupes` <<} — no per-worker dedup state is needed.

### Pagination inside the job, not across jobs

A listing day is one frontier job. The worker walks every page of that day's results *within* `handleJob` and reports the union of detail URLs in a single result POST. This is a deliberate departure from the queue's normal "one URL = one row" model. Rationale: the VIEWSTATE chain is stateful and short-lived, and splitting the walk across reserved jobs would either require persisting VIEWSTATE in the frontier (leaking site-specific state into a generic schema) or refetching page 1 repeatedly (wasteful, and creates a thundering-herd problem for the site). Keeping the walk in-process trades worker memory for protocol cleanliness.

The page-count formula is `total / resultsPerPage` extra POSTs after the initial GET {>> matches the TS reference `i <= count/50` <<}. The `RadDataPager` button suffix wraps after the tenth page in a 10-element block {>> `pageButton` 1..10 → "01".."10", then "02".."11", etc. <<} — this is a protocol fact about RadDataPager, not a choice.

### Two URL classes, dispatched by substring

`isListing` is a substring check on `paieska.aspx`. Everything else is a detail GET. This deliberately avoids URL parsing and a routing table: there are exactly two shapes the worker will ever encounter on this host, and a substring is sufficient and obvious. If a third class appears, it gets its own branch — no premature abstraction.

### Best-effort pagination

A pager failure on page K does not unwind the result: page 1 bytes are already in hand and the cases collected so far are real. The worker logs the warning and proceeds to post {>> `walkListing` returns partial slice + error; caller logs and continues <<}. This trades completeness for liveness on flaky sessions; missing cases will be re-seeded on a future day-window run if the operator cares.

### Lease lifecycle deferred to the registry

The worker speaks the same `reserve → result/fail` shape as the reference worker — it carries the `lease_token` through and POSTs it back. Heartbeats are not implemented; a single listing walk is bounded by `fetch-timeout * (page-count)` plus `page-delay` between POSTs, sized to fit comfortably inside the default lease TTL. If a walk somehow exceeds it, the sweeper requeues and a fresh worker re-fetches page 1 — idempotent because the discovered detail URLs dedup at the frontier hash layer.

### Blob = page 1 only

Only page 1's bytes go into the result blob. Pages 2..N exist solely to extract their case URLs; their bodies are discarded after parsing. The lake stores a single "listing for day D" artifact, and the detail pages will be reaped as separate jobs from the discovered_links.

### Per-domain politeness deferred to the registry

`crawl-delay-ms` is set on the `domains` row at seed time {>> `frepo.UpsertByHost(..., crawl_delay_ms)` <<} — the registry's reserve SQL enforces it. `page-delay` is a *separate* knob, applied between POSTs of the same in-progress walk, because once a job is reserved the registry's politeness no longer applies. The two knobs together cover both "between jobs" and "within a job" pacing.

## Interactions

- **seed → registry DB (direct)** — opens `rwdb` with the same driver/DSN as the registry, calls `FrontierRepo.UpsertByHost` and `Enqueue`. Bypasses the HTTP API by design: seeding is an operator action on a known DB, not a worker action.
- **run → registry HTTP** — `/v1/jobs/reserve`, `/v1/jobs/result` (multipart: meta + blob), `/v1/jobs/fail`. PAT in `Authorization: Bearer`.
- **run → liteko.teismai.lt** — GET for the initial listing or any detail, POST `application/x-www-form-urlencoded` for each pager page.
- **discovered_links → frontier** — the registry enqueues detail URLs at depth+1 under the standard scope-lock rules; since the host matches the seeded domain, they stick.
- **Capabilities** — operator must bind this worker to the Liteko domain (e.g. `domain:liteko.teismai.lt` on the PAT, with `required_capability` on the domain row) if a mixed fleet is used; otherwise a generic worker would reserve a `paieska.aspx` URL and only ever capture page 1.

## Mapping

> [[cmd/litekoworker/main.go]]
> [[cmd/litekoworker/seed.go]]
> [[cmd/litekoworker/run.go]]
> [[cmd/litekoworker/listing.go]]
> [[cmd/litekoworker/listing_test.go]]
> [[internal/domain/frontier/]]
> [[internal/infra/db/gormrepo/]]
> [[internal/infra/db/rwdb/]]
> [[internal/infra/urls/]]
