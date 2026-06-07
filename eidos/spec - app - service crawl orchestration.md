---
tldr: application facade for the crawl queue — reserve/heartbeat/result/fail with scope-locked discovered-link intake
category: core
---

# crawl service

## Target

`internal/app/service.go` — the application-layer `Service` struct that orchestrates the crawl frontier slice end-to-end. Wraps the frontier, lake, blob store, worker, and lease ports into a four-verb facade (Reserve / Heartbeat / Result / Fail) plus a periodic sweeper hook. Pipeline and trigger dispatcher are optional collaborators wired in after construction.

## Behaviour

- A worker can reserve a batch of crawl jobs and get back leased rows plus signed tokens; the batch size has a default if the caller asks for zero or fewer.
- Reserve authorization is driven by the **server-stored** capability set passed by the caller, never by anything in the request body — workers cannot escalate by relabelling themselves.
- Reserve honours per-domain `required_capability` so that domain-bound work is only handed to workers carrying that capability.
- Heartbeat extends the lease of a single job for one increment; an unverifiable or stale token surfaces as an error and changes no state.
- Posting a result for a verified lease is observable as: the body is durably stored, a lake row exists for it, the frontier row is marked done with the worker's reported HTTP status, and any routing side-effects fire.
- A claimed content hash that is the wrong length is rejected up front before any storage IO; an empty claimed hash is allowed and the server computes one.
- If any step of result acceptance (blob, lake, frontier complete) fails, the row is NOT marked done. The lease will expire and the sweeper will requeue — the system never silently swallows an integration failure.
- Posting a failure for a verified lease records the outcome and applies a retry backoff so the job is eligible to be retried later up to its attempt cap.
- Discovered links on a successful result are processed best-effort: they may all silently drop without failing the result the worker just submitted.
- Scope-lock is the default: a discovered link whose host is not present in the `domains` table is dropped. Operators can flip `AllowAutoDomains=true` to fall back to legacy auto-seeding, in which case unseen hosts are upserted with default politeness.
- Links pointing at a domain row marked inactive are dropped, even if `AllowAutoDomains=true`.
- A configured `MaxDepth > 0` globally caps recursion: links whose new depth exceeds it are dropped at intake.
- The discovered URL is canonicalized before scoping and hashing, so equivalent URLs collapse to one frontier row regardless of trailing slashes / fragments / casing.
- A periodic sweep call reclaims any leases whose TTL has elapsed and reports how many rows were affected.
- Routing is pluggable at runtime: with neither pipeline nor dispatcher wired, result acceptance still succeeds — nothing downstream gets enqueued. With a dispatcher wired, a `lake_object_inserted` event fires on every result, carrying the lake row id and content type for trigger matching.

## Design

### Four-verb facade over the frontier queue
The service exposes exactly the uniform queue shape called out at system level — reserve, heartbeat, result, fail — plus a sweeper hook. There is no leasing API, no "list jobs", no admin verbs. Anything richer is a separate use case or an HTTP handler concern. See system spec for the three-queue uniform shape.

### Lease verify-first, side-effects after
Every state-changing verb (heartbeat / result / fail) verifies the HMAC lease token before touching repositories. {>> `s.Lease.Verify` returns the urlHash bound at sign time; that hash is the row key for all downstream calls — workers cannot redirect a result onto a different row.} An unverifiable token is a hard error with no DB writes. The raw token bytes are then forwarded so the frontier repo can do its own stored-token comparison as defense in depth (see system spec).

### Authorization input contract
`ReserveJobs` takes capabilities as a typed field on the request struct, but the docstring and call-site contract require it be the PAT-resolved server-stored set. The service itself does not re-fetch the worker; that's the HTTP boundary's job. This keeps the application layer free of auth I/O while still routing the right caps into the SQL filter.

### Result acceptance is a strict sequence, fail-fast
Order is: stream-store the body → insert lake row → complete frontier row → fire routing. If any step before `Complete` errors, the frontier row stays leased; the sweeper handles it. This matches the system-wide "fail-fast on integrations" rule. {>> Routing fires only after `Complete` succeeds, so a downstream processor never sees a lake id whose frontier row is still leased.}

### Blob layout encodes a sharding decision
`storageKey` shards blobs into 256 first-byte directories before the full hex hash, so a flat directory never grows past one batch's worth of objects. {>> `<firstHexByte>/<fullHex>.<ext>` — the prefix dir is the first byte of the urlHash in hex.} Extension is derived from content type so a human browsing the blob store can tell `.pdf` from `.html` without opening files; unknown types fall back to `.bin` rather than guessing.

### Scope-lock is a default, not a hardcoding
`AllowAutoDomains=false` in `Defaults()` enforces the system-level scope-lock rule. The escape hatch is a single bool flip — legacy "follow anything" still exists for operators that explicitly want it, but it's not the path of least resistance. Inactive domains are dropped even when auto-add is on, because deactivating a domain is the operator's signal to stop crawling it regardless of how new URLs arrived.

### Max-depth is intake-side, not reserve-side
Depth filtering happens at the moment a discovered link is considered, not when it's reserved. This means a depth bump in config does not retroactively delete already-enqueued deep rows — they will still be served — but no new ones will be created beyond the cap. Trade-off chosen for simplicity.

### Discovered-link intake is best-effort
The link-enqueue loop swallows per-link errors (canonicalize fail, parse fail, domain lookup fail, frontier enqueue fail) and continues. {>> The function always returns nil and the caller discards even that.} Rationale: a single malformed `<a href>` must not poison a worker's entire result post. The crawl has already happened; the body is already stored; losing one outlink is acceptable.

### Pipeline and dispatcher are post-construction collaborators
`Pipeline` and `Dispatcher` are nil-able fields with `SetPipeline` / `SetDispatcher` mutators rather than constructor args. This lets the registry wire them only when the relevant adapters are configured, and lets tests construct a bare service. {>> Only `Dispatcher` is consulted on result acceptance in current code; `Pipeline` is held for the case where the legacy hardcoded routing path is re-enabled. Both nil = no downstream routing, result still succeeds.}

### Config defaults are conservative
`Defaults()` picks: 10-minute lease, 60-second heartbeat extension, batch 10, 30-second base backoff capped at 24h, local blob backend, scope-lock on, no depth cap. Operators tune via flags; the code path is the same regardless of where the values came from.

## Interactions

- **Frontier port** — drives all four queue verbs and the sweep. The sign closure passed into `Reserve` is what binds an HMAC-signed token to each leased row.
- **Lake port + BlobStore port** — result acceptance writes a blob, then a row that points at it; the backend identifier on the row is whatever the blob store reports as its own name (so swapping FS→S3 changes the column value without code change).
- **DomainRepo port** — discovered-link intake reads (and, in auto-add mode, writes) domain rows for scope-lock enforcement.
- **Lease signer** — same signer signs new leases inside Reserve and verifies tokens on heartbeat/result/fail.
- **TriggerDispatcher** — optional. When wired, fires `lake_object_inserted` after every successful result; downstream processing-job enqueues are its concern, not the service's.
- **Pipeline** — optional, currently held but not invoked on result acceptance; routing has moved to declarative triggers (see system spec on `pipeline_triggers`).
- **HTTP handlers** (registry) — the only legitimate callers. Handlers do PAT auth, materialize the server-stored capability set, and invoke the four verbs; they do not bypass the service to touch repositories directly.
- **Registry sweeper goroutine** — calls `SweepExpiredLeases` on a tick; the service itself owns no goroutines.

## Mapping

> [[internal/app/service.go]]
> [[internal/domain/frontier]]
> [[internal/domain/lake]]
> [[internal/domain/triggers]]
> [[internal/domain/workerid]]
> [[internal/infra/lease]]
> [[internal/infra/urls]]
