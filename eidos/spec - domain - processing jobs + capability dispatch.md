---
tldr: processing_jobs port — every pipeline stage is a row keyed by a processor string, leased and dispatched the same way regardless of what the stage actually does
category: core
---

# processing domain

## Target

`internal/domain/processing/` — the port definitions and pure types that describe one row of `processing_jobs`. Covers `Job`, `Processor` constants, `Status` lifecycle, `Repository` interface (both the internal-worker `Claim` path and the external-worker lease path), and the catalog-only `Capability` / `CapabilityRepo` types.

This package is purely declarative. It contains no adapters, no SQL, no HTTP — only the contract that `internal/app` use cases compose against and `internal/infra/gorm/...` implements.

## Behaviour

- One asset can move through many pipeline stages, and each stage is one row. A row is uniquely identified by `(lake_object_id, processor)` at the conceptual level — the same blob can be html-stripped and chunked, but it is not html-stripped twice.
- The processor field is a **string**, not a sum type. New stages (`pdf_ocr`, `office_to_pdf`, future kinds nobody has named yet) are added by inserting rows that name them; no domain code needs to know the new kind exists for the queue to carry it.
- A queued row eventually reaches one of three terminal states: `done`, `failed`, or `skipped`. `running` is a transient lease state — a row stuck in `running` past its lease is reclaimed and returned to `queued` by the system-level sweeper, not by anything in this package.
- The bulk-enqueue surface accepts either an explicit list of lake IDs or a backfill filter (content-type + `since_id` + limit). Both produce the same row shape; backfill is just a convenience for re-driving an existing corpus through a new stage.
- Reservation is batched and lease-token-signed. A worker asks for up to N rows of one-or-more processor kinds and gets back leased jobs plus enough source-blob metadata (content-type, size) to fetch the input without a second round-trip to the registry.
- Heartbeat extends an existing lease; complete and fail both close it. All three require the original lease-token bytes — the caller cannot release a row they did not lease.
- Fail carries a `retryable` flag. Non-retryable failure is terminal even if `attempt_count < max_attempts`; retryable failure returns the row to `queued` with the attempt counter incremented, until `max_attempts` is reached.
- A separate operator-driven requeue path can flip rows back to `queued` selected by status / worker / processor. This is for manual recovery, not the normal flow.
- The `Capability` catalog is **descriptive, not prescriptive**. Listing a row in the catalog does not authorize a worker to claim that kind, and absence does not deny it. Authorization for processing rows lives elsewhere (worker PAT capability set, declarative `pipeline_triggers`); the catalog only powers operator discoverability (`list-capabilities`).

## Design

### Processor = opaque string, with named constants for the known set

`Processor` is `type Processor string`. The package ships constants for the stages the registry ships with (`html_strip`, `pdf_ocr`, `office_to_pdf`, `text_passthrough`, `chunk`, plus legacy `docx_to_pdf`). The constants exist so handler code can reference a stable identifier and so the catalog has something to seed — they are not a closed set.

A new external processor binary that declares `weird_proprietary_thing` as its capability and a matching `pipeline_triggers` row is sufficient to make the queue carry `weird_proprietary_thing` rows end-to-end. Nothing in this package needs to know.

{>> The legacy `ProcDOCXToPDF = "docx_to_pdf"` constant is kept alongside `ProcOfficeToPDF = "office_to_pdf"` deliberately — old rows in production still carry the legacy string, and the constant documents that fact rather than silently rewriting history.}

### Status is a closed lifecycle, with one transient state

Five statuses, exactly one of which (`running`) is transient and lease-bound. Two of them (`done`, `skipped`) are success-shaped terminals; the difference is intent — `skipped` records "we looked at this and decided no work was needed" (e.g. content-type didn't match what the processor actually handles), so it stays out of the failure metrics.

### Two reservation paths for two worker tiers

The `Repository` exposes both:

- **Internal claim** — `ClaimNext(p)` returns one job for the in-process pipeline goroutine. No lease, no token. The goroutine is in the registry's address space; if it crashes, the registry crashes, and recovery is "restart the registry, sweeper reclaims `running` rows on next tick."
- **External lease** — `ReserveBatch` / `Heartbeat` / `Complete` / `Fail`. This is the three-queue uniform shape (see system spec). Lease tokens are minted by the caller (a `signLease` callback injected by the app layer) so the domain port stays oblivious to HMAC.

The split keeps cheap CPU stages in-process without paying for HMAC signing, while every external stage uses the same shape as crawl and embed queues.

{>> `signLease` is passed in as a function so the domain port has no `crypto/hmac` import. The infra layer wires the registry's HMAC secret; the domain port just sees "give me bytes, store them, hand back the string form."}

### LeasedTask carries enough to skip a second fetch

A leased task includes the blob's `content_type` and `size_bytes` joined in at reservation time. A worker fetching a 4 GB PDF wants to know that before it starts streaming; a worker that only handles `text/html` wants to reject mismatches fast. Returning the join saves a round-trip and matches what every existing worker actually needs.

### `signLease` is injected, not embedded

The reservation method takes `signLease func(taskID int64, expires time.Time) (string, []byte)` as a parameter. The domain doesn't know what an HMAC is. The infra adapter gets the function from the app layer at startup; the app layer pulls it from the registry's lease-signer service. This keeps the system-level "HMAC stateless leases" decision (see system spec) out of the domain package entirely.

### `Capability` is a catalog, not a gate

The capability catalog is one of the more counterintuitive parts of this package: it looks like authorization metadata but is not. Authorization for processing work comes from two places, neither of them this catalog:
1. The worker's stored PAT capability set (loaded server-side, never trusted from the wire — see system spec on capabilities-as-strings).
2. The `pipeline_triggers` table (which decides what kinds get enqueued in the first place).

The catalog is for `list-capabilities` output and for seeding documentation. It is intentionally separable from PAT and from queue routing because the system goal is "any string is a valid capability." Mandatory cataloging would re-introduce the central registration this design rejects.

{>> The `Internal bool` field on `Capability` exists for the same reason — it labels which kinds the in-process pipeline goroutine will claim, so the operator can see at a glance which stages need an external worker and which don't.}

### TaskRequeueFilter as an AND-of-non-zero struct

The filter struct is built so a zero value means "no constraint on this dimension." This is the standard Go optional-fields idiom and keeps the operator CLI surface compact (one flag per dimension, all optional). It is mentioned here only because the same shape repeats in the crawl and embed queue ports — operators get the same mental model for requeueing across all three queues.

### What this package deliberately does NOT model

- **Triggers / dispatch rules** — declarative routing lives in its own domain package (`pipeline_triggers`). This package just stores rows once something has decided they should exist.
- **Worker identity beyond an int64** — `workerID` is opaque. Capability matching, PAT verification, domain binding are all decided before `ReserveBatch` is called; this port trusts its caller has done the gating.
- **Blob storage** — `OutputLakeObjectID` is the only hook into the lake. The processor writes a blob via the lake port, gets an ID back, and hands it to `MarkDone` / `Complete`. This port never touches bytes.

## Interactions

- **Producer side** — `internal/app/pipeline` (the dispatcher) calls `Enqueue` / `BulkEnqueue*` after every lake-object insert that matches a `pipeline_triggers` rule.
- **Consumer side (internal)** — the in-process pipeline goroutine calls `ClaimNext` on a tick for each `Pipeline.InternalProcessors` kind, runs the work, and calls `MarkDone` / `MarkFailed` / `MarkSkipped`.
- **Consumer side (external)** — the HTTP `/v1/tasks/reserve` handler calls `ReserveBatch`; the worker calls back into `/v1/tasks/{id}/result|fail|heartbeat`, which translate into `Complete` / `Fail` / `Heartbeat` with the lease token from the request.
- **Sweeper** — `SweepExpired` is called every 30s by the system-level sweeper goroutine (shared between all three queues). It does not distinguish processing rows from any other queue.
- **Operator CLI** — `list-capabilities`, `requeue-tasks --status=failed --processor=pdf_ocr`, `task-status-counts` all bottom out in `CapabilityRepo` and `Repository` methods.
- **Metrics** — `StatusCounts` returns a per-processor × per-status map for Prometheus-style exposure.
- **Lease signing** — depends on whatever the app layer injects as `signLease`. Default in this codebase is HMAC-SHA256 over `(taskID, expires)` with the registry-wide lease secret.

## Mapping

> [[internal/domain/processing/job.go]]
> [[internal/domain/processing/repository.go]]
> [[internal/domain/processing/capability.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
