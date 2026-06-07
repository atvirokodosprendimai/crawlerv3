---
tldr: application-level blob mover — paged, idempotent migration of lake objects between two BlobStore backends with audit and optional source delete
category: core
---

# app migrator

## Target

The `Mover` use case in `internal/app/migrator.go`. A registry-side application service that walks lake objects pinned to a source storage backend and re-homes them onto a destination backend, updating each row's `storage_backend` pointer and recording where the blob came from. Invoked by the `migrator` worker binary (see system spec, Worker tiers).

## Behaviour

- An operator points the mover at a (source backend, destination backend) pair and gets back a `Stats` summary: how many rows were scanned, copied, skipped, and errored.
- Source and destination must be different backends. Pointing both at the same backend is rejected up front, not silently no-op'd.
- A run can be re-executed safely. Objects whose row already says `storage_backend == dst` are not visited again — only rows still tagged as the source backend are listed, so a crashed or interrupted run resumes by simply being re-launched.
- A per-object failure (copy error, size mismatch, row update failure) does not abort the run. The error is reported, counted, and the next object is attempted. Partially-migrated batches are an expected, recoverable state.
- After a successful copy, the lake row records where the blob used to live (`migrated_from`) as an audit trail; an operator can later answer "where did this blob originally come from" without consulting external logs.
- The storage key itself is preserved across backends — only the backend pointer changes. Downstream readers that joined on `storage_key` continue to work.
- Source deletion is opt-in and destructive. The default is to leave the source copy intact so the operator has a manual grace period; `DeleteSrc=true` is an explicit, separate decision.
- If source deletion fails for a single object, the migration of that object is still considered successful — the row already points at the destination and the orphaned source blob is logged but does not become an error.
- The run is cancellable via the passed context; in-flight list/copy/update calls observe cancellation.

## Design

### Application service, not infrastructure

The mover lives in `internal/app/` and depends only on domain ports (`lake.Repository`, `lake.BlobStore`). It has zero knowledge of whether either side is local FS, S3, or something added later. {>> swapping `Src`/`Dst` to a future `gcs` adapter requires no change here; the system-spec "Pluggable everything" decision is honored.}

### Idempotency via row-side truth

The lake row's `storage_backend` is the single source of truth for "where this blob lives now". Idempotency is not bookkept in a side table or a checkpoint file — it falls out of the listing filter. {>> `Lake.ListByBackend(src, …)` only returns rows still pointing at the source; once `UpdateStorage` flips the pointer, the row vanishes from subsequent pages.}

This also makes the mover crash-safe: a kill mid-batch loses at most one in-flight object, never re-copies completed ones, and never needs a "resume from offset" flag.

### Keyset pagination over offset

The list is walked by ascending `id` with an `afterID` cursor advanced from the last row of each page. {>> avoids the classic offset-shift bug: as rows are migrated and disappear from the result set, an `OFFSET` would skip survivors; an `id > afterID` cursor is stable under deletion-from-result.}

### Verify-before-commit

Before the row is flipped, the destination `Put` result is checked against the source row's recorded `FileSize`. A mismatch aborts that object, leaves the source row pointing at src, and counts an error. {>> rejects silent truncation / partial uploads; the next run will retry because the row still says src.}

### Audit, not history

`migrated_from` is a flat `"<backend>:<storage_key>"` string, not a structured history list. The mover writes only the most recent prior location. {>> repeated migrations overwrite; the field answers "where did this come from before now", not "every backend it ever lived on" — sufficient for the audit story the operator was promised and cheap to store.}

### Per-object errors are isolated

The inner loop swallows errors into the `Stats.Errors` counter and continues. {>> a single bad blob (corrupt source, S3 throttled, hash mismatch) does not poison the rest of the batch; the operator sees the error count and can re-run.}

Sentinel-style aborts are reserved for setup-time misconfiguration (nil deps, same-backend src=dst), where continuing would be meaningless.

### Destructive delete is opt-out by default

`DeleteSrc` defaults to false. The decision reflects the operator-facing promise in the system spec ("Migrate blobs … without downtime"): the safest correct behavior is to leave the source intact so a misconfigured destination can be rolled back by simply pointing the row back. The destructive variant is one flag away but never the default.

A delete failure after a successful row flip is downgraded to a log line, not an error. {>> the row already points at dst; the blob is reachable; an orphan on src is a cleanup task, not a data-integrity failure.}

### Streaming copy, no staging

`Get` returns a reader that's piped straight into `Put`. {>> the mover never materializes a blob in memory or on local disk; large PDFs and small JSONs cost the same RAM. The decision propagates the `BlobStore` port's streaming contract end-to-end.}

### Batch size is a knob, not a policy

`BatchSize` defaults to 100 and is settable per run. {>> trades list-call overhead against memory-per-page and lease-on-rows-while-iterating. No adaptive logic — the operator picks based on backend latency.}

## Interactions

- **Caller (`migrator` worker binary in `cmd/`)** — constructs the `Mover`, wires concrete `lake.Repository` (gorm-backed) and two `lake.BlobStore` adapters (local + S3), calls `Run`.
- **`lake.Repository`** — `ListByBackend` drives the keyset walk; `UpdateStorage` performs the atomic backend-pointer flip + `migrated_from` write per object.
- **`lake.BlobStore` (src)** — `Get` streams bytes; `Delete` only when `DeleteSrc` is set; `Backend()` names the filter.
- **`lake.BlobStore` (dst)** — `Put` streams + returns size for the verify check; `Backend()` names the new pointer value.
- **System-level "Fail-fast on integrations"** — honored here: a failed `Put` or `UpdateStorage` leaves the row unchanged so re-running the mover (or a subsequent run after the backend recovers) retries cleanly.
- **No queue involvement** — unlike crawl/processing/embed, the mover is a one-shot batch driven by direct port calls, not the three-queue protocol. It does not consume from `processing_jobs`.

## Mapping

> [[internal/app/migrator.go]]
> [[internal/domain/lake/]]
> [[cmd/migrator/]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
