---
tldr: application service for /v1/tasks/* — reserve by kinds, heartbeat, accept text or blob results, chain follow-ups via next-processor or blob_produced triggers
category: core
---

# tasksvc external workers

## Target

`internal/app/tasksvc.go` — the application-layer use case driving the `processing_jobs` queue from the worker's side of the wire. Composes the processing repository, lake repository + blob store, extraction repository, chunk sink, lease signer, and optional dispatcher / collection resolver / FTS service. Bound to HTTP at `/v1/tasks/*`.

## Behaviour

- A worker reserves up to a small batch of processing tasks by listing the processor kinds it is willing to handle. Selection across kinds is repo-driven; the service caller does not get to bias one kind over another beyond what they list.
- Each reserved task comes back with an opaque lease token. That token is the only thing the worker needs to later heartbeat, succeed, or fail the same task — no separate session.
- A worker can extend its lease at any time before expiry by presenting the token; the extension is by a fixed configured amount, not "until now+TTL".
- Two result shapes are accepted, and the worker picks which one the processor produced:
  - **Text result** — extracted text plus optional language and page count. The system persists it as an extracted document, optionally splits it into chunks, and optionally mirrors it into full-text search. The original lake object is left untouched.
  - **Blob result** — a new content stream plus its content-type and SHA256. The system stores it as a fresh lake object that shares the source's URL hash (same crawl, derived artifact), links it back on the completing task row, and optionally enqueues one named follow-up processor against the new lake object.
- Both result paths reject the request if the lease token does not validate against the claimed task ID — workers cannot complete tasks they did not reserve.
- A blob result, once persisted, announces `blob_produced` on the trigger bus so declarative pipeline rules can enqueue further work without the worker (or this service) knowing what comes next.
- A worker can declare a failure with a retryable flag; retryable failures re-enter the queue with backoff, terminal failures do not. The lease must still validate.
- Tasks whose workers go silent past their lease are reclaimed by a periodic sweep and become reservable again — no manual intervention, no operator visibility required.
- Integration failures on the result path (blob store put, lake insert, extraction upsert, chunk insert, downstream enqueue) leave the task uncompleted so the next worker can retry. The service never partially-completes a task and never silently swallows.

## Design

### Worker-driven kinds, server-driven everything else
Reserve takes the *list of kinds the worker is willing to do* and a batch size. The service does not interpret the kinds or enforce capability matching — capability gating happens upstream at the HTTP layer against the PAT-stored capability set, and per-row routing happens inside the repository's SELECT. This keeps the service ignorant of the capability vocabulary, so adding a new processor needs no edits here. {>> `ReserveBatch(ctx, workerID, kinds, batch, LeaseTTL, sign)` — the closure injects the HMAC signer so the repo can stamp tokens during the same TX that writes the lease <<}

### Lease is the only handle a worker holds
Heartbeat, success, and failure all take `(taskID, token)`. The token is verified statelessly first (HMAC), then the *raw* signature bytes are passed to the repo so the row's stored token is what actually authorizes the state change. Verification rejects → no DB touch. {>> `Lease.VerifyTask` then `lease.Raw(token)` then repo call — defense in depth as per system spec <<}

### Two result shapes, hard split
`AcceptText` and `AcceptBlob` are distinct entry points rather than one polymorphic accept. The decision is the worker's: a processor that produces text takes the text path; a processor that produces a derived file takes the blob path. This avoids ambiguous "both" cases and keeps each path's invariants tight.

### Text path: extracted document is the unit
The extracted text upserts against the source lake object (one canonical extracted doc per source). Chunking is conditional on a chunk sink being attached and the text being non-empty — a worker that delivers empty text completes the task without inserting anything downstream. FTS mirroring is best-effort and never blocks completion. {>> `t.Chunks != nil && in.Text != ""` and `if t.FTS != nil { t.FTS.OnExtracted(...) }` are the conditional guards <<}

### Blob path: provenance via URL hash, chaining via next-processor or triggers
The new blob inherits the source's URL hash so audit can answer "everything derived from this crawl". The storage key is content-addressed by `(url_hash, content_type)` so re-running the same conversion produces the same key. The output lake object is linked back onto the completing task row (not the source row) — the source remains the input pointer, the task row records the output.

The worker may include a `NextProcessor` to enqueue one follow-up directly against the new lake object — used when the chain is fixed (docx→pdf→pdf_ocr) and the worker already knows it. The service *also* fires `blob_produced` on the dispatcher so declarative triggers can enqueue *additional* follow-ups the worker doesn't know about. Both mechanisms can fire on the same blob; the trigger-cache+dedupe layer is responsible for not double-enqueueing.

### Optional collaborators, never required
`Dispatcher`, `Resolver`, and `FTS` are nil-checked at every use site. A minimal deployment can run with just processing + lake + extraction + chunks + lease and lose only chaining/collections/FTS — the core reserve/complete loop still works. {>> set via `SetDispatcher` / `SetResolver` / `SetFTS` rather than constructor args, to keep `NewTaskSvc` stable across feature additions <<}

### Local interface aliasing avoids a dependency cycle
The chunk persistence port is declared *inside* this package as a tiny `chunkInserter` interface, and the row type as a local `chunkRow`, rather than importing the chunking domain directly. This is because downstream wiring composes a facade that would otherwise pull tasksvc and chunking into a circular import. The conversion to/from the chunking domain happens at the adapter seam, not here.

### Failure-mode policy
Every external write in a result path that fails returns the error *before* `Processing.Complete` is called. The lease therefore remains live, the row stays `running`, the sweeper eventually requeues it. The one tolerated exception is the blob path's `Enqueue` next-processor failure after a successful `Complete` — the source task is already done, so the error is returned but the lake object and completion stand. {>> AcceptBlob returns `newID, fmt.Errorf("enqueue next: %w", err)` in that case — caller gets both the ID and the partial failure <<}

### `fetchTask` as a documented seam
The processing repository port intentionally does not yet expose a stable `GetByID`. Until it does, this service downcasts via a local interface assertion and errors out cleanly if the concrete adapter doesn't implement it. The comment in the code explicitly flags this as a slice-6 expedient — the design decision is that the cost of adding `GetByID` to the port is deferred until a second caller needs it.

## Interactions

- **HTTP layer (`/v1/tasks/*`)** — authenticates the PAT, enforces the `crawl`/processor capabilities and any per-domain binding, then calls this service. Body shapes map 1:1 to `Reserve`, `Heartbeat`, `TextResult`, `BlobResult`, and the failure call.
- **`processing.Repository`** — reserve, heartbeat, complete, fail, enqueue, sweep. The repo owns the lease-write half of the HMAC defense-in-depth (stores the raw signature) and the kinds-filtering SELECT.
- **`lake.Repository` + `lake.BlobStore`** — blob path only. Insert returns the new ID that becomes both the completion's output pointer and the trigger payload's `LakeObjectID`.
- **`extraction.Repository`** — text path only. Upserts so a re-run replaces.
- **Chunk sink** — text path, conditional. Adapter converts `chunkRow` back into the chunking domain.
- **`lease.Signer`** — verifies inbound tokens, signs outbound ones via the closure passed to `ReserveBatch`.
- **`TriggerDispatcher`** — blob path. Fires `EvtBlobProduced` with the new lake object ID, content type, and source processor name so triggers can route downstream by any of those fields.
- **`CollectionResolver`** — text path. Resolves the per-domain embed collection for the extracted document at upsert time so the embed worker downstream knows where to push vectors.
- **`FTSSvc`** — text path. Best-effort mirror to Quickwit; never blocks task completion.
- **Sweeper goroutine** — calls `SweepExpired` on a tick to reclaim leases from silent workers.

## Mapping

> [[internal/app/tasksvc.go]]
> [[internal/domain/processing/]]
> [[internal/domain/lake/]]
> [[internal/domain/extraction/]]
> [[internal/domain/triggers/]]
> [[internal/infra/lease/]]
> [[internal/infra/pipeline/chunker/]]
> [[internal/app/dispatcher.go]]
> [[internal/app/collections.go]]
> [[internal/app/fts.go]]
