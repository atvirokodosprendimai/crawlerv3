---
tldr: in-process goroutine that claims cheap CPU-only processing jobs (html_strip, text_passthrough) and produces extracted text, chunks, and optional FTS forward
category: core
---

# pipeline internal processors

## Target
`internal/app/pipeline.go` — the `Pipeline` use case that runs inside the registry process as a single goroutine, draining `processing_jobs` for the subset of processors marked internal.

## Behaviour
- A subset of processor kinds runs "for free" inside the registry process — the operator never needs to launch a worker box to get plain HTML and plain-text content extracted.
- Default internal set is `html_strip` and `text_passthrough`. Anything else (PDF OCR, office conversions) stays `queued` and is the property of external workers.
- Each internally-processed job ends with the same observable side effects as if an external worker had handled it: one upsert into `extracted_documents`, N inserts into `document_chunks`, optionally one FTS forward — and the job row moves out of `queued`.
- The internal loop never starves: it ticks on a caller-supplied interval, drains everything claimable per tick, and stops cleanly when its context is cancelled.
- Empty or whitespace-only extraction is a no-op success — no extracted row, no chunks, no FTS forward, but the job is still marked done.
- Per-domain embed-collection routing is honoured if a resolver is wired; otherwise the extraction is written with empty collection and the embed stage decides downstream.
- FTS forwarding is best-effort: when wired, every successful extraction is mirrored; failures inside the FTS service never fail the job.
- Processors that the in-process goroutine knows it cannot do (`docx_to_pdf`, `office_to_pdf`) are marked `skipped` with a stable sentinel reason — they don't sit in the queue forever waiting for an external worker that may not exist, and they don't get retried by the sweeper.

## Design

The internal-vs-external split is the project's answer to "where does the CPU go?". HTML stripping and treating-bytes-as-text are pure-Go, allocation-bounded, and want to share memory with the rest of the registry (blob store handles, DB pools, resolver cache). Anything that shells out (OCR, LibreOffice) or wants a GPU is shoved across the network boundary so the registry stays responsive. The set is a field on the `Pipeline` value {>> `InternalProcessors []processing.Processor` defaulted in `NewPipeline` <<} so the wiring layer can shrink or grow it without touching this file.

The drain shape is "for each internal processor, claim until empty". This is deliberately simpler than per-processor goroutines: internal work is meant to be cheap, and serialising it inside one goroutine bounds memory and lets a slow processor naturally backpressure the others on the same tick. The polling cadence is injected, not chosen here, so smokes can run at 100ms and prod at 1s without code change.

Both internal processors share an identical post-extract tail: `Extractions.Upsert → optional FTS.OnExtracted → writeChunks`. This is not coincidence — every successful processor in the system, internal or external, is expected to land in the same three places. Keeping the tail in this file (instead of in the per-processor functions) makes the contract obvious: an "extracted document" is the canonical join point between fetched bytes and downstream embed/FTS pipelines.

The `Resolver` and `FTS` fields are optional collaborators wired via setters after construction {>> `SetResolver` / `SetFTS` <<}. They are nil-safe at every call site. This keeps the pipeline buildable in tiny smokes (no FTS, no per-domain routing) while still letting prod attach both without a different constructor.

Error handling is binary per job: anything the processor can't recover from is `MarkFailed` with the error string. The one exception is PDF OCR, which is internal in the code but only as a leftover dispatch arm — when it errors it goes to `MarkSkipped`, matching the policy that internal-side OCR is a fallback path and PDFs are really the external worker's job. The DOCX/Office arms exist purely to short-circuit the queue: they `MarkSkipped` immediately with the conversion package's sentinel {>> `docxproc.ErrSkip` <<} so those rows don't stall internal claims.

The HTML and text-passthrough paths differ only in the bytes-to-string step. HTML goes through a streaming stripper that takes the blob `ReadCloser` directly; passthrough reads the whole blob and casts it to string. Both then trim, bail on empty, and otherwise proceed to the shared tail. This intentional symmetry means adding a new "text-shaped" processor (e.g. markdown, RTF) is mostly a new bytes-to-text function and a new arm.

Chunking is hard-wired to the chunker package's defaults at construction time {>> `chunker.Defaults()` into `ChunkCfg` <<}. The cfg is on the `Pipeline` struct so an integration test or future per-domain override can swap it, but the default is one number set, one policy. Chunks carry a freshly-generated UUID per row so downstream consumers (embed, FTS) can address them stably even before the DB returns an autoincrement.

## Interactions
- Reads `processing_jobs` via `processing.Repository.ClaimNext(processor)` — single-row claim, scoped to one processor kind at a time, so the SQL knows exactly which rows to lock.
- Reads blobs via `lake.BlobStore.Get(storageKey)` — same port any external processor would use; the goroutine has no special access path.
- Writes extracted text via `extraction.Repository.Upsert` and chunks via `chunking.Repository.InsertMany`. Both are the same ports external workers' accept-result handlers ultimately reach.
- Optionally calls `CollectionResolver.ResolveForLakeObject` to attach an embed-collection hint per extraction.
- Optionally calls `FTSSvc.OnExtracted` after upsert — fire-and-log, never fails the job.
- The drain only sees rows that the dispatcher (driven by `pipeline_triggers`) has already enqueued; it does not decide what to process, only how to process the kinds claimed as internal.
- The HMAC/lease machinery from the three-queue system does not apply inside this goroutine — internal claims complete or fail synchronously and skip the lease/sweeper path entirely.

## Mapping
> [[internal/app/pipeline.go]]
> [[internal/domain/processing]]
> [[internal/domain/extraction]]
> [[internal/domain/chunking]]
> [[internal/domain/lake]]
> [[internal/infra/pipeline/htmlproc]]
> [[internal/infra/pipeline/pdfproc]]
> [[internal/infra/pipeline/docxproc]]
> [[internal/infra/pipeline/chunker]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
