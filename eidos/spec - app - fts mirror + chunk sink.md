---
tldr: optional FTS mirror (Stanza→Quickwit) plus chunk-persistence adapter; both run inside the result-accept path as best-effort sinks
category: core
---

# fts mirror + chunk sink

## Target

Two small `internal/app` facades that hang off the extracted-text path:

- `FTSSvc` — forwards extracted text through Stanza rewriting into Quickwit, and runs the same Stanza rewrite on inbound search queries.
- `ChunkRepoSink` — adapts a `chunking.Repository` to the local `chunkInserter` shape TaskSvc uses to persist chunk rows.

Both are wired by `Pipeline` (internal processors) and `TaskSvc.AcceptText` (external processor results); see the system spec for how those upstream paths fit the bigger picture.

## Behaviour

- The FTS mirror is strictly optional. With Quickwit unconfigured the service is a no-op; the rest of the pipeline runs unchanged.
- The same Stanza rewrite is applied symmetrically on the write path (ingest of extracted text) and the read path (`/v1/search/fts` queries), so a query and the document it should match share normalization.
- When Stanza fails for a single document or query, the raw text is used instead — Stanza outage degrades quality but does not lose data or break search.
- When Quickwit ingest fails, the failure is logged and discarded. The extracted document and its chunks remain canonical; FTS will be out of sync until rebuilt, but no upstream caller observes the failure.
- Each FTS document carries enough joinable identity (`document_id`, `lake_object_id`, `collection`) to be reconciled back against the lake without storing source URLs in the index.
- Search refuses to run when no index is configured at either the service or call site, rather than silently picking a default that may not exist.
- The chunk sink persists a batch of in-memory chunk rows atomically from TaskSvc's perspective: either the whole batch is accepted by the chunking repository or the accept-text path errors out, leaving the task uncompleted for retry.
- An empty chunk batch is a no-op success, so callers do not need to special-case documents that produced no chunks.

## Design

### FTS as a best-effort sink, not a pipeline stage

FTS is treated as a side-channel mirror, not part of the canonical data flow. The decision is explicit: the lake (blobs), `extracted_documents`, and `document_chunks` are the source of truth; Quickwit is a derived view that can be rebuilt at any time. Consequently the result-accept paths must never fail because the search index is unhappy. {>> `OnExtracted` logs and swallows both Stanza and Quickwit errors; it never returns an error type at all <<} This is the opposite of the fail-fast contract that governs blob/vector writes (system spec §"Fail-fast on integrations") — and the asymmetry is intentional: a lost FTS doc can be re-derived, a lost blob cannot.

### Stanza symmetry across write and read

Both `OnExtracted` and `SearchByText` run text through the same `stanza.Client.Rewrite` before talking to Quickwit. The design constraint is symmetry: if Stanza changes lemmatization or stemming, the existing index becomes stale, but writes and reads stay aligned because they go through one client. Stanza is independent of Quickwit — the service still works in passthrough mode if only Quickwit is configured.

### Enabled() gates the call, not the caller

The pipeline and TaskSvc always call `OnExtracted` when an FTSSvc is attached; the gating (`Enabled()` + empty-text shortcut) lives inside the service. {>> `Enabled()` collapses nil-receiver and nil-Quickwit and disabled-Quickwit into one check <<} This keeps callers from sprouting `if f != nil && f.Quickwit != nil && ...` everywhere and lets the service evolve its idea of "enabled" without touching upstream sites.

### Chunk sink as an anti-corruption adapter

`TaskSvc` declares a private `chunkInserter` interface and a private `chunkRow` struct rather than depending on `chunking.Repository` / `chunking.Chunk` directly. The stated rationale in code is import-cycle avoidance when downstream packages compose facades, but the effect is also clean DDD: TaskSvc speaks app-local types and never leaks domain types upward. {>> `ChunkRepoSink.InsertMany` is the only place that translates `chunkRow → chunking.Chunk` <<} The sink is the seam where app types meet the domain repository, isolating the conversion.

### Empty input is a success, not an error

`InsertMany` short-circuits on a zero-length batch without calling into the repository. This encodes the decision that "no chunks produced" is a valid outcome (e.g. very short extracted text, or a chunker tuned to drop tiny fragments) rather than a degenerate edge case worth special-casing at every call site.

### Configuration via defaults, overrides via call site

`FTSSvc.Index` carries a process-wide default index name. `SearchByText` accepts a per-call index override, falling back to the default when empty, and erroring when neither is set. This matches the operator-facing pattern: one `--quickwit-index` flag is enough for a single-tenant deploy, but multi-index callers can target a specific one without reconfiguring the registry.

## Interactions

- **Pipeline (internal processors)** — `Pipeline.execHTML` / `execPDF` / `execTextPassthrough` call `FTSSvc.OnExtracted` after writing `extracted_documents`. The chunk sink is reached indirectly via the same `chunkInserter` shape on `TaskSvc`.
- **TaskSvc.AcceptText (external processor results)** — calls `OnExtracted` and then persists chunks through `Chunks chunkInserter`, which the wire-up code populates with a `ChunkRepoSink`.
- **HTTP handler `/v1/search/fts`** — calls `FTSSvc.SearchByText`. Stanza rewriting on the query path lives here, not in the handler.
- **Stanza client** — optional, owned by `internal/infra/stanza`. Independent enable/disable from Quickwit.
- **Quickwit client** — optional, owned by `internal/infra/quickwit`. Disabled state collapses the service to a no-op.
- **Chunking repository** — concrete adapter under `internal/infra` implementing `chunking.Repository`; `ChunkRepoSink` wraps any implementation transparently.

## Mapping

> [[internal/app/fts.go]]
> [[internal/app/chunksink.go]]
> [[internal/app/tasksvc.go]]
> [[internal/app/pipeline.go]]
> [[internal/infra/stanza/]]
> [[internal/infra/quickwit/]]
> [[internal/domain/chunking/chunk.go]]
> [[internal/infra/http/fts.go]]
