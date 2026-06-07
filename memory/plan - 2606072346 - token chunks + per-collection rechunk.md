---
title: token-sized chunks + per-collection rechunk
status: active
created: 2026-06-07 23:46
branch: task/eidos-spec-chunking-rechunk
spec: [[spec - chunking - token-sized chunks with neighbor overlap and per-collection rechunk]]
---

## Context

Spec [[spec - chunking - token-sized chunks with neighbor overlap and per-collection rechunk]] defines:
- New chunk shape: `prev-overlap || core || next-overlap` in token space (defaults 400/2800/400 = 3600 total).
- `chunker.Tokenizer` interface; default `cl100k_base` via tiktoken-go.
- New `collections` table (name, chunk_tokens, overlap_prev, overlap_next, tokenizer) for per-collection sizing; empty rows use registry defaults.
- `registry rechunk --collection <name>` to drop + respit + re-enqueue every document in a collection; deletes corresponding Qdrant points post-commit.

This plan ships it in five phases. Each phase ends with a runnable system + a check; phases 1-2 change the behavior of new ingest, phases 3-5 add the operator surface to repair the backlog.

## Goal

After this plan, the operator can: (a) run `registry rechunk --collection lithuania_courts` and watch the embed worker drain fresh 3600-token chunks; (b) tune chunk sizing per collection without re-deploying; (c) trust that future ingest produces the new chunk shape automatically.

---

## Phase 1 — Tokenizer abstraction + dependency

**Goal:** plumb tokenizer support without behavior change. Chunker keeps current word-based logic; tokenizer is wired but unused.

1. [ ] Add `tiktoken-go` to `go.mod`. Verify it builds on the registry box (no cgo, pure Go).
2. [ ] Create `internal/infra/tokenizer/tiktoken/tiktoken.go` implementing a small `Tokenizer` interface:
   ```go
   type Tokenizer interface {
       Name() string
       Encode(s string) []int
       Decode(ids []int) string
   }
   ```
3. [ ] Declare the interface in `internal/infra/pipeline/chunker/tokenizer.go` (alongside `chunker.Config`).
4. [ ] Wire a default tokenizer in `cmd/registry/main.go buildService`. Store on the bundle.
5. [ ] Unit test: `Encode/Decode` round-trips equal string for a few Lithuanian + English samples.

**End-of-phase visible result:** registry binary builds, starts, holds a tokenizer instance — no behavior change. `go test ./internal/infra/tokenizer/...` green.

## Phase 2 — Token-sized chunker with neighbor overlap

**Goal:** ship the new chunk shape. Defaults globally; no per-collection config yet.

1. [ ] Rewrite `internal/infra/pipeline/chunker/chunker.go`:
   - New `Config{ChunkTokens, OverlapPrev, OverlapNext int; Tok Tokenizer}`. Keep `Defaults()` returning `2800 / 400 / 400` with the registry's default tokenizer.
   - `Split(text string, cfg Config) []Chunk` reimplemented as a token-space sliding window. Step = `ChunkTokens`. Each chunk: `prev-overlap || core || next-overlap`. Boundary chunks have empty prev/next as documented in the spec.
   - `chunk.TokenCount` records the **core** size, not total.
2. [ ] Add table-driven tests `chunker_test.go`:
   - 10k-token synthetic doc → expected 4 chunks, prev/next sizes per spec verification §1.
   - 500-token doc → 1 chunk, both overlaps empty.
   - Empty doc → 0 chunks.
3. [ ] Update `internal/app/pipeline.go` and `internal/app/tasksvc.go` call sites to pass the tokenizer-bearing config. Wire from the bundle.
4. [ ] Add a one-line bump in `CHANGELOG.md`.
5. [ ] Smoke test: run `scripts/smoke.sh` end-to-end. New documents should produce one or more 3600-token chunks. Inspect via `sqlite3 ... "SELECT length(text) FROM document_chunks ORDER BY id DESC LIMIT 3;"` — should show ~10-20kB depending on encoding.

**End-of-phase visible result:** every new document ingested goes through the new chunker. Old chunks still exist in DB unchanged. Embed worker receives bigger chunks and forwards them to the model — verify no errors in worker log.

## Phase 3 — `collections` table + per-collection config

**Goal:** operator can override chunk sizing per collection at runtime.

1. [ ] Add migration `0010_collections.sql` in all three dialect dirs. Schema per spec §Design / "collections table".
2. [ ] New port `internal/domain/chunking/collection_config.go`:
   ```go
   type CollectionConfig struct { Name, Tokenizer string; ChunkTokens, OverlapPrev, OverlapNext int }
   type CollectionConfigRepo interface {
       Get(ctx context.Context, name string) (*CollectionConfig, error)
       Upsert(ctx context.Context, cfg CollectionConfig) error
       List(ctx context.Context) ([]CollectionConfig, error)
       Delete(ctx context.Context, name string) error
   }
   ```
3. [ ] Implement in `internal/infra/db/gormrepo/collection_config_repo.go`.
4. [ ] New service `app.CollectionConfigResolver` mirroring the existing `CollectionResolver`. Method `ResolveConfig(ctx, collectionName) (chunker.Config, fromTable bool, err)`. Falls back to registry defaults on no-row.
5. [ ] Wire `Pipeline` and `TaskSvc` to call `Resolver.ResolveConfig(ctx, collection)` per document, then pass that config into `chunker.Split`.
6. [ ] Operator CLI: `registry list-collections` / `set-collection --name X --chunk-tokens N --overlap-prev N --overlap-next N --tokenizer T` / `delete-collection --name X`. Follow flag conventions from `update-domain` (e.g. `-1` = leave unchanged, `-` = clear/reset).
7. [ ] Smoke `scripts/smoke_collections.sh`: ingest a doc with no collection row → default sizing. Add a row with smaller chunk_tokens. Ingest a doc that maps to that collection → smaller chunks. Assert.

**End-of-phase visible result:** `registry list-collections` works; per-collection overrides applied at chunk time; new smoke passes.

## Phase 4 — `registry rechunk` CLI

**Goal:** operator can rebuild the chunk backlog for an existing collection in place.

1. [ ] New service method `app.Chunker.Rechunk(ctx, collection string, opts RechunkOpts) (RechunkReport, error)`.
   - Find all extracted_documents whose resolved collection matches.
   - Per document, in a `WriteTX`: collect old chunk_ids → DELETE old rows → re-split text with current resolved config → INSERT new rows with `embed_status='pending'`.
   - Return per-document line items: `(doc_id, old_count, new_count)`. Qdrant cleanup is Phase 5; phase 4 emits the old chunk_ids only.
2. [ ] CLI subcommand `registry rechunk --collection <name> [--dry-run] [--since-doc-id N] [--limit N]`. Output one line per document plus a totals line.
3. [ ] Test `tasksvc_rechunk_test.go` (or smoke) covering: full collection rechunk; dry-run reports counts only; `--limit` honored; doc with no chunks is a no-op.
4. [ ] Smoke `scripts/smoke_rechunk.sh`: seed 3 docs, run rechunk with different config, assert (a) old chunk rows gone, (b) new chunk count matches `ceil(token_count / chunk_tokens)`, (c) all new chunks `pending`.

**End-of-phase visible result:** operator can run `registry rechunk --collection X` and watch `queue-stats` show the chunk queue spike. Qdrant still holds the old vectors at this point — addressed in Phase 5.

## Phase 5 — Qdrant cleanup integration

**Goal:** rechunk removes stale vectors from Qdrant so search doesn't see ghosts.

1. [ ] Verify `internal/infra/qdrant/client.go` has a `DeletePoints(ctx, collection, ids []string) error`; add it if not (Qdrant exposes `POST /collections/{name}/points/delete`).
2. [ ] Extend `Rechunk` to call `qdrant.DeletePoints(collection, oldChunkIDs)` AFTER the per-document `WriteTX` commits. Log on failure; do not fail the rechunk — stale points are recoverable, partial DB state is not.
3. [ ] Extend the rechunk report with `qdrant_deleted` per document.
4. [ ] Extend `scripts/smoke_rechunk.sh` to run against a fake Qdrant and assert the right point IDs were deleted.

**End-of-phase visible result:** post-rechunk, Qdrant collection size drops by the old-chunk count and grows back as the embed worker fills new vectors. `/v1/search` no longer returns matches against orphaned point IDs.

---

## Verification

The plan is done when:

- `go test ./...` passes including new chunker, collection-config, and rechunk tests.
- `scripts/smoke.sh` and the two new smokes (`smoke_collections.sh`, `smoke_rechunk.sh`) all pass.
- A manual end-to-end on a dev DB: ingest 5 docs, set a per-collection override, run rechunk, watch the embed worker drain → Qdrant points refreshed → `/v1/search` returns hits against new chunks.
- The bug spec's open items have answers (cl100k_base accuracy noted, storage cost noted, chunk_id stability documented).

## Adjustments

(none yet)

## Progress log

- **2026-06-07 23:46** — plan created from [[spec - chunking - token-sized chunks with neighbor overlap and per-collection rechunk]]. On branch `task/eidos-spec-chunking-rechunk`. Awaiting start.
