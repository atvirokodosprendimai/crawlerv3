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

## Phase 1 — Tokenizer abstraction + dependency — [completed]

**Goal:** plumb tokenizer support without behavior change. Chunker keeps current word-based logic; tokenizer is wired but unused.

1. [x] Add `tiktoken-go` to `go.mod`. Pure Go (no cgo). Transitive: `dlclark/regexp2`.
2. [x] `internal/infra/pipeline/chunker/tokenizer.go` declares the `Tokenizer` interface.
3. [x] `internal/infra/tokenizer/tiktoken/tiktoken.go` implements it via `pkoukk/tiktoken-go`. Default encoding `cl100k_base`. `New` / `MustNew` constructors.
4. [x] `cmd/registry/main.go` wires the tokenizer in `buildService` and exposes the `--tokenizer` flag (env `TOKENIZER`, default `cl100k_base`). Stored on `registryBundle.Tokenizer`.
5. [x] `tiktoken_test.go` round-trips empty / ASCII / Lithuanian (diacritics) / mixed / newlines / emoji. Token-density spot check: Lithuanian over-segments to ratio 2.66 vs English — well within the 5× upper guard.

**End-of-phase outcome:** `go build ./...` + `go vet ./...` clean. `go test ./internal/infra/tokenizer/...` PASS in 0.5s. Tokenizer hangs off the bundle but no caller consumes it yet — chunker still word-based. Ready for Phase 2 chunker rewrite.

## Phase 2 — Token-sized chunker with neighbor overlap — [completed]

**Goal:** ship the new chunk shape. Defaults globally; no per-collection config yet.

1. [x] `internal/infra/pipeline/chunker/chunker.go` rewritten. Config now `{ChunkTokens, OverlapPrev, OverlapNext, Tok}`. Defaults `2800/400/400`. Split iterates token-space cores; each chunk's `Text = Decode(prev_ids || core_ids || next_ids)` in **one** Decode call so concat-safe tokenizers (tiktoken BPE) keep their roundtrip property.
2. [x] Tests in `chunker_test.go` use a rune-per-token fake (lossless inverse). Spec verification §1 covered: 10k tokens / 2800 cores → 4 chunks with cores [2800, 2800, 2800, 1600] and exact boundary text. Plus empty / short / overlap-cap / negative-overlap.
3. [x] `NewPipeline` and `NewTaskSvc` take `chunker.Tokenizer` as a new last arg, stamp into `ChunkCfg`. Registry wires `bundle.Tokenizer` through.
4. [x] End-to-end real-tiktoken integration test in `tiktoken_test.go` confirms `sum(core tokens) == len(Encode(doc))` for a multi-chunk doc — concat-Decode assumption holds for cl100k_base.
5. [x] `scripts/smoke.sh` passes end-to-end. Surfaced a pre-existing bug (`UpsertByHost` never set `parallel_fetches`, so reserve's `rn <= pf` filtered every row out on a fresh domain). Fixed in commit `4f78337`. Smoke output: `lake=1 extracted=1 chunks=1 token_count=23` — short doc → one chunk, no overlaps, core-only TokenCount. Matches spec.

**End-of-phase outcome:** every new document goes through the token-sized chunker. Old chunks unchanged. Defaults are global (`2800/400/400` via `cl100k_base`). Phase 3 will add per-collection overrides.

## Phase 3 — `collections` table + per-collection config — [completed]

**Goal:** operator can override chunk sizing per collection at runtime.

1. [x] Migration `0010_collections.sql` shipped in all three dialect dirs.
2. [x] Port `chunking.CollectionConfigRepo` (Get/Upsert/List/Delete) + `ErrCollectionNotFound` sentinel.
3. [x] `gormrepo.CollectionConfigRepo` implementation with `ON CONFLICT … DO UPDATE` for `Upsert`.
4. [x] `app.CollectionConfigResolver` with `ResolveConfig(ctx, name) (chunker.Config, fromTable bool, err)`. Empty table → defaults. Lookup errors fall back to defaults so ingest never blocks.
5. [x] `Pipeline` and `TaskSvc` thread `collection` into the chunk-writing path; both gained `ConfigResolver` field + `SetConfigResolver` method. The chunker is run with the resolved config (falling back to bundle defaults) on every document.
6. [x] CLI: `list-collections` / `set-collection --name X --chunk-tokens N --overlap-prev N --overlap-next N --tokenizer T` / `delete-collection --name X`. Partial updates preserve existing fields (read-modify-write).
7. [x] `scripts/smoke_collections.sh` covers: empty → set → list → partial update preserves → tokenizer change → delete → empty. Passes.

**End-of-phase outcome:** operator can tune per-collection chunk sizing at runtime; ingest picks up changes on the next document. Phase 4 (rechunk) next.

## Phase 4 — `registry rechunk` CLI — [completed]

**Goal:** operator can rebuild the chunk backlog for an existing collection in place.

1. [x] New `app.RechunkSvc.Rechunk(ctx, collection, opts) (*RechunkReport, error)`. Walks `extraction.ListByCollection` in pages of 100; per document calls the new `chunking.Repository.ReplaceByDocument(ctx, docID, fresh)` which deletes old rows and inserts the fresh slice in **one** `WriteTX`. Returns per-doc `(OldCount, NewCount, OldChunkIDs)` so Phase 5 can pick up the Qdrant cleanup without recomputing.
2. [x] CLI `registry rechunk --collection <name> [--dry-run] [--since-doc-id N] [--limit N]`. `--collection -` targets documents whose Collection field is empty (default bucket); any other value matches by equality. Output one line per document plus a totals line with `config_source` (`collections-row` or `defaults`) and the resolved sizing.
3. [x] `scripts/smoke_rechunk.sh` covers: plant 3 docs of varying length with stale chunks → dry-run leaves DB intact → set tiny per-collection config → apply → assert stale gone, all new `pending`, every doc gets ≥2 chunks → rerun is idempotent (same row count). Passes.

**End-of-phase outcome:** the operator can run `registry rechunk --collection liteko` and watch `queue-stats` show the chunk queue spike. Qdrant still holds the old vectors at this point — Phase 5 (DeletePoints) is the last piece.

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
- **2026-06-07 23:58** — Phase 1 completed. tiktoken-go (pure Go) added; `chunker.Tokenizer` interface + tiktoken adapter shipped; `--tokenizer` flag wired into registry; round-trip tests cover Lithuanian + English + emoji. Lt/En token-density ratio = 2.66, defaults (3600 cl100k_base tokens) still fit a 4k window comfortably for Lithuanian text. No behavior change yet — chunker call sites still word-based. Phase 2 next.
- **2026-06-08 00:05** — Phase 2 completed. Chunker rewritten to token-space sliding window with `prev||core||next` shape; `Defaults()` = 2800/400/400. `NewPipeline` and `NewTaskSvc` take `Tokenizer` arg; registry wires `bundle.Tokenizer` through. Spec §1 verification covered in `chunker_test.go`. End-to-end with real cl100k_base passes. `scripts/smoke.sh` green — surfaced a pre-existing `UpsertByHost` bug (parallel_fetches=0 → no reserves) and fixed in `4f78337`. Also pulled two unrelated operational fixes (`sweep-now` CLI + idempotent `lake_objects.Insert` on UNIQUE) into commits `104aa79` to address concurrent prod issues; merged to main in `7509375`. Phase 3 next.
- **2026-06-08 00:11** — Phase 3 completed. Migration `0010_collections.sql` (3 dialects). New `chunking.CollectionConfigRepo` port + `gormrepo` impl using `ON CONFLICT … DO UPDATE`. New `app.CollectionConfigResolver` resolves per-collection sizing with fallback to registry defaults. `Pipeline.writeChunks` and `TaskSvc.AcceptText` now resolve config per document; existing call shape kept by keeping `ChunkCfg` as the fallback. Three CLI subcommands: `list-collections`, `set-collection` (read-modify-write so partial flags don't clobber), `delete-collection`. New `scripts/smoke_collections.sh` covers the full lifecycle. Phase 4 (rechunk CLI) next.
- **2026-06-08 00:20** — Phase 4 completed. Added `chunking.Repository.ReplaceByDocument` (single-WriteTX delete-and-replace, returns old chunk IDs for Phase 5) and `extraction.Repository.ListByCollection`. New `app.RechunkSvc` orchestrates the walk in pages of 100. CLI `registry rechunk --collection <name>` with `--dry-run`, `--since-doc-id`, `--limit`. `scripts/smoke_rechunk.sh` plants stale chunks, asserts dry-run leaves the DB intact, then applies tiny per-collection sizing and verifies stale rows gone, every new chunk pending, idempotent rerun. Phase 5 (Qdrant cleanup) is the only piece remaining.
