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
- **2026-06-07 23:58** — Phase 1 completed. tiktoken-go (pure Go) added; `chunker.Tokenizer` interface + tiktoken adapter shipped; `--tokenizer` flag wired into registry; round-trip tests cover Lithuanian + English + emoji. Lt/En token-density ratio = 2.66, defaults (3600 cl100k_base tokens) still fit a 4k window comfortably for Lithuanian text. No behavior change yet — chunker call sites still word-based. Phase 2 next.
- **2026-06-08 00:05** — Phase 2 completed. Chunker rewritten to token-space sliding window with `prev||core||next` shape; `Defaults()` = 2800/400/400. `NewPipeline` and `NewTaskSvc` take `Tokenizer` arg; registry wires `bundle.Tokenizer` through. Spec §1 verification covered in `chunker_test.go`. End-to-end with real cl100k_base passes. `scripts/smoke.sh` green — surfaced a pre-existing `UpsertByHost` bug (parallel_fetches=0 → no reserves) and fixed in `4f78337`. Also pulled two unrelated operational fixes (`sweep-now` CLI + idempotent `lake_objects.Insert` on UNIQUE) into commits `104aa79` to address concurrent prod issues; merged to main in `7509375`. Phase 3 next.
