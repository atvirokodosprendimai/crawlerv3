# ADR-0009 — Token-sized chunks + per-collection chunker config

- Status: accepted
- Date: 2026-06 (chunker plan phases 1–5)

## Context

Pre-2026-06 chunker used byte-length windows. Two failures:

1. Embedding models tokenize, not bytes. Byte-windows overshoot the model's context (truncation = silent quality loss) or undershoot it (wasted recall).
2. Different collections embed against different models with different context budgets. One global chunk size is wrong for everyone.

## Decision

Two coupled changes:

**(a) Token-sized chunks with neighbor overlap.** `internal/infra/chunker/` got a tokenizer abstraction with a tiktoken adapter. Chunk windows now measured in tokens, with N-token overlap into the neighbor on each side.

**(b) Per-collection chunker config.** New table `collection_configs(collection, chunk_size_tokens, overlap_tokens, tokenizer)`. Resolved at chunking time via `app.CollectionConfigResolver`. CLI: `registry collection-config set/get/list/delete`. Falls back to global config when no row exists.

Operationally:
- `registry rechunk --collection X` reprocesses an existing collection's documents under new config, and **deletes old Qdrant points** for replaced chunks (no stale vectors).
- `registry sweep-now` forces an immediate lease sweep, useful in smoke + ops.

## Consequences

**+** Chunks fit the model. No silent truncation.
**+** Per-collection tuning without a redeploy.
**+** Idempotent rechunk: collection-scoped, with Qdrant cleanup.
**−** Tokenizer dep on the registry (tiktoken). Acceptable — small.
**−** `rechunk` is heavy. Operators must understand the embed-queue impact before running.
**−** Two sources of chunk truth (`document_chunks.text` + Qdrant payload). Drift risk — see AGENTS.md §14d.

## See also

- AGENTS.md §14d
- `eidos/` plans + specs for token-sized chunks
- `internal/infra/chunker/` — tokenizer adapter
- `internal/app/collection_resolver.go`
- README "Per-collection chunker config"
