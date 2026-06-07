---
tldr: token-sized chunks (default 2800 core + 400 prev + 400 next = 3600 tokens) with per-collection sizing and an operator CLI to drop+rechunk+re-embed every document in a collection
category: core
status: draft
---

# token-sized chunking + per-collection rechunk

## Target

Three coupled changes:

1. **`internal/infra/pipeline/chunker/`** — replace the word-count split with a token-aware split sized to fit a model's context window with neighbor overlap on both sides.
2. **New `collections` table** (or `domains` columns — see Design) — per-collection chunk sizing config (tokens, prev-overlap, next-overlap, tokenizer).
3. **New `registry rechunk` CLI subcommand** — drop every chunk for documents in a given collection, re-split with the current config, re-enqueue for embed, and delete the stale vectors from the corresponding Qdrant collection.

Touches: `internal/domain/chunking/`, `internal/app/pipeline.go` + `tasksvc.go` (where chunker.Split is called), `internal/infra/qdrant/`, `cmd/registry/main.go`.

## Behaviour

### Chunk shape

- A chunk's payload is exactly three concatenated regions: **prev-overlap**, **core**, **next-overlap**. Default sizes in tokens: `400 / 2800 / 400` → 3600 tokens total — fits comfortably inside a 4k context window with prompt overhead.
- A chunk's **core** never overlaps with a sibling chunk's **core**. Step between consecutive cores equals the core size — no duplication of unique content across chunks.
- The **prev-overlap** of chunk `i` is exactly the last `W_prev` tokens of chunk `i-1`'s core. The **next-overlap** of chunk `i` is exactly the first `W_next` tokens of chunk `i+1`'s core. Boundary chunks have an empty prev-overlap (chunk 0) or empty next-overlap (last chunk).
- For documents shorter than one core, a single chunk holds the entire document and both overlap regions are empty.
- Token counting uses the registry-configured tokenizer (default: `cl100k_base` via tiktoken-go). A per-collection override may select a different tokenizer for collections whose embedding model has a known incompatible BPE.
- `chunk.TokenCount` records the **core** size, not the total — downstream consumers reason about new information per chunk.
- The stored `chunk.Text` is the concatenated `prev || core || next`, ready for the embed worker to pass straight to the model.

### Per-collection config

- A collection has a row in `collections (name TEXT PRIMARY KEY, chunk_tokens INT, overlap_prev INT, overlap_next INT, tokenizer TEXT, ...)`.
- Documents whose resolved collection has no row use the registry's default (2800 / 400 / 400 / `cl100k_base`).
- An operator can change a collection's config at runtime; the change applies to **future** chunking only. Existing chunks keep their original sizing until a `rechunk` is run.
- A collection's `tokenizer` field is informational/operational: the chunker uses it; the embed worker is not required to use the same tokenizer at inference (most embedding APIs handle whatever string they receive).

### Rechunk CLI

- `registry rechunk --collection <name>` drops every `document_chunks` row whose document's resolved collection equals `<name>`, re-splits the document's `extracted_text` with the collection's current config, inserts fresh rows with `embed_status='pending'`, and deletes the corresponding points from the Qdrant collection. The embed worker picks the new rows up on the next `/v1/embed/reserve`.
- `--collection -` matches the default-collection bucket (documents whose host has no `embed_collection` override).
- `--dry-run` reports counts without writing.
- `--since-doc-id N` and `--limit N` cap scope for incremental runs.
- The whole operation is **idempotent**: running it twice with no config change is a no-op for the second run after the embed worker has drained the queue (chunks are at `embed_status='done'`, re-splitting yields the same byte content, but the run still touches them — see Friction).
- A rechunk in flight is observable via `registry queue-stats` showing the chunk queue growing.
- The operation is **best-effort transactional per document**: each document's old chunks, Qdrant points, and new chunks are committed in one DB transaction; a registry crash mid-run leaves documents in either fully-old or fully-new state, never a mix.

### What does NOT happen

- Rechunk does **not** re-fetch source URLs or re-process raw HTML. It re-splits whatever `extracted_text` is already on disk.
- Rechunk does **not** rerun extraction (`html_strip`, `pdf_ocr`, etc.) — the extracted-text contract is the input.
- Rechunk does **not** touch documents whose collection differs from `--collection`.
- Old chunk UUIDs are **not** preserved across a rechunk. The embed worker treats new chunks as brand-new work; any references held externally (e.g., a search-result citation that included a chunk_id) become stale after rechunk. Operators should treat citations of `chunk_id` as ephemeral.

## Design

### Token-aware sliding window
The split loop is a sliding window in token space, not word space. {>> `tokens := tok.Encode(text); for start := 0; start < len(tokens); start += chunkTokens { … }` where `chunkTokens` is the *core* size, not the *total* size <<} For each step, the core is `tokens[start : start+chunkTokens]`, the prev-overlap is the last `W_prev` tokens of the previous core, the next-overlap is the first `W_next` tokens of the next core. Encode→slice→decode the three regions, concatenate, store.

The split returns chunks in document order; their `chunk_index` is the loop iteration. `chunk.TokenCount` is the core length, used for analytics and to verify the contract `Σ core ≈ document total`.

### Tokenizer abstraction
`chunker.Tokenizer` interface:
```go
type Tokenizer interface {
    Name() string
    Encode(s string) []int
    Decode(ids []int) string
}
```
Default registry-wired implementation: `tiktoken-go` with `cl100k_base`. Plugging in `sentencepiece` for collections that pair with a SentencePiece-tokenized embed model is a one-package addition. The chunker package depends only on the interface; the wiring lives in `cmd/registry/main.go`. {>> tokenizer choice is a startup decision per default-collection + override per collection row; the chunker does not auto-detect from the embed model <<}

### Per-collection config — collections table
A new `collections` table over a `domains` column extension because:
- A collection is shared by N domains today (multiple sites can target one collection); the config lives where the entity does.
- Future per-collection knobs (search re-ranking, hybrid weights, vector dim, …) all want this table.
- Migration adds the table; the resolver becomes "look up collection name from the existing domain resolver → look up config from `collections` → fall back to defaults". Same shape as `embed_collection` resolution today.

Schema:
```sql
CREATE TABLE collections (
    name          TEXT PRIMARY KEY,
    chunk_tokens  INTEGER NOT NULL DEFAULT 2800,
    overlap_prev  INTEGER NOT NULL DEFAULT 400,
    overlap_next  INTEGER NOT NULL DEFAULT 400,
    tokenizer     TEXT    NOT NULL DEFAULT 'cl100k_base',
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```
Pre-seeded: no rows. Empty table means "use defaults for every collection." Operators add rows only when they want a deviation.

### Resolver — CollectionConfigResolver
Mirrors the existing `CollectionResolver`. Service method: `ResolveConfig(ctx, collectionName) → (Config, fromTable bool, err)`. Returns the table row if present, otherwise the registry defaults. The chunker is constructed with this resolver and asks for config per document, not per chunk — so a config change between chunks of the same document is impossible.

### Reindex transaction shape
Per document:
1. Open `WriteTX`.
2. Collect chunk UUIDs for the document into a slice (held in memory).
3. `DELETE FROM document_chunks WHERE document_id = ?`.
4. Re-split text → `INSERT` new chunk rows with `embed_status='pending'`.
5. Commit.
6. Outside the DB transaction, call `qdrant.DeletePoints(collection, oldChunkUUIDs)`.

The Qdrant delete is *after* commit because Qdrant is not part of the DB transaction; if Qdrant delete fails, the registry logs and proceeds — the stale points will be overwritten/orphaned. A periodic Qdrant orphan-sweep is a follow-up, not part of this spec.

### Why not a pipeline trigger
A `rechunk` could in theory be modeled as a trigger event that re-runs the chunking processor over existing extracted_documents. It is not, because:
- The processing pipeline is forward-only: lake_object → extraction → chunks → embed. Rechunk is a backwards operation that mutates a downstream stage in place.
- Triggers fire on events. There is no natural "config changed" event in the trigger schema, and adding one is more surface than a CLI command.
- The CLI command makes scope explicit (`--collection`), supports dry-run, and is the right shape for an out-of-band repair.

### CLI shape
`registry rechunk --collection <name>` (uppercase verb in source: `actionRechunk`). Flag conventions follow the existing `requeue-*` family — at least one filter required, `--dry-run` reports counts, `--limit` caps. Output is line-per-document `doc_id=X chunks_old=N chunks_new=M qdrant_deleted=K`.

### Storage cost
Chunks become roughly 9× larger in text bytes than today (3600 vs ~400 token-equivalents). The chunks table grows accordingly. For collections that don't need the larger window, the per-collection override is the lever. Operators should not naively run a global rechunk to 2800/400/400 across every collection.

## Verification

A passing implementation can be checked:

1. **Unit on chunker.Split**: given a 10,000-token synthetic document and config 2800/400/400:
   - Number of chunks = `ceil(10000 / 2800)` = 4.
   - Chunk 0: prev=∅, core=2800, next=400.
   - Chunk 1: prev=400, core=2800, next=400.
   - Chunk 3 (last): prev=400, core=1600, next=∅.
   - For every chunk, prev tokens == previous chunk's last 400 core tokens (encode-decode round-trip identity assumed for the test tokenizer).
2. **Short-document case**: 500-token doc → 1 chunk, prev=∅, next=∅, core=500.
3. **Rechunk smoke**: seed 3 docs into a test collection, run rechunk with `chunk_tokens=200, overlap_prev=20, overlap_next=20`, assert (a) old chunk rows deleted, (b) new chunk count matches `ceil(token_count / 200)` per doc, (c) all new chunks at `embed_status='pending'`, (d) Qdrant has zero points referencing old UUIDs.
4. **Config-update + rechunk integration**: update a `collections` row's `chunk_tokens`, run rechunk, assert new chunks reflect new sizing.
5. **Dry-run**: assert exit zero, no writes, count-only report.

## Interactions

- **`chunker.Tokenizer` interface** — new in this spec. Default impl in `internal/infra/tokenizer/tiktoken/`.
- **`internal/app/pipeline.go` (`execHTMLStrip`) + `tasksvc.go` (`AcceptText`)** — both call `chunker.Split(...)`. Both gain a `Tokenizer` field on their struct and pass it into the chunker; both also need access to the `CollectionConfigResolver` to ask for the right size per document.
- **`collections` table** — read at chunk time; mutated by a new `registry create-collection` / `update-collection` CLI (defer this to a follow-up if the table is operator-edited via SQL initially).
- **`document_chunks` rows** — same schema; only content changes. `chunk.TokenCount` semantic clarified to mean the *core* size.
- **Qdrant client (`internal/infra/qdrant/`)** — needs a `DeletePoints(collection, ids)` method if not already present. Add if missing.
- **Embed worker** — no protocol change. Sees new pending chunks via the existing reserve endpoint.
- **`registry queue-stats`** — already shows `document_chunks` pending counts; will surface the spike during a rechunk run.
- **`registry reprocess`** — orthogonal: that re-enqueues processing_jobs (extraction). Rechunk operates downstream of extraction.

## Friction / Open Questions

- **Tokenizer accuracy.** `cl100k_base` is a strong default for English/Western text but undercounts tokens for Lithuanian (and other agglutinative or non-Latin scripts) compared to a model's own tokenizer. For Liteko's Lithuanian text + `bge-m3`, a SentencePiece tokenizer would be more accurate. The chunker over-shoots the model's true context budget by ~10-20% with cl100k_base; the conservative defaults (3600 total inside a 4k window) absorb this. Per-collection override is the escape hatch.
- **Idempotence under embed-worker concurrency.** If a rechunk runs while the embed worker is mid-batch on the same collection, the worker may post a result for a chunk_id that no longer exists. The embed-result path already handles "chunk gone" gracefully (lease verification fails) but worth confirming under load.
- **Chunk UUID stability across rechunks.** Old citations of `chunk_id` break. Acceptable but should be called out in the search/citation surface docs (the consumer surface area isn't fully specced today).
- **Storage size.** 9× growth in `document_chunks.text` bytes at the new defaults. For a 100k-document collection with average 10k tokens/doc this is multi-GB. Operators sizing the registry box need to know.
- **Qdrant orphan vectors.** If `qdrant.DeletePoints` fails (registry crash, network), stale points linger and pollute search until a sweep. Not addressed here.
- **Cross-collection moves.** If a domain's `embed_collection` is changed, existing chunks remain in the old collection until a rechunk on both sides. Out of scope here; behavior of "move docs between collections" is its own spec.
- **Chunk-text duplication tax.** Each token in a doc is now physically stored ~`(W_prev + chunk_tokens + W_next) / chunk_tokens` times in the chunks table (= 3600/2800 = 1.29×). The Qdrant payload duplicates that again. Not pathological at defaults but compounds with bigger overlaps.

## Mapping

> [[internal/infra/pipeline/chunker/chunker.go]]
> [[internal/domain/chunking/chunk.go]]
> [[internal/app/pipeline.go]]
> [[internal/app/tasksvc.go]]
> [[internal/infra/qdrant/]]
> [[cmd/registry/main.go]]
> [[eidos/spec - infra - in-process processors htmlproc + chunker]]
> [[eidos/spec - domain - extraction + chunking + context join]]
> [[eidos/spec - app - embed svc + qdrant search + per-domain collection]]
