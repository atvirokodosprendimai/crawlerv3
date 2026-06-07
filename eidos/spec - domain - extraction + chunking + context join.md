---
tldr: post-lake plain-text + chunk domain — defines the extracted_documents and document_chunks ports, the chunk→doc→lake→frontier context join, and the RequeueFilter convention
category: core
---

# extraction+chunking domain

## Target

The two domain packages that own the post-extraction text pipeline:
`internal/domain/extraction/` (plain text derived from a lake blob) and
`internal/domain/chunking/` (the embed-queue rows). Together they define
the ports the registry uses between "we have a clean text body" and "we
have a vector in the store".

This subsection inherits the system-level conventions (DDD layering, CQRS
R/W pools, three-queue shape, HMAC stateless lease, capabilities-as-strings,
declarative `pipeline_triggers`) from the system spec and does not restate
them.

## Behaviour

- An extracted document is a 1:1 derivative of one lake object — given a
  `lake_object_id` there is at most one extracted row, identified by a
  registry-assigned `int64` id. Re-running extraction is idempotent: the
  same source produces the same row (upsert semantics), not a duplicate.
- An extracted document carries the cleaned text plus a small bag of
  hints — `language`, `page_count`, and a `collection` string that
  pre-decides which vector-store collection the downstream chunks will
  land in. The collection is decided once, at extraction time, and
  rides with every chunk produced from this document.
- A chunk is identified by a UUID string (not an int64), is indexed by a
  per-document ordinal, and carries its own `embed_status` lifecycle:
  `pending → leased → done | failed`. Chunks are created in batches per
  document (insert-many is the only ingest path); they are never edited
  in place except via the status transitions.
- The chunk queue obeys the same reserve / lease / result / fail / sweep
  shape as the other two queues, with the embed worker as consumer. A
  reserve returns a batch of `LeasedChunk` (chunk + its HMAC token + TTL);
  result and fail must echo the lease token bytes back.
- A pending-count and a status-counts readout are first-class — the
  operator can ask "how big is the embed backlog" and "how is the embed
  fleet doing" without scanning rows.
- `ListSince` exists on both ports so external indexers can tail by
  cursor: extractions by monotonic `int64` id, chunks by
  `(embed_status, created_at)` because a chunk's interesting moment is
  when it changes status, not when it was first inserted.
- `GetContext(chunkID)` returns one denormalised view — the chunk's text
  and index, its parent document id and collection, the lake object id,
  and the canonical URL of the page the whole chain came from — in a
  single read. This is the only payload the embed accept-path needs to
  build a vector-store point; the embed worker never has to issue
  follow-up reads to assemble a payload.
- `RequeueByFilter` is the operator's reset knob. Any AND-combination of
  `(status, worker_id, document_id)` can be flipped back to `pending`,
  with zero-value meaning "no constraint on this dimension". A bare
  filter (all zeros) means "everything not already done" — explicitly
  not "literally every row", so a re-embed sweep can't trample
  successful work.
- `SweepExpired` honours the same three-queue contract: a chunk whose
  lease has elapsed without a result or heartbeat returns to `pending`
  and becomes reservable again. The sweeper is the only path that
  un-leases a row without a worker assertion.

## Design

### Two narrow ports, one join

The split into `extraction.Repository` and `chunking.Repository` mirrors
the underlying tables but, more importantly, mirrors who writes what:
extraction rows are written by the processing pipeline (one per lake
blob), chunk rows are written by the chunker step (many per extraction)
and mutated by the embed queue. Keeping them as separate ports stops
either ring of callers from accidentally reaching into the other's
lifecycle. {>> Both interfaces sit in `internal/domain/`, import only
`context` and `time`, and obey the "no infra imports" rule from the
system spec. <<}

### Collection is a property of the document, not the chunk

The vector-store collection is per-domain policy, decided once when text
is extracted. Chunks inherit it. The chunk-side `Collection` field is a
denormalisation, populated by the reserve-time join so the embed worker
gets it without an extra read. {>> Storing it on the chunk row at
ReserveBatch time, instead of re-joining at result time, keeps the
embed-result write path single-table. <<}

### Chunk IDs are UUIDs, document IDs are int64s

Documents are owned by the registry's monotonic id space — they need
`since_id` tailing, which is cheap on an int64. Chunks need to be
created in bulk, shipped to external workers, and referenced by lease
tokens and Qdrant point ids; a client-generatable UUID lets the
chunker assign ids before the insert round-trips and lets the vector
store treat the id as its own point id without an extra mapping table.

### Embed status is a string enum on the domain type

`EmbedStatus` is exposed as a typed `string` with four named constants
(`pending`, `leased`, `done`, `failed`). The status is part of the
domain language because external callers (`ListSince`, `RequeueFilter`,
`StatusCounts`) need to talk about it; it is not just an internal column.

### Lease signing is injected, not built in

`ReserveBatch` receives a `signLease func(chunkUUID, expires) (string, []byte)`
closure. The domain port does not know what HMAC secret is in play or
what header the worker will send back — that is the registry's
responsibility, supplied at call time. {>> This is how the same domain
package works under the system-level "HMAC stateless lease token"
contract without importing crypto or config. <<} The closure returns
both the wire string and the raw bytes because `MarkEmbedded` /
`MarkEmbedFailed` expect the raw bytes (defense-in-depth: the bytes
are also stored, per system spec).

### GetContext is a denormalised read, not a domain entity

The `Context` struct is deliberately *not* a chunk and *not* a document
— it is a read-model assembled from a four-way join
(chunk → extracted_document → lake_object → crawl_frontier) so the
embed accept-path has everything Qdrant needs in one shot: text,
collection, source-document id, lake id, and the original page URL.
Exposing this join as a domain method, rather than letting callers
hand-roll the join in app code, pins the join shape — if the chain
changes (e.g. lake gains an intermediate table) only this one method
moves. {>> The fact that `CanonicalURL` makes it onto the read model
means crawl_frontier is part of the join even though chunks are far
downstream of it; this is intentional, because the URL is the user-
visible "where did this come from" that the vector payload needs. <<}

### RequeueFilter — AND of optionals, zero means "any"

The filter is a flat struct of three fields, all optional, AND-ed
together. The convention "zero value = no constraint" is shared across
the codebase's filter types and makes the call site readable without a
builder. The intentional asymmetry is `Status == ""` meaning "any
non-done", not "literally any" — a guard against accidentally
re-embedding chunks that already succeeded. {>> A future fourth
dimension can be added without breaking the call signature, since the
filter is a struct, not positional args. <<}

### Insert-many is the only ingest path for chunks

The chunker produces all chunks for a document and hands them over as a
slice; there is no `Insert(one)`. This locks in the invariant that the
set of chunks for a document is materialised atomically — partial
chunk sets for a document should not be observable by the embed queue.

### ListSince cursors differ by table for good reasons

Extractions tail by `(since_id, limit)` because they're write-once and
their interesting moment is creation. Chunks tail by
`(status, since_created_at, limit)` because their interesting moment
is a status flip, and external indexers care about a specific status
slice (typically `done`). Putting status into the cursor signature
keeps the read path narrow.

## Interactions

- **Pipeline (`internal/app/pipeline`)** writes via `extraction.Repository.Upsert`
  after an internal or external processor produces clean text; then
  invokes a chunker that calls `chunking.Repository.InsertMany`.
- **Embed reserve handler** drives `ReserveBatch`, passing the HMAC
  signer wired up from the registry's lease secret.
- **Embed result handler** calls `GetContext` to build the Qdrant
  payload, then `MarkEmbedded` (or `MarkEmbedFailed`). Per the
  system-level fail-fast rule, a Qdrant upsert error bubbles out
  *before* `MarkEmbedded` is called, so the lease will expire and the
  sweeper requeues.
- **Sweeper goroutine** calls `SweepExpired` alongside the other two
  queues' equivalents on the same tick.
- **Read API for indexers** exposes `extraction.ListSince` and
  `chunking.ListSince` behind the `extracted_read` / `chunks_read`
  capabilities described in the system spec.
- **Operator CLI** uses `CountPending`, `StatusCounts`, and
  `RequeueByFilter` for backlog visibility and re-embed runs.
- **`pipeline_triggers`** drive extraction and chunking implicitly: the
  `lake_object_inserted` event is what causes a processor to eventually
  call `Upsert`; the chunker's completion is what causes an embed-queue
  row to exist. This domain package itself knows nothing about
  triggers — it only exposes the writes that triggers ultimately
  produce.

## Mapping

> [[internal/domain/extraction/document.go]]
> [[internal/domain/chunking/chunk.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
