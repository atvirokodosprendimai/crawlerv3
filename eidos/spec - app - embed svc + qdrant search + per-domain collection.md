---
tldr: embed reserve/result writes raw vectors to per-domain Qdrant collections resolved from chunk→lake→frontier→domain; SearchSvc queries the same collection
category: core
---

# embed svc + search + collection

## Target

The three app-layer facades that own the vector side of the system: `EmbedSvc` (chunk reserve / accept-vector / accept-fail), `SearchSvc` (vector + text retrieval), and `CollectionResolver` (the chain that decides which Qdrant collection a chunk belongs to). Covers `internal/app/embed.go`, `internal/app/search.go`, `internal/app/collection.go`.

## Behaviour

- An embed worker reserves a batch of pending chunks, embeds them off-process, and posts back **the vector itself** — the registry is the only thing that talks to the vector store. Legacy workers may still post a pre-resolved `vector_id` instead, and that path still works.
- Each chunk ends up in a collection derived from the **source domain**, not from the chunk's content or the worker's choice. Two chunks from the same domain land in the same collection; a domain with no override lands in a collection named after its host.
- The collection lookup is best-effort. If any link in the chain (chunk → lake row → frontier row → domain) is missing or unresolvable, the resolver returns empty and the embed worker is told to decide — the system never invents a collection name from partial data.
- Empty vectors and bad lease tokens are rejected before any external call. A valid token plus a non-empty vector is the only thing that earns a Qdrant write.
- When the Qdrant upsert fails, the chunk is **not** marked done. The lease expires, the sweeper requeues, and another worker tries again. The registry never silently swallows a vector that didn't reach the store.
- Collections are created on demand at the dimensionality of the first vector that arrives — the registry does not pre-declare them.
- Vector ID written back to the row encodes where the vector actually lives: `qdrant:<collection>:<chunk_id>` when Qdrant accepted it, `raw:<chunk_id>` when no vector store is configured (a test-only fallback that effectively drops the vector).
- The search facade refuses to run without a configured vector store and without an explicit collection name — callers must say where to look.
- A search caller can pass either a precomputed vector or raw text. Text-mode requires a configured embedding client; otherwise it fails loudly rather than guessing.
- Search hits surface the chunk's identity (chunk_id, lake_object_id, document_id, chunk_index), its retrieval payload (text, URL), and the collection it came from — enough for the caller to display a result and to fetch the canonical row.

## Design

### EmbedSvc is a facade, not the embed pipeline

The embedding *work* lives outside the registry — in `embedworker`, `agent`, etc. EmbedSvc only owns the **lease lifecycle** and the **persistence of results**. This keeps the registry single-writer for chunk rows and means the vector-store integration sits in exactly one place. {>> `Service` and `EmbedSvc` are separate so the registry can run with embed disabled — no Qdrant, no embed routes, no problem <<}

### Vectors-on-the-wire, not vector_ids

Earlier worker protocols had the worker pick a `vector_id` and the registry just record the string. That made the worker responsible for talking to Qdrant, which meant N copies of vector-store config, N retry policies, and no central enforcement of collection naming. `AcceptVectorResult` flips that: the worker ships raw `[]float32`, the registry owns the upsert. `AcceptResult` is kept only for legacy workers. {>> Two methods exist side by side; the legacy path will be removed once no worker uses it <<}

### Fail-fast on external integrations

When Qdrant returns an error, the handler returns the error **without** marking the chunk embedded. This deliberately exploits the three-queue contract — lease expiry + sweeper requeue is the retry mechanism, so EmbedSvc has no retry logic of its own. The same rule applies system-wide; see system spec § Fail-fast on integrations.

### Collection naming is a domain-level decision

Two layers of choice on the `domains` row:
1. Explicit `embed_collection` override — operator picks the name.
2. Fall back to `host` — one collection per crawled site.

Never the chunk, never the lake object, never the worker. The reasoning: collections are operationally scoped (you might want to drop, re-embed, or re-index all of `foo.com` independently of `bar.com`), and the domain is the natural unit for that. Per-document collections would explode collection count; per-MIME would scatter related content.

### Resolver is best-effort, callers handle empty

`ResolveForLakeObject` returns `""` on any failure rather than an error. The decision: the resolver is advisory — when the chain is broken (missing frontier row, deleted domain, etc.) the embed worker can still process the chunk, it just won't have a collection hint. The empty-string contract is documented at the type level so callers cannot accidentally treat absence as a real collection name. {>> Inside `AcceptVectorResult`, an empty `cc.Collection` is replaced by `_default` for the Qdrant call — this is the only place a synthetic name appears, and only because Qdrant itself requires a non-empty string <<}

### Resolver chain is layered through domain ports

`lake.Repository` → `frontier.Repository` → `frontier.DomainRepo`. Three separate ports, composed in `app/`, not a single super-repo. This preserves the DDD rule that domain packages don't know about each other's storage and keeps the join logic at the use-case layer where it belongs.

### Payload is what makes hits useful without a second round-trip

`AcceptVectorResult` stuffs `lake_object_id`, `document_id`, `chunk_index`, `text`, `url`, and `collection` into the Qdrant point payload. `SearchSvc.mapHits` reads them straight back out. The decision: callers should be able to render a search result page from the vector store alone, without round-tripping to SQL for every hit. SQL is still authoritative for ranking/filtering at query time; payload is for the display path.

### SearchSvc is read-only and stateless

No DB handle, no lease signer, no chunk repository. Just a Qdrant client and an optional embed client. {>> This is the only `app/` service that does not touch `rwdb.DB` — search reads from Qdrant directly <<} Enforces the CQRS rule from the system spec (see § CQRS): the read side stays out of the write pool.

### Text search degrades to a clean error, never to a heuristic

When `SearchByText` is called but no embed client is wired, the response is an explicit error mentioning the missing `--embed-url` flag. The decision: never fall back to keyword search or to a zero-vector — that would silently degrade the result set. Operator must see the misconfiguration.

### Optional Qdrant, optional embed client

Both `SearchSvc.Qdrant` and `EmbedSvc.Qdrant` are nil-safe. The registry can boot, accept chunks, and let the legacy `AcceptResult` path store vector_ids, all without a vector store. This matches the broader pluggable-infra stance from the system spec (see § Pluggable everything) — vector storage is an adjunct, not a hard requirement.

## Interactions

- **HTTP layer** — `/embed/reserve`, `/embed/result`, `/embed/fail` are gated by the `embed` capability (endpoint-gated; see system spec § Capabilities). `/v1/search` reads from SearchSvc.
- **lease.Signer** — issues chunk leases on reserve, verifies on result/fail. Same HMAC machinery as the crawl and processing queues (system spec § Three queues).
- **chunking.Repository** — single writer of chunk rows. `MarkEmbedded`, `MarkEmbedFailed`, `SweepExpired` are its only write methods exposed here.
- **lake / frontier / DomainRepo** — read-only join chain for the resolver. Resolver is constructed once and called per chunk reserve to stamp `embed_collection_hint` on the leased rows.
- **qdrant.Client** — single instance owned by the registry. EmbedSvc writes, SearchSvc reads. Collection creation is lazy; dimensionality comes from the first vector.
- **embedclient.Client** — optional, used only by SearchSvc to embed query text. The same kind of client lives independently inside `embedworker` for chunk embedding — they do not share an instance.
- **Sweeper goroutine** — calls `EmbedSvc.SweepExpired` on tick. Stuck leases re-enter the pending pool; payload (vector) is never carried across attempts because workers always recompute.

## Mapping

> [[internal/app/embed.go]]
> [[internal/app/search.go]]
> [[internal/app/collection.go]]
> [[internal/domain/chunking/]]
> [[internal/domain/lake/]]
> [[internal/domain/frontier/]]
> [[internal/infra/qdrant/]]
> [[internal/infra/embedclient/]]
> [[internal/infra/lease/]]
