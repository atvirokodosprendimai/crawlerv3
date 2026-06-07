---
tldr: four tiny per-system HTTP clients (Qdrant vectors, Ollama-style embed, Quickwit FTS, Stanza rewrite) — each enable-by-URL, each owning only the verbs the registry needs
category: core
---

# external clients

## Target

`internal/infra/qdrant/`, `internal/infra/embedclient/`, `internal/infra/quickwit/`, `internal/infra/stanza/`. Four sibling packages, each one Go file, each wrapping one external HTTP service the registry optionally talks to. None of them are ports — they are concrete adapters consumed by app-layer wiring (embed accept-path, FTS mirror, search endpoints).

## Behaviour

### Shared shape across all four
- An empty `BaseURL` constructs a disabled client. `Enabled()` reports the state. Disabled clients do not error on hot paths — they are silent no-ops on write/mirror, and return either an empty result or a clear "not configured" error on read.
- Each client owns its own `http.Client` with a `Timeout` (default 30s). No global pool, no shared transport.
- Optional API key is sent in whatever header the target system expects — Qdrant uses `api-key`, the other three use `Authorization: Bearer …`.
- Non-2xx responses surface the status + raw body in the returned error. Transport failures surface as-is. No retries, no circuit-breaker, no backoff — the caller decides what to do.
- Each package is self-contained: no shared HTTP helper, no shared envelope type. Copy-paste is preferred over premature abstraction.

### Qdrant (`qdrant`)
- Three verbs only: `EnsureCollection`, `Upsert`, `Search`.
- `EnsureCollection(name, dim)` is idempotent and memoises per-name in-process — subsequent calls for the same collection are free. The vector dimension is supplied lazily by the first observed embed; the package does not own it.
- Existence check tolerates Qdrant deployments that return 4xx for "missing" — anything other than 200 is treated as absent, only transport failures bubble.
- Create defaults: cosine distance, 9 shards, replication 1. Distance and shard count are config-tunable; replication is not.
- `Upsert` is point-keyed and idempotent on the caller's ID. Empty point slices are silently dropped. The call waits for indexing (`wait=true`) so the search path sees the upsert immediately. {>> `PUT /collections/{name}/points?wait=true` <<}
- `Search` is vector-in / hits-out with optional Qdrant filter map. Default limit 10. Payloads are returned, vectors are not.

### Embed client (`embedclient`)
- One verb: `Embed(text) → []float32`. Wire-compatible with Ollama `/api/embeddings`, which is also spoken by llama.cpp server and LocalAI — the same body and response shape works across all three.
- Default model `nomic-embed-text`, overridable per Config.
- Calling `Embed` on a disabled client is an error (not a silent passthrough) — there is no meaningful "empty vector" answer. The caller (search endpoint) explicitly checks `Enabled()` and chooses whether to refuse the request or fall back.
- An empty vector in the response is treated as a server-side failure and returned as an error, not as a zero-length success.

### Quickwit (`quickwit`)
- Two verbs: `Ingest(index, doc)` and `Search(index, query, limit)`.
- `Ingest` is one-doc-per-call, NDJSON-framed (single line + newline). The caller chooses the doc-mapping primary key — idempotency on re-ingest is delegated entirely to the Quickwit index config.
- Empty docs are silently skipped (same disabled-no-op contract).
- `Search` is plain query-string-in / hits-out. The full Quickwit hit envelope is preserved in `Hit.Doc`; the `_score` is also surfaced as a typed float for sorting/threshold use.
- Disabled-read returns an explicit "disabled" error so the FTS search endpoint can refuse cleanly rather than silently returning zero hits.

### Stanza (`stanza`)
- One verb: `Rewrite(text) → text`. The service is treated as opaque: text in, text out, no schema beyond `{"text": "..."}` both directions.
- Disabled client is a true passthrough — returns the input unchanged with a nil error. The pipeline can unconditionally call `Rewrite` regardless of configuration.
- Empty input also passes through. An empty response is treated as "no change" and the original is returned instead of an empty string.
- Default path `/rewrite`, overridable for deployments that mount under a different prefix.

## Design

### Why four sibling packages instead of one
Each external system has its own auth header convention, its own URL grammar, its own success/failure idioms (Qdrant's 4xx-for-missing, Ollama's empty-array-as-failure, Quickwit's NDJSON ingest, Stanza's opaque text echo). Folding them into a shared HTTP helper would either flatten away those quirks or grow into a leaky abstraction. Keeping each client small and self-contained lets the file fit in one screen and lets each one evolve independently when the upstream API changes.

### Empty-BaseURL-as-disabled is the universal toggle
No `if cfg.Enabled { ... }` plumbing at the registry boot site, no separate "enabled" boolean to keep in sync with the URL. Constructing the client is unconditional; the runtime cost of a disabled client is one branch per call. This is what makes Qdrant/Quickwit/Stanza individually optional adjuncts — operators set zero, one, two, or three URLs and the system trims itself accordingly.

### No retries, no circuit-breaker
The system-level "fail-fast on integrations" contract pushes retry semantics up to the queue layer: a failed upsert returns the error from the accept-path handler, the row is not marked done, the lease expires, the sweeper requeues. Adding retries inside the client would conflict with that — duplicate work, swallowed signal, harder reasoning about idempotency. The client's job is one round-trip per call.

### Per-name collection memoisation, not shared state
The Qdrant client caches "I have seen this collection" in a `sync.Map` keyed by name. {>> One `EnsureCollection` per collection per process lifetime <<} The decision is to keep the cache process-local and lossy: if the process restarts, the first call pays one HEAD-style GET. There is no negative cache, no TTL, no cross-process coordination — the registry is the only writer, and "exists once, exists forever" holds.

### Idempotency is delegated upward
None of the four clients enforce idempotency themselves:
- Qdrant relies on the caller-chosen point ID and Qdrant's own upsert semantics.
- Quickwit relies on the index doc-mapping primary key.
- Embed and Stanza are stateless single-call services.

The clients are dumb transport. Idempotency lives in the schema choices made by the calling app code.

### Defaults bias toward "works locally with zero config"
Qdrant cosine + 9 shards + replication 1; Ollama `nomic-embed-text`; Stanza `/rewrite`; 30s timeout everywhere. The intent is that an operator who spins up the canonical local stack (Qdrant + Ollama + Quickwit + a Stanza container) gets a working system by setting four URLs and nothing else.

### Why `Enabled()` rather than nil-check
A constructed-but-disabled client is still a real object that can be passed around, stored in app structs, and method-called safely. Storing `*Client` and conditionally constructing it would force every caller to nil-check. The chosen shape mirrors the `BlobStore` / `rwdb` style elsewhere in `internal/infra/` — adapters that are always present, sometimes inert.

## Interactions

- **embed accept-path → qdrant.Upsert** — the only write into Qdrant; per-domain collection name decided upstream in the embed service.
- **search endpoints → qdrant.Search / quickwit.Search** — read paths consumed by the public search API.
- **search endpoint → embedclient.Embed** — converts `query_text` to a vector server-side so HTTP callers don't need an embedding model.
- **FTS mirror (chunksink + pipeline) → stanza.Rewrite → quickwit.Ingest** — extracted text is rewritten by Stanza before landing in the Quickwit index; both steps are no-op if either URL is empty.
- **System-level "fail-fast on integrations" contract** — these clients return errors; the queue layer's lease-expire / sweeper requeue path handles retry.
- **No domain or app package imports any of these directly** — ports & adapters layering is preserved: the app layer holds the typed client struct as a dependency, the domain layer never references it.

## Mapping

> [[internal/infra/qdrant/client.go]]
> [[internal/infra/embedclient/client.go]]
> [[internal/infra/quickwit/client.go]]
> [[internal/infra/stanza/client.go]]
> [[eidos/spec - app - embed svc + qdrant search + per-domain collection.md]]
> [[eidos/spec - app - fts mirror + chunk sink.md]]
