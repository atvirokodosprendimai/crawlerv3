---
tldr: crawlerv3 system-level design — distributed crawler+data-lake with capability-gated any-language workers, three uniform queues, pluggable infra
category: core
---

# crawlerv3 — system design

## Target

The whole repository. A central Go `registry` process is the only stateful node; everything else is a stateless worker that can run on any host on the public internet.

## Behaviour

### From the operator's seat
- Seed a domain and one or more URLs. The crawler stays within seeded hosts by default; discovered links to unseeded hosts are silently dropped.
- Issue a PAT per worker box. PATs carry a capability set that gates everything the worker can do.
- See the pool, the queue depths, and the worker activity through CLI subcommands. No GUI exists.
- Mutate domains, triggers, workers, and queues at runtime — no restart, no migration round-trip.
- Migrate blobs between local FS and S3 without downtime; rows record `migrated_from` for audit.
- Switch the DB between SQLite, Postgres, and MySQL with the same code path and parallel migrations.

### From the worker's seat
- Reserve a small batch of work. Get back leased rows + an HMAC token.
- Do the work. Post the result or fail.
- Optionally heartbeat to extend the lease. If silent past the TTL, the registry's sweeper reclaims and re-queues.
- One protocol shape across crawl, processing, and embed queues — implementations differ only in payload.

### From the indexer's seat
- A separate worker class with `lake_read` / `extracted_read` / `chunks_read` capabilities tails the read API by `since_id` cursors and pushes into Qdrant / Quickwit / SQL indexes.
- Vector search is built in (registry-owned Qdrant client); FTS is built in (Quickwit + Stanza rewriting).

## Design

### Layering — DDD ports & adapters
Three concentric rings, strict import direction:

- `internal/domain/` — pure types, port interfaces. No infra imports of any kind. The "what".
- `internal/app/` — use cases, compose ports. The "when and why".
- `internal/infra/` — concrete adapters (gorm, chi, qdrant, FS, S3, …). The "how".
- `cmd/` — process entrypoints, wire concrete adapters to use cases.

Rule the codebase enforces: a domain package that imports gorm is a bug. Adapters can swap freely (FS↔S3, SQLite↔Postgres, Qdrant↔future-vector-store) without touching app or domain.

### CQRS — single writer, many readers
Every DB handle is two pools:
- `R` (read) — `NumCPU` for SQLite, `NumCPU*4` for Postgres/MySQL.
- `W` (write) — `1` for SQLite (avoids `database is locked`), `NumCPU` for Postgres/MySQL.

App code stays dialect-agnostic by following the rule: SELECTs on R, writes on W, multi-statement units via `WriteTX` / `ReadTX`. The same shape is kept across all three DBs even where Postgres/MySQL don't strictly need it.

### Capabilities — strings drive everything
A worker is a row with a JSON array of capability strings. Two flavors look identical on the row but differ in origin:
- **Endpoint-gated** (`crawl`, `embed`, `lake_read`, `extracted_read`, `chunks_read`) — hard-coded `wk.Can(...)` checks inside registry handlers.
- **Worker-declared** (`pdf_ocr`, `html_strip`, `vvtat`, `domain:foo.com`, anything) — opaque strings the registry only string-matches against. Adding a new processor or per-domain binding requires zero registry code change.

Empty capabilities array = "any" for backcompat with early slices. As soon as the operator sets one cap, the empty-shortcut is gone — every cap the worker needs must be listed.

The capability set on the wire is for operator visibility only. Authorization always reads the server-stored set (loaded at PAT-auth time), never the request body — workers cannot spoof their way into a bound domain or a privileged endpoint.

### Three queues, one shape
`crawl_frontier`, `processing_jobs`, `document_chunks`. Each goes through:

```
reserve → lease + sign HMAC token → result/fail → release
                                  ↘ heartbeat → extend
                                  ↘ silence → sweeper reclaim → requeue with backoff
```

The lease token is stateless (HMAC-SHA256, no DB lookup needed to verify), and the bytes are *also* stored once issued — defense in depth, so a leaked secret doesn't immediately enable forging leases against stored rows.

### Scope-lock by default
`AllowAutoDomains=false` is the default. A discovered `<a href>` is only enqueued if its host already exists in `domains` and is active. The crawler will not wander off `9g.lt` because a page links to `google.com`. Legacy "follow anything" is one flag away (`--allow-auto-domains`) but explicitly opt-in.

A `--max-depth` cap can globally bound recursion; links exceeding it are dropped at intake.

### Declarative routing — `pipeline_triggers`
Replaces the original hardcoded MIME→processor map. Rows describe `(event, filter) → enqueue_kind`. The dispatcher fires after every result write. Events today: `lake_object_inserted`, `blob_produced`. Cache TTL 5s — edits propagate without restart. Many triggers can match one event; each enqueues one row.

A new processor + a default trigger seeded by migration is sufficient to wire a new pipeline stage end-to-end. No handler code touched.

### Internal vs external processors
`Pipeline.InternalProcessors` lists what the in-process goroutine claims (`html_strip`, `text_passthrough` today). Everything else stays `queued` for an external task worker to reserve. Cheap CPU-only work goes internal; anything needing GPU / shell-out / external tooling stays external.

### Per-domain politeness + per-domain binding
Two columns on each `domains` row:
- `crawl_delay_ms` (default 1000) — min gap between reserves of that domain.
- `parallel_fetches` (default 1) — max URLs handed out per reserve from that domain.

Peak rate per domain ≈ `parallel_fetches / crawl_delay_ms`. Cooperative hosts can be turned up; the same field set enforces politeness uniformly across worker types.

Optional `required_capability` column binds a domain to a worker class (`js_render`, `auth_required`, `domain:foo.com`, etc). The reserve SQL filters server-side using the *stored* worker capability set.

### Worker tiers
A spectrum from "one binary, many roles" to "one binary, one site":

| Bin | Role |
|---|---|
| `worker` | reference Go crawl worker, streaming pipeline |
| `taskworker` | generic external processor, shell-out, single kind |
| `ocrworker` | dedicated PDF OCR, page-parallel inside one PDF |
| `embedworker` | Ollama fleet round-robin, batch `/api/embed` |
| `agent` | unified — crawl + multiple kinds + embed in one process |
| `litekoworker` | Liteko WebForms POST pagination, no browser |
| `unicrawler` | YAML-configured Selenium-driven universal crawler |
| `migrator` | local↔S3 blob mover |

### Pluggable everything
- **Blob store** — `local`, `s3`. Adding `gcs` = one package implementing `lake.BlobStore`. Lake rows track `storage_backend` + `migrated_from`.
- **DB driver** — `sqlite`, `postgres`, `mysql`. Same migrations (mirrored 3x), same gorm models, dialect branches only where time/SQL syntax forces it.
- **Vector store** — Qdrant lives behind a small client struct in `internal/infra/qdrant/`. Same shape can be re-implemented for any other vector backend.
- **FTS** — Quickwit + Stanza rewriting are optional adjuncts; the pipeline only forwards if `FTS` is set.

### Fail-fast on integrations
When an accept-result path hits an external failure (Qdrant upsert, BlobStore put), the handler returns the error WITHOUT marking the row done. The lease expires → sweeper requeues → another worker retries. The system never silently swallows.

### Verified via shell smokes
No unit tests yet. End-to-end smokes under `scripts/smoke*.sh` exercise every slice against ephemeral state (fake Qdrant, fake Ollama, ephemeral SQLite, real binaries). All must pass before declaring a slice done.

## Interactions

- **Operator → registry CLI** — all control is CLI flags + env vars + JSON over HTTP.
- **Worker → registry HTTP** — PAT-authenticated, JSON. Three reserve/result/fail families share shape.
- **Registry → blob store** — `lake.BlobStore` port. Adapter picked at startup by `--blobs-root` / `--s3-*` flags.
- **Registry → DB** — `rwdb.DB` (R+W pools). Driver picked at startup.
- **Registry → Qdrant** — optional. Embed accept-path upserts vectors; `/v1/search` queries.
- **Registry → Quickwit + Stanza** — optional. `chunksink` + `pipeline` mirror extracted text after FTS rewrite.
- **Pipeline goroutine ↔ processing_jobs** — claims internal-processor rows on a tick.
- **Sweeper goroutine ↔ all three queues** — every 30s, reclaims expired leases.
- **External indexer worker → read API** — tails `lake_objects` / `extracted_documents` / `document_chunks` by `since_id` cursor.

## Mapping

> [[README.md]]
> [[AGENTS.md]]
> [[internal/domain/]]
> [[internal/app/]]
> [[internal/infra/]]
> [[cmd/]]
> [[scripts/smoke*.sh]]
