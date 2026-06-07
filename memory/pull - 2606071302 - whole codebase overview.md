# pull - whole codebase overview

**Target:** entire `crawlerv3` repo. ~84 Go files, ~13k LOC.
**Mode:** multi-pass. This doc maps territory; subsection pulls follow via plan-continue.

## Territory map

System = distributed web crawler + data lake. Central Go `registry` hands work to anonymous-but-authed workers over HTTP. Pluggable blob store (local FS / S3). Pluggable DB (SQLite / Postgres / MySQL). Optional Qdrant for vector search + Quickwit for FTS.

Architecture = DDD ports-and-adapters + CQRS (R/W pool split, even on SQLite). Capabilities-as-strings drive endpoint auth, task-kind dispatch, and domain↔worker binding. Three queues share one shape: reserve → lease → result/fail → release. HMAC lease tokens, sweeper goroutine reclaims expired.

## Major subsections

### A. domain layer (`internal/domain/`)
Pure types + port interfaces. No infra imports.
- `frontier/` — URL queue, domain table, politeness rules
- `lake/` — blob index + `BlobStore` port
- `processing/` — processing_jobs + capability matching
- `extraction/` — extracted_documents
- `chunking/` — document_chunks + chunk context join
- `triggers/` — pipeline_triggers declarative routing
- `workerid/` — worker pool + PAT

### B. app layer (`internal/app/`)
Use-case orchestrators. Compose ports.
- `service.go` — crawl Reserve/Heartbeat/Result/Fail facade
- `pipeline.go` — internal processor goroutine (`html_strip`, `text_passthrough`)
- `tasksvc.go` — external task svc (`/v1/tasks/*`)
- `embed.go` — embed svc + Qdrant upsert path
- `dispatcher.go` — trigger evaluation + fanout
- `collection.go` — per-domain embed collection resolver
- `search.go` + `fts.go` — vector + FTS query orchestration
- `chunksink.go` — chunk insert + Quickwit mirror
- `migrator.go` — blob backend mover

### C. infra/db (`internal/infra/db/`)
- `rwdb/` — two-pool CQRS handle (R+W, dialect-aware)
- `gormrepo/` — gorm-tagged models + port impls
- `migrations/{sqlite,postgres,mysql}/` — goose, dialect-mirrored

### D. infra/http (`internal/infra/http/`)
chi router + PAT auth + handlers. One file per concern: `jobs.go`, `tasks.go`, `embed.go`, `reads.go`, `blobs.go`, `search.go`, `fts.go`, `auth.go`, `access_log.go`, `policy.go`, `server.go`, `json.go`.

### E. infra/store (`internal/infra/store/`)
- `local/` — filesystem BlobStore
- `s3/` — aws-sdk-go-v2 BlobStore (MinIO/R2 compatible)

### F. infra/pipeline (`internal/infra/pipeline/`)
In-process processors.
- `htmlproc/` — HTML → plain text
- `chunker/` — word-based chunking
- `pdfproc/` + `docxproc/` — stubs

### G. infra/lease (`internal/infra/lease/`)
HMAC-SHA256 stateless lease tokens. Three shapes: crawl, chunk, task.

### H. infra/urls (`internal/infra/urls/`)
Canonicalization (host lowercasing, default-port strip, fragment strip, etc).

### I. infra/qdrant + embedclient + quickwit + stanza
External-system HTTP clients.

### J. cmd/registry (`cmd/registry/`)
Control plane: `serve`, `migrate`, `create/list/update/ban-worker`, `seed/list/update/activate/deactivate-domain`, `enqueue`, `reprocess`, `trigger-*`, `queue-stats`, `requeue-*`, `release-worker`.

### K. cmd workers (`cmd/{worker,taskworker,ocrworker,embedworker,agent,litekoworker,unicrawler,migrator}/`)
- `worker` — reference crawl worker (streaming pipeline, single reserver + N fetchers)
- `taskworker` — generic single-kind external processor (shell-out, text or blob mode)
- `ocrworker` — dedicated PDF OCR (mutool + gs + tesseract page-parallel)
- `embedworker` — Ollama fleet round-robin, `/api/embed` batch
- `agent` — unified worker, many kinds in one process
- `litekoworker` — Liteko-specific WebForms POST pagination
- `unicrawler` — universal YAML-configured Selenium-driven worker
- `migrator` — local↔S3 blob mover

### L. config + logx (`internal/config`, `internal/infra/logx`)
Config loader + structured logging.

## Cross-cutting concepts

These threads cross multiple files — spec separately to avoid duplication:
- **Capabilities model** — endpoint-gated vs worker-declared, empty=any backcompat trap
- **Lease lifecycle** — three queues, one shape, sweeper TTL, HMAC stateless
- **CQRS / dialect parity** — every migration mirrored 3x, R for SELECT, W for write
- **Trigger evaluation** — declarative routing replaces hardcoded MIME map
- **Scope-lock** — `AllowAutoDomains` default false; only seeded hosts get crawled

## Existing spec coverage

None. Empty `eidos/` dir.

## Intent sketch (overview level)

What the system promises:
- Operator seeds domains + URLs; system crawls within scope, indexes blobs, processes per content-type, chunks + embeds, exposes vector + FTS search.
- Any-language workers, anywhere on internet, with PAT auth + capability gating.
- Three queues, same protocol (reserve → lease → result/fail → release). Sweeper guarantees liveness.
- Pluggable storage (FS↔S3), pluggable DB (sqlite↔pg↔mysql), pluggable vector store. Same code path.
- Declarative routing (`pipeline_triggers`) — operator wires new processors with zero registry code change.
- Per-domain politeness + per-domain worker binding via capability strings.

Behavioral claims survive rewrite. Mechanism (gorm/chi/goose) does not — those are adapter choices.

## Subsection pull plan

One spec per subsection. Roughly:

1. `spec - architecture - ddd ports adapters + cqrs + capabilities`
2. `spec - domain - frontier + url scope + politeness`
3. `spec - domain - lake + blob store port`
4. `spec - domain - processing jobs + capability dispatch`
5. `spec - domain - extraction + chunking + context join`
6. `spec - domain - triggers declarative routing`
7. `spec - domain - workerid + PAT + capabilities`
8. `spec - app - service crawl orchestration`
9. `spec - app - pipeline internal processors`
10. `spec - app - tasksvc external workers`
11. `spec - app - embed svc + qdrant + search + fts`
12. `spec - infra - rwdb CQRS + dialect parity + migrations`
13. `spec - infra - http chi + PAT auth + handlers`
14. `spec - infra - lease HMAC stateless tokens`
15. `spec - infra - blob stores local + s3`
16. `spec - infra - pipeline processors htmlproc + chunker + stubs`
17. `spec - infra - urls canonicalization`
18. `spec - infra - external clients qdrant + embedclient + quickwit + stanza`
19. `spec - cmd - registry CLI surface`
20. `spec - cmd - worker reference crawl streaming pipeline`
21. `spec - cmd - taskworker generic shell-out`
22. `spec - cmd - ocrworker pdf page-parallel`
23. `spec - cmd - embedworker ollama fleet`
24. `spec - cmd - agent unified worker`
25. `spec - cmd - litekoworker webforms paginator`
26. `spec - cmd - unicrawler yaml selenium`
27. `spec - cmd - migrator blob mover`

Each subsection pull = focused read of that subset, produce dedicated spec in `eidos/`.
