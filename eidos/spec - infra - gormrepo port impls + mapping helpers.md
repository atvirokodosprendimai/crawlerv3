---
tldr: gorm-backed adapters for every domain port — tagged row structs, db↔domain mapping helpers, dialect-aware politeness SQL, and a strict R/W pool split
category: core
---

# gormrepo port impls

## Target

`internal/infra/db/gormrepo/` — the single concrete `Repository` implementation set used in production today. One package per persistence vendor was avoided; gorm is the only adapter for sqlite/postgres/mysql because the SQL shape is shared (see system spec).

Covers: `FrontierRepo`, `LakeRepo`, `ExtractionRepo`, `ChunkRepo`, `ProcessingRepo`, `WorkerRepo`, `TriggersRepo`, `CapabilityRepo`, and the row models they read/write.

## Behaviour

- Every domain port has exactly one constructor here that takes the shared `rwdb.DB` handle and returns a struct satisfying the matching interface in `internal/domain/...`. No further wiring per caller.
- Reads return either a populated domain struct or `(nil, nil)` for "not found" — gorm's `ErrRecordNotFound` is translated to a nil pointer at the boundary so callers never import gorm to check existence.
- Lease-bearing updates (`Complete`, `Heartbeat`, `Fail`, `MarkEmbedded`, …) are no-ops unless the caller-supplied lease token matches the stored token; mismatch surfaces as `"lease not held"`. This holds across all three queues uniformly.
- "Requeue by filter" operations on the three queues require at least one filter from the caller; passing an empty filter is permitted at the repo layer and will mass-requeue — the CLI is documented as the guardrail.
- The crawl reserve operation honours per-domain `crawl_delay_ms` and `parallel_fetches` server-side, picks one or more rows per eligible domain in a single transaction, and never returns work whose lease wouldn't yet be valid.
- `politeness` works identically against SQLite, Postgres, and MySQL despite their incompatible time arithmetic.
- Nullable database columns survive a round-trip without ever leaking `*string` / `*int` into domain code — domain structs see zero-values for absent fields.
- Worker capability arrays survive the round-trip as `[]string` regardless of how they're stored on the wire.
- Bulk inserts (chunks, processing-job back-fills) succeed at any list size; the adapter chunks the writes to avoid per-dialect parameter limits.
- Cross-table reads needed for hot paths (chunk → document → lake → frontier for embed payload; per-doc collection for embed reserve) are served in a single SQL round-trip, not N+1.

## Design

### One row struct per table, one TableName per struct
Each table is mirrored as a flat struct with `gorm` tags ({>> `column:`, `primaryKey`, `uniqueIndex` — no embedded `gorm.Model`}) and an explicit `TableName()` returning the migration table name. The table name is the source of truth, not a struct-name pluralisation rule, because the migrations are the contract gorm has to match.

### Tagged structs live alongside the repo, not in domain
The row types (`Domain`, `Worker`, `Frontier`, `LakeObject`, `ProcessingJob`, `ExtractedDocument`, `DocumentChunk`, `PipelineTrigger`, `Capability`) are unexported-from-domain-on-purpose: domain packages MUST NOT import gorm. Keeping row structs inside `gormrepo` enforces the layering rule from the system spec at the package-import level.

### `mapXxx` translation at the boundary
Every repo has a small `mapDomain` / `mapWorker` / `mapExtracted` / `mapCapability` / `mapTriggers` helper (and inline equivalents for the larger lease-bearing types) whose only job is db→domain translation. The reverse direction (domain→row) is done inline at insert sites because it usually involves applying defaults (`MaxAttempts=5`, `ReputationScore=100`, `MaxConcurrent=4`) — defaults belong with the write, not with a generic mapper.

### Nullable columns use Go pointers, mappers collapse to zero-values
Every nullable column is `*string`/`*int`/`*time.Time` on the row struct. Mappers dereference into a non-pointer domain field when set, and leave the domain field at its zero value when null. This keeps domain code free of nil checks and free of any `sql.NullX` types. {>> Writes use `var v interface{} = nil` or `*string` indirection so gorm writes SQL NULL rather than empty-string.}

### Worker capabilities are JSON in a single text column
Persisted as a JSON array string in `workers.capabilities` rather than a join table. Rationale: the set is short, mutated atomically by CLI, queried by string membership in SQL (`IN ?` against `required_capability`), and JSON-encoding keeps the migrations identical across all three dialects. {>> `encodeCaps` returns `"[]"` (not NULL) when empty so a present-but-empty cap set is distinguishable from the legacy NULL = "any" interpretation.}

### Dialect-aware politeness via one switch
The only SQL that materially diverges between drivers is "elapsed milliseconds since `last_request_at`" because each dialect spells time arithmetic differently. This is isolated to a single `politenessSQL(driver)` helper returning the dialect's expression — every other query is plain ANSI-ish SQL or gorm builder calls. {>> Postgres: `EXTRACT(EPOCH FROM (NOW()-…))*1000`. MySQL: `TIMESTAMPDIFF(MICROSECOND, …)/1000`. SQLite: `strftime('%s', …)` diff times 1000.}

### Hand-written SQL for hot paths, gorm builder for CRUD
Reserve operations, sweepers, status histograms, and the embed-context join are written as `Raw`/`Exec` strings because:
1. They use window functions (`ROW_NUMBER() OVER (PARTITION BY …)`) or multi-statement transactional logic that gorm's builder doesn't express cleanly.
2. They are the queries we most need to read and audit during incidents.

CRUD (`Create`, `First`, `Updates(map)`, `Delete`) stays on the gorm builder where it's terser and equally clear.

### CQRS rule applied everywhere
Every method reads through `r.DB.R` and writes through `r.DB.W`. Multi-statement units wrap in `WriteTX` (or `ReadTX` for the few multi-read paths). This is the system-spec rule made mechanical at the repo layer — no exceptions, even on Postgres/MySQL where a single pool would technically work, because the same code must keep SQLite safe.

### Conditional lease updates are the concurrency primitive
Lease state transitions all use the same pattern: `UPDATE … WHERE id = ? AND status = ? AND lease_token = ?` and treat `RowsAffected == 0` as "lease not held". This subsumes optimistic concurrency without needing row versions. The same pattern handles reserves that race (the post-pick update silently skips contended rows).

### Constant-time token comparison
The lease-token equality check used inside `Fail` paths is an `equalBytes` that XORs all bytes — never a length-only or short-circuit `bytes.Equal`. {>> Defence in depth: timing-leak resistance even though the token is also a stateless HMAC.}

### `FirstOrCreate` for idempotent enqueue
`FrontierRepo.Enqueue` uses gorm's `Where(...).Attrs(m).FirstOrCreate(&Frontier{})` so re-seeding the same URL is a no-op, and discovered-link spam can't multiply rows. The boolean return distinguishes "inserted" from "already there" so the caller can count new work.

### Bulk inserts run in fixed-size sessions
Chunks and processing-job back-fills go through `Session(&gorm.Session{CreateBatchSize: 500})` rather than one `INSERT … VALUES (...), (...)` per call. {>> 500 was picked low enough to stay under MySQL/SQLite parameter caps for the widest row in the repo (a chunk row is ~10 columns).}

### Capability writes use ON CONFLICT, not read-then-write
`CapabilityRepo.Upsert` uses gorm's `clause.OnConflict{Columns, DoUpdates}` so a register-on-startup call from the registry is one round-trip and immune to the read-modify-write race that bit the early `processing_jobs` writes.

### Cross-table joins are written once at the repo boundary
The embed-result hot path needs chunk + extracted-doc collection + lake + canonical URL together. Rather than three sequential repo calls (would be three round-trips, possibly across two pools), `ChunkRepo.GetContext` issues one `JOIN` and returns a `chunking.Context`. The repo absorbs the SQL complexity; the app code sees a single typed accessor.

## Interactions

- **Implements** the port interfaces in `internal/domain/frontier`, `internal/domain/lake`, `internal/domain/extraction`, `internal/domain/chunking`, `internal/domain/processing`, `internal/domain/workerid`, `internal/domain/triggers`.
- **Depends on** `internal/infra/db/rwdb` for the `R`/`W` pools, `WriteTX`/`ReadTX` helpers, and the `Driver` discriminator used by `politenessSQL`.
- **Consumed by** every `cmd/registry` wiring path and (transitively) the app services in `internal/app/*` — but app code only sees domain interfaces.
- **Migrations** (`internal/infra/db/migrate/`) define the column shapes these row structs mirror; any column add/rename must be made in both places.
- **Sweepers** in `internal/app/sweeper` call `SweepExpired` on each of the three lease-bearing repos every 30s; the same mechanism handles all three queues.
- **CQRS guarantees** rely on every method here picking the right pool — this package is where the system-spec "single writer, many readers" rule physically holds.

## Mapping

> [[internal/infra/db/gormrepo/models.go]]
> [[internal/infra/db/gormrepo/models2.go]]
> [[internal/infra/db/gormrepo/frontier_repo.go]]
> [[internal/infra/db/gormrepo/lake_repo.go]]
> [[internal/infra/db/gormrepo/extraction_repo.go]]
> [[internal/infra/db/gormrepo/chunk_repo.go]]
> [[internal/infra/db/gormrepo/processing_repo.go]]
> [[internal/infra/db/gormrepo/worker_repo.go]]
> [[internal/infra/db/gormrepo/triggers_repo.go]]
> [[internal/infra/db/gormrepo/capability_repo.go]]
> [[internal/infra/db/rwdb]]
> [[internal/domain/frontier]]
> [[internal/domain/lake]]
> [[internal/domain/extraction]]
> [[internal/domain/chunking]]
> [[internal/domain/processing]]
> [[internal/domain/workerid]]
> [[internal/domain/triggers]]
