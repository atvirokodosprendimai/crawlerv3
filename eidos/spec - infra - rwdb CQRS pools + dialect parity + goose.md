---
tldr: two-pool R/W gorm handle with driver-specific sizing, transactional callbacks, and embedded goose migrations mirrored across three SQL dialects
category: core
---

# rwdb + migrations

## Target

The `rwdb` package (process-wide DB handle and TX helpers) and the `migrations` package (embedded SQL migrations for SQLite, Postgres, MySQL). These are the only places in the codebase that know about SQL dialect differences; everything above them treats the database as a single logical store with separate read and write entrypoints.

## Behaviour

- A caller asks for a database once, gets back a single handle with two visible faces: a read pool and a write pool. The choice of which face to use is the caller's, not the package's — passing the wrong one is a logic error the package will not correct.
- The handle multiplexes transparently across SQLite, Postgres, and MySQL. Choosing a driver at startup is the only dialect-aware act anywhere in the system; all later code is identical regardless of which engine answered.
- Pool sizes adapt to the engine's concurrency model. The operator does not have to know that SQLite serializes writers — the handle already does, and sizes the write pool accordingly.
- Read replicas are a config-time option, not a code-time one: callers that pass a separate read DSN get reads routed to it; everyone else gets reads against the primary.
- Transactional units of work are expressed as a callback. The body either fully commits or fully rolls back; partial state is never observable to other readers.
- A read transaction declares its intent — the driver may use that hint, but even when it is purely advisory, the codebase still uses the read pool, preserving the CQRS contract from the system spec.
- Closing the handle drains both pools, surfacing aggregated errors rather than the first one and silently swallowing the rest.
- Migrations are shipped inside the binary. There is no separate migration tarball, no out-of-tree directory to mount, no risk of binary/schema drift on a fresh host.
- Three dialect trees exist side-by-side. Every numbered migration appears in all three with the same number and the same semantic effect; the SQL differs only where the dialect forces it (type names, autoincrement syntax, foreign-key clause placement). A migration that exists in one tree but not the others is a bug, not a feature flag.
- The migration file numbering is the source of truth for schema version, ordering, and parity. Renumbering or skipping is not a supported operation.
- Pragmas and session-level settings that the storage engine needs to behave as the codebase assumes (WAL, foreign keys, busy timeout for SQLite) are applied at open time, not left to the operator's DSN.
- Logging verbosity is operator-controlled at process start, not per-query. A slow-query threshold is built in; record-not-found is never noisy.

## Design

### Two pools as a physical encoding of the CQRS rule

The system spec mandates "SELECTs on R, writes on W." `rwdb` makes that rule physical: there is no shared handle that hides which pool you used. A function signature that takes the write pool announces its intent in its parameter list. The read/write split is therefore enforced at the type-system boundary of every call site, not by convention. {>> `DB.R` and `DB.W` are both `*gorm.DB` — same type, different identity. The package exposes both as struct fields rather than a single accessor with a flag, because the goal is to make the wrong call look wrong at the call site, not to centralize the decision. <<}

### Driver-specific sizing without driver-specific callers

Each driver gets its own constructor that knows its concurrency model:
- SQLite: write pool capped at 1 connection because the engine serializes writers at the file level; concurrent writes would manifest as `database is locked` retries, not parallelism. The read pool is sized to `NumCPU` since WAL allows many concurrent readers against a single writer.
- Postgres/MySQL: MVCC makes the distinction advisory rather than enforced; sizes mirror the contract anyway (`NumCPU*4` reads, `NumCPU` writes) so that the same code shape ports cleanly across all three and the writer never starves the connection budget.

{>> SQLite uses two *separate* `gorm.Open` calls against the same file path, not one handle exposed twice. That is what allows the W pool to be `MaxOpenConns(1)` while the R pool is unlimited — `database/sql` connection pools are per-handle. <<}

### Read-replica plumbing is config-only

A separate `ReadDSN` field exists but is opt-in. The default is "read DSN = write DSN," so the day-1 deployment looks like one database to the operator. Pointing reads at a replica is a flag flip, no code change. {>> Read-only SQLite "replicas" make no sense; only Postgres/MySQL constructors consult `ReadDSN`. <<}

### Transactions as callbacks, not as exposed handles

`ReadTX` and `WriteTX` take a body and run it inside `Transaction(...)`. The package never hands a raw transaction back to the caller, so there is no way to forget to commit, no way to leak a transaction across a goroutine boundary, and no way to mix a transaction handle with a non-transactional `*gorm.DB`. The `Tx` wrapper exists purely to give the caller a stable type to declare in their own ports.

`ReadTX` passes `&sql.TxOptions{ReadOnly: true}` even on engines where it is advisory — this is the codebase's load-bearing self-documentation: a `ReadTX` call site is a promise that nothing inside writes, and a future reviewer can grep for the misuse.

### Embedded migrations, three parallel trees

Migrations live inside the binary via `embed.FS`, one filesystem per dialect. The decision is deliberate:
- No deployment-time copy step.
- No version skew between binary and on-disk SQL.
- Migration discovery is a `fs.ReadDir`, not a host filesystem walk.

The three-tree convention says: every change to the schema lands in all three on the same PR, with the same migration number, and is verified by the smoke harness against at least SQLite. The number is the contract — `0005_pipeline_triggers.sql` *is* migration 5 in every dialect, semantically equivalent across all of them.

{>> `//go:embed sqlite/*.sql` (and the same for `postgres/`, `mysql/`) — three exported `embed.FS` values rather than one nested tree, so callers select dialect by picking a variable, not by computing a sub-path. The runner itself lives outside this package; `migrations` only ships the bytes. <<}

### Per-statement goose directives, not file-level

Every statement is wrapped in `+goose StatementBegin/StatementEnd`. The reason is uniform behavior across drivers that disagree on semicolon-delimited multi-statement execution (MySQL's driver in particular). One file = many statements, each independently shipped to the driver. The convention is mandatory: a contributor adding raw SQL without the markers will see the migration fail on at least one of the three engines.

### Pragmas baked in at open time

For SQLite, four pragmas are applied unconditionally inside the open path:
- WAL for read concurrency.
- `synchronous=NORMAL` to trade a window of crash-time durability for throughput acceptable to a crawler.
- `foreign_keys=ON` because SQLite ships them off by default and the schema depends on cascades.
- `busy_timeout=5000` so that the rare write-vs-write race blocks briefly instead of failing fast.

These are not configurable. The codebase treats them as the *definition* of "SQLite as we use it" — a deployment that disables WAL is no longer a supported configuration.

### Logger discipline

Slow-query threshold is one second, hardcoded. Record-not-found is suppressed because gorm treats it as an error and the app treats it as a normal control-flow signal. Verbosity is operator-controlled via a debug flag or `DB_LOG_LEVEL=debug` — query bodies are parameterized in logs (`ParameterizedQueries: true`) so that secrets in WHERE clauses do not leak to stdout.

### `Close` aggregates, does not short-circuit

A close error on one pool does not abort the close of the other. Errors are joined and returned, so a half-closed handle is never left dangling. {>> `errors.Join` rather than first-error-wins; both pools must be drained even if one fails. <<}

## Interactions

- **`rwdb.DB`** is the single port through which every adapter in `internal/infra/db/...` reaches the database. Repositories choose `R` or `W` per method.
- **`rwdb` and migrations** are decoupled — `rwdb` opens a connection but does not run migrations. A separate runner (under `cmd/registry` and friends) selects the right `embed.FS` based on `Driver` and applies it via goose. This means a process can open `rwdb` against a database that is already at the right version without re-running migrations every boot.
- **System CQRS rule** (system spec §"single writer, many readers"): `rwdb` is the mechanism that makes the rule physical; the rule itself is documented at the system level.
- **Smoke harness** (system spec §"Verified via shell smokes") exercises the migration set against ephemeral SQLite on every CI/local run. Postgres and MySQL parity is a manual smoke when the dialect SQL changes.
- **Operator startup flags** decide the driver, DSN, optional read DSN, and debug logging. No runtime mutation of the DB handle is supported — a driver swap is a process restart.
- **Lake / queue / domain repositories** all use `WriteTX` for multi-statement units (reserve, result/fail, sweeper reclaim) so that lease state and queue-row state move atomically. A partial reserve must never be observable.

## Mapping

> [[internal/infra/db/rwdb/rwdb.go]]
> [[internal/infra/db/migrations/embed.go]]
> [[internal/infra/db/migrations/sqlite/]]
> [[internal/infra/db/migrations/postgres/]]
> [[internal/infra/db/migrations/mysql/]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
