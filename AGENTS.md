# AGENTS.md

Guide for AI agents (and humans) onboarding onto this codebase. Read this **before** you write code. It tells you where things live, what conventions hold, and how to extend the system without breaking the existing slices.

---

## 1. Read these first (15 minutes)

In this order:

1. **`README.md`** — operator-facing overview. Read at least: *Repository layout*, *HTTP API*, *Processing pipeline*, *Schema reference*. You can skim the rest.
2. **`internal/domain/`** — every subpackage. These are the pure types and ports. Total ~700 lines. The whole product is reasoning over these.
3. **`internal/app/service.go` + `internal/app/pipeline.go` + `internal/app/dispatcher.go`** — the orchestration layer. ~600 lines combined.

After those three steps you can navigate any other file.

---

## 2. System at a glance

**Distributed crawler + data lake.** A central Go process (the *registry*) holds:

- the URL queue (`crawl_frontier`)
- the processing queue (`processing_jobs`)
- the embedding queue (`document_chunks`)
- the blob index (`lake_objects`) + a pluggable `BlobStore`
- the routing rules (`pipeline_triggers`)
- the workers (`workers`) and their capabilities

Workers (any language) reserve work via HTTP, do it, push results back. Each queue uses the same shape: **reserve → lease → result/fail → release**. Leases expire automatically (a sweeper goroutine runs every 30 s).

**Bins** (under `cmd/`):

| Bin | Role |
|---|---|
| `registry` | HTTP server + migrations + operator CLI |
| `worker` | reference Go crawl worker |
| `taskworker` | reference processing worker (PDF OCR / Office→PDF / …) |
| `embedworker` | reference embed worker (Ollama-style or shell-out) |
| `agent` | unified worker — one process, many capabilities |
| `migrator` | local↔S3 blob mover |

**Vector store**: optional Qdrant client lives inside the registry. Embed workers send raw vectors back; the registry handles upserts. Search uses `POST /v1/search`.

---

## 3. Architectural patterns in use

### 3a. DDD + ports & adapters (hexagonal)

```
internal/
├── domain/          # pure types + interfaces ("ports")
│   ├── frontier/    # URL queue + domain table
│   ├── lake/        # blob index + BlobStore port
│   ├── workerid/    # workers + Repository port
│   ├── processing/  # processing_jobs domain + filters
│   ├── extraction/  # extracted_documents
│   ├── chunking/    # document_chunks + Context
│   └── triggers/    # pipeline_triggers
├── app/             # use cases (Service / Pipeline / TaskSvc / EmbedSvc / SearchSvc / TriggerDispatcher / CollectionResolver / Mover)
└── infra/           # concrete adapters (gorm, http, qdrant, local FS, s3, …)
    ├── db/
    │   ├── rwdb/    # CQRS DB handle (R + W pools)
    │   ├── gormrepo/  # Repository implementations
    │   └── migrations/{sqlite,postgres,mysql}/*.sql
    ├── store/{local,s3}/
    ├── http/        # chi router + handlers
    ├── lease/       # HMAC lease tokens
    ├── pipeline/    # in-process processors (html_strip, text_passthrough, …)
    ├── qdrant/      # HTTP client
    ├── embedclient/ # Ollama-style client
    ├── store/local/ + store/s3/ # BlobStore implementations
    └── urls/        # canonicalization
```

**Rule**: `domain/` packages MUST NOT import `gorm`, `chi`, `qdrant`, or any infra. They define types and port interfaces only. Infra adapters implement those interfaces. App services depend on ports; the registry main wires concrete adapters in.

Example port:

```go
// internal/domain/frontier/repository.go
type Repository interface {
    Enqueue(ctx context.Context, j Job) (bool, error)
    Reserve(ctx context.Context, workerID int64, capabilities []string, batch int,
            leaseTTL time.Duration, signLease ...) ([]LeasedJob, error)
    // ...
}
```

Concrete impl:

```go
// internal/infra/db/gormrepo/frontier_repo.go
type FrontierRepo struct{ DB *rwdb.DB }
func (r *FrontierRepo) Enqueue(...) (bool, error) { /* gorm code */ }
func (r *FrontierRepo) Reserve(...) ([]frontier.LeasedJob, error) { /* gorm + raw SQL */ }
```

Wire in `cmd/registry/main.go`:

```go
frepo := gormrepo.NewFrontierRepo(db)
svc := app.New(cfg, frepo /* as frontier.Repository */, frepo /* as frontier.DomainRepo */, ...)
```

### 3b. CQRS — single writer, many readers (`rwdb`)

`internal/infra/db/rwdb/rwdb.go` returns a `*DB` with TWO pools:

```go
type DB struct {
    R *gorm.DB   // read pool — concurrency = NumCPU (sqlite) / NumCPU*4 (pg/mysql)
    W *gorm.DB   // write pool — concurrency = 1 (sqlite) / NumCPU (pg/mysql)
    Driver Driver
}
```

**Rule of thumb**:
- All `SELECT`s → `db.R.WithContext(ctx)`.
- All `INSERT/UPDATE/DELETE`s → `db.W.WithContext(ctx)`.
- Multi-statement transactions: `db.WriteTX(ctx, func(tx *rwdb.Tx) error {...})` for read/write, `db.ReadTX(...)` for read-only.

Example from `gormrepo.LakeRepo`:

```go
// Read uses R
func (r *LakeRepo) GetByID(ctx, id int64) (*lake.Object, error) {
    var m LakeObject
    err := r.DB.R.WithContext(ctx).Where("id = ?", id).First(&m).Error
    ...
}

// Write uses W
func (r *LakeRepo) Insert(ctx, o lake.Object) (int64, error) {
    m := LakeObject{...}
    if err := r.DB.W.WithContext(ctx).Create(&m).Error; err != nil { ... }
    return m.ID, nil
}

// Multi-statement: WriteTX
func (r *FrontierRepo) Reserve(...) {
    err := r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
        var picks []pickRow
        if err := tx.Raw(pickSQL, args...).Scan(&picks).Error; err != nil { return err }
        for _, p := range picks {
            if err := tx.Exec(`UPDATE crawl_frontier SET ...`, ...).Error; err != nil { return err }
        }
        return nil
    })
}
```

**Why two pools on SQLite?** SQLite serializes writers. Capping the write pool at 1 connection avoids `database is locked` errors. Reads use a separate pool with WAL mode for concurrent reads. PostgreSQL/MySQL don't strictly need this, but we keep the same shape so app code is dialect-agnostic.

### 3c. Capabilities (strings) drive everything

Workers register with a JSON array of capability strings. The server uses string-membership checks for:

- **Endpoint authorization** (`Worker.Can("crawl")`, `Worker.Can("embed")`, `Worker.Can("search")`, …)
- **Task kind matching** (`POST /v1/tasks/reserve {kinds:["pdf_ocr"]}` — each kind must be in `worker.capabilities`)
- **Per-domain binding** (`domains.required_capability` → matched against `worker.capabilities` in the reserve SQL)

Empty `capabilities` array on a worker is treated as **"any"** for backward-compat with slices 1–6.

When adding a new endpoint or new restriction: pick a cap string, document the convention, do the `wk.Can(...)` check in the handler.

### 3d. Lease tokens (HMAC, stateless)

`internal/infra/lease/`. All three queues issue HMAC-SHA256 lease tokens (base64url-encoded). Layout:

```
| 32B hash | 8B workerID | 8B expiry (unix sec) | 16B truncated HMAC |
```

The `hash` slot is:
- crawl: `sha256(canonical URL)` (the frontier PK)
- chunk: `sha256(chunk UUID)`
- task:  `sha256("task:" || BE8(task_id))`

Tokens are verified server-side without any DB state. The server then matches the token bytes against `lease_token` columns (defense in depth — the token is also stored once issued, so a leaked secret doesn't immediately let an attacker forge leases for stored rows).

When adding a new queue with leases: define `SignXxx` / `VerifyXxx` helpers in `internal/infra/lease/` following the same shape.

### 3e. Triggers (declarative routing) — replaces hardcoded MIME map

`pipeline_triggers` table is queried by `app.TriggerDispatcher.Fire(event, payload)` after every result-write. Triggers seeded via migrations. Operators add/disable triggers at runtime via `registry trigger-add` / `trigger-disable`.

Default events:
- `EvtLakeObjectInserted` (fires from `Service.AcceptResult`)
- `EvtBlobProduced` (fires from `TaskSvc.AcceptBlob`)

Adding a new event:

1. Add the const to `internal/domain/triggers/trigger.go`.
2. Fire from the appropriate write path: `s.Dispatcher.Fire(ctx, triggers.EvtMyEvent, EventPayload{...})`.
3. (Optional) Seed default triggers in a new migration.
4. Extend `app/dispatcher.go`'s `matches()` if the filter needs new fields.

### 3f. Internal vs external processors

`Pipeline.InternalProcessors` lists processor kinds the in-process goroutine handles (today: `html_strip`, `text_passthrough`). Everything else stays in `processing_jobs` waiting for an external `taskworker`.

To turn an external processor into an internal one: add it to `InternalProcessors`, write the `execXxx` method in `pipeline.go`, register the case in `exec()`. Cheap CPU-only work belongs internal; anything needing GPU / shell-out / external tooling should stay external.

---

## 4. The "where do I put this?" checklist

| New thing | Goes in | Touches |
|---|---|---|
| **A new domain model field** (e.g. `domains.foo TEXT`) | new SQL migration + `domain/<area>/` type + `gormrepo` model + `mapXxx` helper | `cmd/registry/main.go` if a CLI flag/column is needed |
| **A new repo method** | port interface in `domain/<area>/repository.go`, then impl in `gormrepo/` | every wrapper struct in `cmd/registry/main.go` if signature changes |
| **A new HTTP endpoint** | `internal/infra/http/<area>.go` handler + `Router(...)` mount | maybe a new capability string |
| **A new processor (internal)** | constant in `domain/processing/job.go`, `execXxx` in `app/pipeline.go`, add to `InternalProcessors` | maybe a trigger seed migration |
| **A new processor (external)** | constant in `domain/processing/job.go`, default trigger via migration | operators run `taskworker --kind X --extract-cmd "..."` |
| **A new event** | const in `domain/triggers/trigger.go`, fire from the write path | filter shape in `app/dispatcher.go matches()` |
| **A new BlobStore** | `internal/infra/store/<name>/` implementing `lake.BlobStore` | flag in `cmd/registry/main.go` + `cmd/migrator/main.go` |
| **A new vector store** | new package under `internal/infra/` implementing the small shape `qdrant.Client` already has | `cmd/registry` swaps the client; consider a single port interface if needed |
| **A new DB driver** | branch in `rwdb.New`, migrations dir, dialect check in `migrateAction`, dialect-specific SQL in `gormrepo` | |
| **A new CLI subcommand** | `cmd/registry/main.go` Subcommands + an `actionXxx` func | README CLI reference |

---

## 5. Concrete recipe: adding a new feature, end to end

Worked example — add a new task kind `image_caption` that converts uploaded images to a textual caption.

### Step 1 — Domain layer

Open `internal/domain/processing/job.go` and add:

```go
const ProcImageCaption Processor = "image_caption"
```

That's it. No type changes needed.

### Step 2 — Migration to seed the default trigger

`internal/infra/db/migrations/sqlite/0008_image_caption.sql`:

```sql
-- +goose Up
-- +goose StatementBegin
INSERT INTO pipeline_triggers (when_event, when_filter, enqueue_kind) VALUES
('lake_object_inserted', '{"content_type_prefix":"image/jpeg"}', 'image_caption'),
('lake_object_inserted', '{"content_type_prefix":"image/png"}',  'image_caption'),
('lake_object_inserted', '{"content_type_prefix":"image/webp"}', 'image_caption');
-- +goose StatementEnd
-- +goose Down
-- +goose StatementBegin
DELETE FROM pipeline_triggers WHERE enqueue_kind = 'image_caption';
-- +goose StatementEnd
```

Mirror in `postgres/0008_image_caption.sql` and `mysql/0008_image_caption.sql` (same content; only ALTERs need dialect tweaks).

### Step 3 — Operators run a worker

No registry code changes for an external task. Operators do:

```bash
registry create-worker --label captioner-1 --capabilities image_caption --max-concurrent 2
taskworker --kind image_caption --mode text \
           --extract-cmd "python3 /opt/caption.py {input}"
```

The result text becomes `extracted_documents.text`, which then chunks → embeds → searches automatically.

### Step 4 — Smoke

Write `scripts/smoke_caption.sh` following the same pattern as `smoke_files.sh`. Plant a fake JPEG into `lake_objects`, run `reprocess`, run a fake `taskworker` shelling out to `cat {input}` (or similar), assert `extracted_documents` and `document_chunks` populated.

### Step 5 — README

Update the per-MIME table in the "Processing pipeline" section.

---

## 6. Repository / port conventions

### 6a. Interface lives in `domain/`, impl in `gormrepo/`

Don't reference `gorm` from domain code, ever. Pure types + interfaces.

### 6b. Mapping helpers in `gormrepo/`

For every model with nullable columns or domain↔db transformations, write a `mapXxx(m *DbModel) *domain.Type` helper.

```go
// internal/infra/db/gormrepo/frontier_repo.go
func mapDomain(d *Domain) *frontier.DomainRow {
    row := &frontier.DomainRow{...}
    if d.EmbedCollection != nil { row.EmbedCollection = *d.EmbedCollection }
    if d.RequiredCapability != nil { row.RequiredCapability = *d.RequiredCapability }
    return row
}
```

### 6c. Nullable handling

DB columns that can be NULL: `*string`, `*time.Time`, `*int64` on the gorm model. Domain types use the bare type with empty/zero meaning "absent". Conversion happens in `mapXxx`.

Updating a nullable column to NULL:

```go
var val interface{}
if collection == "" {
    val = nil  // → SQL NULL
} else {
    val = collection
}
r.DB.W...Update("embed_collection", val)
```

### 6d. Dialect-aware SQL

`FrontierRepo` has `politenessSQL(driver)` helper that emits dialect-specific time-arithmetic. Pattern this for any SQL that's not portable. Inspect `r.DB.Driver` (`rwdb.DriverSQLite | DriverPostgres | DriverMySQL`).

### 6e. Filter structs (bulk updates)

For bulk operations (slice 11 `RequeueByFilter`), define a `*Filter` struct in the domain package:

```go
// domain/chunking/chunk.go
type RequeueFilter struct {
    Status     EmbedStatus // "" = no constraint
    WorkerID   int64       // 0 = no constraint
    DocumentID int64       // 0 = no constraint
}
```

**Convention**: empty / zero value = "no constraint on that field". Fields AND-ed. The CLI layer adds a guard that requires at least one non-empty filter — avoids mass-update accidents.

---

## 7. HTTP layer conventions

### 7a. Handlers go in `internal/infra/http/<area>.go`

Each handler struct holds its dependencies. Construct in `Router()`:

```go
// server.go
if tasks != nil {
    th := NewTasksHandler(tasks, workers)
    r.Post("/tasks/reserve", th.Reserve)
    r.Post("/tasks/heartbeat", th.Heartbeat)
    r.Post("/tasks/result", th.Result)
    r.Post("/tasks/fail", th.Fail)
}
```

Optional services: pass `nil` when not configured. The mount is gated.

### 7b. Capability check is the first line in every handler

```go
func (h *MyHandler) Foo(w http.ResponseWriter, r *http.Request) {
    wk, ok := WorkerFromCtx(r.Context())
    if !ok {
        writeError(w, http.StatusUnauthorized, "no_worker", "")
        return
    }
    if !wk.Can("my_capability") {
        writeError(w, http.StatusForbidden, "capability_denied", "...")
        return
    }
    // ...
}
```

Then concurrency enforcement (for reserve-like endpoints):

```go
effBatch, err := effectiveBatch(r.Context(), h.Workers, wk, req.Batch)
if err != nil { writeError(w, http.StatusInternalServerError, "cap_check", err.Error()); return }
if effBatch == 0 { writeJSON(w, http.StatusOK, emptyResp); return }
```

### 7c. Error envelope is consistent

Use `writeError(w, status, code, message)` — emits the canonical `{"error":code,"code":code,"message":message}` shape. Don't roll your own error responses.

### 7d. Use **stored** worker capabilities, not request body

The capability set on the wire is for **operator visibility**, not authorization. The handler reads `wk.Capabilities` (loaded server-side at PAT-auth time). Never trust client-supplied capability fields for auth.

---

## 8. App / service layer conventions

### 8a. Constructors take ports; services compose them

```go
func New(cfg Config, f frontier.Repository, d frontier.DomainRepo, l lake.Repository,
        b lake.BlobStore, w workerid.Repository, s *lease.Signer) *Service { ... }
```

### 8b. Optional collaborators wired via setters

```go
svc.SetPipeline(pipe)
svc.SetDispatcher(disp)
tasks.SetResolver(resolver)
embed.SetQdrant(qcli)
```

This keeps constructors small and lets the registry wire features incrementally as flags are set.

### 8c. Fail-fast on integrations; the sweeper does cleanup

When an external integration (Qdrant, BlobStore, …) fails inside an accept-result path, return the error WITHOUT marking the row done. The chunk/task stays leased → its lease expires → the sweeper requeues it → another worker retries. We never silently swallow.

```go
// app/embed.go AcceptVectorResult
if err := e.Qdrant.Upsert(ctx, collection, ...); err != nil {
    return fmt.Errorf("embed result: qdrant upsert: %w", err)
}
return e.Chunks.MarkEmbedded(ctx, chunkID, vectorID, raw)
```

### 8d. Context propagation

Every function takes `ctx context.Context` as the first argument. Pass it through. Don't store it on structs.

---

## 9. Migrations

`internal/infra/db/migrations/{sqlite,postgres,mysql}/NNNN_name.sql`. Goose format. Each statement wrapped in `-- +goose StatementBegin/End`. Up + Down both required.

**Numbering**: zero-padded 4-digit prefix. Pick the next free number; don't insert in the middle.

**Dialect parity**: every new migration MUST exist in all three dialect directories. Even if it's pure DML (INSERT into pipeline_triggers), keep all three in sync so a deployment can switch DB at any time.

**Embedded via `//go:embed`** in `internal/infra/db/migrations/embed.go`. Goose runs from the embedded FS at startup. Don't add migrations to that file manually — it uses glob patterns.

**Seeded default data** goes in `INSERT` statements inside Up. Examples: 0005 seeds default pipeline_triggers, 0006 adds office/text triggers. Same pattern for any future seed data.

---

## 10. Testing — smoke scripts

We don't have unit tests (yet). The product is verified end-to-end via shell smokes under `scripts/`. Pattern:

1. Build binaries into a temp dir.
2. Start fakes (e.g. fake Qdrant, fake Ollama via inline Python).
3. Start `registry serve` against an ephemeral SQLite DB.
4. Drive the protocol with `curl` + `python3 -c` for JSON parsing.
5. Assert via `sqlite3` queries + `[[ "$x" == "y" ]]` checks.
6. `cleanup` trap kills child PIDs.

Existing 9 smokes:

```
smoke.sh           crawl → pipeline → embed roundtrip (slices 1-5)
smoke_tasks.sh     external task worker (slice 6)
smoke_pool.sh      worker pool + capabilities + caps (slice 7)
smoke_lake.sh      read API + triggers (slice 8)
smoke_scope.sh     scope-locked crawl (slice 8 bugfix)
smoke_files.sh     office_to_pdf + text_passthrough + per-domain collection (slice 9)
smoke_qdrant.sh    registry-owned Qdrant + /v1/search (slice 10)
smoke_queueops.sh  embedworker + queue ops + ban-release (slice 11)
smoke_bind.sh      domain ↔ worker binding (slice 12)
```

When you add a feature, add a smoke. Use existing smokes as templates.

**Run all**:

```bash
for s in smoke smoke_tasks smoke_pool smoke_lake smoke_scope \
         smoke_files smoke_qdrant smoke_queueops smoke_bind; do
  echo -n "$s: "
  bash scripts/$s.sh >/dev/null 2>&1 && echo PASS || echo FAIL
done
```

All 9 must pass before declaring a slice done.

---

## 11. CLI conventions

`cmd/registry/main.go` uses `urfave/cli/v3`. Every subcommand has its own `actionXxx` function. Flags go in the subcommand definition.

**Flag conventions**:
- `--id <int>` — numeric ID
- `--host <string>` — domain host
- Default `-1` for "leave unchanged" on integer mutation flags.
- `-` as flag value to **clear** a string field (e.g. `--embed-collection -`).
- Empty string = "leave unchanged" for string mutation flags.

**Output**:
- Print `key=value` lines for parsing (smokes use `awk -F=`).
- Table output for `list-xxx` commands: `%-4s %-30s ...` style with header row.
- One result line per row.

---

## 12. Git workflow

A pre-tool-use hook (`pre-tool-use-branch-check.sh`) blocks writes on `main`. **Before editing**:

```bash
git checkout -b task/slice-NN-short-description
```

Convention: `task/slice-NN-short-description` for feature slices, `task/fix-X` for bugfixes.

Pre-commit hooks may exist — never use `--no-verify` unless explicitly authorized.

---

## 13. Wire-up cheat sheet — `cmd/registry/main.go`

`buildService(cmd, db)` constructs the whole graph. To add a new component:

```go
// 1. Construct the adapter
myRepo := gormrepo.NewMyRepo(db)

// 2. Construct the service if needed
mySvc := app.NewMySvc(cfg, myRepo, ...)

// 3. Wire optional collaborators
mySvc.SetSomething(somethingElse)

// 4. Add to the bundle
return &registryBundle{
    Svc: svc, ..., MyService: mySvc,
}
```

Then `actionServe` passes the bundle pieces to `httpapi.Router(...)`. The Router signature is positional — add new params at the END to minimize diff churn.

---

## 14. Common pitfalls

### 14a. Don't forget the dialect mirror

When adding a migration, write it in **all three** dialects. The smoke runs on sqlite, but production may use postgres/mysql. A missing postgres migration breaks the deployment silently.

### 14b. SQLite politeness is 1-second resolution

`strftime('%s')` truncates to integer seconds. Sub-second `crawl_delay_ms` doesn't work on sqlite. Don't use values < 1000 in smokes unless you sleep ≥ 1.2 s between operations.

### 14c. LSP shows stale errors

After editing an interface, the LSP often reports "type X does not implement Y" for several seconds while it rescans. If the `go build` exits cleanly, ignore the LSP errors — they'll clear on the next reindex.

### 14d. Don't put text in Qdrant payload AND in DB and let them drift

Today we duplicate `text` in `document_chunks.text` and the Qdrant payload. That's intentional: search avoids a DB round-trip. But it means a chunk text update has to propagate both places. If you write code that mutates chunk text, also upsert the Qdrant point.

### 14e. Empty capabilities = "any" — backwards compat trap

A worker created with no `--capabilities` flag has `capabilities = []`. The reserve query treats that as "any kind, any domain binding". Convenient for legacy workers but means binding enforcement is **opt-in** — operators must assign explicit caps to every new worker. Document this whenever you add a new capability check.

### 14f. Don't bypass `wk.Can(...)` for "convenience"

Capability checks are cheap. Every new HTTP endpoint that does writes or reveals data MUST check a capability. Capabilities are also the only thing operators have to constrain workers — bypassing the check means operators can't control the new endpoint.

### 14g. `lake_object_id` is the join pivot

When you need to surface "the source URL" or "the source domain" or "the source collection" anywhere, the chain is:

```
document_chunks
  → extracted_documents.source_lake_object_id
  → lake_objects.url_hash
  → crawl_frontier.{canonical_url, domain_id}
  → domains.{host, embed_collection, required_capability}
```

`chunking.Repository.GetContext(chunkID)` does this in one query. Use it (don't reimplement).

### 14h. Reads use R, writes use W

If you find yourself doing `db.W.WithContext(ctx)....First(...)`, ask whether you actually need a transaction. Single SELECTs go on R. The only reason to read on W is when you need to combine the read with subsequent writes inside `WriteTX`.

---

## 15. Where to look for examples

When confused about the right pattern, find the closest existing example:

| If you're doing... | Look at |
|---|---|
| A new external task kind | `cmd/taskworker/main.go` and how `office_to_pdf` is set up |
| A new internal processor | `app/pipeline.go` `execTextPassthrough` |
| A new HTTP read endpoint | `infra/http/reads.go` `LakeList` |
| A new HTTP reserve-style endpoint | `infra/http/tasks.go` `Reserve` (cap + concurrency cap) |
| A new repo method with a JOIN | `gormrepo/chunk_repo.go` `GetContext` |
| A new domain-mutation CLI | `cmd/registry/main.go` `actionUpdateDomain` |
| A new bulk-update CLI | `cmd/registry/main.go` `actionRequeueChunks` (with filter guard) |
| A new vector backend | `internal/infra/qdrant/client.go` |
| A new BlobStore | `internal/infra/store/local/local.go` + `store/s3/s3.go` |
| A new lease scheme | `internal/infra/lease/{lease,chunk_lease,task_lease}.go` |
| A new daemon goroutine | `cmd/registry/main.go` `sweeper` |

---

## 16. Slice-by-slice quick recap

Each slice was a self-contained increment. Reading commits in order tells the whole story; this section gives the highlights.

| Slice | What landed |
|---|---|
| 1 | Crawl loop end-to-end. Frontier + lake + workers + reserve/result + LocalFS BlobStore. Sqlite only. |
| 2 | Processing pipeline (`processing_jobs`, `extracted_documents`, `document_chunks`). `html_strip` internal. |
| 3 | Embed protocol (`/v1/embed/{reserve,result}`). Chunk lease scheme. |
| 4 | S3 BlobStore + `cmd/migrator`. |
| 5 | Postgres + MySQL migrations. |
| 6 | External task workers (`/v1/tasks/*`, `/v1/blobs/{id}`). `cmd/taskworker`. Multi-stage chains via `output_lake_object_id`. |
| 7 | Worker pool: `capabilities`, `max_concurrent`, ban/unban, `cmd/agent`. |
| 8 | Read API + pipeline triggers (replaces hardcoded routeFor). Plus scope-lock bugfix (`AllowAutoDomains` default false). |
| 9 | Office formats + `text_passthrough` + per-domain `embed_collection` + `update-domain` CLI. |
| 10 | Registry-owned Qdrant client + `/v1/search`. Embed accepts raw vectors. |
| 11 | `cmd/embedworker` + queue ops (`requeue-*`, `release-worker`, `ban-worker --release`, `queue-stats`). |
| 12 | Domain↔worker binding via `domains.required_capability`. |

The Git log on `task/slice-NN-*` branches preserves the exact diffs.

---

## 17. When in doubt

1. **Add a smoke first.** The act of writing the smoke usually clarifies the design.
2. **Mirror the closest existing feature.** Conventions matter more than novelty.
3. **Don't break port purity.** If you find yourself wanting to import gorm into a domain package, rethink.
4. **Don't add a knob unless you have a concrete use case.** Config flags become permanent — every new flag is a future support burden.
5. **Read the relevant migration before changing the schema.** Often the columns you need already exist.

Welcome aboard.

---

## 18. Architecture Decision Records (ADRs)

The `ADR/` folder at the repo root holds the load-bearing design decisions and the reasoning behind them. AGENTS.md tells you **what** the conventions are; ADRs tell you **why** — and what trade-offs you'd be reopening if you changed them.

Read the relevant ADR before:

- Proposing a structural change (new layer, new cross-cutting concern, replacing a port).
- Reopening a trade-off ("can we just use gorm in domain?", "do we really need three migration dirs?", "why HMAC not JWT?").
- Adding a new component that resembles an existing one — confirm you're following the same pattern, not inventing a parallel one.

Current ADRs (see `ADR/README.md` for the live index):

| # | Title |
|---|---|
| 0001 | DDD + ports & adapters layout |
| 0002 | CQRS via `rwdb` — single writer, many readers |
| 0003 | Capability strings for authz + routing |
| 0004 | HMAC stateless lease tokens |
| 0005 | Declarative `pipeline_triggers` |
| 0006 | Multi-dialect SQL via Goose migrations |
| 0007 | HTTP polling protocol: reserve → lease → result/fail |
| 0008 | Internal vs external processors split |
| 0009 | Token-sized chunks + per-collection chunker config |
| 0010 | Smoke shell scripts as primary test layer |

**When to add a new ADR.** Any time you make a decision that:

1. A future contributor (human or agent) is likely to question or try to undo without context.
2. Affects more than one slice / subsystem.
3. Locks in a trade-off (perf vs flexibility, type-safety vs operator-runtime-configurability, etc.).

Format and process: see `ADR/README.md`. Keep them short — the decision matters, the prose doesn't.
