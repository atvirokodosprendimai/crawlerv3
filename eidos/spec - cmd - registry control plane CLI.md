---
tldr: urfave/cli/v3 control-plane binary — one entrypoint that both serves the HTTP API and exposes every operator mutation as a subcommand
category: core
---

# registry CLI

## Target

`cmd/registry/main.go` — the only entrypoint to the stateful node. Wraps the `internal/app` services in a urfave/cli/v3 command tree and is also the boot path for `serve`. There is no separate admin CLI; every operator action is a sibling subcommand of `serve` on the same binary, against the same DB and blob store.

## Behaviour

- The operator gets one binary with one global flag set (DB, blobs, lease secret, optional Qdrant / embed / Stanza / Quickwit) and a flat list of subcommands grouped by noun: `worker`, `domain`, `trigger`, `capability`, `queue`, plus `serve`, `migrate`, `enqueue`, `reprocess`.
- Every subcommand is self-contained: opens the DB, does its work, closes. The exception is `serve`, which keeps the DB open for the lifetime of the HTTP server and runs background loops (sweeper, pipeline) alongside it.
- Mutations take effect immediately against the live registry — no restart of `serve` is needed for a worker, domain, trigger, or capability change to be visible to the next reserve / dispatch.
- Numeric "leave unchanged" flags use `-1`; string "clear this column" uses the literal `"-"`. Operators can update one field of a domain or worker without having to re-specify the rest.
- Bulk requeue commands refuse to run with no filter — at least one of `--status` / `--worker` / `--domain|document|processor` must be given, so a typo cannot accidentally requeue the entire queue.
- `serve` is gracefully shutdownable on SIGINT / SIGTERM with a 10s drain. Background sweeper and pipeline goroutines share the request context and exit on the same cancel.
- Migrations are part of the same binary: `registry migrate up|down|status|reset` against whichever DB driver the global `--db-driver` selects; the bundled embedded FS is sliced per dialect so the operator never points goose at a directory.
- The CLI is the documentation of the registry's capability vocabulary: `list-capabilities` prints the registry-defined endpoint-gated set, the operator-registered catalog, and any free-form strings actually attached to workers — so the operator can see which capabilities are spoken anywhere in the system without reading source.
- `create-worker` prints the PAT exactly once; only its sha256 is persisted. There is no recovery path.
- `queue-stats` is one command that summarises all three queues plus worker pool health (total / banned / stale-over-5m) — the operator's single-pane status view, since there is no GUI.

## Design

### One binary, two modes

`serve` and the mutation subcommands share `buildService` / `openDB` so that ad-hoc CLI work goes through exactly the same wiring as the running server — same DB pools, same repos, same lease signer. The CLI is not a separate admin tool talking to the API; it talks to the DB directly with the same code. This keeps the control plane consistent even when `serve` is down.

{>> `buildService` returns a `registryBundle` containing every constructed service (Svc, EmbedSvc, TaskSvc, SearchSvc, FTSSvc, Pipeline, TriggerDispatcher) plus the raw repos and blob store. `serve` hands the whole bundle to `httpapi.Router`; CLI subcommands ignore it and build only the repo they need. <<}

### Flag conventions encode intent

Two cross-cutting conventions exist so partial updates compose:

- `-1` on integer flags = leave unchanged. Used on `update-worker --max-concurrent`, `update-domain --crawl-delay-ms / --parallel-fetches`. The default value of the flag is the "no-op" sentinel, not a real value.
- `"-"` on string flags = explicitly clear the column. Used on `update-domain --embed-collection` and `--required-capability`. Empty string still means "leave unchanged" (the flag wasn't passed); `"-"` is the affirmative "set to NULL".

This pair lets the operator write `update-domain --host x --required-capability -` to drop a binding without touching delay or parallelism.

### Subcommand surface matches the data model

The subcommand tree is a near-1:1 reflection of the writable domain entities: workers (create / list / update / ban / unban / release), domains (seed / list / activate / deactivate / update), triggers (add / list / enable / disable / delete), capabilities catalog (add / rm), queues (stats / requeue-frontier / requeue-tasks / requeue-chunks), plus the two free-form actions `enqueue` and `reprocess`. There is no generic "edit this row" command — each mutation is a named verb the operator can find by `--help`.

### Three uniform queues → three uniform requeue commands

`requeue-frontier`, `requeue-tasks`, `requeue-chunks` mirror the three-queue shape from the system spec: same filter language (`--status` / `--worker` / per-queue third axis), same "at least one filter required" safety, same output `rows=N`. Adding a new queue means adding a fourth command in the same template, not a new flag schema. See system spec for the queue protocol shape.

{>> Each command routes to `<repo>.RequeueByFilter` with the queue-specific `RequeueFilter` struct; the CLI's only job is to translate flag strings into typed filter fields. <<}

### Release-as-a-primitive

`release-worker` and the `--release` flag on `ban-worker` share `releaseWorkerLeases`, which fans out one UPDATE per queue (no cross-table transaction — each queue is independent and idempotent). This is the operational primitive for "this worker host died, give its leases back to the pool" without waiting for the sweeper's lease TTL.

### Capabilities visibility

`list-capabilities` is a deliberate three-way join shown to the operator:

1. Hard-coded endpoint-gated capabilities from `workerid.EndpointGatedCapabilities()` — what the registry's handlers actually check.
2. The `capabilities` catalog table — operator-declared known capabilities with descriptions, optionally `internal=true` if the registry's own pipeline goroutine claims them.
3. Free-form capability strings currently attached to workers but in neither of the above — the "unknown vocabulary" the operator may want to either bless (add to catalog) or audit.

This makes the system spec's "capabilities-as-strings, two flavors" visible from one command without reading code.

### Trigger filter is JSON-on-the-wire

`trigger-add` collects multiple `--content-type` / `--source-processor` flag-pairs and packs them into a single JSON object stored in `pipeline_triggers.when_filter`. The CLI is the only place this JSON is constructed by hand; the dispatcher unmarshals it. Filter format is therefore an internal contract between CLI and dispatcher, not a public schema.

### Migrations are dialect-sliced at the CLI

The three goose migration FSes (`migrations.SQLite`, `.Postgres`, `.MySQL`) are siblings in one embed. The CLI picks the sub-FS based on `--db-driver` before calling `goose.Up/Down/Status/Reset`. This is the only place where dialect branching happens in the CLI — application code stays driver-agnostic. {>> `subFS` reroots the embed at the dialect directory so goose sees migration files at `"./"`. <<}

### Optional integrations are configured at the global level, not per-subcommand

Qdrant, embed-query, Stanza, and Quickwit flags live on the root command, not on `serve`, because `buildService` is also called by some action paths and the registry as a whole has one configured integration set. Each integration is wired only if its base URL is non-empty — `qdrant.New` always returns a struct, but `qcli.Enabled()` gates whether `SearchSvc` is created at all and whether the embed service pushes vectors. The same pattern applies to FTS (`fts.Enabled()` gates wiring it into pipeline and tasks).

### Fail-fast on the lease secret

`leaseSecret` rejects an empty value, accepts both raw and standard base64, and rejects under-16-byte secrets. Every subcommand that does `buildService` therefore fails before touching the DB if the operator forgot `LEASE_SECRET`. This is intentional — there is no fallback to an in-memory secret because that would silently invalidate every issued lease token across restarts.

### Log bootstrap before anything else

The root `Before` hook initialises `logx` from `--log-level` (with `--debug` overriding to `"debug"`) so even DB-open errors and migration output go through the structured logger.

## Interactions

- **Operator → CLI** — sole control surface. No web UI, no REST admin endpoints for mutations.
- **CLI → `rwdb.DB`** — every subcommand opens, defers close. `serve` keeps it open for the server lifetime.
- **CLI → `app` services** — `serve` constructs the full bundle; mutation subcommands construct only the repo they need.
- **`serve` → background goroutines** — sweeper (30s tick over three queues), pipeline (2s tick claiming internal-processor jobs). Both share the request context.
- **CLI → goose** — `migrate` subcommand only; dialect-sliced embed FS.
- **CLI → external integrations** — none directly. Qdrant / Stanza / Quickwit clients are constructed but only exercised by `serve`'s HTTP handlers and the pipeline goroutine.

## Mapping

> [[cmd/registry/main.go]]
> [[internal/app/]]
> [[internal/infra/db/rwdb/]]
> [[internal/infra/db/migrations/]]
> [[internal/infra/db/gormrepo/]]
> [[internal/infra/lease/]]
> [[internal/infra/http/]]
> [[internal/domain/workerid/]]
> [[internal/domain/frontier/]]
> [[internal/domain/processing/]]
> [[internal/domain/chunking/]]
> [[internal/domain/triggers/]]
