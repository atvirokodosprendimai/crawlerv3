---
tldr: standalone CLI that moves lake blobs between local FS and S3, rewriting storage_backend rows idempotently
category: core
---

# migrator bin

## Target

`cmd/migrator/` — a single-purpose process entrypoint that wires `app.Mover` to a chosen DB driver and a pair of `lake.BlobStore` adapters. One run, one direction, then exit. Not a long-lived worker.

## Behaviour

- Operator picks a source backend and a destination backend by name (`local`, `s3`). Both flags are required; there is no implicit default direction.
- Each invocation walks the lake objects whose `storage_backend` matches the source, copies each blob to the destination, and rewrites the row to point at the new backend.
- A run is idempotent and resumable: re-running after a partial migration only touches rows still tagged as source. Rows already on the destination are invisible to this run by construction.
- `--delete-src` is destructive opt-in. Default behaviour copies and leaves the source blob in place, so the operator has a grace window to verify before deletion.
- A delete failure after a successful copy + row update is logged but does not fail the object — the row already points at the new backend, so the blob on the old backend is now orphaned data, not a correctness problem.
- A size mismatch between source read and destination write fails that single object; the row is *not* rewritten, so a retry will try again from the original backend.
- Same-backend runs (`--from local --to local`) are rejected up front; the bin refuses to do nothing.
- The migration walks in batches; progress accumulates as `scanned / copied / skipped / errors` and is logged once at the end.
- All DB flags (`--db-driver`, `--db-dsn`, `--read-dsn`) and all S3 flags accept env-var equivalents, so the same invocation works under systemd/docker without command-line secret exposure.

## Design

### Thin entrypoint, real work in `app`
The bin's only job is flag parsing, adapter construction, and calling `app.Mover.Run`. The migration loop, idempotency guarantee, and row-update sequencing all live in `internal/app/migrator.go`. Rationale: the migrator is one of several callers that could plausibly want to move blobs (an admin HTTP handler is a future possibility); keeping the use case in `app` preserves the ports-and-adapters layering the system spec enforces.

### Idempotency by query, not by state tracking
The Mover paginates `Lake.ListByBackend(ctx, src.Backend(), batch, afterID)` rather than reading a checkpoint file or marking rows in-flight. A row's `storage_backend` is *itself* the progress marker: once `UpdateStorage` flips it to the destination, the next page query will not see it again. Restarts after a crash resume cleanly. {>> the only durable state is the `storage_backend` column on `lake_objects` — no migrator-specific table, no resume file <<}

### Symmetric backend construction
`buildStore` is called twice with identical flag wiring — once for `--from`, once for `--to`. Both backends pull from the same `--local-root` / `--s3-*` flag pool. This makes any pair (local↔s3, s3↔local) work with one flag set; the operator never has to specify `src-bucket` vs `dst-bucket`. Rationale: a typical migration only crosses two backend *kinds*, not two instances of the same kind. Cross-bucket S3 moves are explicitly out of scope.

### Same-backend guard
The Mover refuses when `Src.Backend() == Dst.Backend()`. {>> early return before any DB or blob I/O <<} Rationale: the query would be self-defeating (every row would match both source and destination filters), and "migrate from X to X" is almost always a flag typo. Failing loud beats silent no-op.

### `migrated_from` audit field
On every row rewrite, the Mover writes `migrated_from = "<src-backend>:<original-key>"`. This satisfies the system-level promise (operator-visible audit trail of where the blob used to live). The storage key itself does not change across backends — local paths and S3 keys share the same opaque string — so only the backend tag and the audit field move.

### Storage key preserved across backends
The destination receives the *same* storage key the source held. {>> `m.Dst.Put(ctx, o.StorageKey, …)` <<} Rationale: keys are content-addressed (sha256-derived) and backend-agnostic. Reusing the key keeps reverse-mapping (`migrated_from` → source path) trivial and avoids any rename / re-derivation logic.

### Per-object failure isolation
A single object failing (network blip, size mismatch, source 404) increments `Errors` and continues the batch. A whole-batch failure (DB list error) aborts. Rationale: the operator wants a long migration to finish what it can; one missing blob shouldn't gate the other 10k. The final stats line tells them how many to investigate.

### `--delete-src` ordering
Delete runs *after* successful copy *and* successful row update. {>> only inside the success branch of `moveOne` <<} If the row update fails, the source blob is preserved — the row still points at the source, so the source must still exist. This ordering means the system is never in a state where a row points at a backend that does not have the blob.

### CLI surface comes from `urfave/cli/v3`
Same CLI library as the rest of the bins in `cmd/`. No bespoke flag parsing. Matches the project-wide pattern that operator-facing surfaces are flag-driven, not config-file driven.

### Exit semantics
Non-zero exit on construction failure or whole-run failure. Per-object errors do not raise exit code — they are counted into `stats.Errors` and surfaced in the final log line. Rationale: a partial migration is not "the bin failed" from an automation standpoint; the operator reads the counts and re-runs to mop up.

## Interactions

- **Operator → migrator bin** — single-shot CLI invocation. No daemon, no PAT, no registry HTTP. This bin talks straight to the DB and the blob stores.
- **migrator → DB** — opens its own `rwdb.DB` with the same driver/DSN flags the registry uses. Reads via `ListByBackend`, writes via `UpdateStorage`. Honors the system-wide R/W pool split, but in practice the workload is read-heavy paginated scan + single-row writes.
- **migrator → blob stores** — constructs two `lake.BlobStore` adapters side-by-side. Both adapters are the same ones the registry uses at runtime, so behavior parity is automatic.
- **No worker queue involvement** — this bin does not reserve, lease, heartbeat, or post results. It is outside the three-queue protocol entirely.
- **Coexists with a live registry** — the system-level promise is "migrate without downtime." Achieved because (a) row updates are single-row atomic, (b) the registry's serving path reads the row's current `storage_backend` per request, so a row flipped mid-flight is served from the new backend on the next read. Concurrent writes to the *same* row by the registry and the migrator are not coordinated and are out of scope (migration windows assume blobs aren't being rewritten in place).

## Mapping

> [[cmd/migrator/main.go]]
> [[internal/app/migrator.go]]
> [[internal/domain/lake/]]
> [[internal/infra/store/local/]]
> [[internal/infra/store/s3/]]
> [[internal/infra/db/rwdb/]]
> [[internal/infra/db/gormrepo/]]
