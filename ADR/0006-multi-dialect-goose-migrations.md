# ADR-0006 — Multi-dialect SQL via Goose migrations

- Status: accepted
- Date: slice 5

## Context

Three SQL backends in scope. ORMs auto-migrate but produce divergent schemas across dialects (esp. JSON types, time types, NULL handling, FK semantics). Schema drift between dev (SQLite) and prod (Postgres/MySQL) is a class of bug we refuse to ship.

## Decision

Hand-written, dialect-mirrored Goose migrations:

```
internal/infra/db/migrations/
├── sqlite/NNNN_name.sql
├── postgres/NNNN_name.sql
└── mysql/NNNN_name.sql
```

Rules:
- Zero-padded 4-digit numeric prefix. No insertions in the middle.
- Every migration exists in **all three** dialect directories — even pure DML.
- Each statement wrapped in `-- +goose StatementBegin/End`.
- Up + Down both required.
- Embedded into the binary via `//go:embed`. Goose runs from the embedded FS at registry startup.
- Dialect-specific SQL (time arithmetic, JSON ops) handled in the repo layer via `r.DB.Driver` checks, not in migrations themselves — migrations stay shape-equivalent.

## Consequences

**+** Schema is reproducible. Any deploy of any dialect ends in identical logical state.
**+** Seed data (e.g. default `pipeline_triggers`) shipped as DML inside Up.
**+** No "auto-migrate surprised the production DB" failures.
**−** 3× the migration files. Hand-edit each.
**−** Drift across dialects is silent — sqlite smoke passes but postgres breaks. Reviewer must check all three.
**−** No interactive Goose state on prod — operator-driven rollback is `registry migrate-down` (or manual SQL).

## See also

- AGENTS.md §9, §14a
- `internal/infra/db/migrations/embed.go`
