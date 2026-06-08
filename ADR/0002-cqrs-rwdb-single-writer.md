# ADR-0002 — CQRS via `rwdb`: single writer pool, many readers

- Status: accepted
- Date: slice 1

## Context

SQLite serializes writers at the file level. Naïve `*gorm.DB` with default pooling hits `database is locked` under any concurrent write load. Reservation endpoints fire many concurrent reads + occasional batched writes; queue sweepers fire heavy concurrent writes.

Postgres + MySQL don't have the same hard-serialization constraint, but the *shape* of "writes are scarce vs reads" still holds.

## Decision

`internal/infra/db/rwdb/rwdb.go` returns a `DB` struct with two `*gorm.DB` pools:

```go
type DB struct {
    R *gorm.DB   // SELECTs. Pool size = NumCPU (sqlite) / NumCPU*4 (pg/mysql)
    W *gorm.DB   // INSERT/UPDATE/DELETE. Pool size = 1 (sqlite) / NumCPU (pg/mysql)
    Driver Driver
}
```

Convention:
- `db.R.WithContext(ctx)` for reads.
- `db.W.WithContext(ctx)` for single writes.
- `db.WriteTX(ctx, fn)` for multi-statement transactions that include writes.
- `db.ReadTX(ctx, fn)` for read-only transactions.

WAL mode on SQLite — readers don't block writer.

## Consequences

**+** Zero `database is locked` errors in smokes/production.
**+** Same code shape on all three dialects → app layer is dialect-agnostic.
**+** Clear contract: reads scale horizontally on the read pool; writes are serialized (and that's fine — the work units are large).
**−** Easy footgun: `db.W...First(...)` works but defeats the design. Reviewers must catch.
**−** Multi-statement read-then-write must use `WriteTX` — splitting across pools loses transactional guarantees.

## See also

- AGENTS.md §3b, §14h
- `internal/infra/db/rwdb/rwdb.go`
- `internal/infra/db/gormrepo/frontier_repo.go` — `WriteTX` for atomic reserve
