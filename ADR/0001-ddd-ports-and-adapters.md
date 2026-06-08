# ADR-0001 — DDD + ports & adapters layout

- Status: accepted
- Date: slice 1

## Context

Three storage backends in scope from day one: SQLite (dev), Postgres + MySQL (prod). Two blob backends: local FS, S3. One vector store today (Qdrant), more later. Workers in multiple languages.

A monolith that imports `gorm` + `chi` + `qdrant` directly from business code makes every backend swap a cross-cutting rewrite, and locks the test surface to live infra.

## Decision

Hexagonal layout under `internal/`:

- `domain/<area>/` — pure Go types + port interfaces. **MUST NOT** import `gorm`, `chi`, `qdrant`, any infra.
- `app/` — use cases. Depend on port interfaces only.
- `infra/<adapter>/` — concrete implementations: gorm repos, chi handlers, Qdrant HTTP client, FS/S3 blob stores.
- `cmd/registry/main.go` — composition root. Wires concrete adapters into app services.

Every storage-bound type has two halves: a port (`domain/<area>/repository.go`) and an impl (`infra/db/gormrepo/<area>_repo.go`). `mapXxx` helpers convert between gorm models and domain types.

## Consequences

**+** Adding a new BlobStore = new package implementing `lake.BlobStore`, one flag in `main.go`. No app changes.
**+** Domain types testable without DB.
**+** Backend swap (e.g. swap Qdrant) is local.
**−** Indirection cost: every new field traverses domain type → gorm model → mapper.
**−** Easy to drift: a domain pkg accidentally importing infra breaks the rule silently. Enforce via review + grep.

## See also

- AGENTS.md §3a, §6
- `internal/domain/frontier/repository.go` — canonical port shape
- `internal/infra/db/gormrepo/frontier_repo.go` — canonical impl shape
