# plan - pull subsections for whole codebase

**Created:** 2026-06-07 13:02
**Branch:** task/eidos-pull-whole-codebase
**Overview:** [[pull - 2606071302 - whole codebase overview]]
**System spec:** `eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md`

## Goal

One eidos spec per major module/feature in `crawlerv3`. Overview spec already drafted. Subsection pulls each: focused read of the subset → dedicated spec under `eidos/`.

Use `/eidos:plan-continue` to step through the actions.

## Phases

### Phase 1 — Domain layer specs

Pure types + ports. Smallest, read fastest.

- [ ] **A1** — pull `internal/domain/frontier/` → `spec - domain - frontier url queue + politeness + scope-lock.md`
  - files: `job.go`, `repository.go`, `domain_repo.go`
- [ ] **A2** — pull `internal/domain/lake/` → `spec - domain - lake blob index + blobstore port.md`
  - files: `object.go`, `repository.go`, `store.go`
- [ ] **A3** — pull `internal/domain/processing/` → `spec - domain - processing jobs + capability dispatch.md`
  - files: `job.go`, `repository.go`, `capability.go`
- [ ] **A4** — pull `internal/domain/extraction/` + `internal/domain/chunking/` → `spec - domain - extraction + chunking + context join.md`
- [ ] **A5** — pull `internal/domain/triggers/` → `spec - domain - triggers declarative routing.md`
- [ ] **A6** — pull `internal/domain/workerid/` → `spec - domain - workerid pool + PAT + capabilities.md`

### Phase 2 — App layer specs

Use cases. Medium size.

- [ ] **B1** — pull `internal/app/service.go` → `spec - app - service crawl orchestration.md`
- [ ] **B2** — pull `internal/app/pipeline.go` → `spec - app - pipeline internal processors.md`
- [ ] **B3** — pull `internal/app/tasksvc.go` → `spec - app - tasksvc external task workers.md`
- [ ] **B4** — pull `internal/app/embed.go` + `search.go` + `collection.go` → `spec - app - embed svc + qdrant search + per-domain collection.md`
- [ ] **B5** — pull `internal/app/dispatcher.go` → `spec - app - trigger dispatcher.md`
- [ ] **B6** — pull `internal/app/fts.go` + `chunksink.go` → `spec - app - fts mirror + chunk sink.md`
- [ ] **B7** — pull `internal/app/migrator.go` → `spec - app - migrator blob backend mover.md`

### Phase 3 — Infra layer specs

Adapters. Larger files but mechanical.

- [ ] **C1** — pull `internal/infra/db/rwdb/` + `internal/infra/db/migrations/` → `spec - infra - rwdb CQRS pools + dialect parity + goose.md`
- [ ] **C2** — pull `internal/infra/db/gormrepo/` → `spec - infra - gormrepo port impls + mapping helpers.md`
- [ ] **C3** — pull `internal/infra/http/` (all handlers) → `spec - infra - http chi + PAT auth + error envelope.md`
- [ ] **C4** — pull `internal/infra/lease/` → `spec - infra - lease HMAC stateless tokens 3 shapes.md`
- [ ] **C5** — pull `internal/infra/store/{local,s3}/` → `spec - infra - blob stores local FS + s3 v2.md`
- [ ] **C6** — pull `internal/infra/pipeline/{htmlproc,chunker,pdfproc,docxproc}/` → `spec - infra - in-process processors htmlproc + chunker.md`
- [ ] **C7** — pull `internal/infra/urls/` → `spec - infra - url canonicalization.md`
- [ ] **C8** — pull `internal/infra/qdrant/` + `embedclient/` + `quickwit/` + `stanza/` → `spec - infra - external clients qdrant + embedclient + quickwit + stanza.md`

### Phase 4 — Worker bins

User-facing CLI surfaces; README has most of the surface already, climb to intent.

- [ ] **D1** — pull `cmd/registry/main.go` → `spec - cmd - registry control plane CLI.md`
- [ ] **D2** — pull `cmd/worker/main.go` → `spec - cmd - worker reference crawl streaming pipeline.md`
- [ ] **D3** — pull `cmd/taskworker/main.go` → `spec - cmd - taskworker generic shell-out.md`
- [ ] **D4** — pull `cmd/ocrworker/main.go` → `spec - cmd - ocrworker pdf page-parallel.md`
- [ ] **D5** — pull `cmd/embedworker/main.go` → `spec - cmd - embedworker ollama fleet round-robin.md`
- [ ] **D6** — pull `cmd/agent/main.go` → `spec - cmd - agent unified worker many roles.md`
- [ ] **D7** — pull `cmd/litekoworker/` → `spec - cmd - litekoworker webforms postback paginator.md`
- [ ] **D8** — pull `cmd/unicrawler/` → `spec - cmd - unicrawler yaml selenium universal.md`
- [ ] **D9** — pull `cmd/migrator/main.go` → `spec - cmd - migrator local↔s3 blob mover.md`

## Notes

- System-level overview spec written upfront — subsection specs reference it but don't repeat the cross-cutting concepts (capabilities, lease lifecycle, CQRS, scope-lock, triggers).
- Each subsection pull should produce one focused spec; no working `memory/pull - ...` file per subsection unless the scope warrants it.
- Branch: stay on `task/eidos-pull-whole-codebase`. Commit after each phase.
