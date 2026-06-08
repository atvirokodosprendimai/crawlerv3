# ADR-0005 — Declarative `pipeline_triggers` (replaces hardcoded MIME map)

- Status: accepted (supersedes hardcoded `routeFor`)
- Date: slice 8

## Context

Slices 1–7 routed lake objects to processors via a hardcoded `routeFor(contentType) string` switch. Every new MIME meant a code change + redeploy. Slice 6 added external tasks, slice 7 added per-worker capabilities, and slice 8 needed per-domain routing overrides — the switch couldn't carry all the conditions.

## Decision

`pipeline_triggers` table:

| col | purpose |
|---|---|
| `when_event` | event name (`lake_object_inserted`, `blob_produced`) |
| `when_filter` | JSON predicate over the event payload |
| `enqueue_kind` | processor kind to enqueue |
| `enabled` | toggle |

`app.TriggerDispatcher.Fire(event, payload)` is called from every write path that should trigger downstream work. Matching triggers enqueue `processing_jobs`. Default triggers seeded by migrations. Operators add/disable triggers at runtime via `registry trigger-add` / `trigger-disable`.

Events:
- `EvtLakeObjectInserted` — fires from `Service.AcceptResult`
- `EvtBlobProduced` — fires from `TaskSvc.AcceptBlob`

## Consequences

**+** New MIME → new SQL row, no redeploy.
**+** Operators can disable a misbehaving processor without a code change.
**+** Per-domain / per-collection routing fits as another filter field, no rewrite.
**−** Filter shape is ad hoc JSON. `dispatcher.matches()` must be extended for every new field — the rule isn't fully data-driven.
**−** Trigger seeding via migration is the convention. Forget the seed migration and the feature ships with no defaults.
**−** Loss of compile-time check: a typo'd `enqueue_kind` is only caught when a worker fails to reserve it.

## See also

- AGENTS.md §3e
- `internal/domain/triggers/trigger.go`
- `internal/app/dispatcher.go` — `matches()`
