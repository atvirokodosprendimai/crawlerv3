# ADR-0003 — Capability strings for authorization + routing

- Status: accepted
- Date: slice 7

## Context

Pre-slice-7: every worker could do everything. By slice 6 we had crawl, processing, embed, search, and external task workers. Need to:

1. Stop a crawl-only worker from calling `/v1/tasks/reserve`.
2. Match `processing_jobs.kind` to workers that can actually run it.
3. Let operators bind a specific domain to a specific worker pool (e.g. paid-API crawler).

Options considered: RBAC roles (too heavyweight), per-endpoint PAT scopes (forces operator to manage scopes per endpoint), capability strings on the worker row (lightweight, composable).

## Decision

`workers.capabilities` is a JSON array of strings. The string is the source of truth for:

- **Endpoint authz** — every handler checks `wk.Can("<cap>")` first.
- **Task kind matching** — `POST /v1/tasks/reserve {kinds:[...]}` filters to caps in `worker.capabilities`.
- **Per-domain binding** — `domains.required_capability` is matched against `worker.capabilities` in the reserve SQL.

Caps loaded server-side at PAT-auth time. Client-supplied caps in request bodies are **never** trusted for authz.

Empty array on a worker = "any" (backward compat for pre-slice-7 workers).

## Consequences

**+** New endpoint = pick a string, do one `wk.Can(...)` check. No schema change.
**+** Operator-visible: `registry list-workers` shows caps.
**+** Composable: one worker can hold many caps.
**−** No hierarchy — `"task.pdf_ocr"` is unrelated to `"task.*"`. Add convention later if needed.
**−** Empty=any is a trap. Operators must remember to set caps on every new worker, or binding/kind matching becomes silently permissive.
**−** Capability string typos are silent — no central registry. Grep before inventing.

## See also

- AGENTS.md §3c, §7b, §14e, §14f
- `internal/domain/workerid/worker.go` — `Worker.Can`
- `internal/infra/db/gormrepo/frontier_repo.go` — reserve SQL with capability filter
