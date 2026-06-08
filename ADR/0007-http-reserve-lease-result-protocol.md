# ADR-0007 — HTTP polling protocol: reserve → lease → result/fail

- Status: accepted
- Date: slice 1

## Context

Workers may be in any language, on any host, behind NAT. Push-based work delivery (worker holds a long-lived gRPC stream) means the server needs reachable worker addresses + ongoing connections. Polling is uncool but ships.

Three queues (frontier, chunks, tasks) all need the same shape: hand out work, track a lease, accept a result or failure, requeue on timeout.

## Decision

Every reservable queue exposes the same four-verb HTTP protocol:

| Verb | Purpose |
|---|---|
| `POST /v1/<queue>/reserve` | Atomic SELECT-then-UPDATE inside `WriteTX`. Returns a batch of leased items + lease tokens. |
| `POST /v1/<queue>/heartbeat` | Extends lease. Body: lease token. |
| `POST /v1/<queue>/result` | Reports success. Body: lease token + result payload. |
| `POST /v1/<queue>/fail` | Reports failure. Body: lease token + reason. |

Authentication: PAT (worker's stored token) on every request. Authorization: capability check first line of the handler (see ADR-0003).

Lease enforcement: HMAC token (see ADR-0004). Sweeper goroutine requeues expired-lease rows every 30 s.

Failure mode is *always* "do nothing, let the lease expire" — never silently mark done. The sweeper is the single source of recovery truth.

## Consequences

**+** Worker can be anywhere. No inbound connectivity required.
**+** Same protocol shape across crawl/chunk/task → one mental model.
**+** Restart-safe: server crash mid-lease = sweeper requeues after TTL. No coordinated state.
**−** Polling overhead. Mitigated by long-poll-ish batch reserves (`batch: N` parameter).
**−** Heartbeat is per-batch, not per-item — a worker that finishes 5/10 items in a batch and stalls loses the remaining 5 on lease expiry.
**−** No streaming progress for long-running tasks. Add a separate observability channel if needed.

## See also

- AGENTS.md §7
- `internal/infra/http/{tasks,embed,frontier}.go`
- `cmd/registry/main.go` — `sweeper` goroutine
