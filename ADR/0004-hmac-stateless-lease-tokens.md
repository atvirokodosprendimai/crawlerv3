# ADR-0004 — HMAC stateless lease tokens

- Status: accepted
- Date: slice 1 (crawl), slice 3 (chunk), slice 6 (task)

## Context

Every reservable queue (frontier, chunks, tasks) needs proof-of-lease on every callback (`heartbeat`, `result`, `fail`). Options:

1. **DB-checked opaque token.** Each callback round-trips the DB. Cheap but adds load to the write pool.
2. **JWT.** Overkill, depends on a JWT lib, drags in JSON parsing on the hot path.
3. **HMAC over (workerID, expiry, item-hash).** Self-contained, verified in pure CPU.

We picked (3) and kept (1) as defense-in-depth: the token is also stored in `lease_token` columns once issued, and the server matches token bytes on callback.

## Decision

`internal/infra/lease/` issues HMAC-SHA256 tokens with fixed layout:

```
| 32B item-hash | 8B workerID | 8B expiry (unix sec) | 16B truncated HMAC |
```

Base64url-encoded on the wire. `item-hash` per queue:

- crawl: `sha256(canonical URL)` (the frontier PK)
- chunk: `sha256(chunk UUID)`
- task:  `sha256("task:" || BE8(task_id))`

Server-side verify: recompute HMAC, constant-time compare, check expiry, check item-hash matches the path/body parameter.

Sweeper goroutine (every 30s) requeues anything whose lease expired without callback. Lease secret rotation = redeploy with new env var; existing leases all expire within `lease_ttl`.

## Consequences

**+** Zero DB round-trips on lease verification. Hot path is pure CPU.
**+** Leak-of-storage doesn't immediately let an attacker forge leases for *other* rows (HMAC secret still required).
**+** Same shape across all three queues — easy to add a fourth.
**−** Lease secret rotation is awkward — all in-flight leases die. Operators must drain the queue first.
**−** Token expiry is fixed at issue time. Heartbeat can't extend the cryptographic expiry; must reissue.
**−** Three near-identical files in `internal/infra/lease/`. Mild duplication — keep them parallel rather than DRY-ing into a generic.

## See also

- AGENTS.md §3d
- `internal/infra/lease/{lease,chunk_lease,task_lease}.go`
