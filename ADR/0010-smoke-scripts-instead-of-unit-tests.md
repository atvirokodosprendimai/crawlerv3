# ADR-0010 — Smoke shell scripts as primary test layer

- Status: accepted (revisit when slices stabilize)
- Date: slice 1, reaffirmed slice 12

## Context

The product is a distributed protocol: a registry, multiple workers, a blob store, a vector store, leases, sweepers. Unit tests in pure Go can cover individual repos and services, but the bugs we actually ship are at the seams — lease semantics, trigger payload shape, capability matching SQL, dialect drift, retry/sweeper interactions.

Pre-1.0, every slice's correctness criterion is "the protocol round-trips end to end on a clean DB."

## Decision

Primary test layer = shell smoke scripts under `scripts/smoke_*.sh`. Each smoke:

1. Builds binaries into a temp dir.
2. Starts fakes for external deps (fake Qdrant, fake Ollama via inline Python).
3. Starts `registry serve` against an ephemeral SQLite DB.
4. Drives the protocol with `curl` + `python3 -c` for JSON parsing.
5. Asserts state via `sqlite3` + `[[ "$x" == "y" ]]`.
6. `cleanup` trap kills children.

One smoke per slice. New feature ⇒ new smoke. PR isn't done until all smokes pass.

Unit tests welcome where they pay off — but they're not the gate.

## Consequences

**+** Tests exercise the real binaries, real HTTP, real SQL, real lease tokens. Bugs at the seams surface here, not in prod.
**+** Smokes double as runnable docs of the protocol.
**+** New language workers can be smoke-tested against the same harness.
**−** Slow vs Go unit tests. Acceptable at current scale.
**−** Only SQLite is smoked. Postgres/MySQL parity is by manual review of migrations (see ADR-0006).
**−** Shell + Python in smokes is fragile to dependency drift. Pin nothing → reproducibility risk.
**−** Coverage isn't measured. Add a Go test harness when the product stops being slice-by-slice.

## See also

- AGENTS.md §10
- `scripts/smoke*.sh`
