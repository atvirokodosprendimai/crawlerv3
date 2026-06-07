---
tldr: stateless embedding worker — reserves big chunk batches, fans sub-batches round-robin across an Ollama fleet, falls back to per-text on partial failure
category: core
---

# embedworker

## Target

`cmd/embedworker/` — the dedicated embedding tier in the worker spectrum (see system spec `Worker tiers`). Single binary, single role: drain `document_chunks` into vectors using a horizontally-scaled Ollama fleet.

## Behaviour

- Operator passes one or more Ollama base URLs (`--embed-url` repeated, and/or `--embed-urls-file` with one URL per line). At least one URL is mandatory; zero URLs is a startup error, not a silent no-op.
- Duplicate URLs and blank/`#`-commented lines in the fleet file are discarded; trailing slashes are normalised so the same host written two ways isn't double-counted.
- Worker reserves a large outer batch from the registry (default 1024 chunks), then internally subdivides into sub-batches sized for one Ollama call (default 64). Big outer batch amortises registry round-trips; sub-batch keeps each GPU saturated without overflowing one request.
- Sub-batches are dispatched concurrently. In-flight cap defaults to fleet size — one GPU, one in-flight call — and is operator-tunable (`--max-concurrent`).
- Each sub-batch picks a fleet URL by a global atomic counter; over many sub-batches every host receives equal share regardless of which goroutine issued the request.
- On sub-batch failure (HTTP error, non-200, decode error, or returned vector count not matching input count) the worker retries every text in that sub-batch as a singleton call. One poisoned chunk in a 64-batch does not cost the other 63 their vectors.
- Per-text retry that still fails records a `failed: true` result with a reason string rather than dropping it. The registry sees the explicit failure and routes the chunk accordingly; nothing is silently lost.
- After every outer batch the worker POSTs all results (vectors + failures) in one call. Reserve→embed→result is the only loop; there is no internal queue, no local DB, no on-disk state.
- Empty reserve responses trigger `--idle-sleep` backoff (default 5s). `--max-runtime` lets the operator bound a single process lifetime; `0` runs forever.
- SIGINT/SIGTERM cancels the loop context; in-flight Ollama calls abort, the current batch's leases expire naturally and get reclaimed by the registry sweeper.

## Design

### One role, no abstractions
This binary embodies the "one binary, one site" end of the worker tier spectrum. It does not participate in any other queue, declares no capabilities beyond `embed`, and has no plugin surface. Adding HTML stripping or OCR would mean a different binary, not a flag here.

### Why split outer batch from sub-batch
Two different bottlenecks:
- Registry round-trip + HMAC lease issuance has fixed overhead per `reserve` call. Amortised across a 1024-chunk outer batch.
- Ollama `/api/embed` latency grows roughly linearly with input array length; past some point a single huge request is slower (and riskier) than several mid-size ones in parallel.

Decoupling the two lets the operator tune each independently without affecting the other.

### Round-robin via atomic counter, not per-goroutine state
{>> `atomic.AddUint64` on a shared `fleet.count`, modulo fleet length <<} Any goroutine can pick the next URL without coordination, and across the full run every fleet member sees equal share even though sub-batches launch concurrently. No health-checking, no weighted routing, no sticky sessions — the simplest scheme that gives even load. If one host is slower, the in-flight cap naturally back-pressures the dispatcher.

### Single-text fallback as the failure model
The Ollama `/api/embed` batch endpoint is all-or-nothing per request — a single malformed input or model hiccup loses every vector in the call. Rather than fail the whole sub-batch back to the registry (which would re-lease all 64 chunks on the next sweeper pass and likely re-hit the same poison), the worker performs in-process per-text retries. The poisoned chunk is the only one marked failed; the rest succeed. This costs one round of extra HTTP but converts a correlated failure into independent failures.

Length-mismatch (server returned fewer vectors than inputs) is treated identically to an error — same fallback. The contract is "vectors[i] is for input[i]", and any deviation from that is unsafe to attribute.

### No retries against alternate hosts on first failure
A failed sub-batch retries each text against another round-robin pick, but there is no "try host B if host A failed" logic. Rationale: if the failure was the input (most common case — token-limit overflow, encoding issue), retrying against a different host changes nothing. If the failure was the host, the atomic counter will naturally route the singleton retries elsewhere with high probability.

### Stateless by construction
No local persistence layer. The registry-issued lease token is carried through and returned in every result entry, including failures. {>> Lease tokens are the system-wide HMAC scheme — see system spec `Three queues, one shape` <<} The worker has nothing to crash-recover: a hard kill mid-batch means the registry's sweeper requeues the leased chunks after TTL.

### Failures are first-class results, not silent drops
A failed singleton retry produces `{chunk_id, lease_token, failed: true, reason}` rather than being omitted from the result POST. {>> The registry distinguishes this from a missing chunk_id and can mark the row failed terminally instead of waiting for lease expiry <<} This is the worker side of the system-wide "fail-fast on integrations" stance — but encoded as an explicit signal in the result envelope, not by dropping the row on the floor.

### Fleet config is two paths, merged
`--embed-url` (repeatable flag) and `--embed-urls-file` (file path) are deliberately both supported and merged. Flags are convenient for one-off runs and CI; the file form is convenient for ops who edit `fleet.txt` on a control host. Comment lines (`#`) in the file are tolerated for ops sanity. {>> Order-preserving dedup via a `seen` map ensures the round-robin sequence is deterministic given a config <<}.

## Interactions

- **Registry HTTP** — `POST /v1/embed/reserve`, `POST /v1/embed/result`. PAT in `Authorization: Bearer`. The registry's `embed` endpoint-gated capability check applies; this worker must hold it on its server-stored cap set.
- **Ollama fleet** — `POST {base}/api/embed` with `{model, input: [...]}`, expecting `{embeddings: [[...]]}`. Optional `Authorization: Bearer` if `--embed-api-key` is set (for fronted Ollama deployments).
- **No DB, no blob store, no vector store, no other workers** — by design.
- **Process supervision** — exits non-zero on startup misconfig (no URLs) or fatal `cmd.Run` error; loop errors (reserve/result) are logged and backed off, not fatal. `--max-runtime` enables clean periodic restarts under a supervisor without operator action.

## Mapping

> [[cmd/embedworker/main.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
