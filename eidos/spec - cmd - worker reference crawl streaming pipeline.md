---
tldr: reference Go crawl worker — single reserver feeds a buffered channel that N fetcher goroutines drain in parallel, server-enforced politeness
category: core
---

# reference crawl worker

## Target

`cmd/worker/` — the reference Go implementation of the crawlerv3 crawl-side worker protocol. One binary, one role: reserve crawl jobs, fetch URLs over HTTP, extract `<a href>` discovered links from HTML, post results (body blob + meta) or fail back to the registry. The yardstick other-language worker implementations are measured against.

## Behaviour

- Operator boots one process per host with `--registry` and `--pat`. All other knobs (`--batch`, `--concurrency`, `--fetch-timeout`, `--idle-sleep`, `--user-agent`) have defaults that produce a sane low-throughput worker out of the box.
- The worker runs an unbounded loop: reserve a batch → dispatch jobs into parallel fetchers → reserve again. It never blocks waiting for a whole batch to finish before pulling the next one — a slow fetch by one goroutine never stalls free slots on the others.
- Each reserved batch is at least as large as the parallelism level. The operator can request more (deeper queueing) but never less, so all fetcher goroutines have work to do whenever the registry has work to give.
- When the registry returns an empty batch, the worker idles for a configurable interval, then tries again. There is no exponential backoff; idle behaviour is a flat poll.
- Each fetched body is size-capped. The registry can dictate the cap per-job; if it doesn't, the worker falls back to a generous local default and rejects anything over the limit as `too_large` (non-retryable).
- HTML responses are scanned for `<a href>`, honouring `<base href>`. Discovered links are resolved to absolute http/https URLs and reported back with their new depth so the registry can decide what to enqueue. Non-HTML responses report zero links.
- Every outcome is reported. A successful fetch posts the body blob plus metadata; a failure posts an error code with a `retryable` hint. The worker never silently drops a leased job — the lease is always accounted for.
- SIGINT/SIGTERM cancels the parent context. In-flight fetchers drain their current job, the reserver stops pulling new batches, and the process exits cleanly.

## Design

### Streaming pipeline, not batch-then-wait
The reserver and the fetchers are decoupled by a buffered channel sized to the concurrency level {>> `jobs := make(chan job, conc)` with `conc` fetcher goroutines reading <}. The reserver pushes individual jobs onto the channel as it gets them; the fetchers pull as they finish. A batch of 10 jobs with concurrency 4 is *not* "fetch 4, wait, fetch 4, wait, fetch 2" — it's a continuous stream where the reserver replenishes as soon as the channel has room. This keeps tail-latency jobs from starving the worker.

### Batch ≥ concurrency
A batch smaller than the concurrency level would leave fetcher goroutines idle even when work exists {>> floored at startup: `if batch < conc { batch = conc }` <}. The decision: prefer slight over-reservation (some jobs queueing in the channel briefly) over guaranteed underutilization.

### Server-enforced politeness, not client-side throttling
The worker has no per-domain rate limiter. Politeness lives entirely on the registry side, encoded in `domains.crawl_delay_ms` and `domains.parallel_fetches`. The worker just asks for `batch` jobs and gets back however many the registry's reserve SQL was willing to hand out. Two consequences:
- Adding a new worker box does not require coordinating rate limits — the registry is the single source of truth.
- The worker can ship without per-domain state; it never needs to know which domains it is allowed to hammer.

### PAT-authed, capability-opaque
The worker carries a single PAT in `Authorization: Bearer …` on every call to `/v1/jobs/reserve`, `/v1/jobs/result`, `/v1/jobs/fail`. Capabilities (notably `crawl`) are checked server-side at PAT-auth time; the worker neither sends nor reasons about them. Adding a `domain:foo.com` binding or restricting this worker to one tenant is a registry-side row change with zero worker redeploy.

### Two HTTP clients, two timeout regimes
The fetch client carries the user-configurable `--fetch-timeout` (default 30s) and is used only for the outbound URL grab. A separate API client with a fixed 30s timeout talks to the registry. Decision: a slow target site must never block the worker's ability to report results or fail to the registry on the *same* client's timeout window.

### Fail-loud on unexpected status
Reserve / result / fail all treat any non-200 from the registry as an error. There is no "best-effort, swallow and move on" path. A botched result post returns the error up to the caller; the lease will expire and the sweeper will requeue. Same protocol shape as the system-wide three-queue contract — see system spec.

### Discovery is shallow on purpose
Anchor text is not extracted in v1 — only `href` and `rel`. The decision: the registry decides what enrichment crawls need; the reference worker stays cheap. Non-http(s) schemes are filtered at source so the registry never sees `mailto:` / `javascript:` noise.

### Multipart for results, JSON for everything else
Results carry an arbitrary-size blob, so they go as `multipart/form-data` with a `meta` JSON field and a `blob` file field {>> matches registry's `/v1/jobs/result` handler which spools the file part to disk <}. Everything else (reserve, fail) is plain JSON — small, no streaming need.

## Interactions

- **Registry HTTP API** — three endpoints: `POST /v1/jobs/reserve`, `POST /v1/jobs/result`, `POST /v1/jobs/fail`. All PAT-authed. The reference worker is the canonical client of the shape documented in the system spec.
- **HMAC lease tokens** — the worker treats the lease token as an opaque string echoed back on every result/fail. It does not verify the signature; the registry does.
- **Per-domain politeness columns** — `domains.crawl_delay_ms` and `domains.parallel_fetches` are read by the registry's reserve SQL; the worker observes the throughput consequences but never reads or sets them.
- **Discovered links → frontier intake** — the registry takes `discovered_links[]` from the result body and decides per-link whether to enqueue (scope-lock, max-depth, dedup — all registry-side concerns).
- **Other worker tiers** — `taskworker`, `ocrworker`, `embedworker`, `agent`, `litekoworker`, `unicrawler` reuse the same reserve/lease/result/fail shape against the other two queues (`processing_jobs`, `document_chunks`). This file is the simplest realization of that shape and the template the others diverge from.

## Mapping

> [[cmd/worker/main.go]]
> [[internal/infra/logx]]
