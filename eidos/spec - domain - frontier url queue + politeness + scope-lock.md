---
tldr: pure-type port surface for the URL crawl queue — Job/Lease/Result shapes, domain politeness fields, and scope-lock semantics
category: core
---

# frontier domain

## Target

`internal/domain/frontier/` — the pure-types ring (no infra imports) that defines what a URL crawl queue *is* in this system. Covers the `Job` / `Lease` / `Result` / `Failure` value types, the `Repository` port for the queue itself, and the `DomainRepo` port for the per-host politeness/binding row that gates reservation.

This is the A1 ring of the [[system spec|spec - system - distributed crawler with capability-gated workers and pluggable infra]]. Everything here is interface and struct; the concrete gorm/SQLite/Postgres implementations live elsewhere in `internal/infra/`.

## Behaviour

- A URL exists in the queue at most once. Re-enqueuing the same URL is a silent no-op; the caller learns whether their attempt was the inserting one. {>> `Enqueue` returns `(inserted bool, err)` keyed by URL hash, not URL string <<}
- A reserved batch comes back as N jobs, each carrying its own opaque lease token and expiry. The worker holds those tokens to later complete, fail, or heartbeat — there is no separate "claim by job id" call.
- Reserve is the only path that picks work. It honours two per-domain knobs simultaneously: a minimum gap between reserves of the same host, and a cap on URLs handed out per reserve from that host. {>> `crawl_delay_ms` + `parallel_fetches` on `DomainRow` <<}
- Reserve is also the enforcement point for per-domain capability binding: a domain with a `required_capability` is invisible to workers whose declared capability set does not include it. A domain with no required capability is visible to everyone.
- An empty capability slice on the reserve call means "any" — backcompat with workers that predate the capability system. As soon as the slice is non-empty, the worker only sees domains whose `required_capability` is either empty or in the slice.
- Heartbeat, Complete, and Fail are all token-gated: the call only succeeds if the supplied lease token bytes match the row's stored token. A worker without the token cannot mutate the row, even with a valid PAT.
- Failure is a first-class outcome with retryability baked in. A retryable failure schedules a next attempt with backoff; a non-retryable failure or an exhausted `MaxAttempts` parks the row in a terminal `dead` state.
- Lease expiry is a separate path from worker action: a sweeper sees `lease_expires_at < now` and flips the row back to queued, so a silent worker never holds work indefinitely.
- Discovered links are observed as part of the `Result` payload — the queue never fetches anything itself, it only records what the worker reported. {>> `Result.DiscoveredLinks` carries the candidates the dispatcher will later filter against scope-lock <<}
- Canonical-URL dedup is a separate query (`LookupCanonical`) keyed by hash, so the link dispatcher can cheaply skip URLs that already exist before deciding whether to enqueue.
- An operator can mass-requeue rows by `(status, worker, domain)` filter without touching any worker flow. This is explicitly an out-of-band surface.
- `StatusCounts` returns a histogram, not a list — observability is always a count by status, never a row dump from this port.
- A domain row may carry an `EmbedCollection` override; the frontier port exposes the lookup `urlHash → domainID` so downstream lake/embed stages can resolve which vector collection a fetched URL should land in.

## Design

### Pure types, no infra
This package contains zero references to a DB driver, a transport, a logger, or a clock. {>> No `gorm`, no `chi`, no `sql` import — verifiable by `grep` <<} Adapters in `internal/infra/` satisfy `Repository` and `DomainRepo`; the rest of the codebase only depends on these interfaces. This is the per-package realisation of the system-level layering decision (see [[spec - system...]] §Layering).

### Reserve as the politeness chokepoint
All politeness lives at the reserve step, not at enqueue and not at result. The rationale: politeness must hold across worker restarts and across multiple workers sharing the same domain, so it has to be enforced by the only authority that decides who fetches what next — the registry-side reserve query. Spreading the rules to enqueue or to worker-side sleeps would let a restarted worker burst.

A consequence: the port deliberately bundles `workerID + capabilities + batch + leaseTTL + tokenSigner` into a single call. Splitting "find candidates" from "lease them" would race; the implementation has to acquire and stamp atomically. {>> The `leaseToken func(...) (string, []byte)` callback is passed in so the domain port stays unaware of HMAC; the signer lives in app/infra <<}

### Tokens at the port boundary, not in the domain
The lease token is two values: a string (for the wire) and bytes (for storage). The domain port treats them as opaque — it does not know that the bytes are HMAC-SHA256 over `(urlHash, expires, secret)`. That knowledge belongs to whoever constructs the `leaseToken` callback. The domain only knows: "given a `urlHash` and `expires`, give me a (token, bytes) pair to attach to this lease, and later I'll compare bytes for equality."

This is the local realisation of the system-level "HMAC stateless lease tokens" decision: the domain port stays clean of the crypto, the bytes-also-stored defense-in-depth lives in the infra adapter.

### Capability gating happens server-side, on the stored set
The `capabilities []string` parameter on `Reserve` is the *server-stored* set loaded at PAT-auth time, never the worker's self-reported set from the request body. This is the trust boundary: a worker cannot reserve URLs of a bound domain by lying about its caps. {>> The handler layer is responsible for loading from the worker row; this port just trusts what it's given <<}

### Status is a closed enum of five
`queued | leased | done | failed | dead`. Three terminal-ish states (`done`, `failed`, `dead`) plus two transit states. `failed` is recoverable on the next sweep+retry tick; `dead` is the absorbing state for exhausted retries or non-retryable errors. The distinction matters because `RequeueByFilter` is the operator's escape hatch to resurrect `failed` (and sometimes `dead`) rows en masse without code change.

### Domain row carries everything reserve needs
`DomainRow` is intentionally not just `(id, host)` — it bundles `IsActive`, `CrawlDelayMS`, `ParallelFetches`, `RequiredCapability`, and the optional `EmbedCollection`. This keeps reserve a single SQL pass: join `crawl_frontier` to `domains`, filter on `is_active = true`, apply per-domain throttles, honour binding. Splitting these across separate tables/rows would force either N+1 lookups or a more complex join graph for no design gain.

### Scope-lock lives one layer up, but is visible here
The frontier port itself does not enforce scope-lock — `Enqueue` will accept any URL with a valid `DomainID`. The scope check ("is this discovered link's host an active domain?") happens in the app-layer dispatcher that processes `Result.DiscoveredLinks` before calling `Enqueue`. This is intentional: keeping the port "dumb" lets the operator's `--allow-auto-domains` flag live entirely in app code, and lets the migrator / seeder / re-crawl flows bypass scope-lock without a port-level escape hatch.

### `DomainRepo` is a mutation surface for live operations
Every `Update*` method exists because the operator must be able to change politeness, scheme, binding, or embed-collection at runtime without restart or migration round-trip. The port is shaped per-field on purpose: a single `UpdateRow` would let a CLI command accidentally clobber unrelated fields it didn't intend to touch. {>> One method per editable column = surgical CLI subcommands like `domain set-delay`, `domain bind`, etc. <<}

### `UpsertByHost` over `Create + Find`
Domain creation is idempotent because seeding the same host twice (operator re-runs, migrations, smoke scripts) is the common case, not the exception. The port exposes the idempotent shape directly rather than asking every caller to wrap `FindByHost` + `Create`.

## Interactions

- **Consumed by app/use-cases**: the crawl reserve/result/fail HTTP handlers, the link dispatcher that enqueues `Result.DiscoveredLinks`, the sweeper goroutine, and the operator CLI for `frontier requeue` / `domain` subcommands.
- **Implemented by infra**: a gorm-backed adapter in `internal/infra/` provides both `Repository` and `DomainRepo`, sharing the `R`/`W` pool split described in the system spec.
- **Read by downstream stages**: lake / embed code paths call `DomainIDByURLHash` to map a fetched URL back to a domain row, then read `EmbedCollection` off it to route vectors.
- **Driven by `pipeline_triggers`**: a result write on this queue is one of the events the dispatcher fans out from, but the trigger machinery itself is not visible to this port.
- **Bound to `workers`**: the `RequiredCapability` field is matched against worker rows by reserve; that matching is the only point of coupling between this domain and the worker domain.

## Mapping

> [[internal/domain/frontier/job.go]]
> [[internal/domain/frontier/repository.go]]
> [[internal/domain/frontier/domain_repo.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
