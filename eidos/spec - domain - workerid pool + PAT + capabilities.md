---
tldr: pure domain model for a PAT-bound worker — identity, capability set, ban/heartbeat state, and the port the registry persists it through
category: core
---

# workerid domain

## Target

`internal/domain/workerid/` — the type that represents an external participant authenticated by a Personal Access Token, plus the persistence port the rest of the registry uses to load and mutate that participant. No HTTP, no SQL, no PAT-issuance logic lives here — only the shape of "what is a worker" and "what can be asked of the worker store".

The package is named `workerid` deliberately to keep it disjoint from the unrelated runtime "worker goroutine" vocabulary used elsewhere.

## Behaviour

- A worker is identified by an opaque server-side hash of its PAT, never by the PAT itself. Two workers presenting the same PAT resolve to the same identity; a worker holding a leaked PAT hash without the original token can do nothing.
- Each worker carries a list of capability strings. The list is the single source of truth for what the worker is allowed to do at the registry boundary.
- An **empty** capability list means "any kind allowed" — a backcompat door for rows created before capabilities existed. The moment one capability is added the door closes and the list becomes exhaustive: every kind the worker needs must be enumerated.
- A worker can answer the question "may I handle this kind of work?" with no external lookup — the decision is a pure function of its loaded state.
- A worker is either banned or not. A banned worker is recognizable from its state alone; the package does not define what banning *does* — only that the fact is recorded with the moment it became true.
- Workers expose human-affordances (label, last-seen IP, reputation score, last-seen timestamp, max concurrent lease cap) that callers may read but the domain itself attaches no behaviour to. These exist so an operator can reason about the pool through a CLI listing without the domain layer dictating how.
- The persistence port supports the full lifecycle an operator expects: enroll, look up by PAT-hash or by id, list, mutate capabilities, mutate concurrency cap, ban, unban, refresh last-seen IP, and ask "how many leases is this worker currently holding?". Everything else (heartbeat extension, reservation, result acceptance) belongs to the queue domains, not here.

## Design

### Identity is a hash, not a token
The domain stores `PATHash` as opaque bytes. {>> `[]byte` rather than `string` signals "binary digest, not human text"; the registry hashes the inbound PAT once at auth-time and matches against this field, so the cleartext token never reaches the DB and never reaches this package.} The choice locks down a property: a DB dump leak cannot be replayed against the registry without also leaking the live PATs out-of-band.

### Capabilities are strings, deliberately
The package declares `Capability` as a richer struct (name, group, description) but workers themselves carry `[]string`. The struct exists *only* to enumerate the endpoint-gated set the registry hardcodes — it is documentation that the CLI and registry handlers can render and validate against. The on-worker representation is intentionally a flat string slice so that worker-declared capabilities (`pdf_ocr`, `vvtat`, `domain:foo.com`, anything an operator invents) live in the same field with zero schema change. See the system spec's "Capabilities — strings drive everything" for the cross-cutting rule.

### Two flavours, one storage
`EndpointGatedCapabilities()` is the enumeration of capability strings that hardcoded HTTP routes check. Worker-declared capabilities are everything else, and the domain deliberately refuses to know what they are — task dispatch elsewhere string-matches them blindly. This is what lets a new processor be wired end-to-end without touching this package.

### `Can()` is the authorization primitive
`Can(kind)` is the only behavioural method on `Worker`. Authorization decisions across the registry funnel through it so that:
- The empty-list-means-any rule lives in exactly one place. {>> `len(w.Capabilities) == 0 → true` — every call site inherits the backcompat door automatically, and removing it later is a one-line change here.}
- A future capability syntax (wildcards, negation, expiry) can be added without touching every handler.

The empty-caps-equals-any rule is a known trap: it must be removed once all legacy rows are migrated, but until then it must not be silently bypassed at any call site. Centralizing it in `Can()` is the mitigation.

### Authorization always reads stored state
The system rule "never trust the request body for capabilities" is enforced *here* by giving the domain no constructor that accepts capabilities from a wire object. Capabilities only enter via the `Repository.UpdateCapabilities` path (operator action) or via load-from-store. The HTTP layer's job is to look up the worker by PAT-hash and read `w.Capabilities` — there is no shape in this package that would let a handler accidentally trust caps the worker sent.

### Ban as a nullable timestamp, not a bool
`BannedAt *time.Time` carries both the fact and the moment. `IsBanned()` reads the fact; audit reads the moment. {>> pointer-to-time, not zero-value-time, so SQL `NULL` survives the round-trip cleanly across sqlite/postgres/mysql and the "never banned" case is unambiguous.}

### Heartbeat and concurrency are fields, not behaviour
`LastSeenAt` and `MaxConcurrent` sit on the struct but no method touches them. The domain reserves the right to *describe* a heartbeating, capacity-bounded worker without committing to *who updates* those fields — the queue/lease domains own that motion. The repository exposes `CountHeldLeases` so the app layer can compare against `MaxConcurrent` without this package needing to know how leases are stored.

### Port is broad but boring
`Repository` lists every CRUD-shape operation the app layer needs and stops. There is no `Save(Worker)` god-method, because each mutation is operator-meaningful on its own and should be auditable as a distinct verb (ban, unban, update caps, update concurrency). The split mirrors the CLI verbs an operator runs.

## Interactions

- **app/auth** — resolves an inbound PAT to a `Worker` via `Repository.FindByPATHash`, then trusts only the returned struct for the rest of the request.
- **app/reserve handlers (crawl, processing, embed)** — call `wk.Can("crawl" | "embed" | …)` before issuing leases; the embed and read endpoints likewise gate on `Can("embed" | "lake_read" | "extracted_read" | "chunks_read")`.
- **app/tasks dispatcher** — string-matches worker-declared capabilities (`pdf_ocr`, `domain:foo.com`, …) against the same `Capabilities` slice; the domain does not distinguish, on purpose.
- **registry CLI (`workers list / ban / unban / caps / concurrency`)** — drives `Repository` directly, one verb per command.
- **infra/gormrepo (or equivalent)** — the only place the `Worker` struct meets a DB row. Domain stays gorm-free per the system layering rule.
- **queue domains (crawl/processing/embed)** — read `MaxConcurrent` and `CountHeldLeases` to bound dispatch; write `LastSeenAt` and `IPLast` through repository touch methods. This domain does not call into them; they pull from here.

## Mapping

> [[internal/domain/workerid/worker.go]]
> [[internal/domain/workerid/repository.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
