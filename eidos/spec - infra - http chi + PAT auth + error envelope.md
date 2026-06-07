---
tldr: chi-routed registry HTTP surface — PAT-authenticated /v1, per-handler capability gate, canonical JSON error envelope, optional services wired by nil-check
category: core
---

# http chi + auth

## Target

`internal/infra/http/` — the registry's only ingress. A single `Router(...)` constructor wires every endpoint workers and indexers ever touch. Public health probe is unauthenticated; everything else lives under `/v1` behind PAT auth.

## Behaviour

- `GET /healthz` is always reachable, returns `200 ok` plaintext, and is logged at Debug so probe traffic never drowns operational logs.
- Every `/v1/*` request requires `Authorization: Bearer <PAT>`. Missing header → `401 missing_bearer`. Unknown token → `401 unknown_pat`. Banned worker → `403 banned`. The token itself is never logged or echoed.
- Authenticated handlers see a fully-hydrated worker record in the request context. Anything checking capability reads that server-stored record — the request body's declared capabilities are never trusted for authorization.
- Each protected handler answers a capability question first and short-circuits with `403 capability_denied` before doing any work. `crawl` → jobs reserve. `embed` → embed reserve. `lake_read` / `extracted_read` / `chunks_read` → corresponding read endpoints. `search` → both vector and FTS search. For tasks reserve, the requested `kinds[]` is checked one-by-one.
- Every error response is one shape: `{"error": code, "code": code, "message": text}`. Workers can switch on `code` without parsing the human message. Status codes follow a small vocabulary: 400 client-shape problems, 401 auth-absent, 403 capability/ban, 404 not-found, 409 lease/state conflict, 500 internal.
- Result endpoints accept `multipart/form-data` with a JSON `meta` part and a `blob` part. Multipart spool files (parts >1MB) are removed on handler return — leaked tempfiles are a known foot-gun of `net/http` and the package owns the cleanup.
- A worker's effective batch size on any reserve is silently clamped to its remaining `MaxConcurrent` headroom; saturated workers get `200 OK` with an empty list, not an error.
- Optional registry services (embed, tasks, blobs, reads, vector search, FTS) are wired only when their dependencies are present. Endpoints they own simply don't exist on a stripped-down deployment — a `404` from chi is the answer, not a runtime nil panic.
- Every request emits one structured access-log line carrying method, path, status, byte count, duration, request-id, and remote addr. Log level rises with status class (4xx → warn, 5xx → error).
- Panics inside handlers do not take the process down; the request gets a generic 500 and the next request is served.

## Design

### One constructor, declarative wiring
`Router(...)` takes every collaborator the registry might expose and assembles the tree. Optional services are gated by `nil` checks — passing `nil` for `embed` means `/v1/embed/*` is never registered, not that it returns 503. The set of routes is therefore a function of what was injected at process startup, which keeps cmd/registry honest about what it depends on.

{>> `if embed != nil { eh := NewEmbedHandler(...); r.Post("/embed/reserve", eh.Reserve) }` — the nil-check is the feature flag <<}

### Two layers of authority, in order
Every protected handler runs the same micro-sequence: pull worker from context, ask `wk.Can(<endpoint cap>)`, then do work. The middleware proves *who* the caller is; the handler decides *what* this caller is allowed to do here. Endpoint capability strings (`crawl`, `embed`, `lake_read`, `extracted_read`, `chunks_read`, `search`) are hard-coded inline at exactly the handler that gates them — there is no central capability registry. Worker-declared kinds (PDF, OCR, per-domain, …) are checked the same way (`wk.Can(k)`), but they originate from operator-configured rows, not from the handler file.

This split — endpoint-gated vs worker-declared, both via `wk.Can` — is the system-level capability model (see system spec). The HTTP layer is the place where the two flavors meet.

### Server-stored authorization, body-driven hints
The reserve bodies carry a `capabilities` array, but the handler passes the *worker row's* capabilities into the use case. The body field is operator-visible noise; trusting it would let a worker grant itself bindings just by sending them. The same rule applies to per-domain `required_capability`: the SQL filter uses the row, not the request.

{>> `frontier.ReserveRequest{Capabilities: wk.Capabilities}` — body's `req.Capabilities` is intentionally unread <<}

### Canonical error envelope
One helper, one shape. The duplicated `error`/`code` fields are deliberate — `error` was the original field, `code` is the structured alternative; emitting both keeps old workers parsing and new workers happy without a versioned API. `message` is optional human prose; workers never branch on it.

### Multipart hygiene as a defaulted concern
The `Result` endpoints on jobs and tasks share the same shape: `MaxBytesReader` cap, `ParseMultipartForm` with 1MB in-memory threshold, deferred `MultipartForm.RemoveAll()`. The cleanup is non-obvious — net/http leaves spool files in `/tmp/multipart-*` if you don't call it — so it's encoded as a near-mechanical pattern in every multipart handler.

### Saturation is a soft signal
`effectiveBatch` collapses "how many can this worker hold?" into a single helper. A saturated worker doesn't see an error — it sees an empty leased batch and a 200, so its polling loop just sleeps and retries. Errors are reserved for things the worker can act on; saturation is just backpressure.

### chi over net/http, but thinly
chi is used for `Route`, `URLParam`, `RequestID`, `Recoverer`, `Timeout`, and `NewWrapResponseWriter` — the boring middleware grid. Nothing leans on chi's exotic features (no per-route middleware stacks, no sub-router type tricks). Swapping to gorilla/mux or net/http 1.22's enhanced patterns would be a localized rewrite of `server.go`.

### Logging follows status, not intent
The access logger picks log level from the response status, not from a handler-side opinion. This keeps the "is this request interesting?" question in one place: handler code emits its own `slog.InfoContext` lines for accepted/rejected business events, and the access line gives the HTTP-level view orthogonally.

## Interactions

- **PAT middleware → workerid.Repository** — every authenticated request hits `FindByPATHash`. The hash is SHA-256 of the bearer token; the plaintext PAT never lands in the DB. `TouchIP` is a fire-and-forget side effect for operator visibility.
- **Handlers → app.{Service, TaskSvc, EmbedSvc, SearchSvc, FTSSvc}** — handlers are translation layers: HTTP DTO ↔ app-layer input/output. No business logic lives here.
- **Handlers → workerid.Repository.CountHeldLeases** — used by `effectiveBatch` and the `/v1/workers/me` introspection endpoint.
- **Blobs handler → lake.Repository + lake.BlobStore** — single-backend assumption: refuses with 409 if the row's stored backend doesn't match the active one. Mixed-backend deployments would need per-row dispatch (not implemented).
- **Reads handler → lake / extraction / chunking repositories** — exposes since-id cursors for external indexer workers (Qdrant, Quickwit, SQL). Pure SELECT path, capability-gated by `*_read`.
- **Server → operator** — access log via `slog.Default()`. Health probes degraded to Debug so noise stays low.

## Mapping

> [[internal/infra/http/server.go]]
> [[internal/infra/http/auth.go]]
> [[internal/infra/http/json.go]]
> [[internal/infra/http/policy.go]]
> [[internal/infra/http/access_log.go]]
> [[internal/infra/http/jobs.go]]
> [[internal/infra/http/tasks.go]]
> [[internal/infra/http/embed.go]]
> [[internal/infra/http/blobs.go]]
> [[internal/infra/http/reads.go]]
> [[internal/infra/http/search.go]]
> [[internal/infra/http/fts.go]]
> [[internal/domain/workerid/]]
