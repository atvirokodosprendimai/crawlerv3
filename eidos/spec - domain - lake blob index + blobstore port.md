---
tldr: lake domain — split index (Repository) and bytes (BlobStore) ports so raw blobs can live anywhere while rows stay queryable and migratable
category: core
---

# lake domain

## Target

`internal/domain/lake/` — the `Object` type plus the two ports (`Repository`, `BlobStore`) that together define the data lake: an index of stored raw fetched bytes, and a backend-pluggable interface for the bytes themselves.

## Behaviour

- A lake object is durably identified by its content (SHA-256). Two fetches that produce the same bytes resolve to the same row {>> `FindBySHA` is the dedup primitive — callers check first, then `Insert` only if absent}.
- The system can tell, for any object, *where* the bytes physically live (`StorageBackend` + `StorageKey`) without opening them.
- Bytes can be moved between backends (local FS → S3, or back) without losing identity: the row keeps its `ID`, the new location is recorded, and `MigratedFrom` preserves the prior backend name for audit.
- An indexer tailing the lake can stream new objects in insertion order by passing the last-seen ID forward, optionally narrowed by backend or content-type prefix. The cursor never goes backwards on its own.
- A blob store reports which backend it represents, accepts bytes under a caller-chosen key, returns them, exposes metadata cheaply, and can delete. Nothing in the contract assumes a filesystem, a bucket, or any particular naming scheme.
- The domain package has zero infra imports — it can be linked into any process (registry, migrator, indexer) without dragging in gorm, S3 SDKs, or HTTP.

## Design

### Two ports, not one
The lake is deliberately split:
- `Repository` — the **index**. Rows describing what exists, where it lives, and how big it is. Backed by the relational DB.
- `BlobStore` — the **bytes**. Raw `io.Reader` / `io.ReadCloser` flow, no row semantics.

This separation is what allows the migrator worker to exist: it reads the index, copies bytes between two `BlobStore` adapters, then calls `UpdateStorage` to flip the row. No single component needs to know both halves intimately {>> `UpdateStorage(id, backend, key, migratedFrom)` is the atomic hand-off between byte-mover and index}.

### Content-addressable identity, location-mutable
The row's stable identity is `(ID, ContentSHA256, URLHash)`. The location fields (`StorageBackend`, `StorageKey`) are explicitly mutable — every call site treats them as "current home", not "where it was put". `MigratedFrom` is the breadcrumb left behind on each move so an operator can reconstruct history.

URL hash is kept alongside the content hash because the same bytes can be fetched from many URLs, and the same URL can produce different bytes over time. Neither alone is sufficient; both are stored.

### Backend as a string, not a type
`StorageBackend` is a free-form string (`"local"`, `"s3"`, hypothetically `"gcs"`). The `BlobStore` interface exposes `Backend() string` so the adapter self-identifies. Adding a new backend = one new adapter package + one new string value; no domain change, no enum to extend.

### Cursor pagination, not offsets
`ListSince` takes a `SinceID` + `Limit` + optional filters. This is the only listing mode meant for indexers and tailers — it survives concurrent inserts, never re-emits a row, and never skips. `ListByBackend` is the sibling for migration scans (still cursor-based via `afterID`). There is no `Offset` knob {>> deliberate omission — OFFSET on a growing table is a footgun for tailers}.

### Filter knobs are scoped to indexer reality
`ListOpts` carries `Backend` and `ContentTypePrefix` because those are the two axes indexers actually slice on: "give me everything S3-hosted past row N", "give me only `application/pdf*` since I last checked". Empty = no filter. The struct stays small; richer queries belong in a future port, not this one.

### Stat as a uniform contract
`Stat` (`Size`, `ContentType`, `SHA256`, `ModTime`) is what every backend must be able to report cheaply. `Put` returns it (so the caller learns the authoritative size/SHA after the write), `Get` returns it alongside the body, `Stat` returns it alone. The shape is identical across backends so the migrator can treat any pair symmetrically.

### `PutMeta` is hints, not truth
The caller can pass `ContentType` and `SHA256` as hints into `Put`, but `Stat`'s returned values are what get written to the row. This lets a backend that computes its own SHA (S3 ETag in the simple case, or a streaming hasher locally) be the source of truth, while still letting callers that already know the digest avoid re-hashing.

### Ports are interfaces, not structs
Both `Repository` and `BlobStore` are Go interfaces in the domain package. Adapters live in `internal/infra/`. This is the DDD layering the system spec describes; the lake package is one of the cleanest expressions of it — every method is `ctx`-first, no concrete type leaks, no infra imports.

## Interactions

- **Crawl result handler** — on a successful fetch, picks a key, calls `BlobStore.Put`, then `Repository.Insert` (or `FindBySHA` first for dedup). Backend string on the row is whichever store the registry was started with.
- **Pipeline / `pipeline_triggers`** — the `lake_object_inserted` event fires off the insert path, so any processor (html_strip, pdf_ocr, …) can be wired declaratively without lake itself knowing about them.
- **Migrator worker** — reads via `ListByBackend`, copies via two `BlobStore` adapters, commits via `UpdateStorage`. The `MigratedFrom` field is its audit trail.
- **Lake-read indexer** — tails via `ListSince(SinceID=cursor, …)`, pushes into Qdrant / Quickwit / SQL indexes. Has `lake_read` capability (endpoint-gated).
- **Read API** — the HTTP `/v1/lake/...` endpoints are thin wrappers over `Repository`; `BlobStore.Get` streams bytes back when the body is requested.
- **System spec** — see `eidos/spec - system - ...` for the DDD layering rule, the capability gate on read endpoints, and the pluggable-backend principle this domain instantiates.

## Mapping

> [[internal/domain/lake/object.go]]
> [[internal/domain/lake/repository.go]]
> [[internal/domain/lake/store.go]]
> [[internal/infra/]]
