---
tldr: pipeline_triggers — table-driven (event, filter) → processor routing that replaces the hardcoded MIME map
category: core
---

# triggers domain

## Target

`internal/domain/triggers/` — the `Trigger` aggregate, the `Event` constant set, the filter contract, and the `Repository` port. The actual dispatcher and SQL adapter live elsewhere; this package only defines the shape both sides agree on.

## Behaviour

- An operator can wire a new processor end-to-end by inserting one row: pick an event, write a filter, name the destination `EnqueueKind`. No code change, no restart.
- A trigger can be paused (`Enabled=false`) instead of deleted, so seeded defaults can be turned off without losing the row for audit/restore.
- Many triggers may match the same event; each matching row produces one enqueue. There is no "first match wins" — fan-out is the contract.
- Two events exist today and are the only vocabulary downstream rows commit to:
  - `lake_object_inserted` — a freshly stored blob from the crawl path is now visible. Filterable by its content type.
  - `blob_produced` — a task worker uploaded a derived blob. Filterable by content type AND by the producing processor's name, so chains like `docx_to_pdf → pdf_ocr` are expressible without recursion on the first event.
- Filters are intentionally narrow: a content-type *prefix* match (so `application/pdf` matches `application/pdf; charset=…`) and an optional source-processor equality. Anything richer is a future event, not a richer filter language.
- A trigger row, once created, is immutable except for its `Enabled` flag. Editing routing means disable + insert, preserving history.

## Design

### Declarative replaces imperative

The original pipeline hardcoded a `switch contentType` map inside the accept-result path. Every new processor meant editing registry Go code and redeploying. The trigger table inverts this: the dispatcher is generic and stateless about *which* processors exist; the table is the source of truth. This is the decision the package encodes — the types here exist to make "routing is data, not code" enforceable.

### Event vocabulary is closed; filter vocabulary is open-ish

`Event` is a typed string constant set {>> `Event` is `type Event string` with named consts, not a free-form string, so a typo in a seed migration fails to compile <<}. Adding an event is a deliberate domain change with a code edit. By contrast, the filter is an opaque JSON blob {>> `WhenFilter string` rather than a typed struct <<} — the dispatcher knows the two recognised keys today (`content_type_prefix`, `source_processor`) but storing JSON means adding a third key tomorrow does not require migrating every existing row.

### Filter shape is documented in the package comment, not enforced in the type

Keeping `WhenFilter` as `string` is a deliberate trade: typing it would force a domain-layer struct that the SQL adapter and the dispatcher both depend on, and any new filter key would ripple. The cost is that a malformed filter is a runtime issue, surfaced by the dispatcher. Acceptable because triggers are operator-curated, not user input.

### EnqueueKind is a processor name, not a queue name

The string written here is matched against worker-declared capabilities and the internal-processor list. This is what lets the same trigger row route to an in-process goroutine OR an external task worker without the operator knowing which — the pipeline goroutine claims what it can, everything else stays `queued`. See system spec → "Internal vs external processors".

### Repository port stays small

`Create`, `List`, `ListByEvent`, `SetEnabled`, `Delete`. No `Update` — see immutability above. `ListByEvent` exists because the dispatcher's hot path looks up by event on every accept; a full `List` would force the cache layer (5s TTL, owned upstream) to filter in memory. {>> separating the two read shapes lets the SQL adapter index on `when_event` without leaking that decision into the dispatcher <<}

### No ordering, no priority

Triggers are an unordered set per event. If the operator needs ordering (e.g. "OCR before classify"), it is expressed by chaining events (`blob_produced` of OCR's output triggers classify), not by a priority column. This keeps the row schema flat and the dispatcher loop trivial.

### CreatedAt is the only audit field

No `updated_at`, no `created_by` — the row is immutable plus an enable flag, so a single timestamp suffices. The migration that seeds defaults stamps `CreatedAt` at install time so operators can distinguish seeded from hand-added rows by age.

## Interactions

- **Dispatcher (in `internal/app/pipeline` or equivalent)** consumes `ListByEvent` after each `Service.AcceptResult` write and after each task-worker blob upload. It is responsible for filter evaluation and for translating `EnqueueKind` into a `processing_jobs` row.
- **Seed migration** inserts the bootstrap rows that recreate the legacy MIME map (`application/pdf` → `pdf_ocr`, `text/html` → `html_strip`, …). Without these, a fresh install does nothing on ingest.
- **CLI** exposes create/list/toggle/delete against the repository for runtime edits.
- **Processing domain** owns the `Processor` name space that `EnqueueKind` strings reference; there is no compile-time check that the name resolves — wrong name → row enqueued but nothing claims it → sweeper-visible.
- **System-level cache** (5s TTL on the dispatcher's side) bounds how quickly edits take effect; the domain itself is cache-agnostic.

## Mapping

> [[internal/domain/triggers/trigger.go]]
> [[internal/domain/triggers/repository.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
