---
tldr: in-process fanout from write-path events to processing_jobs rows via cached, filter-matched pipeline_triggers
category: core
---

# trigger dispatcher

## Target

`internal/app/dispatcher.go` — `TriggerDispatcher` and its `Fire(event, payload)` entrypoint. The component that turns a single write-path event (a lake object landed, a worker produced a blob) into zero or more `processing_jobs` enqueues, by consulting the operator-editable `pipeline_triggers` table.

## Behaviour

- A caller anywhere in the write path can announce "this event just happened with this payload" and resume immediately. The dispatcher never blocks the caller on its own bookkeeping and never propagates an enqueue failure back up.
- One event can produce many enqueues. Every enabled trigger whose `when_event` matches and whose filter accepts the payload contributes exactly one `processing_jobs` row, in trigger-list order.
- One event can also produce zero enqueues. An event with no matching triggers is a no-op — silence is a legitimate outcome, not an error.
- An empty / unset filter on a trigger is an unconditional match: that trigger fires for every event of its kind.
- Filters today discriminate on two payload fields: content-type prefix (case-insensitive) and exact producing-processor name. Either may be omitted; both must hold when present.
- Operator edits to `pipeline_triggers` (create, enable, disable, delete) take effect within a bounded staleness window of ~5 seconds without restart. Within that window, the dispatcher uses the previous snapshot.
- Disabled triggers behave as if absent — they neither match nor enqueue, regardless of payload.
- The dispatcher is safe to call concurrently from many goroutines on the same event or different events.
- A malformed filter string disqualifies that single trigger from matching; it does not break dispatch for sibling triggers on the same event.

## Design

### Role: the seam between "something happened" and "do something next"

The dispatcher is the *only* in-process bridge from the write path (accept-result handlers, blob-produced handlers) to the processing queue. By centralising fanout here, the write-path handlers stay ignorant of which processors exist, and the trigger table — not Go code — owns routing policy. This is the runtime side of the system-level `pipeline_triggers` decision; see the system spec for why it replaced the hardcoded MIME→processor map.

### Best-effort, fire-and-forget

`Fire` returns no value. Enqueue errors are deliberately swallowed {>> `_, _ = d.Processing.Enqueue(...)` <<} because the alternative — surfacing them — would couple the success of a lake-object write to the success of a downstream queue insert, defeating the whole point of declarative triggers. A failed enqueue is observable elsewhere (sweeper, logs); the write that originated the event must still succeed.

### 5-second snapshot cache

Every `Fire` is on the hot write path; reading `pipeline_triggers` from the DB per call would be wasteful and would put SELECTs on the W pool indirectly via load. Instead the dispatcher caches the full enabled-trigger set, indexed by event, with a short TTL {>> `cacheTTL = 5 * time.Second` <<}. The 5s window is the explicit trade between "edits feel live" and "DB load is bounded". It is short enough that a human flipping a trigger in the CLI sees effect almost immediately, long enough that a burst of events shares one DB read.

The refresh path uses double-checked locking {>> RLock fast path, then Lock + re-check expiry before reloading <<} so concurrent firers don't stampede the repository. A failed refresh leaves the cache empty for this call rather than poisoning it permanently — the next call retries.

### Index by event, not by linear scan

The cache is keyed by `Event` so dispatch for one event walks only triggers configured for that event, not all triggers. Filter evaluation is a second, finer-grained pass per matched event bucket.

### Filter as JSON blob, not typed column

The `when_filter` column is opaque JSON {>> unmarshalled into an anonymous struct inside `matches()` <<}. This is intentional: adding a new filter field is a code change in this file, not a schema migration, and old triggers with unknown fields remain valid (extra JSON keys are ignored). The current vocabulary — `content_type_prefix`, `source_processor` — covers every existing pipeline; new fields land here when a real need appears, not speculatively.

Content-type matching is prefix-based and case-folded so that `application/pdf` and `Application/PDF; charset=...` both satisfy a `content_type_prefix: "application/pdf"` filter. Source-processor matching is exact, because processor names are internal identifiers, not user-supplied text.

### Many-to-one fanout, no de-duplication

If two triggers both match, two `processing_jobs` rows are inserted. The dispatcher does not attempt to deduplicate; that is a policy concern of the trigger table itself (the operator shouldn't configure two triggers that mean the same thing). This keeps the dispatcher's contract simple: "for each match, one row".

### What it deliberately is not

- Not a scheduler — there is no delay, no retry, no priority. Those belong to the processing queue.
- Not a transport — the only "side effect" is a single `Processing.Enqueue` call per match.
- Not a port — it is concrete `app`-layer code that composes two domain repositories. It is wired in by `cmd/` and called directly by write-path handlers.

## Interactions

- **Consumed by** the write-path use cases that own the system events: lake-object insert path (after `Service.AcceptResult` stores a blob and indexes it) and blob-produced path (after a task worker uploads an output blob). These call `Fire` once per event with the new `lake_object_id` and content-type, plus the producing processor name where applicable.
- **Reads from** `triggers.Repository` — a port satisfied by the gorm adapter against `pipeline_triggers`. Reads are list-all; the dispatcher does the per-event bucketing and enabled-filtering in memory.
- **Writes to** `processing.Repository.Enqueue` — inserts one `processing_jobs` row per matching trigger, tagged with the trigger's `enqueue_kind` as the processor name.
- **Indirectly drives** the processing queue's reserve/lease/result lifecycle (see system spec, three-queue shape). Whether the resulting job is picked up by the in-process internal pipeline goroutine or by an external task worker is decided by `Pipeline.InternalProcessors`, not here.
- **Operator interaction** is asynchronous: CLI mutations on `pipeline_triggers` rows become visible to `Fire` within the cache TTL.

## Mapping

> [[internal/app/dispatcher.go]]
> [[internal/domain/triggers/trigger.go]]
> [[internal/domain/triggers/repository.go]]
> [[internal/domain/processing/repository.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
