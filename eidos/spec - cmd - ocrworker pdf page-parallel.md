---
tldr: dedicated PDF OCR worker — mutool page count, gs rasterize, tesseract per page, with outer (PDF) and inner (page) concurrency
category: core
---

# ocrworker

## Target

`cmd/ocrworker/main.go` — one binary that claims only `pdf_ocr` tasks from the processing queue and turns PDF blobs into extracted text via shelled-out tools.

## Behaviour

- Reserves work from the same `/v1/tasks/reserve` family every other task worker uses; declares the single kind `pdf_ocr` and nothing else.
- Refuses to start if `mutool`, `gs`, or `tesseract` are missing from `PATH` — the operator sees the failure at launch, not on the first task.
- Processes a batch with two independent levels of parallelism: up to N PDFs concurrently from one reserve, and within each PDF up to M pages concurrently. Both are independently tunable.
- Each page is rasterized with Ghostscript at a configurable DPI to a grayscale PNG and then OCR'd by Tesseract with a configurable language string (default Lithuanian + English).
- All page text is concatenated in page order and posted as a single `extracted_text` field. Page boundaries are preserved as blank-line separators in the joined text; no per-page metadata is returned.
- A single page failure fails the whole PDF task — partial OCR is not posted.
- A PDF that mutool reports as zero pages fails non-retryable; every other failure surface (download, mutool parse, gs, tesseract, scratch dir creation) fails retryable so the lease expires and the registry can hand the row to another worker.
- Long-running PDFs are kept alive by an opt-in heartbeat loop that runs for the duration of the task at a configurable cadence; the lease never silently expires while a worker is still grinding.
- Both the whole-PDF budget (`exec-timeout`) and the per-page budget (`page-timeout`) are enforced as hard wall-clock caps; either firing aborts gs/tesseract and surfaces a retryable failure.
- All intermediate files (input PDF, per-page PNG, per-page TXT) live under one per-task scratch dir that is unconditionally removed before the worker returns — success, failure, or panic.
- Per-page PNGs are deleted as soon as their TXT is written, so peak disk for a large PDF scales with `page-concurrency`, not page count.
- `max-runtime` bounds the worker's own lifetime so the process can be supervised on a schedule (cron, systemd timer, k8s job) without external kill logic.
- SIGINT/SIGTERM stops the reserve loop and exits the process; in-flight PDFs are not gracefully drained — the lease expires and another worker picks them up.

## Design

### Why a dedicated binary instead of `taskworker --kind pdf_ocr`
PDF OCR is the one processor whose work unit has its own internal parallelism. Generic task workers reserve a batch and run K tasks in parallel; that's one knob. OCR needs two — outer (PDFs in flight) and inner (pages per PDF) — because tesseract is single-threaded per invocation and the page count of a single document can dominate end-to-end latency. Splitting it into its own binary gives those two knobs their own flags and keeps the generic worker simple. {>> `--concurrency` (PDFs) and `--page-concurrency` / `PAGE_CONCURRENCY` (pages) are independent semaphores <<}

### Why shell out to mutool/gs/tesseract instead of bindings
The reference stack is whatever the operator already has on their box. Three CLI binaries with stable arg surfaces compose more reliably than three sets of CGo bindings, and any single one (e.g. pdftoppm instead of gs) can be swapped by editing one function without disturbing the others. The pre-flight `LookPath` check makes the dependency explicit; the worker simply refuses to run without the tools. {>> `checkTools` runs once before the reserve loop, not per-task <<}

### Page-count-first, then fan out
Knowing the page count up front is what makes the page-level fan-out possible. mutool's `trailer/Root/Pages/Count` is a cheap structural read of the PDF metadata; no rendering happens. Once the count is known, each page is rendered and OCR'd independently — gs is told `-dFirstPage=N -dLastPage=N` so there is no shared rasterization step that would force serialization. {>> the per-page worker calls `runGs` then `runTesseract` then deletes the PNG, fully self-contained <<}

### One failure kills the PDF, by design
OCR text is consumed downstream as a single extracted document; partial text with silently dropped pages would corrupt the search index without any signal to the operator. The page-fan-out collects every page error but returns the first one and aborts the whole task as retryable. The registry's lease sweeper then requeues — the retry runs the whole PDF again, not just the failed pages. Simpler than per-page state, and the cost is bounded by `attempt_count` on the row. {>> `firstErr` from the error channel, every other error is logged but not propagated <<}

### Retryable vs non-retryable failures
The only non-retryable failure is "PDF has zero pages" — that's a property of the input, not a transient environmental fault. Download errors, gs crashes, tesseract crashes, scratch-dir errors are all retryable because they could be host-specific (disk full, OOM kill, network blip) and a different worker may succeed. The choice is encoded in `postFail`'s `retryable` flag; the registry's accept-fail handler decides what to do with it.

### Heartbeat is opt-in, not implicit
A short PDF finishes well under a lease TTL and doesn't need a heartbeat at all; a long scanned book absolutely does. Rather than picking a one-size value, heartbeat cadence is a flag and `0` disables the loop entirely. The cadence runs for the lifetime of the workOne call and is cancelled by the same context that cancels the PDF. {>> `hbCtx, hbCancel` is paired with the per-task deferred cancel, not the outer ctx <<}

### Two timeouts, two scopes
`exec-timeout` is the budget for one PDF end-to-end (download + mutool + all pages + result post). `page-timeout` is the budget for a single gs+tesseract pair. The per-page timeout is derived from a child context of the per-PDF context, so the PDF budget naturally caps the sum of page work even if no individual page exceeds its own budget. Result POST is *outside* `execCtx` deliberately — the OCR is done; posting under a fresh context lets us avoid losing finished work to a tight whole-PDF budget.

### Scratch hygiene
One temp dir per task, removed via `defer os.RemoveAll` so it survives panics. Per-page PNGs are deleted as soon as the corresponding TXT is written so that the working-set on disk is bounded by `page-concurrency` PNGs at a time, regardless of how many pages the PDF has. The PDF and TXTs survive until the task ends because the join step needs to read every TXT in page order. {>> `scratch/input.pdf`, `scratch/pages/page-NNNN.{png,txt}` <<}

### Multipart even though only text is posted
The result endpoint accepts multipart so that processors which produce derived blobs (HTML strip, normalized PDFs) can post them alongside metadata in one request. The OCR worker has only text to post, but it speaks the same multipart shape as every other processor — one field `meta` containing the JSON envelope including `extracted_text`. Keeping the wire shape uniform across all processors is more valuable than the marginal saving of a JSON-only path. {>> registry's `/v1/tasks/result` consumes the `meta` field as the canonical envelope; multipart tempfile cleanup lives on the registry side <<}

### Reserve loop and SIGINT
The outer loop is the unchanged three-queue reserve→work→repeat shape. A signal handler installed alongside `Notify` exits the process directly on SIGINT/SIGTERM rather than draining in-flight work — losing leases to the sweeper is cheap, while complicating shutdown is expensive. `max-runtime` provides the supervised-lifetime knob; SIGINT provides the operator-cancel knob.

## Interactions

- **Registry HTTP** — `/v1/tasks/reserve` (POST, kinds=[pdf_ocr]), `/v1/tasks/result` (multipart with `meta`), `/v1/tasks/fail`, `/v1/tasks/heartbeat`. Bearer PAT on every call.
- **Registry blob endpoint** — `GET {registry}{BlobURL}` to fetch the source PDF. Uses the same PAT.
- **External binaries on PATH** — `mutool` (page count), `gs` (page render to PNG), `tesseract` (PNG → text). The worker is useless without all three.
- **Registry pipeline_triggers** — operator must seed (or have migration-seeded) a trigger that routes `application/pdf` lake objects to a `pdf_ocr` processing job. The worker is the consumer; the dispatcher decides what gets enqueued.
- **Worker capability set on the registry** — the bound PAT must list `pdf_ocr`; the reserve SQL filters server-side on the stored capability set.
- **Local filesystem** — `os.TempDir()` is used for per-task scratch and must have headroom for `(input PDF size) + (page-concurrency × rasterized PNG size) + (all TXT outputs)`.
- **System-level concerns inherited verbatim** — three-queue protocol, HMAC lease tokens, scope-lock, capabilities-as-strings. See system spec for those.

## Mapping

> [[cmd/ocrworker/main.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
