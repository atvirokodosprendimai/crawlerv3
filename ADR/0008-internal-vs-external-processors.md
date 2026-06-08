# ADR-0008 — Internal vs external processors

- Status: accepted
- Date: slice 2 (internal), slice 6 (external)

## Context

Some processing is cheap CPU-only (HTML strip, text passthrough — microseconds). Some needs heavyweight deps (PDF OCR with Tesseract; LibreOffice for `.docx`; Python ML for image captioning). Forcing everything through the external HTTP-polling worker path adds latency + ops burden for trivial work; forcing everything internal makes the registry a dep-heavy monster.

## Decision

Split:

- **Internal processors** — handled in-process by a goroutine pool inside the registry. List in `Pipeline.InternalProcessors`. Each gets a `execXxx` method in `internal/app/pipeline.go`. The pipeline service polls `processing_jobs` for internal-kind rows and runs them locally.

- **External processors** — `processing_jobs` rows stay queued for external workers (`cmd/taskworker`, `cmd/agent`). Worker reserves via `POST /v1/tasks/reserve` with matching capability.

Today's split:
- Internal: `html_strip`, `text_passthrough`.
- External: `pdf_ocr`, `office_to_pdf`, `image_caption` (example), …

A processor migrates from external to internal when the dep weight is acceptable; the reverse when it grows.

## Consequences

**+** Cheap work is fast (no HTTP hop) and ships in one binary.
**+** Heavy work is operationally isolated — Tesseract crash doesn't kill the registry.
**+** Migration between internal/external is a list-membership change + `execXxx` method.
**−** Two execution paths to keep in sync (failure handling, lease semantics, trigger firing).
**−** Internal processors can't be horizontally scaled independently of the registry. Watch CPU.
**−** `InternalProcessors` is the gate — forget to add a kind and it silently never runs.

## See also

- AGENTS.md §3f
- `internal/app/pipeline.go`
- `cmd/taskworker/main.go` — external pattern
