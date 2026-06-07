---
tldr: generic single-kind external processor — reserves processing tasks, shells out with {input}/{outdir} placeholders, returns either stdout-as-text or a produced blob with a follow-up processor
category: core
---

# taskworker

## Target

`cmd/taskworker/` — the reference external processing-task worker. One binary per processor kind (or several kinds claimed by one instance), wrapping an arbitrary shell command. Implements the worker side of the processing queue described in the system spec.

## Behaviour

- Claims processing tasks by `--kind` (repeatable). Only tasks whose `processor` matches one of the configured kinds are reserved; the operator runs one taskworker per pipeline stage they want to externalize (`pdf_ocr`, `docx_to_pdf`, `vvtat`, …).
- Two output modes the operator picks at startup:
  - `text` — the command's stdout is the extracted text for the task.
  - `blob` — the command writes a file under `{outdir}`; the worker uploads it as a new lake object and optionally names a follow-up processor that downstream triggers will pick up.
- The shell command is operator-supplied with two well-known placeholders: `{input}` (the downloaded source blob on local disk) and `{outdir}` (a scratch directory owned by this task). Any tool reachable on the host's `sh -c` PATH is usable without code changes.
- Each task runs in an isolated per-task scratch directory that is removed when the task ends, regardless of outcome. Tasks do not see each other's working files.
- A batch is reserved, then run with bounded in-process concurrency (`--concurrency`) so a single worker box can saturate CPU without blowing memory on huge batches.
- Long-running commands keep their lease alive: while the command is executing, the worker heartbeats the registry on a fixed interval. Stopping the heartbeat (process death, network partition) lets the registry's sweeper reclaim the task.
- Failures are reported with a stable `error_code` (`scratch_mkdir`, `download`, `outdir`, `extract_cmd`, `no_output`, `bad_mode`) and a `retryable` flag distinguishing transient infra failures from "this input will never produce output".
- The worker exits cleanly on SIGINT/SIGTERM and on `--max-runtime` elapsing — letting an operator run it as a one-shot cron job, a systemd unit, or a long-lived daemon with the same binary.
- Empty reserve responses cause a configurable idle sleep rather than busy-looping the registry.

## Design

### Generic shell-out as the extension point
The whole worker is "reserve → download → run a string → push stdout or a file back". No processor logic lives in Go; the operator owns it via `--extract-cmd`. This is the deliberate counterpart to the typed Go workers (`ocrworker`, `embedworker`): when a processor is cheap to express as a single command, `taskworker` is the path of least resistance — a new pipeline stage is a systemd unit + a registered kind, no recompilation.
{>> `exec.CommandContext(execCtx, "sh", "-c", cmdStr)` — shell, not exec — so operators can use pipes, redirections, env interpolation in their command string. The cost is that the command runs under the worker's UID with the worker's environment. <<}

### Placeholder substitution, not argv parsing
The operator writes one string with `{input}` and `{outdir}` markers. The worker does literal replacement before handing the line to `sh -c`. There is no escaping, no argv splitting, no shell metacharacter sanitization — the operator is trusted because the operator wrote both the kind binding and the PAT.
{>> `strings.NewReplacer("{input}", input, "{outdir}", outdir).Replace(extractCmd)` — placeholder values are tempdir paths the worker generated, never user-supplied content. <<}

### Two modes, one transport
Both `text` and `blob` POST to the same `/v1/tasks/result` endpoint as multipart; the meta JSON tells the registry which it is. The taskworker never opens a second protocol — there is one happy path and one failure path no matter what the command produces.
- `text` mode sends `extracted_text` in the meta field. No file part.
- `blob` mode sends the file part plus `output_content_type`, `output_content_sha256`, and `next_processor` in meta. The hash lets the registry deduplicate or verify; the next-processor string is the operator's hand-wired chain that the system's `pipeline_triggers` can also enforce.

### Output discovery via glob, not fixed name
Blob mode lets the operator declare `--output-glob` (default `{outdir}/output.*`). The command can name its output however its underlying tool prefers (`*.pdf` from libreoffice, `*.txt` from pandoc, etc.) without the worker caring. First match wins — multi-output processors are out of scope for this binary.
{>> `filepath.Glob(strings.ReplaceAll(outputGlob, "{outdir}", outdir))` — same `{outdir}` placeholder so operators don't memorize a second substitution rule. <<}

### Heartbeat decoupled from work
A goroutine ticks heartbeats on an independent context that is cancelled the instant the task ends (success or fail). This means the lease extension stops naturally when the work stops — no race where a finished task keeps extending a lease the registry has already cleared.
{>> A `hbInterval` of `0` disables heartbeats entirely — useful when `--exec-timeout` is already shorter than the lease TTL. <<}

### Per-task scratch, not per-worker scratch
A new `os.MkdirTemp` per task with `defer os.RemoveAll` is the cleanup contract. Crashes leak one tempdir per in-flight task; restarts do not accumulate state.

### Failure classification at the call site
The taskworker decides `retryable` at the spot the error happens, not at a central handler:
- `scratch_mkdir`, `outdir`, `download`, `extract_cmd` → retryable (infra/transient)
- `no_output`, `bad_mode` → not retryable (the input or config will never produce output)
The registry honors this when deciding whether to requeue or terminal-fail.

### Content-type-aware input extension
The downloaded blob is written with an extension derived from `blob_content_type` (`.pdf`, `.html`, `.docx`, else `.bin`) so commands that sniff by extension (libreoffice, tesseract sometimes) work without the operator re-renaming.

### One worker, one role
The binary intentionally does *not* multiplex modes per task. Mode and `extract-cmd` are startup flags, so one taskworker process is one shell pipeline. Multiple processors on one box = multiple taskworker processes, distinguished by `--kind` + `--extract-cmd`. This keeps the per-process flag surface small and the operator's mental model "this systemd unit handles pdf_ocr".

### Bounded concurrency inside a batch
A buffered semaphore caps parallel tasks. The batch is reserved as one unit so the registry sees a single reserve roundtrip, but the actual work fans out under the cap. This lets a small batch ride out long-tail tasks without leaving the worker idle on the next.

## Interactions

- **Registry HTTP API** — `POST /v1/tasks/reserve`, `POST /v1/tasks/result` (multipart, text or blob), `POST /v1/tasks/heartbeat`, `POST /v1/tasks/fail`, `GET {blob_url}`. All PAT-bearer authenticated.
- **Lease token** — opaque string from `reserve`, echoed back unchanged on result/fail/heartbeat. The taskworker neither parses nor stores it durably; it's per-task in-memory state.
- **`pipeline_triggers`** (system-level) — `next_processor` in blob-mode result feeds back through the registry's declarative routing. The taskworker only states "produced a `pdf` and the operator suggests `pdf_ocr` next"; whether that chain actually fires is the registry's decision via trigger rows.
- **External tools** (`tesseract`, `libreoffice`, `pandoc`, …) — must be installed on the worker host and present on `sh -c` PATH. The taskworker has no probe for them; the first task fails loudly with stderr captured in `error_message`.
- **`internal/infra/logx`** — slog setup; the only project import outside `cmd/taskworker/`.
- **Capabilities** — gated by the `processing` (endpoint) capability on the PAT plus any worker-declared capability the registered processor kind requires. The taskworker itself doesn't enforce; the registry rejects reserves that don't match.

## Mapping

> [[cmd/taskworker/main.go]]
> [[internal/infra/logx/]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
