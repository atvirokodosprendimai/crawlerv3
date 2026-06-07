---
tldr: one binary, multiple roles — operator picks any subset of crawl + task kinds + embed via --enable; each role runs as its own loop under a single PAT
category: core
---

# agent

## Target

`cmd/agent/main.go` — the unified worker binary. A single process can play any subset of {crawl, pdf_ocr, docx_to_pdf, html_strip, embed} concurrently, chosen at startup via `--enable`. Distinct from the specialised binaries (`worker`, `taskworker`, `ocrworker`, `embedworker`) in the worker-tier table.

## Behaviour

- Operator enables any non-empty subset of roles with `--enable crawl,pdf_ocr,docx_to_pdf,embed`; the agent runs each enabled role as an independent loop in the same process.
- A single PAT authenticates every role. The PAT's server-side capability set is what actually gates the work — listing a role the PAT cannot perform results in repeated reserve errors, not silent permission escalation.
- Per-kind configuration uses a flat `--<flag>.<kind>` shape (`--extract-cmd.pdf_ocr "..."`, `--mode.docx_to_pdf blob`, etc.) so adding a new kind needs no new flag name, only a new key.
- Task kinds (`pdf_ocr`, `docx_to_pdf`, `html_strip`, …) are fully data-driven: the agent has no kind-specific code path — it shells out to the configured extract command and posts either captured stdout ("text" mode) or a glob-matched output file ("blob" mode).
- The `crawl` role is built in: it fetches over HTTP with a configurable User-Agent and timeout, hashes the body, extracts `<a>` links from HTML, and posts a multipart result.
- The `embed` role uses an inline single-URL Ollama-style client; operators wanting fleet round-robin or shell-out embedding are pointed to the dedicated `embedworker` binary.
- Any role missing its required configuration (embed without `--embed-url`, a task kind without `--extract-cmd.<kind>`) logs an error and exits *that goroutine only* — other enabled roles keep running.
- A single Ctrl-C / SIGTERM cancels the shared context and every role drains and exits cleanly.

## Design

### One binary, many roles
The agent occupies the "convenience" end of the worker-tier spectrum: a single operator process that covers a small cluster's full pipeline without standing up four separate binaries. It deliberately overlaps with `worker`/`taskworker`/`embedworker` rather than replacing them — those remain the production-grade choice when one role needs to scale, harden, or specialise (e.g. embedworker's Ollama fleet round-robin) independently. {>> the package comment originally excluded embed and pointed at `embedworker`; the actual code grew an inline embed loop, but the doc-comment still records the design intent: "model API + vector store client belongs in a dedicated embed worker" <}

### Roles as parallel goroutines, not a multiplexer
Each `--enable` entry spawns its own goroutine running its own reserve→work→post loop against its own registry endpoint. There is no shared scheduler, no fair-share, no cross-role coordination. Rationale: each registry queue already has its own reserve semantics (batch size, scope-lock filtering, lease TTL); multiplexing in the agent would only duplicate and lie about server-side guarantees. {>> `sync.WaitGroup` over the enabled slice, one goroutine per kind, all sharing one cancellable ctx <}

### Per-kind flag shape
Configuration uses urfave/cli's `StringMapFlag` so each per-kind value is a `kind=value` pair under one flag name (`--extract-cmd pdf_ocr=...`). This keeps the flag surface flat and extensible: a new task kind needs only new map entries, never a new flag. Defaults are encoded as a `mapDefault` helper rather than per-kind branches.

### Server is the source of truth
The agent does not know which kinds the PAT is allowed to handle, which domains scope-lock to which workers, how many leases are outstanding, or what the lease TTL is. It reserves; the registry either returns work or doesn't. This matches the system-level capability rule: workers cannot spoof their way into work they're not entitled to, because authorisation is read from the stored cap set at PAT-auth time. {>> there is no client-side cap declaration; `--enable` is purely a local "which loops to spawn" toggle <}

### Task kinds are data, not code
The task loop has zero kind-specific branches. Every kind is `(extract-cmd, mode, output-glob, output-content-type, next-processor)`. The two modes encode the two shapes a processor can produce: extracted *text* (captured stdout, posted as `extracted_text`) or a transformed *blob* (a file under outdir, posted as a multipart upload with sha256 + next-processor chain hint). Templating uses `{input}` and `{outdir}` placeholders so any external CLI tool can be wired in. {>> `strings.NewReplacer("{input}", ..., "{outdir}", ...)` over `cfg.ExtractCmd`; scratch dir is `os.MkdirTemp` and `defer RemoveAll`'d <}

### Crawl loop is built-in because it pre-dates the data-driven model
Unlike task kinds, `crawl` ships with a hand-written fetch+parse path: it bounds body size via `MaxBodyBytes` from the reserve response, hashes content, extracts links with `golang.org/x/net/html`, and resolves them against `<base href>` if present. Link extraction only runs for `text/html` responses. Rationale: the crawl protocol has fields (depth, discovered_links, canonical URL) that don't fit the generic task shape, and the registry expects them. {>> link extraction normalises to http/https only and reports `new_depth = depth+1`; the server is responsible for scope-lock and max-depth enforcement <}

### Embed is a deliberate compromise
The inline embed loop exists for operator convenience but explicitly does not support what `embedworker` does: round-robin over a fleet of Ollama instances, batch `/api/embed` calls, shell-out backends. It calls a single `/api/embeddings` endpoint per chunk, per request. Documentation tells the operator to switch to `embedworker` when this becomes a bottleneck. {>> per-chunk requests with `model+prompt` payload; optional bearer-token API key; no retry, no batching, no parallelism within the batch <}

### Fail-fast per item, not per role
A single task or crawl failure posts a fail with `retryable` set based on the error class (network/io = retryable, bad-url/too-large/no-output = not). The role keeps reserving. This delegates retry policy to the registry's backoff and sweeper, consistent with the three-queue uniform shape: workers never decide when to retry, only whether the registry *may* retry.

### Idle-sleep on empty reserves
Each loop sleeps `--idle-sleep` (default 5s) on an empty reserve or a reserve error. The sleep is cancel-aware: SIGTERM during sleep returns immediately. No exponential backoff — the registry is expected to be responsive, and idle workers are cheap.

## Interactions

- **Registry HTTP API** — consumes `/v1/jobs/{reserve,result,fail}`, `/v1/tasks/{reserve,result,fail}`, `/v1/embed/{reserve,result}` and `GET {blob_url}` (relative path returned by the task reserve, fetched with the same PAT).
- **Capability system** — relies on server-side `wk.Can(...)` checks; the operator must mint the PAT with the right caps (`crawl`, `embed`, plus any worker-declared cap like `pdf_ocr`, `docx_to_pdf`, `html_strip`).
- **External CLI tools** — extract commands shell out via `sh -c` with a configurable `--exec-timeout`. The agent is only as safe as the tools it invokes; no sandbox.
- **logx** — uses the project's `logx.Init("agent", level)` for slog setup so JSON output and log levels match other binaries.
- **System spec** — every cross-cutting guarantee (three-queue shape, HMAC lease tokens, scope-lock, capability authorisation) is enforced server-side; see the system spec for the authoritative description.

## Mapping

> [[cmd/agent/main.go]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
