# crawlerv3 — custom worker examples

Minimal task workers in three languages. All speak the same HTTP
protocol against the registry; pick the language that matches the
runtime where your processor (OCR model, headless browser, custom NLP)
lives.

| Folder | Language | Pattern |
|---|---|---|
| [`golang/`](./golang/) | Go 1.25+ | Native — `import` registry types, no shell-out |
| [`nodejs/`](./nodejs/) | Node 20+ | HTTP poll, `fetch` + `FormData` |
| [`python/`](./python/) | Python 3.10+ | HTTP poll, `httpx` |

## Shared model

A "task worker" claims **processing jobs** from the registry, runs a
domain-specific transform on the downloaded blob, and pushes the result
back. The registry stores results in the lake and (optionally) chains
the next pipeline stage.

```
+----------+   reserve   +----------+   GET /v1/blobs/{id}   +----------+
| registry | <---------> |  worker  | <-------------------> | BlobStore |
+----------+   result    +----------+                        +----------+
              fail/heartbeat
```

Five endpoints, all `Authorization: Bearer <PAT>`:

| Endpoint | Direction | Purpose |
|---|---|---|
| `POST /v1/tasks/reserve` | worker → registry | Lease N tasks for one or more `kinds` |
| `GET  /v1/blobs/{id}` | worker → registry | Download the source bytes |
| `POST /v1/tasks/heartbeat` | worker → registry | Extend the 60 s lease |
| `POST /v1/tasks/result` | worker → registry | Push text or new blob, multipart |
| `POST /v1/tasks/fail` | worker → registry | Report retryable or permanent failure |

Full HTTP reference: [`../README.md#tasks-v1tasks`](../README.md#tasks-v1tasks).

## Result modes

* **Text mode** — `extracted_text` in `meta`, no blob part. Registry
  writes `extracted_documents` + chunks. Use for OCR, NLP, transcripts.
* **Blob mode** — `output_content_type` + `output_content_sha256` in
  `meta`, file in `blob` part. Registry stores a new lake object and
  (if `next_processor` is set) enqueues the downstream stage. Use for
  format conversion (DOCX→PDF, HTML→Markdown, video→audio).

## Loop shape

Every worker, regardless of language, runs this loop:

```
loop:
  tasks = POST /v1/tasks/reserve  {kinds, batch}
  if tasks is empty: sleep(idle); continue
  for each task in tasks:
    blob   = GET /v1/blobs/{id}
    result = process(blob)              # YOUR CODE
    if ok:   POST /v1/tasks/result      # text or blob mode
    else:    POST /v1/tasks/fail        # retryable=true unless input is permanently bad
```

Heartbeat only if a single task takes longer than ~30 s (lease TTL is
60 s server-side).

## Required env / flags

| Name | Meaning |
|---|---|
| `REGISTRY` | Base URL, e.g. `http://localhost:8080` |
| `PAT`      | Personal access token. Create with `registry pat-issue --label foo --capabilities pdf_ocr,docx_to_pdf` |
| `KIND`     | Processor name(s) you can serve, comma-separated |

Issue a token with the capabilities your worker needs. Mismatch → the
registry returns `403 capability_denied` at reserve time.

## Pick your starting point

* New custom processor in Go, deployed alongside the registry → start
  with `golang/`. You can register the kind in
  [`internal/domain/processing/job.go`](../internal/domain/processing/job.go)
  and add a pipeline trigger with `registry trigger-add`.
* Existing Python ML model → `python/`.
* Existing Node service (Puppeteer, sharp, ffmpeg wrappers) → `nodejs/`.
