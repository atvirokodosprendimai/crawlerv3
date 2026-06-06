# Go custom worker

Native Go task worker. ~120 lines, no shell-out, no extra deps beyond
the stdlib. Plug your processor in by replacing one function.

## When to use this shape

* Your transform is already implemented in Go (or vendorable as a Go
  library — e.g. `github.com/ledongthuc/pdf`, `golang.org/x/net/html`).
* You want a single static binary, no Python/Node runtime on the worker
  host.
* You want to import the registry's domain types directly to keep
  enums in sync.

If your transform is a CLI tool (Tesseract, LibreOffice, ffmpeg), use
`cmd/taskworker` instead — it already wraps `exec.CommandContext`.

## Structs

```go
type Task struct {
    TaskID          int64  `json:"task_id"`
    Processor       string `json:"processor"`         // e.g. "html_to_markdown"
    LakeObjectID    int64  `json:"lake_object_id"`
    BlobURL         string `json:"blob_url"`          // relative; prepend Registry
    BlobContentType string `json:"blob_content_type"`
    BlobSizeBytes   int64  `json:"blob_size_bytes"`
    AttemptCount    int    `json:"attempt_count"`
    LeaseToken      string `json:"lease_token"`       // opaque HMAC, send back unchanged
    LeaseExpiresAt  int64  `json:"lease_expires_at"`  // unix seconds
}

type ReserveResp struct {
    Tasks []Task `json:"tasks"`
}

// Result you return from Process(). One of ExtractedText or OutputBlob set.
type Result struct {
    ExtractedText string // text mode

    OutputBlob          []byte // blob mode: raw bytes
    OutputContentType   string //            e.g. "text/markdown"
    NextProcessor       string //            optional follow-up stage
}
```

`LeaseToken` is opaque to the worker — copy it into every result/fail/
heartbeat call. Never mutate it.

## Flow

```
            +--------------------+
            | reserve(kinds, N)  |   POST /v1/tasks/reserve
            +---------+----------+
                      |
              tasks?  v  no  --> sleep(idle) --> loop
                      | yes
                      v
            +--------------------+
            | downloadBlob(t)    |   GET  /v1/blobs/{id}
            +---------+----------+
                      v
            +--------------------+
            | Process(blob)      |   YOUR CODE
            +---------+----------+
                      |
              error?  v  yes --> postFail(retryable=true|false)
                      | no
                      v
            +--------------------+
            | postResult(...)    |   POST /v1/tasks/result (multipart)
            +--------------------+
```

For long-running tasks (>30 s), spawn a goroutine that calls
`postHeartbeat` every 20 s and cancel it on completion.

## Run

```bash
go run ./examples/golang \
  --registry http://localhost:8080 \
  --pat $PAT \
  --kind html_to_markdown
```

The example ships an `html_to_markdown` processor that converts HTML
blobs to a stub markdown string. Replace `processHTMLToMarkdown` in
`main.go` with your real transform.

## Wiring a new processor end-to-end

1. Add the constant to
   [`internal/domain/processing/job.go`](../../internal/domain/processing/job.go):
   ```go
   ProcHTMLToMarkdown Processor = "html_to_markdown"
   ```
2. Issue a PAT with that capability:
   ```bash
   registry pat-issue --label md-worker --capabilities html_to_markdown
   ```
3. Add a pipeline trigger so new HTML lake objects enqueue your kind:
   ```bash
   registry trigger-add --on lake_object_inserted \
     --content-type text/html --enqueue html_to_markdown
   ```
4. Run this worker.

## Errors

| Worker decision | What to send |
|---|---|
| Network or transient (5xx upstream, disk full) | `postFail(code, msg, retryable=true)` |
| Permanently bad input (corrupt PDF, unsupported version) | `postFail(code, msg, retryable=false)` |
| Lease already expired (server returns `409 heartbeat_failed`) | drop the task, do not retry on the same lease |
