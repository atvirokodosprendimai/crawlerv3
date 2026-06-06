# Node.js custom worker

Minimal Node 20+ task worker. Stdlib `fetch` + `FormData`, no deps.

## When to use this shape

* You already have a Node-based transform (Puppeteer screenshot,
  `cheerio` HTML scrape, `sharp` image resize, `@xenova/transformers`
  inference).
* You don't want a build step. Single `worker.mjs`, run with `node`.

## Structs (JSON shapes)

The Go server defines the wire format. From Node, treat them as plain
objects.

```ts
// POST /v1/tasks/reserve  →
type Task = {
  task_id:           number,
  processor:         string,   // e.g. "html_to_markdown"
  lake_object_id:    number,
  blob_url:          string,   // relative; prepend REGISTRY
  blob_content_type: string,
  blob_size_bytes:   number,
  attempt_count:     number,
  lease_token:       string,   // opaque; copy back unchanged
  lease_expires_at:  number    // unix seconds
}

type ReserveResp = { tasks: Task[] }

// POST /v1/tasks/result  meta part — pick one mode
type ResultTextMeta = {
  task_id: number, lease_token: string,
  extracted_text: string,
  language?: string, page_count?: number
}
type ResultBlobMeta = {
  task_id: number, lease_token: string,
  output_content_type:   string,         // e.g. "text/markdown"
  output_content_sha256: string,         // hex
  next_processor?:       string          // optional follow-up stage
}

// POST /v1/tasks/fail
type FailReq = {
  task_id: number, lease_token: string,
  error_code: string, error_message: string,
  retryable: boolean
}
```

Send `meta` as a `FormData` field of type string. Blob mode adds a
`blob` field with the raw bytes.

## Flow

```
loop:
  res = await reserve({kinds, batch})
  if (!res.tasks.length) { await sleep(idle); continue }
  for (const t of res.tasks) {
    try {
      const body = await downloadBlob(t.blob_url)
      const out  = await process(t, body)          // YOUR CODE
      await postResult(t, out)
    } catch (err) {
      await postFail(t, 'process', String(err), true)
    }
  }
```

For tasks running >30 s, set up a `setInterval` calling
`/v1/tasks/heartbeat` every 20 s; `clearInterval` on completion.

## Run

```bash
export REGISTRY=http://localhost:8080
export PAT=...
export KIND=html_to_markdown

node worker.mjs
```

Node 20+ required (built-in `fetch`, `FormData`, `Blob`,
`crypto.subtle`).

## Replacing the processor

Edit the `processHTMLToMarkdown` function in `worker.mjs`. Return one
of:

* `{ extractedText: '...' }` — text mode
* `{ outputBytes: Buffer, outputContentType: 'text/markdown', nextProcessor: 'pdf_ocr' }` — blob mode

## Error contract

| Worker decision | What to send |
|---|---|
| Transient (5xx from upstream, ECONNRESET, ETIMEDOUT) | `retryable: true` |
| Permanently bad input | `retryable: false` |
| `409 heartbeat_failed` from server | lease lost, drop this task silently |
