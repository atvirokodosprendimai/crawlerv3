# Python custom worker

Minimal Python 3.10+ task worker using `httpx`. ~120 lines.

## When to use this shape

* Your processor is an ML model (transformers, spaCy, Whisper,
  Tesseract via `pytesseract`).
* You want to reuse an existing Python codebase without shelling out.

## Dataclasses

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class Task:
    task_id:           int
    processor:         str          # "html_to_markdown", "pdf_ocr", ...
    lake_object_id:    int
    blob_url:          str          # relative; prepend REGISTRY
    blob_content_type: str
    blob_size_bytes:   int
    attempt_count:     int
    lease_token:       str          # opaque; copy back unchanged
    lease_expires_at:  int          # unix seconds

@dataclass
class Result:
    extracted_text:        Optional[str]   = None  # text mode
    output_bytes:          Optional[bytes] = None  # blob mode
    output_content_type:   Optional[str]   = None
    next_processor:        Optional[str]   = None  # optional follow-up
```

Only one of `extracted_text` or `output_bytes` is set per result. The
server picks the mode from which fields the `meta` JSON contains.

## Flow

```
loop:
    tasks = await reserve(kinds, batch)
    if not tasks:
        await asyncio.sleep(idle)
        continue
    for t in tasks:
        try:
            body = await download_blob(t.blob_url)
            result = await process(t, body)         # YOUR CODE
            await post_result(t, result)
        except Exception as e:
            await post_fail(t, "process", str(e), retryable=True)
```

For long-running tasks, run `post_heartbeat` from an `asyncio.create_task`
every 20 s and cancel it when work finishes.

## Run

```bash
pip install httpx

export REGISTRY=http://localhost:8080
export PAT=...
export KIND=html_to_markdown

python worker.py
```

## Replacing the processor

Edit `process_html_to_markdown` in `worker.py`. Return a `Result` with
either `extracted_text=...` or `output_bytes=...`.

## Error contract

| Worker decision | What to send |
|---|---|
| Transient (5xx upstream, network blip, GPU OOM) | `post_fail(retryable=True)` |
| Permanently bad input (corrupt file, wrong format) | `post_fail(retryable=False)` |
| `409 heartbeat_failed` from server | lease lost — drop the task silently |

## Capability mismatch

If `POST /v1/tasks/reserve` returns `403 capability_denied`, the PAT
was issued without the kind. Re-issue:

```bash
registry create-worker --label py-worker --capabilities html_to_markdown,pdf_ocr
```
