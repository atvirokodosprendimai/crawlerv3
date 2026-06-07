---
tldr: pure-Go in-process content processors — htmlproc strips HTML to text, chunker splits text into overlapping word windows, pdfproc/docxproc are skip-stubs awaiting external engines
category: core
---

# in-process processors

## Target

`internal/infra/pipeline/htmlproc/`, `internal/infra/pipeline/chunker/`, `internal/infra/pipeline/pdfproc/`, `internal/infra/pipeline/docxproc/`. The leaf packages that turn raw bytes into the text + chunks the rest of the pipeline (and downstream embed/FTS queues) consumes. Two are real (htmlproc, chunker); two are placeholder seams (pdfproc, docxproc).

## Behaviour

- **htmlproc** takes any HTML byte stream and yields visible plain text with single-space separation. Inline JS, CSS, hidden-mode noscript, and template content never reach the output.
- Malformed HTML is tolerated — the stripper produces whatever text it can up to the parse error point rather than failing the document. Only hard I/O errors escape.
- Whitespace runs in the result collapse to one space, and the result has no leading or trailing whitespace, so downstream word-counting and chunking are deterministic regardless of source-document formatting noise.
- **chunker** takes a text blob and a `Config` and emits an ordered list of overlapping word windows. Each chunk carries its own zero-based index and word count.
- Empty / whitespace-only input produces zero chunks — callers receive an empty slice, not a sentinel chunk.
- Chunk size and overlap are caller-tunable; nonsensical inputs (≤0 size, negative overlap, overlap ≥ size) are silently corrected to defaults rather than erroring, so processors never crash on a misconfigured trigger row.
- The last chunk terminates exactly at the end of input and is shorter than the configured window when the text doesn't divide evenly.
- **pdfproc** accepts a PDF stream and returns `ErrSkip` plus an empty result. The caller's contract is "this is a non-fatal opt-out": the pipeline keeps moving and the row stays in whatever post-skip state the orchestrator chose (typically `queued` for an external OCR worker).
- **docxproc** accepts a DOCX stream and returns `ErrSkip`. The conversion target is PDF (not text directly), reflecting the intended pipeline shape: DOCX → PDF → OCR/extract.
- Both stubs fully drain the input reader before returning so callers that reuse / close the stream see consistent behaviour now and after a real engine is wired.

## Design

### Pure functions, no I/O ports, no DB

All four packages expose package-level functions over `io.Reader` (and `io.Writer` for the converter). They hold no state, take no contexts, log nothing, touch no DB, and import nothing from `internal/domain` or `internal/app`. They are the leaves of the dependency tree on purpose — the pipeline goroutine and external task workers each call them directly without an intermediary port. {>> No `Processor` interface is defined here; the in-process dispatcher in `internal/infra/pipeline/` knows which leaves it owns by name. <<}

### htmlproc — tokenizer, not DOM

HTML is processed as a token stream, not parsed into a tree. The decision: extraction is a single linear pass with O(1) state (a skip-depth counter), so even pathological deeply-nested or 10MB HTML files don't allocate a tree. The cost is that the stripper doesn't understand semantic structure — every visible text node is equivalent. That's the right trade for an indexer feeding embeddings + FTS, where layout doesn't matter and quotability beats fidelity. {>> `golang.org/x/net/html` tokenizer; `skipDepth` integer guards script/style/noscript/template subtrees. <<}

### Two-phase whitespace normalization

The stripper first joins text tokens with single spaces, then runs a second pass that collapses any remaining whitespace runs and trims edges. Doing both is deliberate: token-level joining handles inter-element gaps; the post-pass handles intra-token noise (newlines inside `<pre>`, tabs in JSON-LD payloads, etc). The output invariant — "exactly one space between words, no edge whitespace" — is what makes downstream word-count chunking deterministic. {>> `collapse()` runs after the tokenizer loop. <<}

### Skip-tag set is closed and conservative

Only `script`, `style`, `noscript`, `template` are stripped. The decision: prefer over-inclusion of text (header/footer/nav copy ends up in the index) to under-inclusion (silently dropping the page's actual content because of a sloppy class name). Boilerplate removal is a higher-layer concern; this leaf only removes things that are *definitionally* not page content.

### Tokenizer EOF is success, not error

`io.EOF` from the tokenizer terminates the loop and returns whatever text was accumulated so far; any other error is fatal. This makes the function tolerant of truncated downloads and malformed-but-readable HTML — the indexer gets partial text rather than a queued row stuck in `failed`. {>> The `ErrorToken` branch distinguishes `z.Err() != io.EOF`. <<}

### chunker — word-based, not byte- or token-based

Chunks are bounded by word count, not bytes or model-tokens. The rationale:
- Bytes are wrong for multilingual content (UTF-8 multi-byte runes inflate sizes inconsistently).
- Model tokens would tie the chunker to a specific tokenizer at the wrong layer — embed workers may target different models over time.
- Word count is a robust, language-agnostic-enough proxy that holds across the embed targets we care about and lets the operator reason about chunk size without reading tokenizer docs.

{>> `splitWords` uses `unicode.IsSpace` as the only boundary; no language-specific tokenization. <<}

### Sliding window with overlap

Each chunk advances by `WordsPerChunk - OverlapWords`. Overlap exists so semantic units that straddle a chunk boundary are still recoverable at search time — both neighbouring chunks contain the bridging text. Defaults (400 words / 50 overlap → step of 350) are tuned for current embed models' practical context windows with comfortable headroom; they're parameters, not constants, precisely because they will be re-tuned per model.

### Defensive parameter clamping

Invalid `Config` values are silently rewritten to safe defaults rather than rejected. The decision: a malformed pipeline_triggers row (which is data, not code) must not crash the in-process processor goroutine — at worst it should produce defaultly-chunked output. {>> Both `WordsPerChunk <= 0` and `OverlapWords` outside `[0, WordsPerChunk)` snap to defaults inside `Split`. <<}

### `Chunk` carries its own index

The chunker emits `Chunk{Index, Text, WordCount}` rather than a bare `[]string`. The index is part of the value so callers writing rows to `document_chunks` don't have to maintain a counter, and so debugging tools that drop chunks mid-stream don't have to renumber. WordCount is included to avoid downstream re-tokenization for stats.

### pdfproc & docxproc — `ErrSkip` as a first-class outcome

Both stubs return a named sentinel (`ErrSkip`) rather than `nil` with empty output. The distinction matters: an empty extraction from a real engine is a successful "this PDF had no text" result; a stubbed processor must signal "I didn't run" so the pipeline can route the row elsewhere (e.g., leave queued for an external OCR worker) rather than mark it done with zero text.

### docxproc converts to PDF, not text

The chosen seam for DOCX is `DOCX → PDF` (writing PDF bytes to an `io.Writer`), not `DOCX → text`. Rationale: real conversion will be a `libreoffice --headless` shell-out whose native output is PDF, and the existing OCR pipeline already consumes PDF. Going through PDF means a DOCX upload triggers OCR with no per-format branch downstream, at the cost of one extra blob in the lake. {>> `ConvertToPDF(r io.Reader, w io.Writer) error` — stream-in, stream-out. <<}

### Stubs drain their input

Both placeholders `io.Copy(io.Discard, r)` before returning `ErrSkip`. This isn't paranoia: it preserves the contract that any real engine will obey (consuming the stream fully), so the caller's resource-management code — closing temp files, releasing multipart spool buffers — behaves identically before and after the engine is wired. Swapping the implementation later won't change observable behaviour at the call site.

### Why these belong in-process

These four are CPU-only, deterministic, dependency-light, and bounded in memory. Per the system-level rule "cheap CPU-only work goes internal", they live alongside the pipeline goroutine and run without an HTTP round-trip. The two stubs are in-tree as seams, not implementations — when a real PDF/DOCX engine arrives, the decision of in-process vs. external is reopened (real OCR almost certainly goes to `ocrworker` / `taskworker`, leaving these packages thin or empty).

## Interactions

- **Consumed by the in-process pipeline goroutine** (`internal/infra/pipeline/`) — when `pipeline_triggers` fires a row claimed by an internal processor (`html_strip`, `text_passthrough`), the dispatcher calls these leaves directly.
- **Consumed by external task workers** — `taskworker` / `agent` may also link these packages when running the same processing kinds out-of-process, because they are pure functions with no registry dependency.
- **Feeds `extracted_documents`** — htmlproc output (and future pdfproc/docxproc output) becomes the `text` column persisted by the processing-result accept path.
- **Feeds `document_chunks`** — chunker output becomes `document_chunks` rows, which are then claimed by `embedworker` via the embed queue and optionally mirrored to Quickwit by the FTS sink.
- **Does not interact with** the DB, the blob store, Qdrant, or Quickwit directly — those couplings live one layer up. A rewrite of the storage layer leaves these packages untouched.
- **System-level concepts (see system spec):** the three-queue protocol, capability strings (`html_strip`, `text_passthrough`), `Pipeline.InternalProcessors`, pluggable infra, and pipeline_triggers routing are defined in the system spec and are referenced here only for context.

## Mapping

> [[internal/infra/pipeline/htmlproc/strip.go]]
> [[internal/infra/pipeline/chunker/chunker.go]]
> [[internal/infra/pipeline/pdfproc/stub.go]]
> [[internal/infra/pipeline/docxproc/stub.go]]
> [[internal/infra/pipeline/]]
> [[eidos/spec - system - distributed crawler with capability-gated workers and pluggable infra.md]]
