# crawlerv3

Distributed web crawler + data lake. Central Go registry hands jobs to anonymous-but-authenticated workers over HTTP. Workers can be written in any language. Raw blobs land in a pluggable BlobStore (local FS by default, S3 swappable). Fetched HTML is stripped to text and chunked for an external embedding service to vectorize.

- **Module:** `github.com/atvirokodosprendimai/crawlerv3`
- **Architecture:** Domain-Driven Design (ports + adapters) + CQRS (single writer, many readers)
- **Stack:** Go 1.25+, gorm, go-chi/v5, urfave/cli/v3, pressly/goose/v3, glebarez/sqlite (no cgo), aws-sdk-go-v2 (S3)
- **DBs:** SQLite (default), PostgreSQL, MySQL — same migrations and same code path
- **Status:** Slices 1–12 shipped, smoke-tested end-to-end (crawl + pipeline + embed + S3 migration + multi-DB + external task workers + worker pool + capabilities + concurrency caps + data-lake read API + declarative pipeline triggers + scope-locked crawl + 12+ file types + realtime domain mgmt + per-domain vector collections + registry-owned Qdrant + search endpoint + reference embed worker + queue ops + ban-with-release + domain↔worker binding)

---

## Table of Contents

1. [What it does](#what-it-does)
2. [Repository layout](#repository-layout)
3. [Install & build](#install--build)
4. [Quickstart](#quickstart)
5. [CLI reference](#cli-reference)
   - [`registry`](#registry) — server, migrations, worker pool, frontier, backfill, triggers
   - [`worker`](#worker) — reference crawl worker
   - [`taskworker`](#taskworker) — single-kind external processing worker
   - [`agent`](#agent) — unified worker (crawl + multiple task kinds)
   - [`migrator`](#migrator) — local↔S3 blob mover
6. [Configuration (env vars)](#configuration-env-vars)
7. [HTTP API](#http-api)
   - [Auth](#auth)
   - [Health](#health)
   - [Workers: `/v1/workers/me`](#workers-v1workersme)
   - [Crawl: `/v1/jobs/*`](#crawl-v1jobs)
   - [Embed: `/v1/embed/*`](#embed-v1embed)
   - [Read API: `/v1/lake`, `/v1/extracted`, `/v1/chunks`](#read-api-for-sink-workers-v1lake-v1extracted-v1chunks)
   - [Pipeline triggers (`registry trigger-*`)](#pipeline-triggers-registry-trigger-)
   - [Tasks: `/v1/tasks/*`](#tasks-v1tasks)
   - [Blobs: `/v1/blobs/{id}`](#blobs-v1blobsid)
   - [Error format](#error-format)
8. [Worker protocol](#worker-protocol)
9. [Embed-worker protocol](#embed-worker-protocol)
10. [Task-worker protocol (OCR / DOCX / etc.)](#task-worker-protocol-ocr--docx--etc)
11. [Storage backends](#storage-backends)
12. [Database backends](#database-backends)
13. [Processing pipeline](#processing-pipeline)
14. [Operations](#operations)
15. [Common workflows](#common-workflows)
16. [Extending the system](#extending-the-system)
17. [Schema reference](#schema-reference)
18. [Stubs / future work](#stubs--future-work)

---

## What it does

`crawlerv3` is a control-plane registry that:

1. Holds a **frontier** of URLs to crawl.
2. Hands out work to **workers** that authenticate with a Personal Access Token (PAT). Any host on the public internet can run a worker.
3. Accepts the fetched bodies, stores them in a **data lake** (filesystem or S3), and indexes them.
4. Runs an in-process **processing pipeline** that converts blobs to plain text (HTML stripper today; PDF/DOCX stubs ready to plug into Tesseract/LibreOffice).
5. Splits text into **chunks** that an external **embedding service** can pick up via a separate worker protocol and write to a vector store (e.g. Qdrant).
6. Lets you **migrate** existing local blobs to S3 (or vice versa) without downtime.

There is no admin GUI; everything is driven via CLI flags / env vars and JSON over HTTP.

---

## Repository layout

```
crawlerv3/
├── cmd/
│   ├── registry/      # control-plane: serve + migrate + workers + domains + frontier + triggers + queue ops
│   ├── worker/        # reference Go crawl worker
│   ├── taskworker/    # reference external processing worker (PDF OCR / Office→PDF / …)
│   ├── embedworker/   # reference external embedding worker (Ollama-style or shell-out)
│   ├── agent/         # unified worker: crawl + multiple task kinds + embed in one bin
│   └── migrator/      # local↔s3 blob mover
├── internal/
│   ├── domain/        # pure types + ports (no infra imports)
│   │   ├── frontier/
│   │   ├── lake/
│   │   ├── workerid/
│   │   ├── processing/
│   │   ├── extraction/
│   │   └── chunking/
│   ├── app/           # use cases (Service, Pipeline, EmbedSvc, Mover)
│   └── infra/
│       ├── db/
│       │   ├── rwdb/                  # CQRS pools (sqlite/postgres/mysql)
│       │   ├── migrations/{sqlite,postgres,mysql}/*.sql
│       │   └── gormrepo/              # gorm-tagged models + Repository impls
│       ├── store/
│       │   ├── local/                 # filesystem BlobStore
│       │   └── s3/                    # aws-sdk-go-v2 BlobStore (MinIO compatible)
│       ├── pipeline/
│       │   ├── htmlproc/              # HTML → plain text (working)
│       │   ├── pdfproc/               # stub — wire Tesseract/Tika
│       │   ├── docxproc/              # stub — wire libreoffice
│       │   └── chunker/               # word-based chunking
│       ├── http/                      # chi router + handlers + PAT auth
│       ├── lease/                     # HMAC-SHA256 lease tokens
│       └── urls/                      # canonicalization
└── scripts/smoke.sh                   # end-to-end smoke test
```

---

## Install & build

Requires Go 1.25+.

```bash
git clone https://github.com/atvirokodosprendimai/crawlerv3.git
cd crawlerv3
go mod download
go build -o ./bin/registry ./cmd/registry
go build -o ./bin/worker   ./cmd/worker
go build -o ./bin/migrator ./cmd/migrator
```

SQLite uses `glebarez/sqlite` (modernc.org/sqlite under the hood), so no cgo is required.

---

## Quickstart

```bash
# 1. Generate a strong lease secret (16+ random bytes, base64)
export LEASE_SECRET=$(openssl rand -base64 32)

# 2. Initialize the database (sqlite by default → ./crawler.db)
#    This also seeds the default pipeline triggers (text/html → html_strip, etc.)
./bin/registry migrate up

# 3. Issue PATs with the right capabilities
./bin/registry create-worker --label crawl-1 \
                             --capabilities crawl \
                             --max-concurrent 8
# → worker_id=1
# → pat=ZkZuMkI3q...        (save it; only its sha256 is stored)

./bin/registry create-worker --label embed-1 \
                             --capabilities embed \
                             --max-concurrent 16
# → worker_id=2 pat=...

./bin/registry create-worker --label sink-1 \
                             --capabilities lake_read,extracted_read,chunks_read \
                             --max-concurrent 4
# → worker_id=3 pat=... (used by Qdrant / Quickwit / SQL indexers)

# 4. Seed a target domain and enqueue a URL
./bin/registry seed-domain --host example.com --crawl-delay-ms 1000
./bin/registry enqueue     --url https://example.com/

# 5. Start the registry HTTP server
./bin/registry serve --addr :8080 &

# 6. See the worker pool
./bin/registry list-workers
# ID  LABEL    CAPABILITIES                          MAX  HELD  LAST_SEEN  BANNED
# 1   crawl-1  crawl                                 8    0     -          no
# 2   embed-1  embed                                 16   0     -          no
# 3   sink-1   lake_read,extracted_read,chunks_read  4    0     -          no

# 7. Run the reference crawl worker
./bin/worker --registry http://localhost:8080 --pat <CRAWL_PAT> --batch 8

# 8. Drive the embed protocol (your embed service uses this)
curl -s -X POST -H "Authorization: Bearer $EMBED_PAT" \
  -H 'Content-Type: application/json' \
  -d '{"batch":1000}' \
  http://localhost:8080/v1/embed/reserve

# 9. Drive the read API (your Qdrant / Quickwit / SQL indexer uses this)
curl -s -H "Authorization: Bearer $SINK_PAT" \
  "http://localhost:8080/v1/extracted?since_id=0&limit=100"
```

Full E2E smoke tests under `scripts/smoke*.sh` exercise every slice end-to-end against ephemeral state.

---

## CLI reference

### `registry`

Global flags (applies to all subcommands):

| Flag (or env var) | Default | Meaning |
|---|---|---|
| `--db-driver` (`DB_DRIVER`) | `sqlite` | `sqlite`, `postgres`, or `mysql`. |
| `--db-dsn` (`DB_DSN`) | `crawler.db` | DSN. SQLite: file path. PG/MySQL: standard DSN. |
| `--read-dsn` (`READ_DSN`) | — | Optional read-replica DSN for PG/MySQL. |
| `--blobs-root` (`BLOBS_ROOT`) | `./blobs` | Local BlobStore root. |
| `--lease-secret` (`LEASE_SECRET`) | — *(required)* | Base64-encoded HMAC secret, ≥ 16 raw bytes. |
| `--max-body-bytes` (`MAX_BODY_BYTES`) | `209715200` (200 MiB) | Worker upload cap. |
| `--debug` (`DEBUG`) | `false` | Verbose gorm logging. |

#### Server & migrations

```bash
# Start the HTTP API (runs sweeper + internal pipeline goroutine)
registry serve --addr :8080

# Goose migrations, dialect picked from --db-driver
registry migrate up        # apply all pending
registry migrate down      # revert last
registry migrate status    # show applied/pending
registry migrate reset     # revert everything (DESTRUCTIVE)
```

#### Worker pool management (slice 7)

```bash
# Issue a Personal Access Token. Capabilities and max-concurrent are stored
# server-side and enforced on every reserve.
registry create-worker --label gpu-1 \
                       --capabilities pdf_ocr,docx_to_pdf,embed \
                       --max-concurrent 4
# → worker_id=4
# → pat=Vdy5LJJAtig5fKy8KD3...   (printed once; only sha256 is stored)

# Empty --capabilities = "any kind allowed" (backward-compat for legacy workers)
registry create-worker --label legacy --max-concurrent 2

# Show the pool. HELD = live count of active leases this worker holds across
# crawl_frontier + processing_jobs + document_chunks.
registry list-workers
# ID  LABEL    CAPABILITIES               MAX  HELD  LAST_SEEN              BANNED
# 1   crawl-1  crawl                      8    3     2026-06-05T22:30:11Z   no
# 2   gpu-1    pdf_ocr,docx_to_pdf,embed  4    1     2026-06-05T22:30:09Z   no
# 3   legacy   (any)                      2    0     -                      no

# List capabilities the system recognizes. Endpoint-gated caps are the fixed
# set baked into registry HTTP handlers; worker-declared caps are aggregated
# live from the workers table (processor kinds + tenant tags).
registry list-capabilities
# ENDPOINT-GATED (registry-defined):
#   crawl, embed, lake_read, extracted_read, chunks_read
# WORKER-DECLARED (from workers table):
#   pdf_ocr  (2 workers)
#   vvtat    (1 worker)
#   ...

# Change a worker's permissions or cap on the fly. --max-concurrent -1 leaves it unchanged.
# IMPORTANT: --capabilities REPLACES the list. To add a cap, pass the full new set.
registry update-worker --id 1 --max-concurrent 12
registry update-worker --id 2 --capabilities pdf_ocr,docx_to_pdf,embed,extracted_read

# Ban / unban. Banned workers get 403 on every authenticated call.
registry ban-worker   --id 5
registry unban-worker --id 5
```

#### Crawl frontier

```bash
# Register a domain (politeness delay defaults to 1000ms).
# IMPORTANT: only URLs whose host is in this `domains` table will ever be
# crawled. Discovered links to external hosts are dropped by default.
registry seed-domain --host example.com --crawl-delay-ms 1000
registry seed-domain --host slow.example.org --scheme https --crawl-delay-ms 5000

# Inspect the scope (now also shows per-domain embed_collection override)
registry list-domains
# ID  HOST                  SCHEME  DELAY_MS  ACTIVE  EMBED_COLLECTION
# 1   example.com           https       1000  yes     (host)
# 2   slow.example.org      https       5000  yes     (host)

registry deactivate-domain --host slow.example.org   # stops reserves + drops new discoveries
registry activate-domain   --host slow.example.org

# Mutate a domain at runtime — no restart, no migration
registry update-domain --host example.com --crawl-delay-ms 200
registry update-domain --host example.com --scheme http
registry update-domain --host example.com --embed-collection lithuania_news
registry update-domain --host example.com --embed-collection -    # clear override (chunks fall back to host)

# Bind a domain to a specific worker class (slice 12).
# Only workers with this capability can reserve URLs of this domain.
registry update-domain --host spa.example.com --required-capability js_render
registry update-domain --host api.legacy.com --required-capability auth_required
registry update-domain --host foo.com        --required-capability domain:foo.com   # fine-grained 1:1 binding
registry update-domain --host foo.com        --required-capability -                # clear (any crawl worker)

# Add a URL to the frontier
registry enqueue --url https://example.com/
registry enqueue --url https://example.com/deep --depth 2 --priority 10
```

#### Binding a domain to specific workers (slice 12)

Use cases:

- A JS-rendered SPA needs a Playwright/Puppeteer worker (custom Python/Node bin).
- A legacy site needs auth tokens that only one worker holds.
- A specific domain is so critical it should only be handled by a dedicated GPU box.

Mechanism: each `domains` row has an optional `required_capability TEXT` column. When set, the reserve query filters out URLs from that domain unless the worker's `capabilities` set includes the string. The naming convention is **operator's choice** — the server only does string-membership matching.

Pattern A — semantic capability classes:

```bash
registry update-domain --host spa.example.com   --required-capability js_render
registry update-domain --host api.legacy.com    --required-capability auth_required
registry update-domain --host slow.example.org  --required-capability polite_only

registry create-worker --label playwright-1 --capabilities crawl,js_render     --max-concurrent 4
registry create-worker --label legacy-py-1  --capabilities crawl,auth_required --max-concurrent 2
```

Pattern B — explicit 1:1 binding (worker handles only this one domain):

```bash
registry update-domain --host foo.com --required-capability domain:foo.com
registry update-domain --host bar.com --required-capability domain:bar.com

registry create-worker --label foo-py-1 --capabilities crawl,domain:foo.com --max-concurrent 2
registry create-worker --label bar-node-1 --capabilities crawl,domain:bar.com --max-concurrent 2
```

Pattern C — fallback "generic" workers handle everything unrestricted:

```bash
# No --required-capability on these domains → any crawl-capable worker reserves them.
registry create-worker --label generic-crawl --capabilities crawl --max-concurrent 16
```

Notes:

- `list-domains` shows the binding in the `REQ_CAP` column (`(any)` when unset).
- Empty capabilities on a worker (legacy / no caps stored) is still treated as "any kind allowed" — including bound domains. To opt fully into the capability system, set explicit caps on every worker.
- Clear a binding with `update-domain --host X --required-capability -`.
- The server uses the **server-stored** worker capability set, not the request body — workers cannot spoof their way into a bound domain.

#### Capability model

Two flavors of capability exist. They look identical on a worker row (just strings in `workers.capabilities`) but they originate in different layers:

| Flavor | Defined by | Enforced at | Examples |
|--------|-----------|-------------|----------|
| **Endpoint-gated** | Registry HTTP handlers (hardcoded) | `wk.Can("crawl")` etc. inside `jobs.go` / `embed.go` / `reads.go` | `crawl`, `embed`, `lake_read`, `extracted_read`, `chunks_read` |
| **Worker-declared** | Whatever string you put in `--capabilities` | `tasks.go` matches `req.Kinds[i]` against `wk.Capabilities`; reserve query matches `domains.required_capability` | `pdf_ocr`, `html_strip`, `js_render`, `vvtat`, `domain:foo.com` |

The registry is a pure **orchestrator** for worker-declared caps — it doesn't know or care what `pdf_ocr` *means*. It just dispatches by string match. Adding a new processor or tenant tag requires zero registry code change.

Why `wk.Can("crawl")` fails even though you set `--capabilities vvtat`:

- `update-worker --capabilities vvtat` **replaces** the list (no append flag).
- The reserve handler first checks `wk.Can("crawl")` (endpoint-gated). With caps = `["vvtat"]`, that returns false → 403 `capability_denied`.
- Always include the endpoint-gated cap *and* the worker-declared tag: `--capabilities crawl,vvtat`.

Tip: `(empty capabilities)` is a legacy backcompat that grants ALL caps. As soon as you set even one cap, the empty-list shortcut is gone — you must list every cap the worker uses.

#### Adding a new processor (e.g. `video_transcode`)

End-to-end recipe for a new processor kind. No registry code change is required.

```bash
# 1. Pick a kind name. It's just a string — used as workers.capabilities entry,
#    as processing_jobs.processor, and (optionally) as a pipeline trigger output.
KIND=video_transcode

# 2. Register a trigger so new lake_objects of the right content-type auto-enqueue.
#    (Skip this if you'll backfill or manually enqueue.)
registry trigger-add \
  --on lake_object_inserted \
  --content-type video/ \
  --enqueue "$KIND"

# 3. Issue a PAT for the worker box. Include `crawl` only if the same box also crawls;
#    for a pure transcoder, the only cap needed is the kind itself.
registry create-worker \
  --label gpu-video-1 \
  --capabilities "$KIND" \
  --max-concurrent 2
# → pat=...   (save it)

# 4. Run the worker. Two options:
#    a) `taskworker` for a single kind (mode=blob produces a new lake object):
./bin/taskworker \
  --registry http://registry:8080 \
  --pat $PAT \
  --kind "$KIND" \
  --mode blob \
  --extract-cmd "ffmpeg -i {input} -c:v libx264 -preset fast {outdir}/output.mp4" \
  --output-glob "{outdir}/*.mp4"

#    b) `agent` if this box also handles other kinds — pass per-kind flags
#       (see the `agent` section above for the --<flag>.<kind> shape).

# 5. (Optional) Backfill existing rows.
registry reprocess --processor "$KIND" --content-type-prefix video/

# 6. Verify.
registry list-capabilities    # video_transcode now appears under WORKER-DECLARED
registry queue-stats          # PROCESSING_JOBS section shows the new processor's counts
```

What the registry does behind the scenes:

- Trigger fires on insert of a `video/*` blob → row added to `processing_jobs` with `processor='video_transcode'`.
- Worker calls `POST /v1/tasks/reserve` with `kinds=["video_transcode"]`.
- Handler: `wk.Can("video_transcode")` matches (worker has that cap) → leases a row.
- Worker runs `extract-cmd`, posts result via `POST /v1/tasks/result`. Output blob lands in the lake, can fire its own triggers.

Same shape works for any new kind — `image_resize`, `pii_redact`, `lang_detect`, whatever. The registry stays untouched.

By default the crawler is **scope-locked to seeded hosts**:

- A discovered `<a href>` is only enqueued if its host already exists in `domains` (and is active).
- Links to hosts you didn't seed are silently dropped — the crawler will never wander off `9g.lt` just because the page has a link to `google.com`.
- An inactive domain (`is_active=0`) is excluded both from reserves and from new-link discovery.

If you want the legacy "follow anything" behavior (auto-add any newly-seen host as a fresh domain row), set:

```bash
registry serve --addr :8080 --allow-auto-domains            # or ALLOW_AUTO_DOMAINS=1
```

You can also cap recursion depth globally:

```bash
registry serve --addr :8080 --max-depth 3                   # or MAX_DEPTH=3
# links with new_depth > 3 are dropped at intake
```

Typical recipes:

```bash
# Strict: only crawl 9g.lt, no external links followed
registry seed-domain --host 9g.lt --crawl-delay-ms 500
registry enqueue    --url https://9g.lt/

# Multi-site campaign: seed every host you want crawled, then enqueue seeds
for H in foo.example bar.example baz.example; do
  registry seed-domain --host "$H" --crawl-delay-ms 1000
  registry enqueue     --url "https://$H/"
done

# Open crawl (rare in production): let the crawler add any host it discovers
registry serve --allow-auto-domains
```

#### Backfill processing (slice 6)

```bash
# Enqueue a processing task for every existing lake_objects row matching
# a content-type prefix. Idempotent (skips rows that already have a row for
# the same processor). Use this when you wire up a new OCR/converter and
# want to catch up on history.

# Auto-uses application/pdf prefix
registry reprocess --processor pdf_ocr

# Explicit overrides
registry reprocess --processor pdf_ocr --content-type-prefix application/pdf --limit 50000
registry reprocess --processor docx_to_pdf
registry reprocess --processor html_strip --content-type-prefix text/html
registry reprocess --processor custom_html_indexer --content-type-prefix text/html --limit 10000
```

#### Queue operations (slice 11)

Inspect, force-requeue, release stuck leases.

```bash
# Show per-queue status histogram + worker pool state
registry queue-stats
# CRAWL_FRONTIER
#   queued 12  leased 3  done 12041  failed 18  dead 4
# PROCESSING_JOBS  (per processor)
#   pdf_ocr
#     queued 204  running 1  done 1840  failed 6  skipped 0
#   office_to_pdf
#     queued 7  running 0  done 22  failed 0  skipped 0
#   …
# DOCUMENT_CHUNKS
#   pending 312  leased 2  done 8492  failed 3
# WORKERS
#   total 7  banned 1  stale(>5m) 1

# Bulk requeue — at least one filter required to avoid mass-requeue accidents.
registry requeue-chunks   --status failed
registry requeue-chunks   --document 42                      # force re-embed entire doc
registry requeue-chunks   --worker 5                         # release worker 5's chunk leases
registry requeue-tasks    --status failed --processor pdf_ocr
registry requeue-tasks    --worker 5
registry requeue-frontier --status dead
registry requeue-frontier --domain 1                         # re-crawl entire domain

# Release every lease held by one worker across ALL three queues
# (faster than waiting for the sweeper TTL).
registry release-worker --id 5

# Ban + release in one shot — useful when killing off a misbehaving box.
registry ban-worker --id 5 --release
# banned worker_id=5
# released held leases: frontier=3 tasks=1 chunks=2
```

Filter semantics:

- All filters are AND-ed; empty filter = no constraint on that field.
- `requeue-chunks` / `requeue-tasks` / `requeue-frontier` **require at least one** of `--status`, `--worker`, plus their queue-specific filter (`--document`, `--processor`, `--domain`). Running them with no filters errors out — refusing to touch the whole table.
- `release-worker` does not ban; it just clears the leases. Useful when a worker disappears (process crash, network split) and you want its work picked up faster than the 30-second sweep cycle.
- `ban-worker --release` chains both for the common "this box is broken" case.

#### Pipeline triggers (slice 8)

Declarative routing. Replaces the hardcoded MIME→processor map. Default rows are seeded by `migrate up`.

```bash
# Show the trigger table
registry trigger-list
# ID  EVENT                  FILTER                                      ENQUEUE        ENABLED
# 1   lake_object_inserted   {"content_type_prefix":"text/html"}         html_strip     true
# 2   lake_object_inserted   {"content_type_prefix":"application/pdf"}   pdf_ocr        true
# 3   lake_object_inserted   {"content_type_prefix":"...wordpro..."}     docx_to_pdf    true

# Add: also enqueue a Quickwit indexer for every HTML lake object
registry trigger-add --on lake_object_inserted \
                     --content-type text/html \
                     --enqueue quickwit_indexer

# Add: when docx_to_pdf produces a PDF, fire a watermark check on the output
registry trigger-add --on blob_produced \
                     --source-processor docx_to_pdf \
                     --enqueue watermark_check

# Temporarily turn one off (useful while a processor's worker fleet is down)
registry trigger-disable --id 2
registry trigger-enable  --id 2

# Permanently remove
registry trigger-delete --id 4
```

Trigger evaluation:

- Cache TTL is 5s, so edits propagate within 5 seconds (no server restart).
- Filter shape (JSON): `{ "content_type_prefix": "...", "source_processor": "..." }`. Both optional. CT match is case-insensitive prefix; source_processor is exact.
- Events: `lake_object_inserted` (any new lake_objects row), `blob_produced` (task-worker uploaded a new blob).
- Many triggers can match a single event — each fires once and enqueues one `processing_jobs` row.

### `worker`

Reference crawl worker (reserve → fetch → push). PAT-authenticated.

```
worker \
  --registry      http://localhost:8080  (REGISTRY)
  --pat           <PAT>                  (PAT)
  --batch         10
  --idle-sleep    3s
  --fetch-timeout 30s
  --user-agent    crawlerv3-worker/0.1
```

| Flag (env) | Default | When to use |
|---|---|---|
| `--registry` (`REGISTRY`) | — *(required)* | Base URL of registry, no trailing slash (`http://host:8080`). |
| `--pat` (`PAT`) | — *(required)* | PAT issued by `registry create-worker --capabilities crawl,...`. Must include the `crawl` endpoint-gated cap. |
| `--batch` | `10` | Jobs reserved per `/v1/jobs/reserve` call. Raise on fast networks / lots of small pages; lower (1–4) on slow/large pages so heartbeats don't stack up. Server caps it at registered `max_concurrent`. |
| `--idle-sleep` | `3s` | Wait between empty reserves. Tune up on a quiet frontier to reduce registry load; tune down (≤1s) during a burst to keep CPUs fed. |
| `--fetch-timeout` | `30s` | Per-URL HTTP `GET` deadline. Raise for slow/heavy pages (PDFs, large HTML); below the **60s lease TTL** to avoid heartbeats. |
| `--user-agent` | `crawlerv3-worker/0.1` | UA sent on outbound fetches. Override for branding, contact info (some sites require it), or to match a real browser fingerprint. |

### `taskworker`

Reference external processing worker for a **single** `processing_jobs` kind. Downloads the source blob, shells out to a user-configured command, posts result back. PAT-authenticated.

```
taskworker \
  --registry            http://localhost:8080
  --pat                 <PAT>
  --kind                pdf_ocr               (repeatable)
  --batch               4
  --idle-sleep          5s
  --max-runtime         0                     # 0 = run forever
  --mode                text                  # text | blob
  --extract-cmd         "tesseract {input} - -l eng+lit"
  --output-glob         "{outdir}/output.*"   # blob mode
  --output-content-type application/pdf       # blob mode
  --next-processor      pdf_ocr               # blob mode chains to next stage
  --exec-timeout        5m
```

| Flag (env) | Default | When to use |
|---|---|---|
| `--registry` (`REGISTRY`) | — *(required)* | Registry base URL. |
| `--pat` (`PAT`) | — *(required)* | Issued via `create-worker --capabilities <kind>`. |
| `--kind` | — *(required, repeatable)* | One or more `processing_jobs.processor` names this worker claims (e.g. `pdf_ocr`, `docx_to_pdf`). Worker's `capabilities` must contain each kind. Reserve query filters server-side. |
| `--batch` | `4` | Tasks per reserve. Heavy work (OCR, transcoding) → keep low so one box's stuck task doesn't starve the rest. Light shell-outs can raise to 16+. |
| `--idle-sleep` | `5s` | Sleep when queue empty. Same trade-off as `worker --idle-sleep`. |
| `--max-runtime` | `0` (forever) | Exit cleanly after this duration. Use for cron / k8s CronJob burst patterns (cheap spot GPU hours). Daemonized boxes leave at 0. |
| `--mode` | `text` | `text` = stdout becomes `extracted_documents.text`. `blob` = first file matching `--output-glob` is uploaded as a new lake object. Pick `blob` when the processor produces a file (DOCX→PDF, video transcode). |
| `--extract-cmd` | — *(required)* | Shell command. Placeholders: `{input}` (downloaded blob path), `{outdir}` (empty scratch dir). Examples: `"tesseract {input} - -l eng+lit"`, `"libreoffice --headless --convert-to pdf --outdir {outdir} {input}"`. |
| `--output-glob` | `{outdir}/output.*` | Blob mode: glob used to find produced file. Tighten when extract-cmd writes multiple files (e.g. `"{outdir}/*.pdf"`). Unused in text mode. |
| `--output-content-type` | `application/octet-stream` | Blob mode: MIME stamped on the new lake object. Required to be honest — downstream triggers fire on this. |
| `--next-processor` | `""` | Blob mode chain: after upload, enqueue another processor on the output blob (e.g. `docx_to_pdf` → `pdf_ocr`). Empty = no chain (triggers may still fire). |
| `--exec-timeout` | `5m` | Kill `extract-cmd` after this. Set above your slowest realistic run (OCR on a 200-page scan can take >5m on CPU). |

Two examples:

```bash
# GPU box doing PDF OCR with Tesseract
taskworker --kind pdf_ocr --mode text \
  --extract-cmd "tesseract {input} - -l eng+lit"

# CPU box converting DOCX → PDF with headless LibreOffice, then chaining to OCR
taskworker --kind docx_to_pdf --mode blob \
  --extract-cmd "libreoffice --headless --convert-to pdf --outdir {outdir} {input}" \
  --output-glob "{outdir}/*.pdf" \
  --output-content-type application/pdf \
  --next-processor pdf_ocr
```

Placeholders in `--extract-cmd`:

- `{input}` — absolute path to the downloaded blob in a private scratch directory.
- `{outdir}` — absolute path to an empty scratch directory the command can write into.

In `text` mode, the command's stdout becomes `extracted_text`. In `blob` mode, the first file matching `--output-glob` is uploaded as a new lake object, the source task's `output_lake_object_id` is set, and a fresh `processing_jobs` row is enqueued for `--next-processor` if non-empty.

### `agent`

Unified worker — one process, many capabilities. Replaces running `worker` + multiple `taskworker` instances on one box. Per-kind flags via `--<flag>.<kind>` shape.

```
agent \
  --registry      http://localhost:8080
  --pat           <PAT>
  --enable        crawl,pdf_ocr,docx_to_pdf,html_strip
  --batch         4
  --idle-sleep    5s
  --fetch-timeout 30s
  --exec-timeout  5m

  # per-kind:
  --extract-cmd.pdf_ocr      "tesseract {input} -"
  --extract-cmd.docx_to_pdf  "libreoffice --headless --convert-to pdf --outdir {outdir} {input}"
  --extract-cmd.html_strip   "python3 /opt/strip.py {input}"
  --mode.docx_to_pdf         "blob"
  --output-glob.docx_to_pdf  "{outdir}/*.pdf"
  --output-content-type.docx_to_pdf "application/pdf"
  --next-processor.docx_to_pdf "pdf_ocr"
```

| Flag (env) | Default | When to use |
|---|---|---|
| `--registry` (`REGISTRY`) | — *(required)* | Registry base URL. |
| `--pat` (`PAT`) | — *(required)* | Single PAT covering every `--enable` kind. Worker row's `capabilities` must list each. |
| `--enable` | — *(required)* | Comma-separated kinds: `crawl`, any task processor (`pdf_ocr`, `docx_to_pdf`, …), or `embed`. Drives which inner loops start. Pick when one box has spare CPU/RAM for several roles instead of running 3 processes. |
| `--batch` | `4` | Per-loop reserve size (shared shape across crawl/task/embed loops). Conservative default since task work can be slow. |
| `--idle-sleep` | `5s` | Wait between empty reserves on each loop. |
| `--fetch-timeout` | `30s` | Crawl loop only — per-URL HTTP timeout. |
| `--user-agent` | `crawlerv3-agent/0.1` | Crawl loop only. |
| `--exec-timeout` | `5m` | Task + embed shell-out kill. Applies to every per-kind `--extract-cmd`. |
| `--extract-cmd.<kind>` | — | Per-kind shell command (same `{input}`/`{outdir}` placeholders as `taskworker`). |
| `--mode.<kind>` | `text` | Per-kind `text` or `blob`. |
| `--output-glob.<kind>` | `{outdir}/output.*` | Per-kind blob-mode glob. |
| `--output-content-type.<kind>` | `application/octet-stream` | Per-kind blob-mode MIME. |
| `--next-processor.<kind>` | `""` | Per-kind blob-mode chain. |
| `--embed-url` (`EMBED_URL`) | — | Used when `--enable` includes `embed`. Ollama-style `/api/embeddings` URL. Mutually exclusive with shell-out. |
| `--embed-model` | `nomic-embed-text` | Model name passed in the embed request body. |
| `--embed-api-key` (`EMBED_API_KEY`) | — | Bearer token for `--embed-url` if the server gates it. |

Real-world example — one binary for a multi-purpose box:

```bash
agent --registry https://registry.example.com --pat $PAT \
      --enable crawl,pdf_ocr,docx_to_pdf \
      --batch 4 \
      --extract-cmd.pdf_ocr "tesseract {input} - -l eng+lit" \
      --mode.docx_to_pdf "blob" \
      --extract-cmd.docx_to_pdf "libreoffice --headless --convert-to pdf --outdir {outdir} {input}" \
      --output-glob.docx_to_pdf "{outdir}/*.pdf" \
      --output-content-type.docx_to_pdf "application/pdf" \
      --next-processor.docx_to_pdf "pdf_ocr"
```

The worker must be registered with matching capabilities, e.g.:

```bash
registry create-worker --label multi-1 \
                       --capabilities crawl,pdf_ocr,docx_to_pdf \
                       --max-concurrent 6
```

`embed` is intentionally not handled by `agent` — the embed worker's job (model API call + writing to a vector store) is too service-specific. Build your own embed worker using `/v1/embed/reserve` + `/v1/embed/result`.

### `embedworker`

Reference external embedding worker. Loops `reserve → embed → push vector`. Two backends, pick one (mutually exclusive):

```
embedworker \
  --registry        http://localhost:8080
  --pat             <PAT>
  --batch           64
  --idle-sleep      5s
  --max-runtime     0           # 0 = run forever

  # Backend A — HTTP (Ollama-style /api/embeddings; also LocalAI, llama.cpp server)
  --embed-url       http://localhost:11434
  --embed-model     nomic-embed-text
  --embed-api-key   <optional Bearer>

  # Backend B — shell-out (stdin = chunk text; stdout must contain a JSON line {"embedding":[...]})
  --extract-cmd     "python3 /opt/embed.py"
  --exec-timeout    60s
```

| Flag (env) | Default | When to use |
|---|---|---|
| `--registry` (`REGISTRY`) | — *(required)* | Registry base URL. |
| `--pat` (`PAT`) | — *(required)* | Worker must have the `embed` capability. |
| `--batch` | `64` | Chunks per `/v1/embed/reserve`. Higher = fewer round-trips; lower = faster failure recovery if the model crashes. Tune to your GPU's effective batch limit. |
| `--idle-sleep` | `5s` | Sleep between empty reserves. |
| `--max-runtime` | `0` (forever) | Burst pattern: spot GPU draining queue in a cron window. |
| `--embed-url` (`EMBED_URL`) | — | **Backend A.** Ollama / LocalAI / llama.cpp-server base URL. Worker POSTs to `{url}/api/embeddings`. Pick when you already run a model server. |
| `--embed-model` | `nomic-embed-text` | Model name in the request body. Must match a model the server has loaded. |
| `--embed-api-key` (`EMBED_API_KEY`) | — | Optional Bearer for hosted Ollama/OpenAI-compatible endpoints. |
| `--extract-cmd` (`EXTRACT_CMD`) | — | **Backend B.** Shell command — stdin = chunk text, stdout = JSON `{"embedding":[...]}`. Pick when you want a custom Python wrapper or no HTTP layer. Mutually exclusive with `--embed-url`. |
| `--exec-timeout` | `60s` | Backend-B kill. Raise for large models / cold-start latency. |

GPU box typical setup:

```bash
# On registry: PAT for the embed worker
registry create-worker --label gpu-embed-1 --capabilities embed --max-concurrent 8

# On GPU box: run Ollama (or LocalAI / llama.cpp-server) and point embedworker at it
ollama serve &
embedworker --registry https://registry.example.com --pat $PAT \
            --batch 64 --idle-sleep 3s \
            --embed-url http://localhost:11434 --embed-model nomic-embed-text
```

Or via the unified `agent`:

```bash
agent --registry $URL --pat $PAT \
      --enable embed \
      --embed-url http://localhost:11434 --embed-model nomic-embed-text \
      --batch 64
```

Same protocol either way — server handles Qdrant upserts when `--qdrant-url` is configured on the registry; the embed worker just hands back vectors.

### `migrator`

Move blobs between BlobStores. The lake_objects row is rewritten with the new backend; the `migrated_from` column keeps an audit trail.

```
migrator \
  --from local --to s3 \
  --local-root ./blobs \
  --s3-bucket crawler-lake \
  --s3-region us-east-1 \
  --s3-endpoint http://minio:9000   # optional, for MinIO/R2/etc.
  --s3-path-style                   # MinIO requires path-style
  --s3-access-key AKID --s3-secret-key SECRET   # or fall back to AWS SDK chain
  --batch 100 \
  --delete-src                      # off by default; verify first!
```

`--from` and `--to` accept `local | s3`. Already-migrated rows (whose `storage_backend` already matches `--to`) are skipped, so the run is idempotent.

| Flag (env) | Default | When to use |
|---|---|---|
| `--db-driver` (`DB_DRIVER`) | `sqlite` | Same as registry — must point at the same DB the registry uses. |
| `--db-dsn` (`DB_DSN`) | `crawler.db` | DSN for that DB. |
| `--read-dsn` (`READ_DSN`) | — | Optional read-replica DSN for the scan query. |
| `--from` | — *(required)* | Source backend: `local` or `s3`. Reads existing blob bytes from here. |
| `--to` | — *(required)* | Destination backend: `local` or `s3`. Writes copies here, then updates `lake_objects.storage_backend`. |
| `--local-root` (`BLOBS_ROOT`) | `./blobs` | Filesystem root when `local` is involved (either side). Must match the registry's `--blobs-root`. |
| `--s3-bucket` (`S3_BUCKET`) | — | Required when either side is `s3`. |
| `--s3-region` (`S3_REGION`) | — | AWS region; ignored by MinIO but the SDK still requires a value. |
| `--s3-endpoint` (`S3_ENDPOINT`) | — | Override for MinIO / Cloudflare R2 / Wasabi. Leave empty for real AWS. |
| `--s3-access-key` (`S3_ACCESS_KEY`) | — | Static key. Omit to fall back to AWS default chain (env, IAM role, `~/.aws/`). |
| `--s3-secret-key` (`S3_SECRET_KEY`) | — | Static secret, same rule. |
| `--s3-path-style` (`S3_PATH_STYLE`) | `false` | Force path-style addressing. **Required by MinIO**; harmless for AWS. |
| `--batch` | `100` | Rows scanned per page. Lower for huge blobs (memory); higher for many small ones. |
| `--delete-src` | `false` | **Destructive.** Removes the source blob after the copy succeeds + verifies. Always do a dry run first (`--delete-src` off), then re-run with it on. |

---

## Configuration (env vars)

All listed CLI flags also accept env vars. Common ones:

| Env | Used by | Notes |
|---|---|---|
| `LEASE_SECRET`        | registry | HMAC secret. Required to serve or migrate. |
| `DB_DRIVER`           | registry, migrator | `sqlite` / `postgres` / `mysql` |
| `DB_DSN`              | registry, migrator | Driver-specific DSN |
| `READ_DSN`            | registry, migrator | Optional PG/MySQL read replica DSN |
| `BLOBS_ROOT`          | registry, migrator | Local BlobStore root |
| `MAX_BODY_BYTES`      | registry | Body upload cap |
| `ADDR`                | registry | `serve --addr` |
| `ALLOW_AUTO_DOMAINS` / `MAX_DEPTH` | registry serve | Crawl scope knobs (slice 8) |
| `QDRANT_URL`          | registry | Qdrant base URL, e.g. `http://localhost:6333`. Empty disables vector push/search. |
| `QDRANT_API_KEY`      | registry | optional Qdrant Cloud auth |
| `QDRANT_SHARDS`       | registry | shard_number on collection auto-create. Default **9** |
| `QDRANT_DISTANCE`     | registry | Cosine / Dot / Euclid. Default Cosine |
| `EMBED_URL`           | registry | Ollama-style `/api/embeddings` for `POST /v1/search` with `query_text` |
| `EMBED_MODEL`         | registry | model name for `EMBED_URL` calls. Default `nomic-embed-text` |
| `EMBED_API_KEY`       | registry | optional Bearer for `EMBED_URL` |
| `REGISTRY`            | worker | Base URL like `http://host:8080` |
| `PAT`                 | worker | Personal Access Token |
| `S3_BUCKET` / `S3_REGION` / `S3_ENDPOINT` / `S3_ACCESS_KEY` / `S3_SECRET_KEY` / `S3_PATH_STYLE` | migrator | S3/MinIO config |
| `DEBUG`               | registry | Verbose logging |
| `DB_LOG_LEVEL=debug`  | registry | gorm query logging |

---

## HTTP API

Base path: `http://<host>:<port>/v1`.

### Auth

Every `/v1/*` endpoint requires:

```
Authorization: Bearer <PAT>
```

The server hashes the PAT with SHA-256 and looks it up in the `workers.pat_hash` column. If the worker is banned (`workers.banned_at` set), all requests return `403 banned`.

### Health

```http
GET /healthz
```

Returns `200 OK` with body `ok`. No auth.

### Workers: `/v1/workers/me`

```http
GET /v1/workers/me
Authorization: Bearer <PAT>
```

```json
{
  "id": 1,
  "label": "dev-crawl",
  "reputation": 100,
  "banned": false
}
```

### Crawl: `/v1/jobs/*`

#### `POST /v1/jobs/reserve`

Lease up to `batch` URLs. One job per domain per batch (respects `crawl_delay_ms`).

Request:

```json
{
  "batch": 10,
  "capabilities": ["http", "pdf"]
}
```

Response `200 OK`:

```json
{
  "jobs": [
    {
      "job_id":           "0f115db062b7c0dd030b16878c99dea5c354b49dc37b38eb8846179c7783e9d7",
      "url":              "https://example.com/",
      "canonical_url":    "https://example.com/",
      "depth":            0,
      "attempt_count":    1,
      "lease_token":      "iAhdAFhsmL...",
      "lease_expires_at": 1717634400,
      "max_body_bytes":   209715200
    }
  ]
}
```

* `job_id` = lowercase hex of the SHA-256 URL hash (opaque to the worker).
* `lease_token` = base64url HMAC-SHA256 (32B urlHash || 8B workerID || 8B expiry || 16B MAC). Stateless — verified server-side from the secret.
* `lease_expires_at` = Unix seconds. Default TTL **60s**.
* `max_body_bytes` = configurable upload cap (default **200 MiB**).

If no eligible work, returns `{"jobs":[]}`.

#### `POST /v1/jobs/heartbeat`

Extend the lease while still fetching.

```json
{ "lease_token": "iAhdAFhsmL..." }
```

```json
{ "lease_expires_at": 1717634460 }
```

Returns `409 heartbeat_failed` if the lease isn't held (expired/swept/wrong token).

#### `POST /v1/jobs/result`

`multipart/form-data` with two parts.

Part 1 — `meta` (JSON):

```json
{
  "lease_token": "iAhdAFhsmL...",
  "http_status": 200,
  "content_type": "text/html; charset=UTF-8",
  "content_sha256": "0f115db062b7c0dd030b16878c99dea5c354b49dc37b38eb8846179c7783e9d7",
  "size": 528,
  "discovered_links": [
    { "url": "https://www.iana.org/domains/example", "anchor": "More information", "rel": "", "new_depth": 1 }
  ]
}
```

Part 2 — `blob` (raw bytes ≤ `max_body_bytes`).

Response `200 OK`:

```json
{ "lake_object_id": 1, "accepted": true }
```

Side effects on success:

1. Blob written to BlobStore at `<hashPrefix2>/<urlHashHex>.<ext>`.
2. Row inserted into `lake_objects` with backend, key, sha256, size.
3. Frontier row marked `done`, `http_status` recorded.
4. If `content_type` matches a known route (`text/html`, `application/pdf`, DOCX), a `processing_jobs` row is enqueued.
5. Each `discovered_links` entry is canonicalized + per-host upsert + idempotently enqueued to the frontier (must be **absolute** URLs — worker resolves `<base href>` first).

#### `POST /v1/jobs/fail`

```json
{
  "lease_token": "iAhdAFhsmL...",
  "http_status": 504,
  "error_code": "fetch_timeout",
  "error_message": "context deadline exceeded",
  "retryable": true
}
```

```json
{ "recorded": true }
```

If `retryable && attempt_count < max_attempts`, the row goes back to `queued` with `next_retry_at = now() + backoff`. Otherwise `status = 'dead'`.

### Embed: `/v1/embed/*`

External embedding service drives these.

#### `POST /v1/embed/reserve`

```json
{ "batch": 1000 }
```

```json
{
  "chunks": [
    {
      "chunk_id":         "4a3538bf-ade7-4f46-a5f7-e495aede7f6c",
      "document_id":      1,
      "chunk_index":      0,
      "text":             "Example Domain Example Domain This domain is for use in documentation examples...",
      "token_count":      21,
      "collection":       "lithuania_news",
      "lease_token":      "Vj6f9wVa1...",
      "lease_expires_at": 1717634460
    }
  ]
}
```

`token_count` currently stores **word count**; swap to a real tokenizer when needed. Default batch when omitted: 1000.

`collection` is the per-domain vector-store hint (e.g. Qdrant collection name). Resolved at extraction time from the source lake_object → domain → `domains.embed_collection`, with the host as fallback. Empty string means no override — your embed worker picks. See [Per-domain vector collections](#per-domain-vector-collections-qdrant--others).

#### `POST /v1/embed/result`

Push embed outcomes — mix successes and failures, two acceptance modes:

**Vector mode (preferred, slice 10+)** — worker sends the raw vector; registry auto-creates the Qdrant collection (≥9 shards by default) and upserts the point with payload `{lake_object_id, document_id, chunk_index, text, url, collection}`:

```json
{
  "results": [
    {
      "chunk_id":     "4a3538bf-ade7-4f46-a5f7-e495aede7f6c",
      "vector":       [0.012, -0.041, 0.083, ...],
      "lease_token":  "Vj6f9wVa1..."
    }
  ]
}
```

Server stamps `document_chunks.vector_id = "qdrant:{collection}:{chunk_id}"`.

**Legacy mode** — worker handled its own vector store and only reports an opaque ID:

```json
{
  "results": [
    {
      "chunk_id":   "4a3538bf-...",
      "vector_id":  "qdrant:point-4a3538bf",
      "lease_token": "Vj6f9wVa1..."
    }
  ]
}
```

**Failure** — same in both modes:

```json
{
  "results": [{
    "chunk_id":    "9b1cb...",
    "failed":      true,
    "reason":      "embedding model timeout",
    "lease_token": "Vj6f9wVa2..."
  }]
}
```

Response:

```json
{ "accepted": 1, "failed_recorded": 0 }
```

`first_error` is added when at least one item fails to record (qdrant down, lease expired, etc.). The vector mode requires `--qdrant-url` on the registry; without it the server falls back to `vector_id = "raw:{chunk_id}"` (effectively drops the vector — useful only for tests).

### Read API for sink workers: `/v1/lake`, `/v1/extracted`, `/v1/chunks`

These let downstream consumers (Qdrant, Quickwit, Elasticsearch, OLAP SQL warehouses, anything else) **pull from the data lake on their own cursor**. Registry stays storage-agnostic; the sink keeps its own watermark.

Capabilities: `lake_read`, `extracted_read`, `chunks_read`.

#### `GET /v1/lake?since_id=N&limit=M&backend=local&content_type_prefix=text/html`

```json
{
  "count": 2,
  "items": [
    {
      "id": 17,
      "url_hash": "0f115db062...",
      "content_type": "text/html",
      "content_sha256": "0f115db062...",
      "file_size_bytes": 528,
      "storage_backend": "local",
      "storage_key": "0f/0f11...e9d7.html",
      "archived_at": 1717634100,
      "blob_url": "/v1/blobs/17"
    }
  ]
}
```

Fetch the raw bytes with `GET /v1/blobs/{id}`. Cursor on `since_id` (highest `id` from last page).

#### `GET /v1/extracted?since_id=N&limit=M`

```json
{
  "count": 1,
  "items": [
    {
      "id": 5,
      "source_lake_object_id": 17,
      "language": "",
      "page_count": 0,
      "extracted_at": 1717634110,
      "text_size_bytes": 1281,
      "text_preview": "Example Domain Example Domain This domain is for use ...",
      "text_url": "/v1/extracted/5/text"
    }
  ]
}
```

#### `GET /v1/extracted/{id}/text`

Returns `text/plain` body. The full extracted text — no preview truncation.

#### `GET /v1/chunks?embed_status=done&since=<unix>&limit=M`

```json
{
  "count": 1,
  "items": [
    {
      "id": "4a3538bf-ade7-4f46-a5f7-e495aede7f6c",
      "document_id": 5,
      "chunk_index": 0,
      "text": "Example Domain Example Domain ...",
      "token_count": 21,
      "vector_id": "qdrant:point-4a3538bf",
      "embed_status": "done"
    }
  ]
}
```

Cursor on `since` (unix timestamp) — pass the most recent `created_at` from your last page. Filter by `embed_status` to consume only finished chunks (or pending, or failed).

### Search: `/v1/search`

Vector retrieval against Qdrant. Returns hits enriched with `lake_object_id`, `document_id`, `chunk_index`, `text`, `url`, `score`. Capability required: `search`.

#### `POST /v1/search`

By precomputed vector:

```json
{
  "collection":   "lithuania_news",
  "query_vector": [0.011, -0.034, ...],
  "limit":        10
}
```

By text — server embeds via the optional `--embed-url` (Ollama-style `/api/embeddings`):

```json
{
  "collection": "lithuania_news",
  "query_text": "kas yra signalų teorija",
  "limit":      10
}
```

Response:

```json
{
  "count": 2,
  "items": [
    {
      "chunk_id":       "fc1241d0-...",
      "lake_object_id": 1,
      "document_id":    1,
      "chunk_index":    0,
      "score":          0.91,
      "text":           "The quick brown fox jumps over the lazy dog...",
      "url":            "https://site-a.test/page",
      "collection":     "lithuania_news"
    }
  ]
}
```

Requires `--qdrant-url` set on the registry; without it the endpoint returns `500 search: qdrant not configured`. `query_text` further requires `--embed-url`.

### Per-domain vector collections (Qdrant / others)

Each `domains` row has an optional `embed_collection TEXT` column. At extraction time the server walks `lake_object → frontier → domain` and stamps the resolved collection onto `extracted_documents.collection`. Chunks inherit it via `document_id`, and `/v1/embed/reserve` surfaces it per chunk.

Operator setup:

```bash
registry seed-domain   --host 9g.lt --crawl-delay-ms 500
registry update-domain --host 9g.lt --embed-collection lithuania_news

registry seed-domain   --host eu-news.example
registry update-domain --host eu-news.example --embed-collection eu_news

# No --embed-collection set? Server defaults to the host: "9g.lt" or "eu-news.example".
# Clear an override:
registry update-domain --host 9g.lt --embed-collection -
```

Embed worker (any language) uses the field:

```python
chunks = registry.reserve_embed(batch=1000)
for c in chunks:
    vec = model.embed(c["text"])
    # Preferred (slice 10+): send the vector itself; registry writes Qdrant.
    registry.post_result(c["chunk_id"], c["lease_token"], vector=vec)

    # Legacy: write Qdrant from the worker yourself, send opaque vector_id back.
    # qdrant.upsert(collection=c["collection"] or "_default", points=[{ ... }])
    # registry.post_result(c["chunk_id"], c["lease_token"], vector_id="qdrant:...")
```

In the preferred mode the registry talks to Qdrant directly: auto-creates the collection (default **9 shards** even on a single host), upserts the point with payload `{lake_object_id, document_id, chunk_index, text, url, collection}`. The embed worker becomes vector-store-agnostic — swap Qdrant for Weaviate/Pinecone by changing one config flag.

### Pipeline triggers: `/registry trigger-*`

Routing rules are stored in the `pipeline_triggers` table and editable at runtime. Replaces the previously-hardcoded MIME→processor map.

Default seeded triggers (after `migrate up`):

```
ID  EVENT                  FILTER                                                ENQUEUE        ENABLED
1   lake_object_inserted   {"content_type_prefix":"text/html"}                   html_strip     true
2   lake_object_inserted   {"content_type_prefix":"application/pdf"}             pdf_ocr        true
3   lake_object_inserted   {"content_type_prefix":"...wordprocessingml..."}      docx_to_pdf    true
```

Operator workflow:

```bash
# View the table
registry trigger-list

# Add: when a new lake object lands with HTML content, also fire a Quickwit indexer
registry trigger-add \
  --on lake_object_inserted \
  --content-type text/html \
  --enqueue quickwit_indexer

# Add: when DOCX→PDF produces a new blob, fire a "watermark" processor on it
registry trigger-add \
  --on blob_produced \
  --source-processor docx_to_pdf \
  --enqueue watermark_check

# Temporarily disable a stage
registry trigger-disable --id 2

# Re-enable
registry trigger-enable --id 2

# Permanently remove
registry trigger-delete --id 4
```

Events fired:

| Event | When | Payload fields used by filters |
|---|---|---|
| `lake_object_inserted` | `Service.AcceptResult` after a crawl/fetch result is stored | `content_type` |
| `blob_produced` | `TaskSvc.AcceptBlob` after a task worker uploads an output blob | `content_type`, `source_processor` |

Filter shape (`when_filter` column, JSON):

```json
{
  "content_type_prefix": "application/pdf",
  "source_processor": "docx_to_pdf"
}
```

Both fields are optional; missing means "any". Comparisons are case-insensitive prefix match for content type and exact for source_processor. Triggers are cached for 5s, so CLI edits propagate within at most a 5-second window.

#### Adding a new processor — no rebuild required

The `Processor` constants in `internal/domain/processing/job.go` are convenience identifiers for the registry binary's *in-process* workers (`html_strip`, `text_passthrough`). Nothing in the request path validates against that list. To add a new **external** processor (e.g. `video_processing` backed by ffmpeg), no registry rebuild or redeploy is needed:

```bash
# 1. Register the kind in the catalog (optional but improves `list-capabilities` output)
registry capability-add --name video_processing \
  --description "Transcode video lake objects to mp4"

# 2. Route matching lake objects to it
registry trigger-add --on lake_object_inserted \
  --content-type video/ --enqueue video_processing

# 3. Issue a PAT for the worker
registry create-worker --label vid-worker --capabilities video_processing
#    → save the printed PAT

# 4. Run your ffmpeg worker (any language; see examples/) with that PAT and KIND=video_processing
```

The worker reserves jobs via `POST /v1/tasks/reserve {kinds:["video_processing"]}` and uploads the result via `POST /v1/tasks/result`. The registry binary did not change.

Use `capability-add` only for catalog discoverability — it does not gate PAT issuance or task reservation, both of which accept any string. `capability-rm --name X` removes a catalog entry.

### Tasks: `/v1/tasks/*`

External processing-task protocol. Same lease/reserve/result/fail shape as crawl, scoped to one or more `processor` kinds.

#### `POST /v1/tasks/reserve`

```json
{
  "kinds": ["pdf_ocr"],
  "batch": 4
}
```

```json
{
  "tasks": [
    {
      "task_id":            42,
      "processor":          "pdf_ocr",
      "lake_object_id":     1,
      "blob_url":           "/v1/blobs/1",
      "blob_content_type":  "application/pdf",
      "blob_size_bytes":    8421,
      "attempt_count":      1,
      "lease_token":        "iAhdAFhsmL...",
      "lease_expires_at":   1717634460
    }
  ]
}
```

`blob_url` is relative to the registry base. Fetch with `GET /v1/blobs/{id}` (same PAT). `kinds` is optional — omit to accept any kind the worker can handle (but you probably want to scope, since OCR/DOCX/embedding have very different runtimes).

#### `POST /v1/tasks/heartbeat`

```json
{ "task_id": 42, "lease_token": "iAhdAFhsmL..." }
```

```json
{ "lease_expires_at": 1717634520 }
```

#### `POST /v1/tasks/result`

`multipart/form-data` with one or two parts.

Part 1 — `meta` (JSON):

```json
{
  "task_id":                42,
  "lease_token":            "iAhdAFhsmL...",

  "extracted_text":         "Page 1 ...",        // text-mode result
  "language":               "eng",
  "page_count":             12,

  "output_content_type":    "application/pdf",   // blob-mode result
  "output_content_sha256":  "<hex>",
  "next_processor":         "pdf_ocr"            // chained downstream stage
}
```

Part 2 — `blob` (optional): the output bytes when `output_content_type` is set.

Responses:

```json
{ "accepted": true }                                  // text-mode
{ "output_lake_object_id": 7, "accepted": true }      // blob-mode
```

Side effects:

* **Text mode** — writes (or updates) the `extracted_documents` row for the source lake object, splits the text into `document_chunks` with `embed_status='pending'`, marks the task `done`.
* **Blob mode** — stores the output bytes in the active BlobStore, inserts a new `lake_objects` row (reusing the source `url_hash` for provenance), sets `processing_jobs.output_lake_object_id`, marks the task `done`. If `next_processor` is non-empty, enqueues a fresh `processing_jobs` row pointed at the new lake object.

#### `POST /v1/tasks/fail`

```json
{
  "task_id":       42,
  "lease_token":   "iAhdAFhsmL...",
  "error_code":    "tesseract_oom",
  "error_message": "killed by OOM",
  "retryable":     true
}
```

```json
{ "recorded": true }
```

Retry semantics mirror crawl: `retryable && attempt_count < max_attempts` → back to `queued`. Otherwise `failed`.

### Blobs: `/v1/blobs/{id}`

```http
GET /v1/blobs/1
Authorization: Bearer <PAT>
```

Streams the raw lake-object body. `Content-Type` is set from the row. The endpoint serves only blobs whose `storage_backend` matches the registry's active backend (during a heterogeneous migration, run the source backend's registry to serve old rows or use the `migrator` to converge first).

### Error format

Every 4xx/5xx returns:

```json
{ "error": "code_string", "code": "code_string", "message": "human-readable" }
```

Common codes: `missing_bearer`, `unknown_pat`, `banned`, `bad_json`, `bad_multipart`, `missing_meta`, `missing_blob`, `bad_sha`, `reserve_failed`, `result_failed`, `heartbeat_failed`, `fail_failed`.

---

## Worker protocol

A crawl worker can be written in any language. The contract:

1. **Loop**: `POST /v1/jobs/reserve` → if jobs returned, work them; else sleep and retry.
2. **For each job**: HTTP `GET` the `url`, honoring `max_body_bytes`. Compute SHA-256 of the body inline.
3. **Discover links**: parse HTML, resolve relative refs against `<base href>` then against the `canonical_url`. Send only **absolute** http/https URLs in `discovered_links`.
4. **Push back**:
   - Success → `POST /v1/jobs/result` (multipart).
   - Network/parse error → `POST /v1/jobs/fail` with `retryable=true`.
   - Permanently bad input (4xx, body too large) → `retryable=false`.
5. **Heartbeat** if processing exceeds the 60s lease TTL.

The reference Go worker (`cmd/worker`) implements all of this in ~250 lines.

---

## Embed-worker protocol

A separate worker dedicated to vectorization:

1. `POST /v1/embed/reserve` with `{"batch": 1000}` (or smaller).
2. For each returned chunk: call your embedding model on `text`.
3. Write the vector to your vector store (e.g. Qdrant) and capture its point ID.
4. `POST /v1/embed/result` with the array — successes carry `vector_id`, failures carry `failed: true` and `reason`.
5. Repeat until `chunks: []`.

Chunks reverted to `pending` via the lease sweeper if the embed worker disappears mid-batch.

---

## Task-worker protocol (OCR / DOCX / etc.)

A "task worker" is a long-running process on whatever machine has the right hardware (GPU box for OCR, CPU box with LibreOffice for DOCX, etc.). Same loop as the crawl worker, different endpoint:

1. `POST /v1/tasks/reserve` with `{"kinds": ["pdf_ocr"], "batch": 4}`.
2. For each task: `GET {blob_url}` to download the source blob to a temp file.
3. Run the heavy work locally (Tesseract / Tika / LibreOffice / your own model).
4. Push back:
   - Text result → `POST /v1/tasks/result` (multipart, `meta` only).
   - New blob (e.g. converted PDF) → `POST /v1/tasks/result` with `meta + blob`.
5. On error → `POST /v1/tasks/fail`.
6. `POST /v1/tasks/heartbeat` if the task takes longer than the 60s lease TTL.
7. When `reserve` returns an empty list, sleep `--idle-sleep` and retry.

The reference Go worker (`cmd/taskworker`) implements all of this and lets you plug in **any external tool** via `--extract-cmd`. No code changes needed to swap Tesseract for `pdftotext`, swap LibreOffice for Pandoc, etc.

### Deploying on a GPU server

There's no separate scheduler. The worker is the schedule: it loops until the queue is empty, sleeps, and retries.

**systemd unit** (recommended — daemon stays alive forever):

```ini
# /etc/systemd/system/crawlerv3-pdf-ocr.service
[Unit]
Description=crawlerv3 PDF OCR worker
After=network.target

[Service]
Type=simple
Environment=REGISTRY=https://registry.example.com
Environment=PAT=...           ; or use EnvironmentFile=/etc/crawlerv3/pat
ExecStart=/usr/local/bin/taskworker \
  --kind pdf_ocr \
  --batch 4 \
  --idle-sleep 10s \
  --mode text \
  --extract-cmd "tesseract {input} - -l eng+lit"
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Then `systemctl enable --now crawlerv3-pdf-ocr`.

**Burst pattern** (for cheap spot GPU hours): a cron / k8s `CronJob` that fires `taskworker --max-runtime 1h`. It drains the queue, then exits when either `--max-runtime` elapses or no more work arrives.

**Scale-out**: just run more workers. Each box claims its own batch under separate leases; the only contention is the brief reserve-update transaction, which serializes safely on every supported DB.

### Bulk-enqueuing old data

When you wire in a new processor (e.g. you just got a GPU and want to OCR every PDF you've crawled in the last year), use `registry reprocess`:

```bash
registry reprocess --processor pdf_ocr                              # auto: content-type starts with application/pdf
registry reprocess --processor pdf_ocr --content-type-prefix application/pdf --limit 50000
registry reprocess --processor docx_to_pdf                          # auto: docx mime
```

Then deploy the task worker. It picks up the new backlog automatically.

---

## Storage backends

The `lake.BlobStore` interface is small and storage-agnostic:

```go
type BlobStore interface {
    Backend() string
    Put(ctx, key, r, meta) (Stat, error)
    Get(ctx, key) (io.ReadCloser, Stat, error)
    Stat(ctx, key) (Stat, error)
    Delete(ctx, key) error
}
```

Two implementations ship today:

| Backend | Package | Notes |
|---|---|---|
| `local` | `internal/infra/store/local` | Default. Filesystem under `--blobs-root`. SHA verified on Put. |
| `s3`    | `internal/infra/store/s3`    | aws-sdk-go-v2 client. Works with AWS S3, MinIO (`--s3-endpoint --s3-path-style`), Cloudflare R2, etc. |

Each `lake_objects` row carries its own `storage_backend` + `storage_key`, so the cluster can serve a heterogeneous mix during migrations.

### Switching backends at runtime

The Service constructor takes whichever BlobStore you wire in; new writes go to that backend. Existing rows keep their own `storage_backend`, so reads continue to work seamlessly. The recommended flow:

1. Run `migrator --from local --to s3` to copy historical blobs.
2. Verify in a non-destructive run.
3. Re-run with `--delete-src` to free disk.
4. Restart `registry` configured with the new backend; new writes land in S3.

---

## Database backends

The `rwdb` package gives every repository a single-writer / many-readers split:

| Driver | Read pool | Write pool | Notes |
|---|---|---|---|
| SQLite     | `MaxOpenConns=NumCPU`  | `MaxOpenConns=1` | WAL mode, busy_timeout=5000 |
| PostgreSQL | `MaxOpenConns=NumCPU*4`| `MaxOpenConns=NumCPU` | `PrepareStmt: true`. `--read-dsn` lets R point at a replica. |
| MySQL      | same as PG | same as PG | `--read-dsn` likewise |

Use `db.ReadTX(ctx, fn)` for read-only transactions and `db.WriteTX(ctx, fn)` for read-write. Simple Get/Insert/Update use `db.R` / `db.W` directly.

Migrations live in `internal/infra/db/migrations/{sqlite,postgres,mysql}/` and are embedded via `//go:embed`. `registry migrate up` picks the right dialect off `--db-driver`.

---

## Processing pipeline

After a fetch result is accepted, the Service routes it by MIME:

| Content-Type | Processor | Handled by |
|---|---|---|
| `text/html`                                                                                | `html_strip`        | **internal goroutine** |
| `text/plain`, `text/csv`, `application/json`, `application/xml`, `text/xml`                | `text_passthrough`  | **internal goroutine** (body IS the text) |
| `application/pdf`                                                                          | `pdf_ocr`           | **external `taskworker`** (e.g. Tesseract) |
| `application/vnd.openxmlformats-officedocument.wordprocessingml.document` (DOCX)           | `office_to_pdf`     | **external `taskworker`** (e.g. LibreOffice) |
| `application/vnd.ms-excel`, `…spreadsheetml…` (XLS / XLSX)                                 | `office_to_pdf`     | external `taskworker` |
| `application/vnd.ms-powerpoint`, `…presentationml…` (PPT / PPTX)                           | `office_to_pdf`     | external `taskworker` |
| `application/rtf`, `application/vnd.oasis.opendocument…` (RTF / ODT / ODS / ODP)           | `office_to_pdf`     | external `taskworker` |
| other                                                                                      | none                | blob stored, no extraction |

`pipe.Run(ctx, 2*time.Second)` polls `processing_jobs` and dispatches only the processors in `Pipeline.InternalProcessors` (default: `html_strip`, `text_passthrough`). Everything else stays in `queued` waiting for an external `taskworker` to claim it via `/v1/tasks/reserve`.

1. **html_strip** — `golang.org/x/net/html` tokenizer, drops `<script>/<style>/<noscript>`, collapses whitespace. Runs in-process because it's cheap and CPU-only.
2. **text_passthrough** — body IS already text; reads it, writes `extracted_documents`, chunks. Handles `text/plain`, `text/csv`, `application/json`, `application/xml`, `text/xml`. No worker needed.
3. **pdf_ocr** — claimed by GPU/CPU boxes running `taskworker --kind pdf_ocr --extract-cmd "<your OCR tool>"`.
4. **office_to_pdf** — generalized DOCX/XLS/XLSX/PPT/PPTX/RTF/ODT/ODS/ODP → PDF converter. One worker fleet handles every office format. Output PDF becomes a new lake object; trigger then enqueues `pdf_ocr` on the converted file. Run as `taskworker --kind office_to_pdf --mode blob --extract-cmd "libreoffice --headless --convert-to pdf --outdir {outdir} {input}" --output-glob "{outdir}/*.pdf" --next-processor pdf_ocr`.

Per-extracted-document, the registry also stores a `collection` hint derived from the source domain's `embed_collection` (or the host as fallback). The embed worker receives this in `/v1/embed/reserve` and uses it to route vectors into the right Qdrant collection. See [Per-domain vector collections](#per-domain-vector-collections-qdrant--others).

The chunker splits extracted text into **400-word** chunks with **50-word** overlap (`chunker.Defaults()`), assigns UUIDv4 IDs, and bulk-inserts into `document_chunks` with `embed_status='pending'`. The embed worker takes it from there.

---

## Operations

### Lease sweeping

Three sweeps run every 30 seconds inside `registry serve`:

* **Frontier**: rows with `status='leased' AND lease_expires_at < now()` go back to `queued`.
* **Chunks**: same for `document_chunks` rows with `embed_status='leased'`.
* **Tasks**: same for `processing_jobs` rows with `status='running' AND lease_expires_at < now()`.

### Politeness / per-domain delay

The reserve query joins `domains` and skips hosts whose `last_request_at` is younger than `crawl_delay_ms`. The chosen URL also bumps that domain's `last_request_at` inside the same transaction. Each batch lease delivers **at most one URL per domain**, enforced with a window function (`ROW_NUMBER() OVER (PARTITION BY domain_id ORDER BY priority DESC, scheduled_for ASC)`).

### Crawl scope (no internet-wide escape)

A discovered link is only enqueued when its host already lives in the `domains` table and `is_active=true`. Out-of-scope hosts are dropped at result-ingest time. To open it up, run `registry serve --allow-auto-domains`. To cap recursion, run `registry serve --max-depth N`. See [Scoping the crawl](#scoping-the-crawl-dont-let-it-escape) in the CLI reference.

### Retries / backoff

`crawl_frontier.max_attempts` defaults to 5. Failures bump `attempt_count`; if `retryable && attempt < max`, the row goes back to `queued` with `next_retry_at = now() + 30s` (today; exponential backoff knob is in `Service.Cfg`). Otherwise the row becomes `dead` and is excluded from future reserves.

### Worker management

```bash
# Issue a PAT with capabilities and concurrency cap
registry create-worker --label gpu-1 --capabilities pdf_ocr,docx_to_pdf --max-concurrent 4

# View the whole pool with live HELD counts
registry list-workers

# Adjust without restart
registry update-worker --id 1 --max-concurrent 12
registry update-worker --id 1 --capabilities crawl,html_strip,extracted_read

# Suspend / restore a worker (middleware returns 403 banned on every authed call)
registry ban-worker   --id 5
registry unban-worker --id 5
```

Plaintext PAT is printed once on creation; only its SHA-256 lives in `workers.pat_hash`. Each authenticated call updates `workers.last_seen_at` and `workers.ip_last` automatically — `list-workers` surfaces both, so an unresponsive worker shows up as a stale `LAST_SEEN`.

### Observability

* `GET /healthz` → liveness.
* `--debug` (`DB_LOG_LEVEL=debug`) → gorm SQL logging.
* `chi` request IDs propagate via `X-Request-Id`.
* `registry list-workers` shows live held leases + last-seen per worker.
* `registry trigger-list` shows current routing rules.
* Plain text status is easy to query directly:

  ```bash
  sqlite3 crawler.db "SELECT status, COUNT(*) FROM crawl_frontier GROUP BY 1;"
  sqlite3 crawler.db "SELECT processor, status, COUNT(*) FROM processing_jobs GROUP BY 1,2;"
  sqlite3 crawler.db "SELECT embed_status, COUNT(*) FROM document_chunks GROUP BY 1;"
  sqlite3 crawler.db "SELECT when_event, enqueue_kind, enabled FROM pipeline_triggers;"
  ```

---

## Common workflows

A few end-to-end recipes that combine the CLI commands above.

### 1. Add a new OCR fleet

```bash
# On the registry host: create a worker identity for the GPU box
registry create-worker --label gpu-ocr-1 --capabilities pdf_ocr --max-concurrent 4
# → pat=...        copy this to the GPU box as $PAT

# (Optional) Backfill: enqueue pdf_ocr tasks for every PDF already in the lake
registry reprocess --processor pdf_ocr

# On the GPU box: deploy taskworker (or agent)
taskworker --registry https://registry.example.com --pat $PAT \
           --kind pdf_ocr --batch 4 --idle-sleep 10s \
           --mode text --extract-cmd "tesseract {input} - -l eng+lit"
```

### 2. Plug a Quickwit (or Elasticsearch, OpenSearch, etc.) FTS indexer onto the lake

```bash
# On the registry host: PAT for the sink
registry create-worker --label quickwit-1 --capabilities extracted_read --max-concurrent 1

# Tell the registry to also enqueue a quickwit_indexer task whenever an
# extraction lands (operator decides whether to use this push-style trigger
# or skip it and rely purely on the pull-style /v1/extracted polling below).
registry trigger-add --on lake_object_inserted \
                     --content-type text/html \
                     --enqueue quickwit_indexer

# On the indexer host: poll /v1/extracted with a cursor stored locally.
LAST=$(cat /var/lib/quickwit-indexer/cursor 2>/dev/null || echo 0)
while true; do
  RESP=$(curl -sS -H "Authorization: Bearer $SINK_PAT" \
              "https://registry.example.com/v1/extracted?since_id=$LAST&limit=500")
  COUNT=$(echo "$RESP" | jq '.count')
  echo "$RESP" | jq -c '.items[]' | while read DOC; do
    ID=$(echo "$DOC" | jq -r '.id')
    TEXT=$(curl -sS -H "Authorization: Bearer $SINK_PAT" \
                "https://registry.example.com/v1/extracted/$ID/text")
    echo "$DOC" | jq --arg t "$TEXT" '.text = $t' | quickwit ingest --index docs
  done
  if [ "$COUNT" -gt 0 ]; then
    echo "$RESP" | jq '.items[-1].id' > /var/lib/quickwit-indexer/cursor
  fi
  sleep 5
done
```

### 3. Wire a multi-stage DOCX→PDF→OCR chain

```bash
# Workers (split or combined boxes — doesn't matter to the registry)
registry create-worker --label docx-1 --capabilities docx_to_pdf --max-concurrent 2
registry create-worker --label ocr-1  --capabilities pdf_ocr     --max-concurrent 4

# Routing is already covered by default triggers, but you can explicitly add
# a follow-up rule that catches the produced PDF and enqueues OCR.
registry trigger-add --on blob_produced \
                     --source-processor docx_to_pdf \
                     --content-type application/pdf \
                     --enqueue pdf_ocr

# Optional: also send the produced PDF through a watermark check
registry trigger-add --on blob_produced \
                     --source-processor docx_to_pdf \
                     --enqueue watermark_check

# Deploy the docx worker (writes a new PDF blob; produces blob_produced event)
taskworker --kind docx_to_pdf --mode blob \
           --extract-cmd "libreoffice --headless --convert-to pdf --outdir {outdir} {input}" \
           --output-glob "{outdir}/*.pdf" \
           --output-content-type application/pdf \
           --next-processor pdf_ocr      # explicit chain; trigger above also works
```

### 4. Audit + tighten an existing pool

```bash
registry list-workers
# ID  LABEL    CAPABILITIES               MAX  HELD  LAST_SEEN              BANNED
# 1   crawl-1  crawl                      8    8     2026-06-05T22:30:11Z   no
# 2   gpu-1    pdf_ocr,docx_to_pdf,embed  4    1     2026-06-05T22:30:09Z   no
# 3   noisy    (any)                      32   28    2026-06-05T22:30:00Z   no

# noisy = legacy worker with no capabilities. Lock it down:
registry update-worker --id 3 --capabilities crawl --max-concurrent 8

# Pause pdf_ocr while you redeploy the GPU box's image
registry trigger-disable --id 2     # the pdf_ocr trigger
# … deploy …
registry trigger-enable  --id 2
```

### 5. Dry-run a new routing rule before turning it loose

```bash
# Add the trigger disabled (no, the CLI doesn't have --disabled; toggle right after)
registry trigger-add --on lake_object_inserted --content-type application/xml --enqueue xml_classifier
registry trigger-disable --id 7

# Inspect what would match (sqlite ad-hoc query against the existing lake)
sqlite3 crawler.db "SELECT id, content_type FROM lake_objects WHERE content_type LIKE 'application/xml%' LIMIT 20;"

# Happy with the scope? Turn it on.
registry trigger-enable --id 7
```

---

## Extending the system

### Adding a new processor

1. Create `internal/infra/pipeline/<name>/`.
2. Add a constant to `internal/domain/processing/job.go`:

   ```go
   const ProcNewThing Processor = "new_thing"
   ```

3. Route in `internal/app/pipeline.go` `routeFor()` and add a case in `exec()`.
4. Done — the pipeline poller picks it up automatically.

### Adding a new BlobStore

Implement `lake.BlobStore` in `internal/infra/store/<name>/` and wire it into `cmd/registry/main.go` (and `cmd/migrator/main.go` if you want to migrate to it).

### Adding a new DB driver

1. Add a `Driver` constant + a `new<Driver>()` builder in `internal/infra/db/rwdb/rwdb.go`.
2. Add a migrations directory under `internal/infra/db/migrations/<driver>/`.
3. Register the dialect in `cmd/registry/main.go` `migrateAction`.
4. If the politeness SQL diverges, add a branch in `gormrepo/frontier_repo.go` `politenessSQL()`.

---

## Schema reference

Slice 1 — crawl loop:

| Table | Purpose |
|---|---|
| `domains` | Per-host config (scheme, crawl_delay_ms, robots cache, last_request_at). |
| `workers` | PAT-bound external participants. `pat_hash` is `sha256(PAT)`. |
| `crawl_frontier` | URL queue. PK = SHA-256 of canonical URL. Status: `queued` / `leased` / `done` / `failed` / `dead`. |
| `lake_objects` | Blob index. One row per stored raw response. `storage_backend` + `storage_key` locate it; `content_sha256` enables dedup. |

Slice 2 — processing pipeline:

| Table | Purpose |
|---|---|
| `processing_jobs` | One row per (lake_object, processor) stage. Status: `queued` / `running` / `done` / `failed` / `skipped`. |
| `extracted_documents` | Cleaned plain text per source `lake_objects`. |
| `document_chunks` | Splitting unit for embedding. PK = UUID. Embed status: `pending` / `leased` / `done` / `failed`. |

Lease tokens (frontier + chunks) are 32 B URL hash || 8 B worker ID || 8 B expiry || 16 B MAC, base64url-encoded.

---

## Stubs / future work

* **PDF OCR engine** — there is no built-in OCR; you pick the tool. Production deployments shell out via `taskworker --kind pdf_ocr --extract-cmd "tesseract {input} -"` (or Tika / unstructured.io / your own model wrapper). The legacy in-process `pdfproc.Extract` stub still returns `ErrSkip` and is no longer in the default `InternalProcessors`.
* **DOCX engine** — same story: `taskworker --kind docx_to_pdf --mode blob --extract-cmd "libreoffice ..."`. No built-in converter.
* **Reference embed worker** — protocol is implemented and tested via curl/Python from `scripts/smoke.sh`. A Go reference embed worker (model client + vector-store client) is left to the operator.
* **Qdrant client** — registry never talks to the vector store; `document_chunks.vector_id` is opaque. The embed worker handles it.
* **NATS fan-out** — pipeline + task drain run as goroutines. Replace with NATS subjects when you outgrow a single registry node.
* **Postgres SKIP LOCKED reserve** — current Reserve uses cross-dialect transaction serialization. A native `SELECT ... FOR UPDATE SKIP LOCKED` fast-path for PostgreSQL can be slotted into `frontier_repo.go` / `processing_repo.go` when needed.
* **Tenancy / multi-campaign** — single-tenant by design today.
* **Exponential backoff** — failures currently retry with a flat backoff (`Cfg.DefaultBackoff`, default 30s). Switching to exponential is a 5-line change.

---

## Testing

```bash
bash scripts/smoke.sh           # crawl → pipeline → embed roundtrip
bash scripts/smoke_tasks.sh     # external task-worker roundtrip (fake PDF OCR)
bash scripts/smoke_pool.sh      # worker pool: capabilities + concurrency caps + ban
bash scripts/smoke_lake.sh      # read API + pipeline triggers (default + custom)
bash scripts/smoke_scope.sh     # scope-locked crawl (discovered links can't escape seed)
bash scripts/smoke_files.sh     # office_to_pdf + text_passthrough + per-domain collection
bash scripts/smoke_qdrant.sh    # registry → Qdrant upsert (9 shards) + /v1/search
bash scripts/smoke_queueops.sh  # embedworker + queue-stats + ban-worker --release + requeue
bash scripts/smoke_bind.sh      # domain ↔ worker binding via required_capability
```

`scripts/smoke.sh` does the slice 1–5 end-to-end:

1. Builds all binaries into a temp dir.
2. Creates a fresh SQLite DB.
3. Issues a crawl PAT + an embed PAT.
4. Seeds `example.com`, enqueues `https://example.com/`.
5. Starts `registry serve`, starts the crawl worker.
6. Waits for the pipeline to chunk the page.
7. Drives `/v1/embed/reserve` + `/v1/embed/result` with curl + Python to set a fake `vector_id`.
8. Asserts: `lake_objects`, `extracted_documents`, `document_chunks` rows; chunks moved to `embed_status='done'`.

`scripts/smoke_tasks.sh` covers slice 6:

1. Seeds a synthetic PDF into the data lake (no real crawl needed).
2. `registry reprocess --processor pdf_ocr` enqueues a task.
3. Starts a `taskworker --kind pdf_ocr --mode text --extract-cmd "cat {input}"` — the `cat` is fake OCR; in production this is your `tesseract` command.
4. Asserts the task ends `done`, `extracted_documents` is populated, chunks are created.

On failure both scripts print the temp directory location for inspection.

---

## License

See `LICENSE` in the repository root.
