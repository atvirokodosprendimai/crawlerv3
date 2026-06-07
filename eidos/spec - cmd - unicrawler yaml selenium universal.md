---
tldr: universal Selenium-driven crawl worker — one YAML describes a site's domain, seeds, URL→behavior page_types, pagination strategy, and field extraction; sidecar JSON carries structured fields
category: core
---

# unicrawler

## Target

`cmd/unicrawler/` — a single Go binary that becomes a site-specific crawler purely by feeding it a YAML config. Drives a real headless browser over Selenium WebDriver so JS-rendered listings (ASP.NET WebForms postbacks, infinite scrolls, SPAs) are reachable without writing site-specific Go. Exposes three subcommands: `validate`, `seed`, `run`.

## Behaviour

- A single YAML file is sufficient to teach the worker about a new site: identity (name, domain, scheme, politeness), how to populate the frontier, and an ordered list of URL-pattern → behavior bindings.
- `validate` parses the YAML, compiles regexes, and lints required-field combinations without dialing the DB or a browser — an operator can iterate on config locally before touching the registry.
- `seed` is a direct-DB action (no HTTP) that upserts the domain row and enqueues the seed URLs. Re-running it is idempotent: the frontier dedups by URL hash.
- Seeds come in two flavors: an explicit `urls:` list, or a `date_range` with a `{day}`-bearing template that fans out one URL per day between `from` and `to` (`to` defaults to today and is overridable from the CLI without editing the config).
- `run` is the registry-backed worker loop. It reserves a batch of jobs over HTTP, matches each URL to the first `page_type` whose regex fires, drives the browser through that page_type's pagination strategy, and posts the page-1 HTML as the blob plus all discovered links.
- URLs that match no `page_type` are still fetched as plain detail pages — the browser loads them, the HTML is uploaded, no links or fields are extracted.
- Pagination strategies cover the four realistic shapes: `next_button`, `numbered_buttons` (total-count divided by per_page), `infinite_scroll` (stop after N idle scrolls), `url_param` (bump a query parameter until the server runs out). `none` is the terminal-page case.
- Structured field extraction supports five modes: `text`, `text_list`, `html` (outerHTML), `attribute` (named attr), and `rows` (table-row → column-name map). Selectors can be CSS (default) or XPath.
- Extracted fields are written as JSON sidecars to a local directory keyed by `sha256(url)` — they do not flow through the registry result payload. The registry only sees the HTML blob and the link list.
- The browser pool maintains N concurrent sessions; each job checks out a session, runs to completion, and returns it. The pool size matches `--concurrency` and consumes that many Selenium grid slots.
- A graceful shutdown on SIGINT/SIGTERM stops the reserve loop, drains in-flight jobs through the workers, and closes the browser pool.

## Design

### One YAML per site, ordered page_types

A `Config` is a flat document: `name`, `domain`, `scheme`, `crawl_delay_ms`, a `seed` block, and an ordered `page_types` array. Order matters — `MatchPageType` returns the first regex hit, so more specific patterns must be listed before catch-alls. {>> `pt.matchRE` is compiled in `Validate()` and cached on the struct so per-URL routing is one `MatchString` <<}

### Two seed shapes, opinionated defaults

`seed.type: urls` is the literal-list case. `seed.type: date_range` is the common archive-by-day case — the template must contain `{day}` (YYYY-MM-DD), and an empty/missing `to` resolves to today UTC. The CLI's `--to` flag overrides the config's `To` without requiring a file edit, so ops can freeze the upper bound for backfills.

### Direct-DB seed bypasses the registry HTTP

`seed` opens `rwdb` and calls `gormrepo.NewFrontierRepo` directly. {>> the worker is treated as trusted infrastructure for the seeding step <<} The same DB-driver/DSN flags as registry let the same binary seed any of sqlite/postgres/mysql. Idempotency comes from `Enqueue` returning `(ok, err)` where `ok=false` means dedup-by-URL-hash.

### A browser pool, not per-job sessions

`BrowserPool` is a fixed-size channel of `*Browser`. {>> pre-opened at startup so the ~1s session-create cost is amortized; checkout/return is a chan recv/send <<} Sessions deliberately leak state across jobs (cookies, sessionStorage) — for sites that need clean state, operators are expected to raise `--concurrency` to job count or extend the type to call `DeleteAllCookies`. The trade is per-job startup cost vs state isolation; the chosen default favors throughput.

### Pluggable pagination behind one function shape

`paginate(ctx, wd, listingURL, pg, onPage)` is the single entry. Each strategy is one function with the same signature; adding a new strategy is one switch arm plus one function. The page-1 HTML is the caller's responsibility — `paginate` only fires `onPage` for pages 2..N. This keeps the "post the blob" decision at the worker layer, separate from the "advance the listing" decision.

- `next_button`: click a selector; aria-disabled/disabled/`.disabled`/`IsEnabled()=false` all mean "done". Defense-in-depth disabled detection so flaky sites still terminate.
- `numbered_buttons`: read a total count (extracts the first run of digits — tolerates "iš 1234" / "1,234 rezultatų" copy), compute `extra = total/per_page`, then click templated buttons. `ButtonIndexFn` is a small registry of index-shaping functions (`linear` and `liteko`) — `liteko` encodes the scrape.ts 01..10 then 02..11 repeat pattern. {>> the indirection is the design escape hatch: new sites with weird button index logic add a function, not a strategy <<}
- `infinite_scroll`: scroll to bottom, sleep, count items by selector. N consecutive no-change rounds (`MaxIdleRounds`, default 2) ends the loop. Tolerant of slow-loading content because the idle counter resets on any change.
- `url_param`: pure URL rewriting — set/replace a query param and re-navigate. Cheaper than DOM driving when the site supports it. First failed GET is treated as "done" rather than an error.

### Five field modes, one extract pass

`extractFields` produces a `map[string]any` per page. Modes returning lists (`text_list`, `rows`) are the ones that accumulate across paginated pages; scalar modes (`text`, `html`, `attribute`) keep their first-page value. {>> `mergeFields` shallow-merges by type-switching on the existing value <<} A spec whose selector matches nothing is silently skipped — partial extraction is preferred over total failure.

### Selector dimorphism, one type underneath

`Selector{Selector, SelectorType}` is the underlying type. Where the field IS the selector (`LinkSpec`, `FieldSpec`), the YAML keys are flattened to top-level `selector:` + `selector_type:` for readability; where a selector is one of several siblings (`Pagination.TotalSelector`, etc.), the nested map form is used. The `Sel()` helpers reconcile the two shapes for consumers. {>> the `by()` helper maps `SelectorType` ∈ {"css", "xpath"} to the Selenium `By*` constants — adding e.g. `link_text` is one switch arm <<}

### Sidecar JSON, not registry payload

Structured fields are persisted to `--sidecar-dir/<sha256(url)>.json`, never sent to the registry. The registry's job-result contract is intentionally HTML-blob + discovered-links; the worker treats extracted fields as a side product for downstream pipelines to consume directly off disk. This keeps the registry contract narrow and lets the same registry serve unicrawler alongside non-extracting workers.

### Same three-queue protocol as every other worker

Reserves jobs via `POST /v1/jobs/reserve`, posts results as multipart `meta + blob` to `/v1/jobs/result`, posts failures to `/v1/jobs/fail`. See the system spec for the lease/HMAC mechanism. {>> Selenium hides HTTP status; the worker assumes 200 if `WD.Get` succeeded <<} The DTOs (`reserveResp`, `job`, `discoveredLink`, `resultMeta`) are local copies that mirror `internal/infra/http/jobs.go` — fully decoupled from registry internals so a wire-format break is a deliberate edit.

### Failure posture

Browser navigation errors and page-source failures post `fail` with `retryable=true` so the sweeper requeues. Pagination errors during a page_type are logged as warnings and the worker posts what it has — partial paginated output is more useful than nothing, and the next reserve naturally re-fetches if needed. Sidecar write errors are warnings only — the blob/links still post to the registry.

## Interactions

- **Registry HTTP (`/v1/jobs/reserve|result|fail`)** — standard PAT-authed worker protocol from the system spec.
- **Registry DB (seed path only)** — direct `rwdb` + `gormrepo.FrontierRepo` writes to upsert the domain row and enqueue seed URLs. Bypasses HTTP because seeding is an operator action, not a hot path.
- **Selenium WebDriver remote (`--webdriver`)** — required; the worker opens N sessions at startup and holds them for its lifetime. A typical setup is a docker-selenium grid alongside the worker.
- **Local filesystem (`--sidecar-dir`)** — created at startup, holds `<sha256(url)>.json` files. No automatic rotation or cleanup; sidecar consumers are expected to drain.
- **Per-domain politeness** — the seeded domain row carries `crawl_delay_ms` from the config; the registry's reserve filter enforces it like for any other worker class.

## Mapping

> [[cmd/unicrawler/main.go]]
> [[cmd/unicrawler/config.go]]
> [[cmd/unicrawler/seed.go]]
> [[cmd/unicrawler/worker.go]]
> [[cmd/unicrawler/browser.go]]
> [[cmd/unicrawler/paginate.go]]
> [[cmd/unicrawler/extract.go]]
> [[internal/infra/db/gormrepo/]]
> [[internal/infra/db/rwdb/]]
> [[internal/infra/urls/]]
> [[internal/domain/frontier/]]
