---
tldr: deterministic URL canonical form + sha256 hash that drives frontier dedup and lake keying across all enqueue paths
category: core
---

# url canonicalization

## Target

The shared URL normalization adapter used by every enqueue path: discovered-link enqueue inside the app service, operator `enqueue` CLI in the registry, and the seed-file ingestion in the per-site workers (`unicrawler`, `litekoworker`). One function in, one canonical string + one fixed-width hash out.

## Behaviour

- Any URL passed through canonicalization comes out as a stable string such that two URLs that "mean the same fetch" produce the same canonical form, and the same canonical form always produces the same hash.
- Surface differences that do not change what the server returns are erased: host case, fragment, default port for the scheme, ordering of query parameters, ordering of repeated values within a key.
- Differences that *can* change what the server returns are preserved: scheme, userinfo, non-default port, path case, path-component encoding, presence/absence of trailing slash on non-empty paths, and query keys/values themselves.
- An input with no path comes out with `/` as the path — a bare host and a bare host with `/` are the same row.
- An input with no query comes out with no query — canonicalization never invents a `?`.
- Whitespace surrounding the raw input is tolerated and stripped before parsing.
- Malformed input does not panic and does not produce a misleading canonical form; the caller gets an error and is expected to drop the link.
- The hash is fixed-width raw bytes (sha256), suitable for direct use as a primary/unique key column without encoding.
- The function is pure: no I/O, no clock, no network, no `<base href>` resolution. Relative refs MUST be resolved by the caller before canonicalization — workers do that during HTML parsing, the registry does it never.

## Design

The whole module is one tiny adapter sitting in `internal/infra/urls/`. It is consciously placed in `infra/` rather than `domain/` because it speaks RFC-3986 mechanics — a domain concept ("the same fetch") realized through a specific external standard. Domain code references it as a port-shaped utility (a pure function), so swapping it for a stricter normalizer later is a one-package change.

The normalization rule set is intentionally minimal: only erase what *RFC 3986 §6.2.2.1 / 6.2.2.3 / 6.2.3* call "syntax-based" and "scheme-based" equivalences. Aggressive transforms that risk changing meaning are deliberately NOT performed:

- No percent-decoding of path segments — `%2F` and `/` are not the same byte to a server.
- No path-segment collapsing (`/./`, `/../`) — already done by `net/url` parsing, no additional work.
- No `www.` stripping — many sites serve different content at apex vs `www`.
- No trailing-slash normalization on non-empty paths — `/foo` and `/foo/` are routinely distinct.
- No tracking-parameter stripping (`utm_*`, `fbclid`, etc.) — that is a policy decision belonging to a future layer, not a syntactic one.

This minimal-rules stance is the design decision: "false dedup" (treating two distinct resources as one) is strictly worse than "missed dedup" (refetching the same resource), because false dedup silently loses data while missed dedup only costs a request.

Query canonicalization sorts keys and, within a key, sorts repeated values {>> avoids any dependence on the order Go's `url.Values` map happens to walk}. Re-encoding via `url.QueryEscape` means the canonical form's query is also a normalized escaping — `+` vs `%20` etc. collapse.

Default ports are scheme-aware: `:80` only strips under `http`, `:443` only under `https`. {>> Suffix match on `u.Host` rather than parsing port out — keeps userinfo / IPv6-bracketed forms untouched if they ever appear.}

The hash function is a separate exported helper, not folded into `Canonical`, so callers that already hold a canonical string (e.g. when re-deriving a hash for a stored row) skip the parse. Raw `[]byte` return — no hex, no base64 — so the column stores 32 bytes flat and indexes/looksup by `=` are byte-exact.

The package owns no state, no logger, no config. It is safe to call concurrently from any number of goroutines.

## Interactions

- **`crawl_frontier.url_hash`** — primary dedup key. Every enqueue path (`Service.enqueueDiscovered`, registry `enqueue` CLI, worker seed scripts) feeds the URL through `Canonical` then `Hash` and writes the result. Inserts conflicting on `url_hash` are how dedup is enforced at the DB layer.
- **`Service.enqueueDiscovered`** — also re-parses the canonical form to extract the host for the scope-lock domain lookup. Canonicalization happens *before* the `domains` check, so host-case differences don't cause the lookup to miss.
- **Operator `enqueue` CLI** — canonicalizes the operator-supplied URL the same way as discovered links, so manual seeds and crawler discoveries collide on the same `url_hash` instead of creating duplicate frontier rows.
- **Per-site worker seed files** (`cmd/unicrawler/seed.go`, `cmd/litekoworker/seed.go`) — same path, ensuring seed-file URLs dedupe against frontier rows the registry may already hold.
- **Workers (link extraction)** — responsible for `<base href>` resolution and producing absolute URLs *before* posting results back. Canonicalization assumes its input is already absolute.
- **`<base href>` and relative refs** — explicitly out of scope. The Godoc states this; misuse would silently produce host-less canonical forms that fail the downstream domain lookup.

## Mapping

> [[internal/infra/urls/canonical.go]]
> [[internal/app/service.go]]
> [[cmd/registry/main.go]]
> [[cmd/unicrawler/seed.go]]
> [[cmd/litekoworker/seed.go]]
> [[internal/domain/frontier/job.go]]
