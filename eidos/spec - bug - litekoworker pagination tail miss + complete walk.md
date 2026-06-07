---
tldr: litekoworker undercounts cases — pagination loop computes extra pages via integer division and can stop short on exact-multiple totals; spec defines the correct complete-walk behavior
category: core
status: draft
---

# litekoworker pagination — complete walk

## Target

`cmd/litekoworker/run.go` — specifically `walkListing` and its `extraPages` computation. A listing job MUST emit every case URL the site exposes for its day window; today it can stop one or more pages short on some inputs.

Sibling spec: [[spec - cmd - litekoworker webforms postback paginator]] — the high-level shape stands; this spec narrows in on the pagination correctness claim that one already makes.

## Behaviour

### Desired (the claim the worker should uphold)

- Given a paieska.aspx listing whose first page reports `Total = N`, the worker emits exactly `N` distinct case URLs in `discovered_links`, deduplicated, and never more than `N`. {>> N is the ground truth advertised by the site itself; if N is wrong, that's a site-side issue logged separately <<}
- The number of HTTP requests issued for a listing is exactly `ceil(N / resultsPerPage)`: one GET for page 1 plus `ceil(N / resultsPerPage) - 1` POSTs.
- An exact-multiple total (e.g. `N = 50`, `N = 100`, `N = 500`) issues no extra POST beyond what is needed. `N = 50` issues zero POSTs; `N = 100` issues one POST; `N = 500` issues nine POSTs.
- A non-multiple total (`N = 51`, `N = 101`, `N = 499`) issues exactly the pages required to cover the tail. `N = 51` issues one POST that returns the single tail item; `N = 499` issues nine POSTs whose last page returns 49 items.
- Total parsing is robust to the site's actual number formatting: leading/trailing whitespace, thousands separators in any common shape (`.` / `,` / NBSP / regular space), and unicode digit forms. Any parse failure logs a warning that names the raw text, and the worker conservatively assumes "more than one page" rather than silently stopping at page 1.
- A listing whose Total appears legitimately unreachable on the site's own UI (e.g. the result-cap symptom: site claims 22M but per-day windows return ≤50k each) is recorded as a per-day-window observation, so the operator can re-seed with a finer window.

### Currently observed (the diagnosis)

- The aggregate count of case URLs ingested across the full 2005→today seed is ~130k while an independent prior scrape produced ~22M. That gap is ≈170×, far larger than any single off-by-one explains.
- The pagination loop bounds the extras by `extraPages := lp.Total / resultsPerPage` and iterates `i = 1..extraPages`. {>> `cmd/litekoworker/run.go` `walkListing` <<} For `Total = 50`, `extraPages = 1` and the worker issues one unnecessary POST that returns no new rows (deduped). For `Total = 100`, `extraPages = 2` and the same harmless extra POST happens. For `Total = 49`, `extraPages = 0` and the worker stops correctly.
- There is no value of `N` at which the current formula stops *before* reaching the last item — `N / resultsPerPage` always covers the tail. So the integer-division pattern, on its own, **cannot account for the 170× gap**. It is at most an extra-request inefficiency.
- The 170× gap therefore points to other failure modes that this spec scopes as Open Questions: a `Total` parse failure on a thousands-separator (worker silently treats `Total = 0` → no pagination), a block-2 navigation bug in `pageButton(i)` (the `ctl02` ID does not advance to page 11 without an intervening next-block click), or a per-day result cap on the site (each day truncates at some N regardless of how the worker paginates).

## Design

### One source of truth for "how many pages"
`ceilDiv(Total, resultsPerPage)` is the only correct page count. The implementation derives extras as `ceilDiv(Total, resultsPerPage) - 1` and iterates `i = 1..extras`. Integer-division-then-add-one is rejected because it loses the `Total = 0` case (negative extras), and integer-division-on-the-nose is rejected because it over-shoots on exact multiples by 1. {>> `extras := (Total + resultsPerPage - 1) / resultsPerPage - 1; if extras < 0 { extras = 0 }` <<}

### Robust Total parsing as a behaviour, not a performance optimisation
`parseListing` currently does `strconv.Atoi(strings.TrimSpace(text(span)))`. Liteko's `_TotalItemsLabel` renders the count in Lithuanian locale — observed forms in adjacent sites include `1.234`, `1 234`, `1 234`, and `1,234`. Any of these makes `Atoi` fail, which silently sets `lp.Total = 0`, which silently bypasses the entire pagination block. A parse helper strips every rune in `[., NBSP, regular space]` before `Atoi`, and a parse failure emits a `warn` with the raw text rather than degrading to zero.

### Conservative fallback when Total is unparseable
If parse still fails after stripping, the worker does not stop at page 1. Instead it walks pages 2..K where K is a sane upper bound and a stop-signal is "page returned zero new cases after dedup". This converts a silent zero-page bug into a verbose multi-page walk in the worst case — cheaper than the data loss. The default K mirrors the site's known result cap (today: assume 10 pages = 500 cases per day; configurable).

### Pagination is one in-process walk, not split across reserves
Unchanged from the parent spec — the VIEWSTATE chain is stateful and short-lived; the walk happens entirely inside `handleJob`. This spec does not propose persisting VIEWSTATE.

### Per-day window granularity is part of the seed contract, not the runner
If the site caps single-query results below typical daily volume, that is a *seed* concern, not a pagination concern. The pagination layer faithfully exhausts whatever the site exposes for one query; if the operator needs finer granularity, the seed layer slices by half-day, hour, or six-hour windows. A future `seed` flag `--window {day|6h|1h}` is the natural extension — out of scope for this spec but called out so the next change lands cleanly.

### `pageButton` correctness lives in its own spec
The `pageButton(i)` function for block-2-and-beyond navigation (whether RadDataPager really accepts `ctl02..ctl11` directly to jump from page 10 to page 11, or whether an intervening next-block click on `ctl11` is required) is its own correctness question and deserves its own spec + verification HTML fixture. Flagged as an Open Question below; not solved here.

## Verification

A passing implementation can be checked without hitting the live site:

1. **Unit on `extras`**: Table-driven test on the new `paginationExtras(total, perPage)` helper. Cases: `(0, 50) → 0`, `(1, 50) → 0`, `(49, 50) → 0`, `(50, 50) → 0`, `(51, 50) → 1`, `(100, 50) → 1`, `(101, 50) → 2`, `(499, 50) → 9`, `(500, 50) → 9`, `(501, 50) → 10`. {>> note that `(50, 50) → 0` and `(100, 50) → 1` are the cases the current code over-counts <<}
2. **Unit on Total parsing**: Add `parseTotal(s string) (int, error)` and table-test: `"49"`, `" 49 "`, `"1234"`, `"1.234"`, `"1,234"`, `"1 234"`, `"1 234"` all → `1234`. Empty / garbage → error.
3. **Integration via fixture HTML**: Save a real `paieska.aspx` page-1 body as `testdata/page1_total_*.html` for at least one exact-multiple case and one with thousands separator. Run `parseListing` over it; assert `Total` correct.
4. **Smoke**: Re-run the 2005-01-01 seed window against a real Liteko instance, capture the worker's per-job `links=` log line, assert `links == Total` for every job. Any job where `links < Total` is a bug.

## Interactions

- **`parseListing` (`cmd/litekoworker/listing.go`)** — gains a `parseTotal` helper; the span-text path goes through it.
- **`walkListing` (`cmd/litekoworker/run.go`)** — `extraPages` becomes `paginationExtras(lp.Total, resultsPerPage)`.
- **Frontier scope-lock + URL-hash dedup** — unchanged; the worker still emits the union of detail URLs and the frontier still dedups across days.
- **Operator workflow** — once shipped, a one-time `reprocess`-equivalent re-run of the seeded date range refills the missing cases (frontier dedup keeps the seed URLs from doubling up; new detail URLs just arrive).

## Friction / Open Questions

- **Magnitude mismatch**: 130k seen vs 22M expected is ~170×. The off-by-one alone is +1 per ≥10-page day, i.e. at most a few percent. So this spec, even when implemented, **will not by itself close the gap**. The likely larger contributors are the Total-parse failure mode (worker silently treats every multi-page day as a single page) and possibly the block-2 navigation question. Both deserve their own follow-up specs.
- **`pageButton(i)` for blocks ≥ 2 is unverified against a real DOM dump.** The current mapping `i=11..20 → "02".."11"` mirrors the TS reference but neither has a test fixture. A captured RadDataPager DOM at page 11 would settle this.
- **Per-day cap on Liteko is unmeasured.** The "Liteko silently truncates" claim in the parent spec is unsourced. Sampling: pick five days with known-high case counts (recent date ranges in busy courts), compare worker output to the site's own UI total. If days routinely cap at e.g. 1000, day-slicing is too coarse for those years.
- **Prior-scrape comparison method.** The 22M figure comes from an external dataset; not yet known whether that dataset itself crawled every day or whether it had a different sampling strategy. Worth confirming the apples-to-apples baseline before declaring victory.

## Mapping

> [[cmd/litekoworker/run.go]]
> [[cmd/litekoworker/listing.go]]
> [[cmd/litekoworker/listing_test.go]]
> [[cmd/litekoworker/seed.go]]
> [[eidos/spec - cmd - litekoworker webforms postback paginator.md]]
