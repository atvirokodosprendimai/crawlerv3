---
title: liteko ground truth — phase 1 evidence
created: 2026-06-07 22:50
plan: [[plan - 2606072235 - litekoworker pagination - close the 22M gap]]
spec: [[spec - bug - litekoworker pagination tail miss + complete walk]]
---

## TL;DR

The 22M baseline was wrong by ~10×. Liteko's own `_TotalItemsLabel` for the full 2005–2025 window is **2,235,113** cases. The Total-parse hypothesis is **falsified**: even at 7-digit totals the label renders as plain integers with no thousands separator. The remaining gap (130k ingested vs ~2.2M reachable) still demands a fix, but the magnitude is one order smaller than first feared.

## Evidence captured

### Sampled day-1 page bodies

Saved under `cmd/litekoworker/testdata/probe_<date>.html`. Raw `_TotalItemsLabel` span text reported exactly as the site renders it.

| Date | Total | Format | Page count (ceil) |
|---|---|---|---|
| 2008-10-15 | `143` | plain int | 3 |
| 2024-03-15 | `198` | plain int | 4 |
| 2023-11-15 | `339` | plain int | 7 |
| 2024-03-12 | `368` | plain int | 8 |
| 2022-06-08 | `418` | plain int | 9 |
| 2020-01-15 | `480` | plain int | 10 |
| 2015-04-20 | `1016` | plain int | 21 — crosses RadDataPager block boundary |
| 2024 all   | `67430` | plain int | — |
| **2005–2025 all** | **`2235113`** | plain int | — |

Every total — including the 7-digit aggregate — is a contiguous run of ASCII digits. No `.`, no `,`, no NBSP. `strconv.Atoi` parses every observed sample successfully.

### Page-1 pager structure (any day)

The pager exposes `ctl00$ContentPlaceHolder1$listRez$RadDataPager1$ctl00$ctlNN` for `NN ∈ {00..10}` — eleven buttons. Each appears as a `__doPostBack` href:

```
ctl00$ContentPlaceHolder1$listRez$RadDataPager1$ctl00$ctl00
ctl00$ContentPlaceHolder1$listRez$RadDataPager1$ctl00$ctl01
…
ctl00$ContentPlaceHolder1$listRez$RadDataPager1$ctl00$ctl10
```

Current code maps `pageButton(i)` for `i=1..10 → "01".."10"`. So POST `i=1` targets `ctl01`. **Whether `ctl01` is page 2 or page 1 has not yet been verified** by driving a postback — it requires reproducing the VIEWSTATE + form-encode round-trip. Deferred to action 2 (page-11 capture).

## What this changes for the plan

### Falsified
- **Total-parse failure on locale separators.** Liteko doesn't use them, even at 7-digit values. The `parseTotal` helper still has marginal value as defense-in-depth, but it is no longer the high-impact suspect.

### Resized
- **Baseline gap is ~17×, not ~170×.** Local SQL probe still needed to confirm, but 130k ingested vs 2.2M site-reachable is the working figure.
- **Average per-day volume is ~200–500 across 2008–2024.** With 5,800 seeded days that projects to ~1.2–2.9M case URLs in the wild. Close enough to 2.2M that perfect pagination should close the gap.

### Also falsified (after driving the postback chain — see "Live postback evidence" below)
- **`pageButton` block-2 correctness.** Empirically verified by replaying the VIEWSTATE chain on 2015-04-20 (`Total=1016`). The code is correct. `POST ctl10` from block-1 lands on page 11 and re-renders the pager with ctl00..ctl11 (12 buttons, block 2 visible). The subsequent `POST ctl02` in block 2 lands on page 12 (offset 551/1016) — exactly what the current code expects.

### Unchanged
- **Off-by-one in `extraPages` over-paginates by one POST on exact multiples.** Harmless inefficiency; not a tail miss. Still worth tightening per spec.

## Live postback evidence (2015-04-20, Total=1016)

A standalone Go driver replayed the VIEWSTATE chain through the first 13 pages. Each row is one `__doPostBack`. `CurrentPageLabel` turned out to be the **1-based item offset**, not the page number; every advance increments by exactly 50, confirming each POST navigates one page.

| Step | Target ctl | CurrentPageLabel | Pager-button count | Notes |
|---|---|---|---|---|
| initial GET | — | 1 | 11 (ctl00–ctl10) | block 1 visible, page 1 active |
| `pageButton(1)=01` | ctl01 | 51 | 11 | page 2 |
| `pageButton(2)=02` | ctl02 | 101 | 11 | page 3 |
| `pageButton(9)=09` | ctl09 | 451 | 11 | page 10 |
| `pageButton(10)=10` | ctl10 | 501 | **12** (ctl00–ctl11) | **page 11, block 2 opens, pager grows a next-block arrow** |
| `pageButton(11)=02` | ctl02 | 551 | 12 | **page 12** — matches code's expectation |
| `pageButton(12)=03` | ctl03 | 601 | 12 | page 13 |

The driver source is at `/tmp/drive_liteko_pager.go` (sample-only, not part of the repo); the page-11 and page-12 bodies are saved under `cmd/litekoworker/testdata/probe_2015-04-20_after_10.html` and `_after_02.html`.

### New question raised — now the *only* live hypothesis
- **130k ingested implies ~22 cases/day average — far below the 200+ seen on every sampled day, and below even the 50-per-day "stuck at page 1" ceiling.** All three code-level hypotheses are now falsified by direct evidence. That leaves operational causes:
  - many seeded jobs are still queued/leased/failed (the worker simply hasn't drained the frontier yet);
  - the litekoworker PAT was issued but never run, and a generic `worker` bin reserved the `paieska.aspx` URLs instead — that bin doesn't know WebForms pagination, so it would only ever capture page 1 (gives 50/day, still higher than observed);
  - a different upstream filter (scope-lock, depth cap, content-type policy) is dropping discovered detail URLs before they reach the lake;
  - the worker has been crashing or returning `retryable=true` failures, and the frontier shows the affected day URLs back as `queued` with elevated `attempt_count`.
- The Phase-1 SQL probe (deferred — no Liteko data in local DB) is now the only cheap way to settle this. The Phase-2 and Phase-3 code fixes are no longer load-bearing for closing the gap; they remain correct but the spec body's Friction section should be updated to reflect the new picture.

## Open actions surfaced by this note

1. Drive a postback to page 11 on the 2015-04-20 fixture (`Total=1016`). Capture body. Inspect what `ctl02` in the block-2 pager actually navigates to. This is the remaining Phase-1 action 2.
2. Run the deferred Phase-1 SQL probe against the operator's prod DB:
   - Total seeded days vs days with `status='done'` results.
   - Per-day result count distribution: how many days hit exactly 50 (page-1 ceiling)? How many <50 (failed mid-walk)? How many >50 (paginated)?
   - Whether the litekoworker PAT has been used at all, or whether the reference `worker` has been reserving liteko URLs (and only ever fetching page 1).
3. Confirm with operator: was the 22M figure a typo for 2.2M, or is the prior scrape counting something larger than the public search index (e.g. each case + every linked document)?

## Files

- `cmd/litekoworker/testdata/probe_2008-10-15.html` — 143 cases
- `cmd/litekoworker/testdata/probe_2015-04-20.html` — 1016 cases (block-boundary stress)
- `cmd/litekoworker/testdata/probe_2020-01-15.html` — 480 cases (exact 10 pages)
- `cmd/litekoworker/testdata/probe_2022-06-08.html` — 418 cases
- `cmd/litekoworker/testdata/probe_2023-11-15.html` — 339 cases
- `cmd/litekoworker/testdata/probe_2024-03-12.html` — 368 cases
- `cmd/litekoworker/testdata/probe_2024-03-15.html` — 198 cases (first probe)
- `/tmp/all_time.html` — 2005-2025 aggregate, 2,235,113 cases
- `/tmp/y2024.html` — 2024 aggregate, 67,430 cases
