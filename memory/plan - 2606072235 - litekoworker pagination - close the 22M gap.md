---
title: litekoworker pagination — close the 22M gap
status: active
created: 2026-06-07 22:35
branch: task/eidos-spec-liteko-pagination
spec: [[spec - bug - litekoworker pagination tail miss + complete walk]]
parent_spec: [[spec - cmd - litekoworker webforms postback paginator]]
---

## Context

Observed: ~130k Liteko case URLs ingested vs ~22M from an independent prior scrape — ≈170× gap.

Spec [[spec - bug - litekoworker pagination tail miss + complete walk]] documents the diagnosis and lists four candidate failure modes:

1. **Off-by-one** in `extraPages := Total / resultsPerPage` — over-paginates on exact multiples, never undershoots. Cannot explain the gap on its own. **Low impact, easy fix.**
2. **`Total` parse failure on locale-formatted strings** — `Atoi("1.234")` etc. fails silently → `Total=0` → no pagination → only 50 results per day. **Highest-impact suspect.**
3. **`pageButton` block-2 navigation unverified** — `ctl02..ctl11` after `ctl10` may not advance past page 10 without a next-block click. If broken, every >10-page day truncates at 500. **Needs DOM evidence to confirm.**
4. **Per-day result cap on Liteko itself** — site may silently truncate single-query results below daily volume. **Out of worker's control; mitigated by finer seed windows.**

This plan closes all four. Phase 1 surfaces ground truth from real HTML — cheap, settles which fixes matter — so later phases are evidence-driven, not speculation-driven.

## Goal

After this plan, a re-seed of the full 2005→today window produces a case-URL count within 10% of the 22M baseline (or surfaces a quantified shortfall attributable to a specific cause — e.g. "site itself caps at X per query, finer windows recover Y%").

---

## Phase 1 — Capture ground truth (no code changes) — [completed]

**Goal:** end the phase knowing *exactly* what's broken. Cheap evidence collection.

1. [x] Fetch a real `paieska.aspx` page-1 body for days of varying size.
   - => 7 daily probes saved under `cmd/litekoworker/testdata/probe_<date>.html` (range 143–1016 cases)
   - => plus two aggregates: 2024 = 67,430; 2005–2025 = **2,235,113**
   - => **baseline was off by 10×**: site total is 2.2M, not 22M
   - => Total label always plain ASCII integers — **Total-parse hypothesis falsified**
2. [x] On the high-volume day, drive the pager manually (via `curl` reproducing `__doPostBack`) up to page 11 and capture the response body.
   - => standalone Go driver `/tmp/drive_liteko_pager.go` replayed VIEWSTATE chain through pages 1→13 on 2015-04-20 (Total=1016)
   - => block-2 transition verified: `POST ctl10` from block 1 lands on page 11 (offset 501), pager re-renders with ctl00..ctl11, then `POST ctl02` lands on page 12 (offset 551)
   - => **`pageButton` hypothesis falsified — current code is correct**
3. [ ] Run a one-off SQL probe against the operator's prod registry DB: per-day case count, distribution.
   - => deferred — local `crawler.db` has only 9g.lt + vvtat.lrv.lt data, no Liteko rows
   - => operator must run the probe on the box where the 130k figure was observed
4. [x] Write evidence note `memory/note - 2606072235 - liteko ground truth.md`. Done.

**End-of-phase outcome:** *all three code-level hypotheses falsified by direct evidence.* The remaining gap (130k ingested vs ~2.2M reachable) must be operational — frontier never drained, wrong worker class reserving the listings, depth/scope filter, or per-job failures. The deferred SQL probe is now the only cheap way to settle it.

## Phase 2 — Fix the cheap & certain (off-by-one + Total parse)

**Goal:** ship the fixes that are correct regardless of phase 1 findings. The Total-parse fix is high-impact whether or not phase 1 fully confirms it (locale stripping is a no-regression change).

1. [ ] `/eidos:push spec - bug - litekoworker pagination tail miss + complete walk` — Stage 1 only: the `paginationExtras` helper + the `parseTotal` helper.
   - Implementation: `cmd/litekoworker/listing.go` gains `parseTotal(s string) (int, error)` that strips `.`, `,`, NBSP (U+00A0), and regular whitespace before `Atoi`. `parseListing` routes the `_TotalItemsLabel` text through it. On parse failure, log `warn` with the raw bytes (hex-encoded if non-ASCII) and leave `Total = 0` *but set a sentinel* `lp.TotalUnknown = true` for phase 2-action-3.
   - Implementation: `walkListing` derives `extras := paginationExtras(lp.Total, resultsPerPage)` where `paginationExtras(total, perPage int) int` is `(total + perPage - 1) / perPage - 1`, clamped to ≥0.
   - => add `cmd/litekoworker/listing_test.go` table tests for both helpers using the formats observed in phase 1.
2. [ ] Add fixture-based test: load `testdata/page1_<volume>.html` from phase 1 into `parseListing`, assert `Total` matches the value the site rendered.
3. [ ] Implement the unknown-Total fallback: if `lp.TotalUnknown`, `walkListing` paginates pages 2..K with `K = 11` (covers one full RadDataPager block) and stops early when a page yields zero new case URLs after dedup. Log `warn` with the page count walked.
   - => converts silent zero-pagination into verbose multi-page walk in the worst case.
4. [ ] Re-run `go test ./cmd/litekoworker/...`. All green.
5. [ ] Smoke: pick the highest-volume day from phase 1, run the worker against a fresh registry DB scoped to that single day, assert `links == Total` in the result log line.

**End-of-phase visible result:** the worker correctly counts pages on every day with ≤10 result pages. If phase 1 attributed the bulk of the gap to Total-parse, the next re-seed should jump from ~130k toward ~1–2M.

## Phase 3 — `pageButton` block-2 correctness (evidence-driven)

**Goal:** resolve the ctl-suffix question with the DOM evidence captured in phase 1.

1. [ ] Read the page-11 fixture. Identify the actual ctl-suffix sequence Liteko uses for blocks ≥ 2. Three possibilities:
   - a) `pageButton(11) = "02"` is correct — site re-maps button positions within each block. Current code is right.
   - b) Code needs to click a separate next-block arrow (e.g. `ctl11`) once before clicking `ctl02..ctl10` of the new block.
   - c) Some other pattern only visible in the DOM.
2. [ ] **If a):** add a fixture-based test that proves it (POST with current `pageButton(11)`, assert response body's pager-current label says "Page 11"). Mark this question resolved in the spec. Skip remaining actions in this phase.
3. [ ] **If b) or c):** call `/eidos:spec` for a new spec `spec - bug - litekoworker pagebutton block navigation` that captures the corrected pager protocol, then `/eidos:push` it.
   - Implementation: rewrite `pageButton` and the `walkListing` POST loop to issue the next-block arrow click at block boundaries (positions 10, 20, 30, …). VIEWSTATE chaining unchanged.
   - Add `listing_test.go` cases anchored to the fixtures.
4. [ ] Smoke: pick a known >10-page day, run the worker, assert `links == Total`.

**End-of-phase visible result:** worker correctly walks days that span multiple RadDataPager blocks. If phase 1 attributed gap to block navigation, the next re-seed should close the remaining order-of-magnitude gap.

## Phase 4 — Per-day cap & seed window granularity

**Goal:** quantify whether Liteko itself caps single-query results below daily volume; if so, ship a `seed --window` flag.

1. [ ] Sample five recent (2023–2026) high-volume days against the live UI. For each, compare:
   - The `_TotalItemsLabel` value (what the worker sees).
   - The court's published case count for that day (if available).
   - The site's own UI total when slicing the same day into 4×6h windows: do the four 6h totals sum to >day total? If yes → daily query truncates.
2. [ ] If truncation is confirmed at the day level:
   - [ ] Call `/eidos:spec` for `spec - cmd - litekoworker seed window granularity`.
   - [ ] Call `/eidos:push` to add a `--window {day|6h|1h}` flag to `runSeed`. Implementation: extend `dayURL` to a `windowURL(from, to time.Time) string` and the iteration loop to advance by the window size. Idempotence still rides on the frontier URL-hash unique index.
   - [ ] Re-seed the 2023–2026 range at `--window 6h` to validate.
3. [ ] If truncation is NOT confirmed: document the negative result in the plan log. Skip the flag.

**End-of-phase visible result:** either a new flag is shipped and the high-volume years' totals close to expected, or a documented negative finding that rules out the hypothesis.

## Phase 5 — Validate against the 22M baseline

**Goal:** end-to-end re-run + reconciliation.

1. [ ] Wipe the frontier + lake (ephemeral DB or new DSN — never destructively against the prod DB without operator approval). Re-seed the full 2005→today window at the appropriate `--window` per phase 4.
2. [ ] Let the worker drain the frontier. Track progress with `registry queue-stats` and case-URL count over time.
3. [ ] Once done, compare aggregate case count to the 22M baseline. Acceptance: within 10%, OR a documented attribution of the shortfall to a known cause (site cap, baseline scrape included non-judicial pages, etc.).
4. [ ] Update [[spec - bug - litekoworker pagination tail miss + complete walk]] — mark Open Questions resolved, fold the post-mortem into Friction section.
5. [ ] Update [[spec - cmd - litekoworker webforms postback paginator]] Behaviour bullets to reflect the corrected complete-walk claim.

**End-of-phase visible result:** the gap is closed, or precisely accounted for. Both specs reflect reality.

---

## Verification

The plan is done when:

- `go test ./cmd/litekoworker/...` passes including new fixture-based tests.
- A re-seeded 2005→today crawl produces a case-URL count within 10% of the 22M baseline (or has a documented attribution of the shortfall).
- The bug spec's Open Questions are all closed.
- The parent litekoworker spec reflects the actual corrected behaviour.

## Adjustments

(none yet)

## Progress log

- **2026-06-07 22:35** — plan created from [[spec - bug - litekoworker pagination tail miss + complete walk]]. On branch `task/eidos-spec-liteko-pagination`. Awaiting user start signal.
- **2026-06-07 22:50** — Phase 1 executed (evidence-only, no code changes). All three code-level hypotheses (Total parse, off-by-one tail miss, pageButton block-2 navigation) **falsified** by direct evidence — see [[note - 2606072235 - liteko ground truth]]. Site baseline corrected from 22M to **2.235M**. Phases 2–4 no longer load-bearing for closing the gap; they remain correct fixes (defense-in-depth) but no longer urgent. The 130k-vs-2.2M gap is now attributed to **operational causes** — requires operator's prod DB to settle. Awaiting decision on plan re-scoping.
