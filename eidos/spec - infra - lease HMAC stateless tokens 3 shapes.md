---
tldr: stateless HMAC-SHA256 lease tokens — one fixed wire layout, three subject shapes (crawl URL, chunk UUID, task ID), bytes also stored row-side for defense in depth
category: core
---

# lease tokens

## Target

`internal/infra/lease/` — the only package that mints and validates lease tokens for the three queue families described in the [system spec](spec%20-%20system%20-%20distributed%20crawler%20with%20capability-gated%20workers%20and%20pluggable%20infra.md)'s "Three queues, one shape" section. Every reserve handler in the registry calls into this package; every result/fail/heartbeat handler verifies through it.

## Behaviour

- A signed token proves three things at once without a DB lookup: which work item the bearer is allowed to act on, which worker was given it, and until when it remains valid. Verification is offline arithmetic on the token bytes plus the server's secret.
- One token layout serves all three queue families. A crawl lease, a chunk lease, and a task lease are byte-indistinguishable on the wire; only the subject the 32-byte hash slot commits to differs.
- A leaked or stolen token expires on its own. There is no "revoke token" call — operators rely on TTL expiry plus row-side checks to bound the blast radius.
- A token presented for the wrong subject fails verification even if the signature is intact: the caller must name the work item it claims to be acting on, and the hash slot is recomputed and compared in constant time.
- Verification refuses an expired token but still returns the parsed subject, worker, and expiry alongside the error. Callers can log who held a stale lease without re-parsing.
- The secret has a hard minimum length. Construction refuses to proceed with a weak secret rather than silently producing forgeable tokens.
- A caller that passes a wrong-sized subject hash gets a programmer-visible failure, not a truncated token that would later mis-verify.

## Design

This package implements the **HMAC stateless lease tokens** primitive named in the system spec. The decisions below are subordinate to that contract; this file is where it physically lives.

### Stateless by construction, stored anyway

The token carries everything needed to verify itself: subject hash, worker id, expiry, and a truncated HMAC over those three fields. No session table, no cache, no round-trip. The system spec calls this out as "defense in depth" — the **raw bytes are also persisted on the leased row** at issue time {>> `Sign` and the chunk/task variants all return `(string, []byte)` so callers can store the decoded form alongside the row <<}. A leaked HMAC secret therefore does not immediately enable forging leases against rows already in flight: the forger would also need to overwrite the stored bytes.

### One layout, three subjects

The wire format is fixed: `urlHash[32] || workerID[8] || expiresUnix[8] || mac[16]`, base64url-encoded. Three subject shapes plug into the same 32-byte hash slot:

- **crawl** — the slot holds the URL hash already used as the frontier row's primary key.
- **chunk** — the slot holds `sha256(chunkUUID)`.
- **task** — the slot holds `sha256("task:" || bigEndian(taskID))`.

The choice to hash everything down to 32 bytes — even when the natural identifier is an int64 — is what lets the three queue families share one signer, one verifier, one storage column type, and one set of operator tools {>> chunk_lease.go and task_lease.go are thin wrappers that hash then call `Sign` <<}. The system spec's "one shape across queues" rule applies to the lease primitive too.

### MAC truncation as a deliberate size budget

The HMAC-SHA256 output is truncated to 16 bytes before being appended. The full 32-byte tag would push the encoded token past common URL/header length comfort zones; 16 bytes still gives ~2^128 forgery work against a single secret, which exceeds any practical attack budget given the lease's seconds-to-minutes TTL.

### Symmetric verification per subject

Each subject shape has a paired `Verify…` that takes the *claimed* subject as an argument, recomputes its hash, and asserts equality against the slot returned by the generic verifier {>> `VerifyChunk(token, chunkUUID)` and `VerifyTask(token, taskID)`; comparison is constant-time <<}. This forces handlers to name the row they are acting on. A token that signs hash A cannot be replayed against a different row that happens to accept the same worker — the handler is structurally required to pass the row's identifier and the mismatch surfaces as an opaque error.

### Hard-fail on caller bugs

Two construction-time conditions panic or error out instead of degrading silently:

- **Secret too short** — `New` rejects secrets under 16 bytes with an error, refusing to construct a Signer at all. A weak secret produces weak tokens for the entire process lifetime, so the check belongs at the boundary {>> 16-byte minimum is the chosen threshold; the system spec doesn't pin a number <<}.
- **Subject hash wrong length** — the generic `Sign` panics if handed anything other than exactly 32 bytes. This is a programmer mistake (someone hashed with the wrong algorithm) and a silent truncation would yield tokens that verify successfully against the wrong subject.

Operator-facing errors (bad encoding, bad length, bad signature, expired, subject mismatch) all return opaque, low-entropy error messages so a hostile worker cannot tell *why* a token failed.

### No clock skew tolerance

Verification compares against `time.Now()` exactly. There is no grace window. The expectation is that workers heartbeat well before expiry and the sweeper's reclaim window (30s, per system spec) is the slack budget — extending tokens past their issued expiry would defeat the stateless property.

### Issue-side returns the raw bytes

Every `Sign…` returns both the encoded string (handed to the worker) and the raw decoded bytes (stored on the row). The raw form exists exclusively to satisfy the "token also stored = defense in depth" requirement; callers do not need it for verification.

## Interactions

- **Registry reserve handlers** call `Sign`, `SignChunk`, or `SignTask` after claiming rows; the string goes in the response, the raw bytes go into the queue row's lease-token column.
- **Registry result/fail/heartbeat handlers** call the matching `Verify…` with the row identifier from the request URL or body, then proceed only on a clean parse.
- **Sweeper** does not touch this package — it operates on the expiry timestamp already persisted in the row, not on the token. Stateless verification and row-side expiry are independent reclaim paths.
- **Secret rotation** is a process-level concern outside this package. Rotating the secret invalidates every outstanding lease immediately — workers receive verification failures and re-reserve. There is no key-id or multi-secret negotiation here by design; the queues are short-lived enough to absorb a full invalidation.
- **Cross-queue contract** — because the three subject shapes share a layout, the same Signer instance services crawl, chunk, and task leases. Process wiring constructs exactly one Signer per registry.

## Mapping

> [[internal/infra/lease/lease.go]]
> [[internal/infra/lease/chunk_lease.go]]
> [[internal/infra/lease/task_lease.go]]
