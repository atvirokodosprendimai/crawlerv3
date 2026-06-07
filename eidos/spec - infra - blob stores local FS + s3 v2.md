---
tldr: Two interchangeable BlobStore adapters — local filesystem under a root dir and AWS-SDK-v2 S3 with MinIO-friendly toggles — both verify SHA-256 in-stream on Put
category: core
---

# blob stores

## Target

The two adapters under `internal/infra/store/` that satisfy the `lake.BlobStore` port: `local` (filesystem) and `s3` (AWS S3 / S3-compatible). They are the only places in the codebase that touch raw bytes; everything else — lake rows, migration, deduplication — flows through this port.

## Behaviour

- Either adapter can stand in for the other at runtime; the registry chooses one at startup and the rest of the system never knows which it got. `Backend()` returns the string the lake rows record so a later migration is auditable.
- Put is content-addressable on the caller's terms: if the caller passes a SHA-256, the store rejects the upload when the bytes hash to something else. If the caller passes nothing, the store still computes the digest and hands it back in the `Stat`.
- Put is atomic-or-nothing from the reader's perspective. A half-written blob is never observable under its final key, and a failed Put leaves no garbage behind that a future Get could trip over.
- Get returns a streaming body the caller must close. Stat returns the same metadata shape without paying for the body.
- Delete is idempotent enough to be safe in retry loops — re-deleting an already-gone key is not a hard error on S3, and on local FS the OS-level `ErrNotExist` is the contract the caller already expects.
- The S3 adapter works against AWS, MinIO, and Cloudflare R2 from the same struct — only `Config` toggles change. Credentials may be supplied inline or left to whatever the AWS default chain finds (env, profile, IMDS, container role).
- The local adapter requires nothing at the OS level beyond a writable root directory; the directory layout under that root is whatever the caller's key encodes (slashes in the key become subdirectories), so sharding is a key-naming decision, not a store decision.

## Design

### One port, two adapters, zero leakage
The `lake.BlobStore` interface is deliberately tiny — `Put / Get / Stat / Delete / Backend` — so a third backend (GCS, Azure, IPFS) is an afternoon's work. Each adapter lives in its own package so dependency weight is paid only where used; importing `internal/infra/store/s3` drags in the AWS SDK, importing `local` does not.

### SHA-256 is computed at the store, not at the caller
Both adapters tee the incoming reader through `crypto/sha256` as the bytes pass through {>> `io.TeeReader(r, h)` in both Put paths <<}. Two reasons:
- The caller never has to choose between "stream once and trust the bytes" vs. "buffer to hash, then stream again". The store does it in a single pass.
- If the caller *does* pass an expected digest in `PutMeta.SHA256`, the store enforces it with a constant-time compare {>> `equalBytes` is a hand-rolled `subtle.ConstantTimeCompare` analogue <<} before publishing the blob. Mismatch → no blob written, error returned. This is the corruption fence.

### Local: temp-file + rename for atomicity
The local adapter writes to `.put-*` in the destination's parent directory, then `os.Rename` to the final key once the hash is verified. Same filesystem, so rename is atomic on POSIX. If the copy or the hash check fails, the temp file is removed and no partial blob is ever visible under the real key. Concurrent Puts to the same key race on rename — last writer wins, which matches the "blobs are content-addressed and immutable per SHA" assumption upstream.

### Local: directory layout is caller-controlled
The adapter does `filepath.Join(root, filepath.FromSlash(key))`. Sharding (e.g. `ab/cd/abcdef...`) is encoded in the key by the lake layer, not invented by the store. The store only ensures parent directories exist {>> `os.MkdirAll(filepath.Dir(dst), 0o755)` before each Put <<}. This keeps the store dumb and makes the key the single source of truth for "where on disk".

### S3: buffer-once, not stream
The S3 adapter buffers the full body into memory before calling `PutObject` {>> `bytes.NewBuffer` + `io.Copy` + `bytes.NewReader` into the SDK call <<}. The rationale is the SDK's non-multipart `PutObject` requires a seekable body to compute Content-MD5 and retry, and the crawler's blobs are bounded (HTML pages, PDFs, transcripts — not multi-GB archives). When the working set outgrows RAM, the right move is to add a multipart-upload code path, not to fight the SDK.

### S3: one struct, three deployments
A single `Config` covers AWS, MinIO, and any other S3-compatible service:
- `Endpoint` overrides the SDK's resolver {>> `o.BaseEndpoint = aws.String(cfg.Endpoint)` <<} for self-hosted services.
- `UsePathStyle` flips between `bucket.host/key` (virtual-hosted) and `host/bucket/key` (path-style) — MinIO and many on-prem services only speak path-style.
- `Region` is forwarded as-is; some endpoints accept anything (`us-east-1` is a common MinIO placeholder).

### S3: credentials are inline-or-chain, never half-set
The adapter only installs a static credentials provider when *both* `AccessKeyID` and `SecretAccessKey` are non-empty. Anything else (one set, neither set) falls through to `LoadDefaultConfig`, which walks the SDK's standard chain — env vars, shared profile, IMDS, ECS task role, EKS pod identity. This lets the same binary run unconfigured on an EC2 instance with an IAM role *and* fully configured against MinIO with a static key, without a branch in the operator's mental model.

### Failure semantics align with the system's "fail-fast on integrations" rule
A Put that fails for any reason (sha mismatch, network error, disk full, S3 4xx/5xx) returns the error to the caller without partial success. Upstream — the lake / pipeline accept handlers — treats this as "do not mark the row done", per the system spec's defense-in-depth rule, so the lease expires and another worker retries cleanly.

### Delete tolerates "already gone"
S3 `DeleteObject` against a missing key returns `NoSuchKey`, which the adapter swallows {>> `errors.As(err, &nsk)` for `*s3types.NoSuchKey` <<} so callers can retry deletes without bookkeeping. Local's `os.Remove` propagates `ErrNotExist`, which is the Go-idiomatic shape the rest of the codebase already handles with `errors.Is(err, fs.ErrNotExist)`.

### What is deliberately NOT here
- No presigning, no ACLs, no server-side encryption knobs, no object tagging. The adapters are bytes-in / bytes-out. Anything richer (signed URLs for browser downloads, lifecycle rules, KMS) is configured out-of-band on the bucket.
- No caching, no LRU, no in-memory dedupe. Content addressing is the dedupe; the lake's row uniqueness on `sha256` is the cache.
- No metrics or tracing inside the adapter. The port is small enough that the caller can wrap it with a decorator if observability is needed.

## Interactions

- **Domain port** — `internal/domain/lake/store.go` defines `BlobStore`. Both adapters satisfy it; no other code in `internal/infra/store/` is allowed to depend on the SDK or `os` directly.
- **Lake rows** — every blob written gets a `storage_backend` column matching `Backend()`. The migrator worker reads from one adapter and writes to another, then updates `migrated_from` on the row for audit.
- **Pipeline accept paths** — `blob_produced` triggers route artifacts through whichever store the registry started with. The store choice is invisible to the trigger.
- **CLI wiring** — `--blobs-root` selects `local`; `--s3-bucket`, `--s3-endpoint`, `--s3-use-path-style`, `--s3-region`, `--s3-access-key-id`, `--s3-secret-access-key` select `s3`. Mutually exclusive at startup.
- **Sweeper / retry** — a failed Put surfaces as a queue-result error, the row stays leased until the TTL, the sweeper requeues. No special-case retry logic inside the store.

## Mapping

> [[internal/infra/store/local/local.go]]
> [[internal/infra/store/s3/s3.go]]
> [[internal/domain/lake/store.go]]
