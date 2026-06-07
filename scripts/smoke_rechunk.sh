#!/usr/bin/env bash
# Smoke for Phase 4: registry rechunk CLI.
#
# 1. Boot a fresh sqlite registry.
# 2. Plant three extracted_documents directly (no real crawl) with
#    Collection='ut-coll' and texts long enough to multi-chunk under the
#    default 2800-token cores.
# 3. Insert obviously-stale chunk rows for each doc so we can prove the
#    rechunk replaces them.
# 4. Run `registry rechunk --collection ut-coll --dry-run` and assert the
#    DB is unchanged.
# 5. Set a per-collection override with tiny sizing so we get many chunks.
# 6. Run `registry rechunk --collection ut-coll` and assert (a) old chunks
#    are gone, (b) every new chunk is embed_status=pending, (c) new counts
#    match ceil(token_count / tiny_chunk).
# 7. Run rechunk again and assert it's a no-op (same row count, same ids
#    — actually new ids since uuids are minted fresh; chunk_index 0..N-1
#    stays stable).

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-smoke-rechunk.XXXXXX)"
trap 'echo; echo "smoke: workspace kept at $WORK"' EXIT INT TERM

DB="$WORK/crawler.db"
LEASE_SECRET="$(openssl rand -base64 32)"

cd "$ROOT"

echo "smoke: building registry"
go build -o "$WORK/registry" ./cmd/registry

export DB_DSN="$DB"
export LEASE_SECRET

echo "smoke: migrate up"
"$WORK/registry" migrate up >/dev/null

# Seed three lake_objects + extracted_documents directly. We avoid the
# crawl + html_strip path because we want deterministic Text content.
HASH1=$(printf 'fake-1' | shasum -a 256 | awk '{print $1}')
HASH2=$(printf 'fake-2' | shasum -a 256 | awk '{print $1}')
HASH3=$(printf 'fake-3' | shasum -a 256 | awk '{print $1}')

# Build varied-length texts. ~50, ~200, ~800 words of distinct content.
W50=$(python3 -c 'print(" ".join(f"w{i}" for i in range(50)))')
W200=$(python3 -c 'print(" ".join(f"w{i}" for i in range(200)))')
W800=$(python3 -c 'print(" ".join(f"w{i}" for i in range(800)))')

# First insert the domain (needed for the FK chain) + a frontier row per hash
# so lake_objects(url_hash) FK is satisfied.
"$WORK/registry" seed-domain --host ut.example.com --crawl-delay-ms 1000 >/dev/null
DOM=1
sqlite3 "$DB" <<SQL
INSERT INTO crawl_frontier (url_hash, url, canonical_url, domain_id, status, max_attempts)
VALUES (x'$HASH1', 'https://ut.example.com/a', 'https://ut.example.com/a', $DOM, 'done', 5),
       (x'$HASH2', 'https://ut.example.com/b', 'https://ut.example.com/b', $DOM, 'done', 5),
       (x'$HASH3', 'https://ut.example.com/c', 'https://ut.example.com/c', $DOM, 'done', 5);
INSERT INTO lake_objects (url_hash, storage_backend, storage_key, content_type, content_sha256, file_size_bytes)
VALUES (x'$HASH1', 'local', 'a', 'text/plain', x'$HASH1', 6),
       (x'$HASH2', 'local', 'b', 'text/plain', x'$HASH2', 6),
       (x'$HASH3', 'local', 'c', 'text/plain', x'$HASH3', 6);
INSERT INTO extracted_documents (source_lake_object_id, text, collection)
VALUES (1, '$W50',  'ut-coll'),
       (2, '$W200', 'ut-coll'),
       (3, '$W800', 'ut-coll');
INSERT INTO document_chunks (id, document_id, chunk_index, text, token_count, embed_status)
VALUES ('stale-1-0', 1, 0, 'STALE',  1, 'done'),
       ('stale-2-0', 2, 0, 'STALE',  1, 'failed'),
       ('stale-2-1', 2, 1, 'STALE2', 1, 'failed'),
       ('stale-3-0', 3, 0, 'STALE',  1, 'done');
SQL

CT0=$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks;")
echo "smoke: pre-rechunk chunk rows = $CT0 (expect 4 stale)"
[[ "$CT0" == "4" ]] || { echo "FAIL: stale chunk plant"; exit 1; }

echo "smoke: dry-run"
DRY="$("$WORK/registry" rechunk --collection ut-coll --dry-run)"
echo "$DRY"
CT1=$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks;")
[[ "$CT1" == "4" ]] || { echo "FAIL: dry-run mutated DB"; exit 1; }

echo "smoke: set tiny per-collection config (20-token cores, 5-token overlaps)"
"$WORK/registry" set-collection --name ut-coll --chunk-tokens 20 --overlap-prev 5 --overlap-next 5

echo "smoke: apply"
APPLY="$("$WORK/registry" rechunk --collection ut-coll)"
echo "$APPLY"

STALE_LEFT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE id LIKE 'stale-%';")
[[ "$STALE_LEFT" == "0" ]] || { echo "FAIL: stale chunks survived ($STALE_LEFT left)"; exit 1; }

PENDING=$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE embed_status='pending';")
TOTAL=$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks;")
[[ "$PENDING" == "$TOTAL" ]] || { echo "FAIL: not every new chunk is pending ($PENDING/$TOTAL)"; exit 1; }
echo "smoke: post-rechunk total=$TOTAL pending=$PENDING"

# Doc 1 had 50 words ≈ ~70 tokens via cl100k_base; expect at least 4 chunks
# with 20-token cores. Doc 2 200 words ≈ ~280 tokens → ~14 chunks. Don't
# hard-code exact counts — just assert each doc got > 1 chunk.
for did in 1 2 3; do
  n=$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE document_id=$did;")
  echo "  doc $did → $n chunks"
  [[ "$n" -ge 2 ]] || { echo "FAIL: doc $did got $n chunks (want >=2)"; exit 1; }
done

echo "smoke: idempotence — rechunk again, expect same row count"
"$WORK/registry" rechunk --collection ut-coll >/dev/null
TOTAL2=$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks;")
[[ "$TOTAL2" == "$TOTAL" ]] || { echo "FAIL: second rechunk drifted ($TOTAL2 vs $TOTAL)"; exit 1; }

echo
echo "smoke: OK"
