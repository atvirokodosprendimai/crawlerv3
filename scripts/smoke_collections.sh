#!/usr/bin/env bash
# Smoke for Phase 3: per-collection chunker config.
#
# 1. Boot a fresh sqlite registry (no server needed for this smoke).
# 2. Assert list-collections starts empty.
# 3. set-collection for a name 'tiny' with chunk_tokens=80, overlap_prev=10,
#    overlap_next=10. Re-list, assert the row.
# 4. Re-run set-collection touching only chunk_tokens; assert other fields
#    survived (no clobber).
# 5. delete-collection, re-list, assert empty again.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-smoke-collections.XXXXXX)"
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

echo "smoke: list-collections (expect empty)"
OUT="$("$WORK/registry" list-collections)"
echo "  $OUT"
if [[ "$OUT" != *"no rows"* ]]; then
  echo "FAIL: expected 'no rows' header on empty list, got: $OUT"; exit 1
fi

echo "smoke: set-collection name=tiny chunk_tokens=80 overlap_prev=10 overlap_next=10"
"$WORK/registry" set-collection --name tiny --chunk-tokens 80 --overlap-prev 10 --overlap-next 10

echo "smoke: list-collections (expect 1 row)"
LIST="$("$WORK/registry" list-collections)"
echo "$LIST"
if ! echo "$LIST" | grep -qE '^tiny\s+80\s+10\s+10\s+cl100k_base$'; then
  echo "FAIL: row not as expected"; exit 1
fi

echo "smoke: partial update — touch only chunk_tokens=160"
"$WORK/registry" set-collection --name tiny --chunk-tokens 160
LIST="$("$WORK/registry" list-collections)"
echo "$LIST"
if ! echo "$LIST" | grep -qE '^tiny\s+160\s+10\s+10\s+cl100k_base$'; then
  echo "FAIL: partial update clobbered something"; exit 1
fi

echo "smoke: change tokenizer string"
"$WORK/registry" set-collection --name tiny --tokenizer p50k_base
LIST="$("$WORK/registry" list-collections)"
echo "$LIST"
if ! echo "$LIST" | grep -qE '^tiny\s+160\s+10\s+10\s+p50k_base$'; then
  echo "FAIL: tokenizer not updated"; exit 1
fi

echo "smoke: delete-collection"
"$WORK/registry" delete-collection --name tiny
OUT="$("$WORK/registry" list-collections)"
echo "  $OUT"
if [[ "$OUT" != *"no rows"* ]]; then
  echo "FAIL: row not deleted"; exit 1
fi

echo
echo "smoke: OK"
