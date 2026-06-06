#!/usr/bin/env bash
# Smoke for slice 8 — data-lake read endpoints + pipeline triggers.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-lakesmoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"; BLOBS="$WORK/blobs"; ADDR="127.0.0.1:18110"; URL="http://$ADDR"
LEASE_SECRET="$(openssl rand -base64 32)"
REGISTRY_PID=""; CRAWL_PID=""

cleanup() {
  [[ -n "$CRAWL_PID"   ]] && kill "$CRAWL_PID"   2>/dev/null || true
  [[ -n "$REGISTRY_PID" ]] && kill "$REGISTRY_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "lakesmoke: $WORK"
}

cd "$ROOT"
go build -o "$WORK/registry" ./cmd/registry
go build -o "$WORK/worker"   ./cmd/worker

export DB_DSN="$DB" BLOBS_ROOT="$BLOBS" LEASE_SECRET
"$WORK/registry" migrate up >/dev/null

# Verify default triggers seeded
echo "lakesmoke: trigger-list (defaults)"
"$WORK/registry" trigger-list

# Create a custom trigger — fire any extra processor for HTML
"$WORK/registry" trigger-add --on lake_object_inserted --content-type text/html --enqueue custom_html_indexer
echo "lakesmoke: after adding custom trigger"
"$WORK/registry" trigger-list

# Disable the legacy html_strip trigger to prove disable works
HS_ID="$(sqlite3 "$DB" "SELECT id FROM pipeline_triggers WHERE enqueue_kind='html_strip' LIMIT 1;")"
"$WORK/registry" trigger-disable --id "$HS_ID"

# Then re-enable to keep html_strip happening (we want the chain to complete)
"$WORK/registry" trigger-enable --id "$HS_ID"

# Workers: one crawl, one sink reader
CW_OUT="$("$WORK/registry" create-worker --label crawl-lake --capabilities crawl --max-concurrent 4)"
CRAWL_PAT="$(printf '%s' "$CW_OUT" | awk -F= '/^pat=/{print $2}')"

SINK_OUT="$("$WORK/registry" create-worker --label sink-lake --capabilities lake_read,extracted_read,chunks_read --max-concurrent 4)"
SINK_PAT="$(printf '%s' "$SINK_OUT" | awk -F= '/^pat=/{print $2}')"

LEGACY_OUT="$("$WORK/registry" create-worker --label legacy-lake --max-concurrent 4)"
LEGACY_PAT="$(printf '%s' "$LEGACY_OUT" | awk -F= '/^pat=/{print $2}')"

"$WORK/registry" seed-domain --host example.com --crawl-delay-ms 200
"$WORK/registry" enqueue --url https://example.com/

"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "$URL/healthz" >/dev/null

"$WORK/worker" --registry "$URL" --pat "$CRAWL_PAT" --batch 5 >"$WORK/crawl.log" 2>&1 &
CRAWL_PID=$!

# Wait for pipeline to land chunks
for _ in $(seq 1 30); do
  N="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks;")"
  [[ "$N" -ge 1 ]] && break
  sleep 1
done
[[ "$N" -ge 1 ]] || { echo "lakesmoke: pipeline did not land chunks"; exit 1; }

# Verify custom trigger fired: processing_jobs has custom_html_indexer row.
CUSTOM_COUNT="$(sqlite3 "$DB" "SELECT COUNT(*) FROM processing_jobs WHERE processor='custom_html_indexer';")"
echo "lakesmoke: custom trigger fired = $CUSTOM_COUNT (want 1)"
[[ "$CUSTOM_COUNT" -ge 1 ]] || { echo "lakesmoke: custom trigger FAILED to fire"; exit 1; }

# ----- Read endpoints from sink-lake worker -----
LAKE_JSON="$(curl -fsS -H "Authorization: Bearer $SINK_PAT" "$URL/v1/lake?since_id=0&limit=50")"
LAKE_N="$(printf '%s' "$LAKE_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')"
echo "lakesmoke: /v1/lake count = $LAKE_N"
[[ "$LAKE_N" -ge 1 ]] || { echo "lakesmoke: /v1/lake empty"; exit 1; }

EXT_JSON="$(curl -fsS -H "Authorization: Bearer $SINK_PAT" "$URL/v1/extracted?since_id=0&limit=50")"
EXT_N="$(printf '%s' "$EXT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')"
echo "lakesmoke: /v1/extracted count = $EXT_N"
[[ "$EXT_N" -ge 1 ]] || { echo "lakesmoke: /v1/extracted empty"; exit 1; }

EXT_ID="$(printf '%s' "$EXT_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["items"][0]["id"])')"
TEXT_BODY="$(curl -fsS -H "Authorization: Bearer $SINK_PAT" "$URL/v1/extracted/$EXT_ID/text")"
echo "lakesmoke: /v1/extracted/$EXT_ID/text first chars: ${TEXT_BODY:0:60}..."
[[ -n "$TEXT_BODY" ]] || { echo "lakesmoke: extracted text empty"; exit 1; }

CHUNK_JSON="$(curl -fsS -H "Authorization: Bearer $SINK_PAT" "$URL/v1/chunks?since=0&limit=50")"
CHUNK_N="$(printf '%s' "$CHUNK_JSON" | python3 -c 'import sys,json; print(json.load(sys.stdin)["count"])')"
echo "lakesmoke: /v1/chunks count = $CHUNK_N"
[[ "$CHUNK_N" -ge 1 ]] || { echo "lakesmoke: /v1/chunks empty"; exit 1; }

# ----- Capability denials -----
CODE_DENY="$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $CRAWL_PAT" "$URL/v1/lake?since_id=0&limit=10")"
echo "lakesmoke: crawl-only worker hits /v1/lake -> $CODE_DENY (want 403)"
[[ "$CODE_DENY" == "403" ]] || { echo "lakesmoke: lake_read should be denied to crawl-only"; exit 1; }

# Legacy worker (no caps) should be allowed any read endpoint (backward compat)
CODE_LEGACY="$(curl -s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $LEGACY_PAT" "$URL/v1/lake?since_id=0&limit=10")"
echo "lakesmoke: legacy (no caps) hits /v1/lake -> $CODE_LEGACY (want 200)"
[[ "$CODE_LEGACY" == "200" ]] || { echo "lakesmoke: legacy worker should be allowed"; exit 1; }

echo "lakesmoke: OK"
