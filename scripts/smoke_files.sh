#!/usr/bin/env bash
# Smoke for slice 9:
#  1. text_passthrough internal processor handles text/csv (and text/plain etc.)
#  2. office_to_pdf trigger enqueues a processing_jobs row for an XLSX blob
#  3. per-domain embed_collection surfaces in /v1/embed/reserve responses

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-filesmoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"; BLOBS="$WORK/blobs"; ADDR="127.0.0.1:18130"; URL="http://$ADDR"
LEASE_SECRET="$(openssl rand -base64 32)"
REGISTRY_PID=""

cleanup() {
  [[ -n "$REGISTRY_PID" ]] && kill "$REGISTRY_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "filesmoke: $WORK"
}

cd "$ROOT"
go build -o "$WORK/registry" ./cmd/registry

export DB_DSN="$DB" BLOBS_ROOT="$BLOBS" LEASE_SECRET
"$WORK/registry" migrate up >/dev/null

# Verify new default triggers seeded by 0006
echo "filesmoke: trigger-list (defaults after 0006)"
"$WORK/registry" trigger-list | head -25

# Confirm office_to_pdf and text_passthrough triggers exist; legacy docx_to_pdf gone.
sqlite3 "$DB" "SELECT enqueue_kind, COUNT(*) FROM pipeline_triggers GROUP BY enqueue_kind ORDER BY enqueue_kind;"

# Workers
CRAWL_OUT="$("$WORK/registry" create-worker --label crawl --capabilities crawl --max-concurrent 4)"
CRAWL_PAT="$(printf '%s' "$CRAWL_OUT" | awk -F= '/^pat=/{print $2}')"
EMBED_OUT="$("$WORK/registry" create-worker --label embed --capabilities embed --max-concurrent 8)"
EMBED_PAT="$(printf '%s' "$EMBED_OUT" | awk -F= '/^pat=/{print $2}')"

# Two domains with different embed_collection settings
"$WORK/registry" seed-domain --host site-a.test --crawl-delay-ms 50
"$WORK/registry" seed-domain --host site-b.test --crawl-delay-ms 50
"$WORK/registry" update-domain --host site-a.test --embed-collection news_lt
"$WORK/registry" update-domain --host site-b.test --embed-collection news_eu

echo "filesmoke: list-domains after update"
"$WORK/registry" list-domains

"$WORK/registry" enqueue --url https://site-a.test/data.csv
"$WORK/registry" enqueue --url https://site-b.test/sheet.xlsx

"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "$URL/healthz" >/dev/null

# Plant fake blobs directly (skip the actual HTTP fetch — we want to test
# pipeline routing, not the fetcher).
plant_blob() {
  local URL="$1" CT="$2" BODY="$3"
  local HASHHEX="$(python3 -c "import hashlib; print(hashlib.sha256(b'$URL').hexdigest())")"
  local HASHRAW="$(python3 -c "import hashlib,sys; sys.stdout.buffer.write(hashlib.sha256(b'$URL').digest())" | xxd -p | tr -d '\n')"
  local KEY="$(echo "$HASHHEX" | cut -c1-2)/$HASHHEX.bin"
  mkdir -p "$BLOBS/$(echo "$HASHHEX" | cut -c1-2)"
  printf '%s' "$BODY" > "$BLOBS/$KEY"
  local SHA="$(printf '%s' "$BODY" | shasum -a 256 | cut -d' ' -f1)"
  local SIZE="${#BODY}"
  sqlite3 "$DB" "
UPDATE crawl_frontier SET status='done' WHERE hex(url_hash)='$(echo $HASHHEX | tr a-z A-Z)';
INSERT INTO lake_objects(url_hash, storage_backend, storage_key, content_type, content_sha256, file_size_bytes)
VALUES (X'${HASHRAW}', 'local', '${KEY}', '${CT}', X'${SHA}', ${SIZE});
"
  sqlite3 "$DB" "SELECT id FROM lake_objects WHERE storage_key='${KEY}';"
}

CSV_BODY="name,age,city
alice,30,vilnius
bob,25,riga
carol,40,tallinn"
CSV_LAKE_ID=$(plant_blob "https://site-a.test/data.csv" "text/csv" "$CSV_BODY")
echo "filesmoke: CSV planted as lake_object_id=$CSV_LAKE_ID"

XLSX_BODY="\x50\x4b\x03\x04 fake xlsx bytes"  # not a real xlsx, just bytes — we only care about routing
XLSX_LAKE_ID=$(plant_blob "https://site-b.test/sheet.xlsx" "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" "$XLSX_BODY")
echo "filesmoke: XLSX planted as lake_object_id=$XLSX_LAKE_ID"

# Fire the triggers by simulating a "result accepted" — easiest: directly insert
# processing_jobs row via the same path the dispatcher would take. We instead
# manually invoke the dispatcher through the existing reprocess CLI:
"$WORK/registry" reprocess --processor text_passthrough --content-type-prefix text/csv
"$WORK/registry" reprocess --processor office_to_pdf --content-type-prefix application/vnd.openxmlformats-officedocument.spreadsheetml

# Wait for pipeline goroutine to drain text_passthrough (internal).
for _ in $(seq 1 20); do
  DONE_TP="$(sqlite3 "$DB" "SELECT COUNT(*) FROM processing_jobs WHERE processor='text_passthrough' AND status='done';")"
  if [[ "$DONE_TP" -ge 1 ]]; then
    break
  fi
  sleep 1
done

# Assertions
SKIPPED_O2P="$(sqlite3 "$DB" "SELECT COUNT(*) FROM processing_jobs WHERE processor='office_to_pdf';")"
echo "filesmoke: office_to_pdf rows queued (await external taskworker) = $SKIPPED_O2P (want >=1)"
[[ "$SKIPPED_O2P" -ge 1 ]] || { echo "filesmoke: office_to_pdf not enqueued"; exit 1; }

DONE_TP="$(sqlite3 "$DB" "SELECT COUNT(*) FROM processing_jobs WHERE processor='text_passthrough' AND status='done';")"
echo "filesmoke: text_passthrough done = $DONE_TP (want >=1)"
[[ "$DONE_TP" -ge 1 ]] || { echo "filesmoke: text_passthrough did not run"; exit 1; }

EXT="$(sqlite3 "$DB" "SELECT id, collection, substr(text,1,40) FROM extracted_documents;")"
echo "filesmoke: extracted_documents -> $EXT"

CSV_COL="$(sqlite3 "$DB" "SELECT collection FROM extracted_documents WHERE source_lake_object_id=$CSV_LAKE_ID;")"
echo "filesmoke: CSV doc.collection = '$CSV_COL' (want 'news_lt')"
[[ "$CSV_COL" == "news_lt" ]] || { echo "filesmoke: per-domain collection not set"; exit 1; }

# Embed reserve must surface the collection
RESERVE="$(curl -s -X POST -H "Authorization: Bearer $EMBED_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":1000}' "$URL/v1/embed/reserve")"
echo "filesmoke: embed reserve response head"
echo "$RESERVE" | python3 -c '
import sys, json
d = json.load(sys.stdin)
for c in d["chunks"]:
    print("  chunk_id={} doc={} collection={}".format(c["chunk_id"][:8], c["document_id"], c["collection"]))
print("count=", len(d["chunks"]))
'
COL_FROM_RESERVE="$(printf '%s' "$RESERVE" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["chunks"][0]["collection"])')"
echo "filesmoke: embed reserve [0].collection = '$COL_FROM_RESERVE' (want 'news_lt')"
[[ "$COL_FROM_RESERVE" == "news_lt" ]] || { echo "filesmoke: embed reserve missing collection"; exit 1; }

echo "filesmoke: OK"
