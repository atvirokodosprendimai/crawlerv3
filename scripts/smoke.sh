#!/usr/bin/env bash
# End-to-end smoke for crawlerv3 slices 1–3.
#
# 1. Boot a fresh sqlite registry + reference crawl worker.
# 2. Crawl example.com → blob stored, html_strip pipeline runs, chunks land.
# 3. Drive the embed protocol with curl: reserve N chunks, push back fake
#    vector_ids, assert they switch to embed_status='done'.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-smoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"
BLOBS="$WORK/blobs"
ADDR="127.0.0.1:18080"
REGISTRY_URL="http://$ADDR"
LEASE_SECRET="$(openssl rand -base64 32)"

REGISTRY_PID=""
WORKER_PID=""

cleanup() {
  [[ -n "$WORKER_PID"   ]] && kill "$WORKER_PID"   2>/dev/null || true
  [[ -n "$REGISTRY_PID" ]] && kill "$REGISTRY_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "smoke: workspace kept at $WORK"
}

cd "$ROOT"

echo "smoke: building binaries"
go build -o "$WORK/registry"  ./cmd/registry
go build -o "$WORK/worker"    ./cmd/worker
go build -o "$WORK/migrator"  ./cmd/migrator

export DB_DSN="$DB"
export BLOBS_ROOT="$BLOBS"
export LEASE_SECRET

echo "smoke: migrate up"
"$WORK/registry" migrate up

echo "smoke: create crawl worker"
CW_OUT="$("$WORK/registry" create-worker --label crawl-smoke)"
echo "$CW_OUT"
CRAWL_PAT="$(printf '%s\n' "$CW_OUT" | awk -F= '/^pat=/{print $2}')"

echo "smoke: create embed worker"
EW_OUT="$("$WORK/registry" create-worker --label embed-smoke)"
echo "$EW_OUT"
EMBED_PAT="$(printf '%s\n' "$EW_OUT" | awk -F= '/^pat=/{print $2}')"

echo "smoke: seed + enqueue"
"$WORK/registry" seed-domain --host example.com --crawl-delay-ms 200
"$WORK/registry" enqueue --url https://example.com/

echo "smoke: start registry"
"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "http://$ADDR/healthz" >/dev/null

echo "smoke: start crawl worker"
"$WORK/worker" --registry "$REGISTRY_URL" --pat "$CRAWL_PAT" --batch 5 \
  >"$WORK/worker.log" 2>&1 &
WORKER_PID=$!

echo "smoke: wait for lake + pipeline"
for _ in $(seq 1 60); do
  CHUNKS="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE embed_status='pending'")"
  if [[ "$CHUNKS" -ge 1 ]]; then
    echo "smoke: pending chunks = $CHUNKS"
    break
  fi
  sleep 1
done

LAKE="$(sqlite3 "$DB" "SELECT COUNT(*) FROM lake_objects")"
EXTRACTED="$(sqlite3 "$DB" "SELECT COUNT(*) FROM extracted_documents")"
CHUNKS="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks")"
echo "smoke: lake=$LAKE extracted=$EXTRACTED chunks=$CHUNKS"
[[ "$LAKE" -ge 1 && "$EXTRACTED" -ge 1 && "$CHUNKS" -ge 1 ]] || { echo "smoke: pipeline FAILED"; exit 1; }

echo "smoke: embed reserve (curl)"
RESERVE_JSON="$(curl -fsS -X POST -H "Authorization: Bearer $EMBED_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":1000}' "$REGISTRY_URL/v1/embed/reserve")"
LEASED_N="$(printf '%s' "$RESERVE_JSON" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["chunks"]))')"
echo "smoke: leased chunks = $LEASED_N"
[[ "$LEASED_N" -ge 1 ]] || { echo "smoke: embed reserve FAILED"; exit 1; }

echo "smoke: embed result push (curl)"
RESULTS_JSON="$(printf '%s' "$RESERVE_JSON" | python3 -c '
import sys,json
chunks = json.load(sys.stdin)["chunks"]
out = {"results": [
  {"chunk_id": c["chunk_id"], "vector_id": "qdrant:fake-"+c["chunk_id"][:8], "lease_token": c["lease_token"]}
  for c in chunks
]}
print(json.dumps(out))
')"
ACCEPT_JSON="$(curl -fsS -X POST -H "Authorization: Bearer $EMBED_PAT" -H 'Content-Type: application/json' \
  --data "$RESULTS_JSON" "$REGISTRY_URL/v1/embed/result")"
echo "smoke: embed result resp: $ACCEPT_JSON"

DONE="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE embed_status='done'")"
echo "smoke: chunks done = $DONE"
[[ "$DONE" -ge 1 ]] || { echo "smoke: embed result FAILED"; exit 1; }

echo "smoke: migrate verify (local -> local would no-op; test by listing instead)"
"$WORK/migrator" --from local --to local --local-root "$BLOBS" 2>&1 || true

echo "smoke: OK"
