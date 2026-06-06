#!/usr/bin/env bash
# Smoke test for slice 6 (external task workers).
#
# 1. Seed a fake PDF directly into the lake (no real PDF crawl needed).
# 2. registry reprocess --processor pdf_ocr --content-type-prefix application/pdf
# 3. Start registry + task-worker --kind pdf_ocr --extract-cmd "cat {input}"
#    (cat-as-OCR: the file's text-ish bytes become the "extracted_text".)
# 4. Assert extracted_documents row + processing_jobs.status='done' + chunks.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-tasksmoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"
BLOBS="$WORK/blobs"
ADDR="127.0.0.1:18090"
REGISTRY_URL="http://$ADDR"
LEASE_SECRET="$(openssl rand -base64 32)"

REGISTRY_PID=""
TASKWORKER_PID=""

cleanup() {
  [[ -n "$TASKWORKER_PID" ]] && kill "$TASKWORKER_PID" 2>/dev/null || true
  [[ -n "$REGISTRY_PID"  ]] && kill "$REGISTRY_PID"  2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "tasksmoke: workspace at $WORK"
}

cd "$ROOT"

go build -o "$WORK/registry"   ./cmd/registry
go build -o "$WORK/taskworker" ./cmd/taskworker

export DB_DSN="$DB"
export BLOBS_ROOT="$BLOBS"
export LEASE_SECRET

"$WORK/registry" migrate up

# Workers
TW_OUT="$("$WORK/registry" create-worker --label task-smoke)"
echo "$TW_OUT"
TW_PAT="$(printf '%s\n' "$TW_OUT" | awk -F= '/^pat=/{print $2}')"

# Seed a synthetic PDF-like lake_object so we don't depend on a real PDF crawl.
# We use a fake host so we can drop a frontier row first (lake_objects references frontier).
"$WORK/registry" seed-domain --host fakedocs.test --crawl-delay-ms 1
"$WORK/registry" enqueue --url https://fakedocs.test/manual.pdf

URL_HASH_HEX="$(python3 -c "import hashlib; print(hashlib.sha256(b'https://fakedocs.test/manual.pdf').hexdigest())")"
URL_HASH_RAW="$(python3 -c "import hashlib; import sys; sys.stdout.buffer.write(hashlib.sha256(b'https://fakedocs.test/manual.pdf').digest())" | xxd -p | tr -d '\n')"

# Create a fake PDF body (any bytes — we use plain text so cat-as-OCR returns it).
FAKE_BODY="This is the synthetic PDF body. Section 1. Section 2. Lorem ipsum dolor sit amet."
mkdir -p "$BLOBS/$( echo "$URL_HASH_HEX" | cut -c1-2 )"
KEY_PATH="$( echo "$URL_HASH_HEX" | cut -c1-2 )/${URL_HASH_HEX}.pdf"
echo -n "$FAKE_BODY" > "$BLOBS/$KEY_PATH"

# Insert the lake_objects row directly (registry has no manual-insert CLI yet).
# Mark frontier row as done so the FK is satisfied (it already is by enqueue).
SHA="$(python3 -c "import hashlib,sys; sys.stdout.write(hashlib.sha256(sys.argv[1].encode()).hexdigest())" "$FAKE_BODY")"
SIZE="${#FAKE_BODY}"
sqlite3 "$DB" "
UPDATE crawl_frontier SET status='done' WHERE hex(url_hash)='$(echo $URL_HASH_HEX | tr a-z A-Z)';
INSERT INTO lake_objects(url_hash, storage_backend, storage_key, content_type, content_sha256, file_size_bytes)
VALUES (X'${URL_HASH_RAW}', 'local', '${KEY_PATH}', 'application/pdf', X'${SHA}', ${SIZE});
"

LAKE_ID="$(sqlite3 "$DB" "SELECT id FROM lake_objects ORDER BY id DESC LIMIT 1;")"
echo "tasksmoke: lake_object_id=$LAKE_ID"

# Bulk enqueue a pdf_ocr task for the fake PDF.
"$WORK/registry" reprocess --processor pdf_ocr --content-type-prefix application/pdf

QUEUED="$(sqlite3 "$DB" "SELECT COUNT(*) FROM processing_jobs WHERE status='queued' AND processor='pdf_ocr';")"
echo "tasksmoke: queued pdf_ocr tasks = $QUEUED"
[[ "$QUEUED" -ge 1 ]] || { echo "tasksmoke: reprocess FAILED"; exit 1; }

# Start the registry.
"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "http://$ADDR/healthz" >/dev/null

# Start the task worker: 'cat {input}' acts as a fake OCR that returns the bytes.
"$WORK/taskworker" \
  --registry "$REGISTRY_URL" --pat "$TW_PAT" \
  --kind pdf_ocr --batch 4 --idle-sleep 1s --max-runtime 30s \
  --mode text --extract-cmd "cat {input}" \
  >"$WORK/taskworker.log" 2>&1 &
TASKWORKER_PID=$!

# Wait for completion.
for _ in $(seq 1 30); do
  DONE="$(sqlite3 "$DB" "SELECT COUNT(*) FROM processing_jobs WHERE status='done' AND processor='pdf_ocr';")"
  if [[ "$DONE" -ge 1 ]]; then
    echo "tasksmoke: done tasks = $DONE"
    break
  fi
  sleep 1
done

EXTRACTED="$(sqlite3 "$DB" "SELECT COUNT(*) FROM extracted_documents;")"
CHUNKS="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE document_id IN (SELECT id FROM extracted_documents);")"
echo "tasksmoke: extracted=$EXTRACTED chunks=$CHUNKS"

[[ "$DONE" -ge 1 && "$EXTRACTED" -ge 1 && "$CHUNKS" -ge 1 ]] || {
  echo "tasksmoke: FAILED"
  echo "--- registry.log ---"; cat "$WORK/registry.log" | tail -40
  echo "--- taskworker.log ---"; cat "$WORK/taskworker.log" | tail -40
  exit 1
}

echo "tasksmoke: OK"
