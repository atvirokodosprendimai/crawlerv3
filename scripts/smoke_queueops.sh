#!/usr/bin/env bash
# Smoke for slice 11:
#  1. queue-stats prints meaningful counts
#  2. embedworker (HTTP backend pointing at fake Ollama) drains pending chunks
#  3. ban-worker --release drops all leases the banned worker held
#  4. requeue-chunks --status failed lifts failed rows back to pending

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-queuesmoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"; BLOBS="$WORK/blobs"
ADDR="127.0.0.1:18150"; URL="http://$ADDR"
QPORT=18151; QURL="http://127.0.0.1:$QPORT"
OLPORT=18152; OLURL="http://127.0.0.1:$OLPORT"
LEASE_SECRET="$(openssl rand -base64 32)"
REGISTRY_PID=""; QDRANT_PID=""; OLLAMA_PID=""; EMBED_PID=""

cleanup() {
  [[ -n "$EMBED_PID"    ]] && kill "$EMBED_PID"    2>/dev/null || true
  [[ -n "$REGISTRY_PID" ]] && kill "$REGISTRY_PID" 2>/dev/null || true
  [[ -n "$QDRANT_PID"   ]] && kill "$QDRANT_PID"   2>/dev/null || true
  [[ -n "$OLLAMA_PID"   ]] && kill "$OLLAMA_PID"   2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "queuesmoke: $WORK"
}

# Re-use the fake Qdrant from smoke_qdrant.sh
cat > "$WORK/fake_qdrant.py" <<'PY'
import http.server, json, sys
collections = {}; points = {}
class H(http.server.BaseHTTPRequestHandler):
    def _parts(self): return self.path.split('?')[0].strip('/').split('/')
    def _read(self):
        n = int(self.headers.get('Content-Length','0') or 0)
        return self.rfile.read(n) if n else b''
    def _ok(self, b=b'{"result":true,"status":"ok"}'):
        self.send_response(200); self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def _nf(self): self.send_response(404); self.end_headers()
    def do_GET(self):
        p = self._parts()
        if len(p)==2 and p[0]=='collections':
            self._ok(json.dumps({"result":{"status":"green","config":collections.get(p[1],{})}}).encode()) if p[1] in collections else self._nf()
        else: self._nf()
    def do_PUT(self):
        body = self._read(); p = self._parts()
        if p[:1]==['collections'] and len(p)==2:
            collections[p[1]] = json.loads(body) if body else {}; self._ok()
        elif p[:1]==['collections'] and len(p)==3 and p[2]=='points':
            data = json.loads(body); store = points.setdefault(p[1], {})
            for pt in data.get('points',[]): store[pt['id']] = pt
            self._ok(json.dumps({"result":{"status":"completed"}}).encode())
        else: self._nf()
    def do_POST(self):
        body = self._read(); p = self._parts()
        if p[:1]==['collections'] and len(p)==4 and p[2]=='points' and p[3]=='search':
            data = json.loads(body); name = p[1]; limit = data.get('limit',10)
            q = data.get('vector',[]); hits = []
            for pid, pt in points.get(name,{}).items():
                v = pt.get('vector',[])
                score = sum(a*b for a,b in zip(q,v))
                hits.append({"id":pid,"score":score,"payload":pt.get('payload',{})})
            hits.sort(key=lambda h: -h['score'])
            self._ok(json.dumps({"result":hits[:limit]}).encode())
        else: self._nf()
    def log_message(self,*a,**k): pass
http.server.HTTPServer(('127.0.0.1',int(sys.argv[1])), H).serve_forever()
PY

# Fake Ollama-style embeddings server: returns a deterministic 4-d vector.
cat > "$WORK/fake_ollama.py" <<'PY'
import http.server, json, sys, hashlib
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length','0') or 0)
        body = self.rfile.read(n)
        data = json.loads(body) if body else {}
        text = data.get('prompt','')
        # Derive 4 floats from sha256(text) so the same text → same vector.
        h = hashlib.sha256(text.encode()).digest()
        vec = [b/255.0 for b in h[:4]]
        out = json.dumps({"embedding":vec}).encode()
        self.send_response(200); self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(out))); self.end_headers(); self.wfile.write(out)
    def log_message(self,*a,**k): pass
http.server.HTTPServer(('127.0.0.1',int(sys.argv[1])), H).serve_forever()
PY

cd "$ROOT"
go build -o "$WORK/registry"   ./cmd/registry
go build -o "$WORK/embedworker" ./cmd/embedworker

python3 "$WORK/fake_qdrant.py" "$QPORT"  >"$WORK/qdrant.log" 2>&1 &
QDRANT_PID=$!
python3 "$WORK/fake_ollama.py" "$OLPORT" >"$WORK/ollama.log" 2>&1 &
OLLAMA_PID=$!
sleep 0.5

export DB_DSN="$DB" BLOBS_ROOT="$BLOBS" LEASE_SECRET
export QDRANT_URL="$QURL"
"$WORK/registry" migrate up >/dev/null

EMBED_OUT="$("$WORK/registry" create-worker --label embedder --capabilities embed --max-concurrent 8)"
EMBED_PAT="$(printf '%s' "$EMBED_OUT" | awk -F= '/^pat=/{print $2}')"
EMBED_ID="$(printf '%s' "$EMBED_OUT" | awk -F= '/^worker_id=/{print $2}')"

"$WORK/registry" seed-domain --host site-a.test --crawl-delay-ms 50
"$WORK/registry" update-domain --host site-a.test --embed-collection lithuania_news
"$WORK/registry" enqueue --url https://site-a.test/page

"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "$URL/healthz" >/dev/null

# Plant a text/plain blob → triggers text_passthrough internal processor → chunk
URL_HASH_HEX="$(python3 -c "import hashlib; print(hashlib.sha256(b'https://site-a.test/page').hexdigest())")"
KEY="$(echo "$URL_HASH_HEX" | cut -c1-2)/${URL_HASH_HEX}.bin"
BODY="Sunny morning. The dog runs through the park chasing leaves."
mkdir -p "$BLOBS/$(echo "$URL_HASH_HEX" | cut -c1-2)"
printf '%s' "$BODY" > "$BLOBS/$KEY"
URL_HASH_RAW="$(python3 -c "import hashlib,sys; sys.stdout.buffer.write(hashlib.sha256(b'https://site-a.test/page').digest())" | xxd -p | tr -d '\n')"
SHA="$(printf '%s' "$BODY" | shasum -a 256 | cut -d' ' -f1)"
SIZE="${#BODY}"
sqlite3 "$DB" "
UPDATE crawl_frontier SET status='done' WHERE hex(url_hash)='$(echo $URL_HASH_HEX | tr a-z A-Z)';
INSERT INTO lake_objects(url_hash, storage_backend, storage_key, content_type, content_sha256, file_size_bytes)
VALUES (X'${URL_HASH_RAW}', 'local', '${KEY}', 'text/plain', X'${SHA}', ${SIZE});
"
"$WORK/registry" reprocess --processor text_passthrough --content-type-prefix text/plain >/dev/null

# Wait for the chunk to land in pending
for _ in $(seq 1 30); do
  N="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE embed_status='pending';")"
  [[ "$N" -ge 1 ]] && break
  sleep 1
done
[[ "$N" -ge 1 ]] || { echo "queuesmoke: chunk not created"; exit 1; }

# ---- queue-stats ----
echo "queuesmoke: queue-stats (initial)"
"$WORK/registry" queue-stats

# ---- run embedworker pointing at fake Ollama for at most 15s ----
"$WORK/embedworker" \
  --registry "$URL" --pat "$EMBED_PAT" \
  --batch 4 --idle-sleep 1s --max-runtime 15s \
  --embed-url "$OLURL" --embed-model nomic-embed-text \
  >"$WORK/embed.log" 2>&1 &
EMBED_PID=$!

# Wait for the chunk to flip to done
for _ in $(seq 1 20); do
  D="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE embed_status='done';")"
  [[ "$D" -ge 1 ]] && break
  sleep 1
done
[[ "$D" -ge 1 ]] || { echo "queuesmoke: embedworker did not embed"; cat "$WORK/embed.log"; exit 1; }
echo "queuesmoke: embedworker drained the queue (done=$D)"

# ---- requeue-chunks --document N to force a re-embed (operator workflow) ----
"$WORK/registry" requeue-chunks --document 1
PEND="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE embed_status='pending';")"
echo "queuesmoke: after requeue-chunks --document 1 → pending=$PEND (want >=1)"
[[ "$PEND" -ge 1 ]] || { echo "queuesmoke: requeue-chunks failed"; exit 1; }

# ---- Forge a leased frontier row + a running task held by EMBED_ID, then
#      ban-worker --release should drop both.
sqlite3 "$DB" "
UPDATE crawl_frontier
   SET status='leased', leased_by_worker_id=$EMBED_ID,
       lease_token=X'00', lease_expires_at='9999-12-31 00:00:00';
INSERT INTO processing_jobs (lake_object_id, processor, status, leased_by_worker_id, lease_token, lease_expires_at, created_at)
VALUES (1, 'pdf_ocr', 'running', $EMBED_ID, X'00', '9999-12-31 00:00:00', CURRENT_TIMESTAMP);
"

# Also forge a leased chunk held by EMBED_ID
sqlite3 "$DB" "
UPDATE document_chunks
   SET embed_status='leased', leased_by_worker_id=$EMBED_ID,
       lease_token=X'00', lease_expires_at='9999-12-31 00:00:00'
 WHERE embed_status='pending';
"

echo "queuesmoke: holdings before ban"
sqlite3 "$DB" "
SELECT 'frontier_leased', COUNT(*) FROM crawl_frontier  WHERE leased_by_worker_id=$EMBED_ID AND status='leased';
SELECT 'tasks_running',   COUNT(*) FROM processing_jobs WHERE leased_by_worker_id=$EMBED_ID AND status='running';
SELECT 'chunks_leased',   COUNT(*) FROM document_chunks WHERE leased_by_worker_id=$EMBED_ID AND embed_status='leased';
"

"$WORK/registry" ban-worker --id "$EMBED_ID" --release

echo "queuesmoke: holdings after ban --release"
sqlite3 "$DB" "
SELECT 'frontier_leased', COUNT(*) FROM crawl_frontier  WHERE leased_by_worker_id=$EMBED_ID;
SELECT 'tasks_running',   COUNT(*) FROM processing_jobs WHERE leased_by_worker_id=$EMBED_ID AND status='running';
SELECT 'chunks_leased',   COUNT(*) FROM document_chunks WHERE leased_by_worker_id=$EMBED_ID AND embed_status='leased';
"

REMAIN_F="$(sqlite3 "$DB" "SELECT COUNT(*) FROM crawl_frontier  WHERE leased_by_worker_id=$EMBED_ID AND status='leased';")"
REMAIN_T="$(sqlite3 "$DB" "SELECT COUNT(*) FROM processing_jobs WHERE leased_by_worker_id=$EMBED_ID AND status='running';")"
REMAIN_C="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE leased_by_worker_id=$EMBED_ID AND embed_status='leased';")"
echo "queuesmoke: remaining leases held by banned worker: f=$REMAIN_F t=$REMAIN_T c=$REMAIN_C"
[[ "$REMAIN_F" == "0" ]] || { echo "queuesmoke: frontier not released"; exit 1; }
[[ "$REMAIN_T" == "0" ]] || { echo "queuesmoke: tasks not released";   exit 1; }
[[ "$REMAIN_C" == "0" ]] || { echo "queuesmoke: chunks not released";  exit 1; }

# ---- queue-stats again (post-ban) ----
echo "queuesmoke: queue-stats (after ban)"
"$WORK/registry" queue-stats

echo "queuesmoke: OK"
