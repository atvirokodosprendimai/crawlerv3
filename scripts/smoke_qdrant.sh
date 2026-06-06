#!/usr/bin/env bash
# Smoke for slice 10:
#   1. Embed worker pushes a raw vector → registry auto-creates collection
#      (verifying 9-shard config), upserts the point into fake Qdrant.
#   2. POST /v1/search returns the chunk with object_id + chunk_index + text + url.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-qdrantsmoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"; BLOBS="$WORK/blobs"
ADDR="127.0.0.1:18140"; URL="http://$ADDR"
QPORT=18141; QURL="http://127.0.0.1:$QPORT"
LEASE_SECRET="$(openssl rand -base64 32)"
REGISTRY_PID=""; QDRANT_PID=""

cleanup() {
  [[ -n "$REGISTRY_PID" ]] && kill "$REGISTRY_PID" 2>/dev/null || true
  [[ -n "$QDRANT_PID"   ]] && kill "$QDRANT_PID"   2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "qdrantsmoke: $WORK"
}

# ---- fake Qdrant ---------------------------------------------------------
cat > "$WORK/fake_qdrant.py" <<'PY'
import http.server, json, sys
collections = {}                 # name -> config (so 9 shards is verifiable)
points      = {}                 # name -> {id -> point}

class H(http.server.BaseHTTPRequestHandler):
    def _parts(self):
        return self.path.split('?')[0].strip('/').split('/')
    def _read(self):
        n = int(self.headers.get('Content-Length','0') or 0)
        return self.rfile.read(n) if n else b''
    def _ok(self, body=b'{"result":true,"status":"ok"}'):
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def _nf(self):
        self.send_response(404); self.end_headers()
    def do_GET(self):
        p = self._parts()
        if len(p)==2 and p[0]=='collections':
            if p[1] in collections:
                self._ok(json.dumps({"result":{"status":"green","config":collections[p[1]]}}).encode())
            else:
                self._nf()
        elif p == ['debug','collections']:
            self._ok(json.dumps(collections).encode())
        elif p == ['debug','points']:
            self._ok(json.dumps({k:list(v.values()) for k,v in points.items()}).encode())
        else:
            self._nf()
    def do_PUT(self):
        body = self._read()
        p = self._parts()
        if p[:1]==['collections'] and len(p)==2:
            collections[p[1]] = json.loads(body) if body else {}
            self._ok()
        elif p[:1]==['collections'] and len(p)==3 and p[2]=='points':
            data = json.loads(body)
            store = points.setdefault(p[1], {})
            for pt in data.get('points',[]):
                store[pt['id']] = pt
            self._ok(json.dumps({"result":{"status":"completed"}}).encode())
        else:
            self._nf()
    def do_POST(self):
        body = self._read()
        p = self._parts()
        if p[:1]==['collections'] and len(p)==4 and p[2]=='points' and p[3]=='search':
            data = json.loads(body)
            name = p[1]
            limit = data.get('limit',10)
            hits = []
            q = data.get('vector',[])
            for pid, pt in points.get(name,{}).items():
                v = pt.get('vector',[])
                score = sum(a*b for a,b in zip(q,v))
                hits.append({"id":pid,"score":score,"payload":pt.get('payload',{})})
            hits.sort(key=lambda h: -h['score'])
            self._ok(json.dumps({"result":hits[:limit]}).encode())
        else:
            self._nf()
    def log_message(self,*a,**k): pass

port = int(sys.argv[1])
print(f"fake-qdrant listening on {port}", flush=True)
http.server.HTTPServer(('127.0.0.1',port), H).serve_forever()
PY

cd "$ROOT"
go build -o "$WORK/registry" ./cmd/registry

# ---- start fake Qdrant ----
python3 "$WORK/fake_qdrant.py" "$QPORT" >"$WORK/qdrant.log" 2>&1 &
QDRANT_PID=$!
sleep 0.5
curl -fsS "$QURL/debug/collections" >/dev/null

# ---- start registry pointing at it ----
export DB_DSN="$DB" BLOBS_ROOT="$BLOBS" LEASE_SECRET
export QDRANT_URL="$QURL"
export QDRANT_SHARDS=9
"$WORK/registry" migrate up >/dev/null

EMBED_OUT="$("$WORK/registry" create-worker --label embed --capabilities embed --max-concurrent 8)"
EMBED_PAT="$(printf '%s' "$EMBED_OUT" | awk -F= '/^pat=/{print $2}')"
SEARCH_OUT="$("$WORK/registry" create-worker --label searcher --capabilities search --max-concurrent 4)"
SEARCH_PAT="$(printf '%s' "$SEARCH_OUT" | awk -F= '/^pat=/{print $2}')"

"$WORK/registry" seed-domain --host site-a.test --crawl-delay-ms 50
"$WORK/registry" update-domain --host site-a.test --embed-collection lithuania_news
"$WORK/registry" enqueue --url https://site-a.test/page

"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "$URL/healthz" >/dev/null

# ---- plant a fake CSV blob so text_passthrough produces a chunk ----
URL_HASH_HEX="$(python3 -c "import hashlib; print(hashlib.sha256(b'https://site-a.test/page').hexdigest())")"
KEY="$(echo "$URL_HASH_HEX" | cut -c1-2)/${URL_HASH_HEX}.bin"
BODY="The quick brown fox jumps over the lazy dog. Quick reference text."
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

# Wait for text_passthrough goroutine + chunk to land
for _ in $(seq 1 30); do
  N="$(sqlite3 "$DB" "SELECT COUNT(*) FROM document_chunks WHERE embed_status='pending';")"
  [[ "$N" -ge 1 ]] && break
  sleep 1
done
[[ "$N" -ge 1 ]] || { echo "qdrantsmoke: chunk not created"; exit 1; }

# ---- embed reserve + push raw vector ----
RESERVE="$(curl -fsS -X POST -H "Authorization: Bearer $EMBED_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":1000}' "$URL/v1/embed/reserve")"
echo "qdrantsmoke: reserve = $(printf '%s' "$RESERVE" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d["chunks"]), "chunks, [0].collection=", d["chunks"][0]["collection"])')"

# Build a fake 4-dim vector to push back for each chunk.
PUSH_JSON="$(printf '%s' "$RESERVE" | python3 -c '
import sys, json
d = json.load(sys.stdin)
out = {"results":[]}
for i,c in enumerate(d["chunks"]):
    out["results"].append({
        "chunk_id": c["chunk_id"],
        "vector":   [0.1*(i+1), 0.2, 0.3, 0.4],
        "lease_token": c["lease_token"],
    })
print(json.dumps(out))
')"

PUSH_RESP="$(curl -fsS -X POST -H "Authorization: Bearer $EMBED_PAT" -H 'Content-Type: application/json' \
  --data "$PUSH_JSON" "$URL/v1/embed/result")"
echo "qdrantsmoke: result push -> $PUSH_RESP"

# ---- assert collection auto-created with 9 shards ----
COL_CONF="$(curl -fsS "$QURL/collections/lithuania_news")"
SHARDS="$(printf '%s' "$COL_CONF" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["result"]["config"]["shard_number"])')"
DIM="$(printf '%s' "$COL_CONF" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["result"]["config"]["vectors"]["size"])')"
echo "qdrantsmoke: Qdrant collection lithuania_news shards=$SHARDS dim=$DIM (want 9 / 4)"
[[ "$SHARDS" == "9" ]] || { echo "qdrantsmoke: shard_number != 9"; exit 1; }
[[ "$DIM"    == "4" ]] || { echo "qdrantsmoke: vector dim != 4"; exit 1; }

# ---- assert document_chunks.vector_id stamped + status=done ----
VID="$(sqlite3 "$DB" "SELECT vector_id FROM document_chunks LIMIT 1;")"
STATUS="$(sqlite3 "$DB" "SELECT embed_status FROM document_chunks LIMIT 1;")"
echo "qdrantsmoke: db vector_id=$VID status=$STATUS"
[[ "$STATUS" == "done" ]] || { echo "qdrantsmoke: chunk not marked done"; exit 1; }
case "$VID" in qdrant:lithuania_news:*) ;; *) echo "qdrantsmoke: vector_id wrong: $VID"; exit 1;; esac

# ---- /v1/search by vector returns the chunk ----
SEARCH="$(curl -fsS -X POST -H "Authorization: Bearer $SEARCH_PAT" -H 'Content-Type: application/json' \
  --data '{"collection":"lithuania_news","query_vector":[0.5,0.5,0.5,0.5],"limit":5}' \
  "$URL/v1/search")"
echo "qdrantsmoke: search response:"
echo "$SEARCH" | python3 -m json.tool | head -25

HITS="$(printf '%s' "$SEARCH" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["count"])')"
LO_ID="$(printf '%s' "$SEARCH" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["items"][0]["lake_object_id"])')"
FIRST_TEXT="$(printf '%s' "$SEARCH" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["items"][0]["text"][:30])')"
FIRST_URL="$(printf '%s' "$SEARCH" | python3 -c 'import sys,json; d=json.load(sys.stdin); print(d["items"][0]["url"])')"
echo "qdrantsmoke: hits=$HITS lake_object_id=$LO_ID url=$FIRST_URL text='$FIRST_TEXT...'"
[[ "$HITS" -ge 1 ]] || { echo "qdrantsmoke: search returned no hits"; exit 1; }
[[ "$LO_ID" -ge 1 ]] || { echo "qdrantsmoke: lake_object_id missing"; exit 1; }
[[ "$FIRST_URL" == "https://site-a.test/page" ]] || { echo "qdrantsmoke: url wrong"; exit 1; }

# ---- capability denial: embed worker can't search ----
CODE="$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Authorization: Bearer $EMBED_PAT" -H 'Content-Type: application/json' \
  --data '{"collection":"lithuania_news","query_vector":[0,0,0,0]}' \
  "$URL/v1/search")"
echo "qdrantsmoke: embed-only worker /v1/search -> $CODE (want 403)"
[[ "$CODE" == "403" ]] || { echo "qdrantsmoke: search cap not enforced"; exit 1; }

echo "qdrantsmoke: OK"
