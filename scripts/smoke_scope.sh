#!/usr/bin/env bash
# Smoke: discovered links from outside the seeded domain set must be DROPPED.
# Reproduces the bug "enqueued 9g.lt, crawler escaped to the whole internet".

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-scopesmoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"; BLOBS="$WORK/blobs"; ADDR="127.0.0.1:18120"; URL="http://$ADDR"
LEASE_SECRET="$(openssl rand -base64 32)"
REGISTRY_PID=""

cleanup() {
  [[ -n "$REGISTRY_PID" ]] && kill "$REGISTRY_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "scopesmoke: $WORK"
}

cd "$ROOT"
go build -o "$WORK/registry" ./cmd/registry

export DB_DSN="$DB" BLOBS_ROOT="$BLOBS" LEASE_SECRET

"$WORK/registry" migrate up >/dev/null

PAT_OUT="$("$WORK/registry" create-worker --label crawl --capabilities crawl --max-concurrent 4)"
PAT="$(printf '%s' "$PAT_OUT" | awk -F= '/^pat=/{print $2}')"

"$WORK/registry" seed-domain --host 9g.lt --crawl-delay-ms 50
"$WORK/registry" enqueue --url https://9g.lt/

"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "$URL/healthz" >/dev/null

# Forge a fake "result" that claims to have discovered links to external hosts.
# Reserve a job, then push a fake multipart result containing external hrefs.
RESERVE="$(curl -s -X POST -H "Authorization: Bearer $PAT" -H 'Content-Type: application/json' \
  --data '{"batch":1}' "$URL/v1/jobs/reserve")"
LEASE_TOKEN="$(printf '%s' "$RESERVE" | python3 -c 'import sys,json; print(json.load(sys.stdin)["jobs"][0]["lease_token"])')"

# Compute sha256 of body for the meta.
BODY="<html><body>hi</body></html>"
SHA="$(printf '%s' "$BODY" | shasum -a 256 | cut -d' ' -f1)"
SIZE="${#BODY}"

# meta JSON: ONE inside-domain link + THREE external ones (the bug case).
META=$(cat <<EOF
{
  "lease_token": "$LEASE_TOKEN",
  "http_status": 200,
  "content_type": "text/html",
  "content_sha256": "$SHA",
  "size": $SIZE,
  "discovered_links": [
    {"url":"https://9g.lt/page2","anchor":"in","rel":"","new_depth":1},
    {"url":"https://google.com/","anchor":"out","rel":"","new_depth":1},
    {"url":"https://wikipedia.org/foo","anchor":"out","rel":"","new_depth":1},
    {"url":"https://anywhere.tld/x","anchor":"out","rel":"","new_depth":1}
  ]
}
EOF
)

# Build multipart with a file
BOUNDARY="boundary$$"
{
  printf -- "--%s\r\n" "$BOUNDARY"
  printf 'Content-Disposition: form-data; name="meta"\r\n\r\n'
  printf '%s\r\n' "$META"
  printf -- "--%s\r\n" "$BOUNDARY"
  printf 'Content-Disposition: form-data; name="blob"; filename="body.html"\r\n'
  printf 'Content-Type: text/html\r\n\r\n'
  printf '%s\r\n' "$BODY"
  printf -- "--%s--\r\n" "$BOUNDARY"
} > "$WORK/req.bin"

curl -sS -X POST -H "Authorization: Bearer $PAT" \
  -H "Content-Type: multipart/form-data; boundary=$BOUNDARY" \
  --data-binary "@$WORK/req.bin" "$URL/v1/jobs/result" > "$WORK/result.json"
echo "scopesmoke: result push -> $(cat $WORK/result.json)"

sleep 1

# Assertions.
FRONTIER_HOSTS="$(sqlite3 "$DB" "
SELECT DISTINCT d.host
FROM crawl_frontier cf JOIN domains d ON d.id=cf.domain_id
ORDER BY d.host;
")"
echo "scopesmoke: frontier domain hosts ="
echo "$FRONTIER_HOSTS"

DOMAIN_HOSTS="$(sqlite3 "$DB" "SELECT host FROM domains ORDER BY host;")"
echo "scopesmoke: domains rows ="
echo "$DOMAIN_HOSTS"

# Should contain ONLY 9g.lt.
if echo "$FRONTIER_HOSTS" | grep -v -q '^9g\.lt$'; then
  echo "scopesmoke: BUG — frontier escaped seed scope"
  exit 1
fi
if echo "$DOMAIN_HOSTS" | grep -v -q '^9g\.lt$'; then
  echo "scopesmoke: BUG — domains table got auto-extended"
  exit 1
fi

# Should also have enqueued the in-domain discovered link.
IN_COUNT="$(sqlite3 "$DB" "SELECT COUNT(*) FROM crawl_frontier WHERE url='https://9g.lt/page2';")"
echo "scopesmoke: in-domain enqueued count = $IN_COUNT (want 1)"
[[ "$IN_COUNT" == "1" ]] || { echo "scopesmoke: in-domain link not enqueued"; exit 1; }

echo "scopesmoke: OK (scope-locked to 9g.lt)"
