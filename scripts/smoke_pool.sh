#!/usr/bin/env bash
# Smoke for slice 7 — worker pool, capabilities, server-enforced concurrency.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-poolsmoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"; BLOBS="$WORK/blobs"; ADDR="127.0.0.1:18100"; URL="http://$ADDR"
LEASE_SECRET="$(openssl rand -base64 32)"
REGISTRY_PID=""

cleanup() {
  [[ -n "$REGISTRY_PID" ]] && kill "$REGISTRY_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "poolsmoke: $WORK"
}

cd "$ROOT"
go build -o "$WORK/registry" ./cmd/registry

export DB_DSN="$DB" BLOBS_ROOT="$BLOBS" LEASE_SECRET

"$WORK/registry" migrate up >/dev/null

# Worker A: crawl-only, max_concurrent=1
A_OUT="$("$WORK/registry" create-worker --label pool-a --capabilities crawl --max-concurrent 1)"
A_PAT="$(printf '%s' "$A_OUT" | awk -F= '/^pat=/{print $2}')"

# Worker B: embed-only
B_OUT="$("$WORK/registry" create-worker --label pool-b --capabilities embed --max-concurrent 4)"
B_PAT="$(printf '%s' "$B_OUT" | awk -F= '/^pat=/{print $2}')"

# Worker C: legacy (no capabilities → backward-compat: any)
C_OUT="$("$WORK/registry" create-worker --label pool-c --max-concurrent 2)"
C_PAT="$(printf '%s' "$C_OUT" | awk -F= '/^pat=/{print $2}')"

echo "poolsmoke: list-workers"
"$WORK/registry" list-workers

# Enqueue across 4 distinct domains so the per-domain politeness rule
# doesn't accidentally cap reserves below max_concurrent.
for H in a.test b.test c.test d.test; do
  "$WORK/registry" seed-domain --host "$H" --crawl-delay-ms 1
  "$WORK/registry" enqueue --url "https://$H/page"
done

"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "$URL/healthz" >/dev/null

# Worker B tries crawl reserve — must fail with capability_denied.
CODE_B="$(curl -s -o "$WORK/b.json" -w "%{http_code}" -X POST \
  -H "Authorization: Bearer $B_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":5}' "$URL/v1/jobs/reserve")"
echo "poolsmoke: B crawl reserve -> $CODE_B  body=$(cat $WORK/b.json)"
[[ "$CODE_B" == "403" ]] || { echo "poolsmoke: B should be denied crawl"; exit 1; }

# Worker A reserves crawl — should get exactly 1 (max_concurrent=1).
A_JSON="$(curl -s -X POST -H "Authorization: Bearer $A_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":5}' "$URL/v1/jobs/reserve")"
A_COUNT="$(printf '%s' "$A_JSON" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["jobs"]))')"
echo "poolsmoke: A first reserve count = $A_COUNT (want 1)"
[[ "$A_COUNT" == "1" ]] || { echo "poolsmoke: A should get 1 job"; exit 1; }

# Worker A tries again — already holds 1, should get 0 more.
A_JSON2="$(curl -s -X POST -H "Authorization: Bearer $A_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":5}' "$URL/v1/jobs/reserve")"
A_COUNT2="$(printf '%s' "$A_JSON2" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["jobs"]))')"
echo "poolsmoke: A second reserve count = $A_COUNT2 (want 0, already at cap)"
[[ "$A_COUNT2" == "0" ]] || { echo "poolsmoke: A should be saturated"; exit 1; }

# Worker C (no caps) can also crawl — backward-compat. max_concurrent=2.
C_JSON="$(curl -s -X POST -H "Authorization: Bearer $C_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":5}' "$URL/v1/jobs/reserve")"
C_COUNT="$(printf '%s' "$C_JSON" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["jobs"]))')"
echo "poolsmoke: C reserve count = $C_COUNT (want 2 — capped to max_concurrent)"
[[ "$C_COUNT" == "2" ]] || { echo "poolsmoke: C should get 2 (max_concurrent)"; exit 1; }

# update-worker: raise A's cap to 5 and verify
"$WORK/registry" update-worker --id 1 --max-concurrent 5
A_JSON3="$(curl -s -X POST -H "Authorization: Bearer $A_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":5}' "$URL/v1/jobs/reserve")"
A_COUNT3="$(printf '%s' "$A_JSON3" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["jobs"]))')"
echo "poolsmoke: A after cap-raise reserve = $A_COUNT3 (want 0; nothing left queued)"

# ban-worker: B becomes banned, even embed reserve must fail with 403 banned.
"$WORK/registry" ban-worker --id 2
CODE_BAN="$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -H "Authorization: Bearer $B_PAT" -H 'Content-Type: application/json' \
  --data '{"batch":5}' "$URL/v1/embed/reserve")"
echo "poolsmoke: B after ban -> $CODE_BAN (want 403)"
[[ "$CODE_BAN" == "403" ]] || { echo "poolsmoke: ban not enforced"; exit 1; }

# /v1/workers/me reports capabilities + held + max_concurrent
ME="$(curl -s -H "Authorization: Bearer $A_PAT" "$URL/v1/workers/me")"
echo "poolsmoke: A /workers/me = $ME"
printf '%s' "$ME" | python3 -c 'import sys,json; d=json.load(sys.stdin); assert "crawl" in d["capabilities"]; assert d["max_concurrent"]>=5; print("OK")'

echo "poolsmoke: OK"
