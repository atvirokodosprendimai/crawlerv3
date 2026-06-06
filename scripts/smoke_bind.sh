#!/usr/bin/env bash
# Smoke for slice 12: domain ↔ worker binding via required_capability.

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d -t crawlerv3-bindsmoke.XXXXXX)"
trap 'cleanup' EXIT INT TERM

DB="$WORK/crawler.db"; BLOBS="$WORK/blobs"; ADDR="127.0.0.1:18160"; URL="http://$ADDR"
LEASE_SECRET="$(openssl rand -base64 32)"
REGISTRY_PID=""

cleanup() {
  [[ -n "$REGISTRY_PID" ]] && kill "$REGISTRY_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  echo
  echo "bindsmoke: $WORK"
}

cd "$ROOT"
go build -o "$WORK/registry" ./cmd/registry

export DB_DSN="$DB" BLOBS_ROOT="$BLOBS" LEASE_SECRET
"$WORK/registry" migrate up >/dev/null

# Three worker profiles
PLAIN_OUT="$("$WORK/registry" create-worker --label plain --capabilities crawl --max-concurrent 4)"
PLAIN_PAT="$(printf '%s' "$PLAIN_OUT" | awk -F= '/^pat=/{print $2}')"
JS_OUT="$("$WORK/registry" create-worker --label js --capabilities crawl,js_render --max-concurrent 4)"
JS_PAT="$(printf '%s' "$JS_OUT" | awk -F= '/^pat=/{print $2}')"
ANY_OUT="$("$WORK/registry" create-worker --label legacy --max-concurrent 4)"  # no caps = any (backcompat)
ANY_PAT="$(printf '%s' "$ANY_OUT" | awk -F= '/^pat=/{print $2}')"

# Two domains: one open, one bound to js_render
"$WORK/registry" seed-domain --host open.example --crawl-delay-ms 50
"$WORK/registry" seed-domain --host spa.example  --crawl-delay-ms 50
"$WORK/registry" update-domain --host spa.example --required-capability js_render

echo "bindsmoke: list-domains"
"$WORK/registry" list-domains

# Enqueue one URL per domain
"$WORK/registry" enqueue --url https://open.example/a
"$WORK/registry" enqueue --url https://spa.example/b

"$WORK/registry" serve --addr "$ADDR" >"$WORK/registry.log" 2>&1 &
REGISTRY_PID=$!
sleep 1
curl -fsS "$URL/healthz" >/dev/null

reserve_count() {
  local pat="$1"
  curl -s -X POST -H "Authorization: Bearer $pat" -H 'Content-Type: application/json' \
    --data '{"batch":10}' "$URL/v1/jobs/reserve" \
    | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["jobs"]))'
}

reserve_urls() {
  local pat="$1"
  curl -s -X POST -H "Authorization: Bearer $pat" -H 'Content-Type: application/json' \
    --data '{"batch":10}' "$URL/v1/jobs/reserve" \
    | python3 -c 'import sys,json; print(",".join(j["url"] for j in json.load(sys.stdin)["jobs"]))'
}

# 1. Plain (no js_render): must only get open.example/a; spa.example/b is bound-out.
echo "bindsmoke: plain worker reserves"
PLAIN_URLS="$(reserve_urls "$PLAIN_PAT")"
echo "  → $PLAIN_URLS"
case "$PLAIN_URLS" in
  *open.example/a*)
    case "$PLAIN_URLS" in
      *spa.example*) echo "bindsmoke: plain leaked into bound domain"; exit 1;;
    esac
    ;;
  *) echo "bindsmoke: plain did not reserve the open URL"; exit 1;;
esac

# Release plain's lease so the other workers can reserve the remaining work.
PLAIN_ID="$(printf '%s' "$PLAIN_OUT" | awk -F= '/^worker_id=/{print $2}')"
"$WORK/registry" release-worker --id "$PLAIN_ID" >/dev/null
sleep 1.2   # wait for per-domain politeness window (sqlite politeness has 1s resolution)

# 2. JS worker: must get spa.example AND open.example (caps={crawl,js_render}).
echo "bindsmoke: js worker reserves"
JS_URLS="$(reserve_urls "$JS_PAT")"
echo "  → $JS_URLS"
case "$JS_URLS" in
  *spa.example/b*) ;; *) echo "bindsmoke: js worker missed bound domain"; exit 1;;
esac
case "$JS_URLS" in
  *open.example/a*) ;; *) echo "bindsmoke: js worker missed open domain"; exit 1;;
esac

# Release js's leases.
JS_ID="$(printf '%s' "$JS_OUT" | awk -F= '/^worker_id=/{print $2}')"
"$WORK/registry" release-worker --id "$JS_ID" >/dev/null
sleep 1.2

# 3. Legacy worker (no caps stored → treated as "any") reserves both.
echo "bindsmoke: legacy worker reserves"
ANY_URLS="$(reserve_urls "$ANY_PAT")"
echo "  → $ANY_URLS"
case "$ANY_URLS" in *open.example/a*) ;; *) echo "bindsmoke: legacy missed open"; exit 1;; esac
case "$ANY_URLS" in *spa.example/b*) ;; *) echo "bindsmoke: legacy missed bound"; exit 1;; esac

# 4. Clear the binding, plain worker can now reserve spa.example too.
ANY_ID="$(printf '%s' "$ANY_OUT" | awk -F= '/^worker_id=/{print $2}')"
"$WORK/registry" release-worker --id "$ANY_ID" >/dev/null
"$WORK/registry" update-domain --host spa.example --required-capability -
sleep 1.2

echo "bindsmoke: after clearing binding, plain reserves"
PLAIN_URLS2="$(reserve_urls "$PLAIN_PAT")"
echo "  → $PLAIN_URLS2"
case "$PLAIN_URLS2" in *spa.example/b*) ;; *) echo "bindsmoke: plain should reach unbound domain"; exit 1;; esac

echo "bindsmoke: OK"
