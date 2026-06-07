#!/usr/bin/env bash
#
# liteko_requeue.sh — recover from the enqueue-contention bug fixed in 4de0ef8.
#
# The bug silently dropped most discovered_links from each listing job. After
# upgrading the registry binary, run this on the host that owns crawler.db to:
#   1. snapshot the DB (safety net),
#   2. delete the listing lake_objects so re-fetch won't hit the UNIQUE
#      constraint on (content_sha256, url_hash),
#   3. flip the 7,827 paieska.aspx rows back to status='queued',
#   4. print before/after histograms so the operator sees the diff.
#
# Defaults assume the prod layout on dc-head (/home/ubuntu/crawler.db, domain
# id=2). Override via env or flags; --dry-run prints planned changes only.
#
# Usage:
#   bash scripts/liteko_requeue.sh [--dry-run] [--db <path>] [--domain-id N] \
#                                  [--no-backup] [--yes]

set -euo pipefail

DB="${DB:-crawler.db}"
DOMAIN_ID="${DOMAIN_ID:-2}"
LISTING_PATTERN="${LISTING_PATTERN:-%paieska.aspx%}"
DRY_RUN=0
BACKUP=1
ASSUME_YES=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)     DRY_RUN=1; shift ;;
        --db)          DB="$2"; shift 2 ;;
        --domain-id)   DOMAIN_ID="$2"; shift 2 ;;
        --no-backup)   BACKUP=0; shift ;;
        --yes|-y)      ASSUME_YES=1; shift ;;
        -h|--help)
            sed -n '2,17p' "$0"; exit 0 ;;
        *)
            echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

if ! command -v sqlite3 >/dev/null 2>&1; then
    echo "sqlite3 not on PATH" >&2; exit 1
fi
if [[ ! -f "$DB" ]]; then
    echo "db not found: $DB" >&2; exit 1
fi

q() { sqlite3 -bail "$DB" "$1"; }

echo "==> using db=$DB domain_id=$DOMAIN_ID dry_run=$DRY_RUN backup=$BACKUP"

# --------------------------------------------------------------------------
# 1. Before snapshot
# --------------------------------------------------------------------------
echo
echo "==> BEFORE — frontier counts for domain $DOMAIN_ID"
q "
SELECT
  CASE
    WHEN canonical_url LIKE '$LISTING_PATTERN' THEN 'listing'
    WHEN canonical_url LIKE '%tekstas.aspx%'   THEN 'detail'
    ELSE 'other'
  END AS kind,
  status,
  COUNT(*) AS n
FROM crawl_frontier
WHERE domain_id=$DOMAIN_ID
GROUP BY kind, status
ORDER BY kind, status;
"

LISTING_DONE=$(q "SELECT COUNT(*) FROM crawl_frontier WHERE domain_id=$DOMAIN_ID AND canonical_url LIKE '$LISTING_PATTERN' AND status='done';")
DETAIL_TOTAL=$(q "SELECT COUNT(*) FROM crawl_frontier WHERE domain_id=$DOMAIN_ID AND canonical_url LIKE '%tekstas.aspx%';")
AVG=$(q "SELECT printf('%.2f', CAST($DETAIL_TOTAL AS REAL) / NULLIF($LISTING_DONE, 0));")
echo "    listings(done)=$LISTING_DONE  details(any)=$DETAIL_TOTAL  details/listing=$AVG"

if [[ "$LISTING_DONE" -eq 0 ]]; then
    echo "==> no done listings to requeue. exiting." ; exit 0
fi

# --------------------------------------------------------------------------
# 2. Confirm
# --------------------------------------------------------------------------
echo
echo "==> WILL: requeue $LISTING_DONE listings AND delete their lake_objects."
if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "==> DRY-RUN — stopping before any write."
    exit 0
fi
if [[ "$ASSUME_YES" -ne 1 ]]; then
    read -r -p "proceed? [y/N] " ans
    [[ "${ans:-N}" =~ ^[Yy]$ ]] || { echo "aborted."; exit 1; }
fi

# --------------------------------------------------------------------------
# 3. Backup
# --------------------------------------------------------------------------
if [[ "$BACKUP" -eq 1 ]]; then
    STAMP=$(date '+%Y%m%d-%H%M%S')
    BACKUP_PATH="${DB}.bak.${STAMP}"
    echo
    echo "==> backing up $DB to $BACKUP_PATH (sqlite VACUUM INTO — atomic)"
    q "VACUUM INTO '$BACKUP_PATH';"
    ls -lh "$BACKUP_PATH"
fi

# --------------------------------------------------------------------------
# 4. Delete + requeue in one transaction
# --------------------------------------------------------------------------
echo
echo "==> executing delete + requeue inside a single transaction"
q "
BEGIN;

DELETE FROM lake_objects
WHERE url_hash IN (
  SELECT url_hash
  FROM crawl_frontier
  WHERE domain_id=$DOMAIN_ID
    AND canonical_url LIKE '$LISTING_PATTERN'
    AND status='done'
);

UPDATE crawl_frontier
SET status              = 'queued',
    leased_by_worker_id = NULL,
    lease_token         = NULL,
    lease_expires_at    = NULL,
    completed_at        = NULL,
    attempt_count       = 0,
    next_retry_at       = NULL,
    http_status         = NULL,
    error_code          = NULL
WHERE domain_id=$DOMAIN_ID
  AND canonical_url LIKE '$LISTING_PATTERN'
  AND status='done';

COMMIT;
"

# --------------------------------------------------------------------------
# 5. After snapshot
# --------------------------------------------------------------------------
echo
echo "==> AFTER — frontier counts for domain $DOMAIN_ID"
q "
SELECT
  CASE
    WHEN canonical_url LIKE '$LISTING_PATTERN' THEN 'listing'
    WHEN canonical_url LIKE '%tekstas.aspx%'   THEN 'detail'
    ELSE 'other'
  END AS kind,
  status,
  COUNT(*) AS n
FROM crawl_frontier
WHERE domain_id=$DOMAIN_ID
GROUP BY kind, status
ORDER BY kind, status;
"

echo
echo "==> done. next steps:"
echo "    1. confirm new registry binary is running (commit 4de0ef8+)"
echo "    2. start litekoworker conservatively, e.g.:"
echo "         ./litekoworker run --registry http://localhost --pat \$PAT \\"
echo "                            --batch 8 --concurrency 8 --idle-sleep 1s"
echo "    3. watch logs for 'database is locked (517)' — should be gone"
echo "    4. once stable, raise --concurrency to taste"
