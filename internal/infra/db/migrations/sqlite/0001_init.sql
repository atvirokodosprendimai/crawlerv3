-- +goose Up
-- +goose StatementBegin
CREATE TABLE domains (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    host            TEXT    NOT NULL UNIQUE,
    scheme          TEXT    NOT NULL DEFAULT 'https',
    is_active       INTEGER NOT NULL DEFAULT 1,
    crawl_delay_ms  INTEGER NOT NULL DEFAULT 1000,
    robots_body     TEXT,
    robots_fetched_at DATETIME,
    last_request_at DATETIME,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workers (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    pat_hash         BLOB    NOT NULL UNIQUE,
    label            TEXT    NOT NULL,
    ip_last          TEXT,
    reputation_score INTEGER NOT NULL DEFAULT 100,
    banned_at        DATETIME,
    created_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE crawl_frontier (
    url_hash            BLOB    PRIMARY KEY,
    url                 TEXT    NOT NULL,
    canonical_url       TEXT    NOT NULL,
    domain_id           INTEGER NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    depth               INTEGER NOT NULL DEFAULT 0,
    priority            INTEGER NOT NULL DEFAULT 0,
    status              TEXT    NOT NULL DEFAULT 'queued',
    leased_by_worker_id INTEGER REFERENCES workers(id),
    lease_token         BLOB,
    lease_expires_at    DATETIME,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 5,
    next_retry_at       DATETIME,
    scheduled_for       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    discovered_at       DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at        DATETIME,
    parent_url_hash     BLOB,
    http_status         INTEGER,
    error_code          TEXT,
    error_message       TEXT
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_frontier_status_sched  ON crawl_frontier(status, scheduled_for);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_frontier_domain_status ON crawl_frontier(domain_id, status);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_frontier_lease_expiry  ON crawl_frontier(status, lease_expires_at);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE lake_objects (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url_hash        BLOB    NOT NULL REFERENCES crawl_frontier(url_hash) ON DELETE CASCADE,
    storage_backend TEXT    NOT NULL,
    storage_key     TEXT    NOT NULL,
    content_type    TEXT,
    content_sha256  BLOB    NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    archived_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    migrated_from   TEXT,
    UNIQUE(content_sha256, url_hash)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX idx_lake_content ON lake_objects(content_sha256);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_lake_url ON lake_objects(url_hash);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS lake_objects;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS crawl_frontier;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS workers;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS domains;
-- +goose StatementEnd
