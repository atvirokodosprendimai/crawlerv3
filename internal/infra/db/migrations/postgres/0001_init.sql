-- +goose Up
-- +goose StatementBegin
CREATE TABLE domains (
    id                BIGSERIAL PRIMARY KEY,
    host              TEXT NOT NULL UNIQUE,
    scheme            TEXT NOT NULL DEFAULT 'https',
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    crawl_delay_ms    INTEGER NOT NULL DEFAULT 1000,
    robots_body       TEXT,
    robots_fetched_at TIMESTAMPTZ,
    last_request_at   TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workers (
    id               BIGSERIAL PRIMARY KEY,
    pat_hash         BYTEA NOT NULL UNIQUE,
    label            TEXT  NOT NULL,
    ip_last          TEXT,
    reputation_score INTEGER NOT NULL DEFAULT 100,
    banned_at        TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE crawl_frontier (
    url_hash            BYTEA PRIMARY KEY,
    url                 TEXT  NOT NULL,
    canonical_url       TEXT  NOT NULL,
    domain_id           BIGINT NOT NULL REFERENCES domains(id) ON DELETE CASCADE,
    depth               INTEGER NOT NULL DEFAULT 0,
    priority            INTEGER NOT NULL DEFAULT 0,
    status              TEXT  NOT NULL DEFAULT 'queued',
    leased_by_worker_id BIGINT REFERENCES workers(id),
    lease_token         BYTEA,
    lease_expires_at    TIMESTAMPTZ,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 5,
    next_retry_at       TIMESTAMPTZ,
    scheduled_for       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    discovered_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    parent_url_hash     BYTEA,
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
    id              BIGSERIAL PRIMARY KEY,
    url_hash        BYTEA NOT NULL REFERENCES crawl_frontier(url_hash) ON DELETE CASCADE,
    storage_backend TEXT  NOT NULL,
    storage_key     TEXT  NOT NULL,
    content_type    TEXT,
    content_sha256  BYTEA NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    archived_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
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
