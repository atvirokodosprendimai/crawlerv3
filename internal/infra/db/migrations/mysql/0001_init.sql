-- +goose Up
-- +goose StatementBegin
CREATE TABLE domains (
    id                BIGINT  NOT NULL AUTO_INCREMENT PRIMARY KEY,
    host              VARCHAR(255) NOT NULL UNIQUE,
    scheme            VARCHAR(16)  NOT NULL DEFAULT 'https',
    is_active         TINYINT(1)   NOT NULL DEFAULT 1,
    crawl_delay_ms    INT          NOT NULL DEFAULT 1000,
    robots_body       MEDIUMTEXT,
    robots_fetched_at DATETIME(6),
    last_request_at   DATETIME(6),
    created_at        DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE workers (
    id               BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    pat_hash         VARBINARY(32) NOT NULL UNIQUE,
    label            VARCHAR(255) NOT NULL,
    ip_last          VARCHAR(64),
    reputation_score INT NOT NULL DEFAULT 100,
    banned_at        DATETIME(6),
    created_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE crawl_frontier (
    url_hash            VARBINARY(32) NOT NULL,
    url                 TEXT NOT NULL,
    canonical_url       TEXT NOT NULL,
    domain_id           BIGINT NOT NULL,
    depth               INT NOT NULL DEFAULT 0,
    priority            INT NOT NULL DEFAULT 0,
    status              VARCHAR(16) NOT NULL DEFAULT 'queued',
    leased_by_worker_id BIGINT,
    lease_token         VARBINARY(64),
    lease_expires_at    DATETIME(6),
    attempt_count       INT NOT NULL DEFAULT 0,
    max_attempts        INT NOT NULL DEFAULT 5,
    next_retry_at       DATETIME(6),
    scheduled_for       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    discovered_at       DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    completed_at        DATETIME(6),
    parent_url_hash     VARBINARY(32),
    http_status         INT,
    error_code          VARCHAR(64),
    error_message       TEXT,
    PRIMARY KEY (url_hash),
    KEY idx_frontier_status_sched  (status, scheduled_for),
    KEY idx_frontier_domain_status (domain_id, status),
    KEY idx_frontier_lease_expiry  (status, lease_expires_at),
    CONSTRAINT fk_frontier_domain FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE,
    CONSTRAINT fk_frontier_worker FOREIGN KEY (leased_by_worker_id) REFERENCES workers(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE lake_objects (
    id              BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    url_hash        VARBINARY(32) NOT NULL,
    storage_backend VARCHAR(16) NOT NULL,
    storage_key     VARCHAR(1024) NOT NULL,
    content_type    VARCHAR(128),
    content_sha256  VARBINARY(32) NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    archived_at     DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    migrated_from   VARCHAR(1024),
    KEY idx_lake_content (content_sha256),
    KEY idx_lake_url     (url_hash),
    UNIQUE KEY uq_lake_sha_url (content_sha256, url_hash),
    CONSTRAINT fk_lake_frontier FOREIGN KEY (url_hash) REFERENCES crawl_frontier(url_hash) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
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
