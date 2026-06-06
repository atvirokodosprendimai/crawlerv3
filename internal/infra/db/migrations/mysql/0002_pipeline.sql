-- +goose Up
-- +goose StatementBegin
CREATE TABLE processing_jobs (
    id                    BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    lake_object_id        BIGINT NOT NULL,
    processor             VARCHAR(32) NOT NULL,
    status                VARCHAR(16) NOT NULL DEFAULT 'queued',
    attempt_count         INT NOT NULL DEFAULT 0,
    max_attempts          INT NOT NULL DEFAULT 3,
    last_error            TEXT,
    started_at            DATETIME(6),
    finished_at           DATETIME(6),
    output_lake_object_id BIGINT,
    created_at            DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    KEY idx_proc_status (status, processor),
    KEY idx_proc_lake   (lake_object_id),
    CONSTRAINT fk_proc_lake   FOREIGN KEY (lake_object_id)       REFERENCES lake_objects(id) ON DELETE CASCADE,
    CONSTRAINT fk_proc_output FOREIGN KEY (output_lake_object_id) REFERENCES lake_objects(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE extracted_documents (
    id                    BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    source_lake_object_id BIGINT NOT NULL UNIQUE,
    text                  MEDIUMTEXT NOT NULL,
    language              VARCHAR(16),
    page_count            INT,
    extracted_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT fk_ext_lake FOREIGN KEY (source_lake_object_id) REFERENCES lake_objects(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE document_chunks (
    id                  CHAR(36) NOT NULL PRIMARY KEY,
    document_id         BIGINT NOT NULL,
    chunk_index         INT NOT NULL,
    text                MEDIUMTEXT NOT NULL,
    token_count         INT NOT NULL,
    vector_id           VARCHAR(128),
    embed_status        VARCHAR(16) NOT NULL DEFAULT 'pending',
    leased_by_worker_id BIGINT,
    lease_token         VARBINARY(64),
    lease_expires_at    DATETIME(6),
    attempt_count       INT NOT NULL DEFAULT 0,
    created_at          DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uq_chunk_doc_idx (document_id, chunk_index),
    KEY idx_chunks_embed (embed_status),
    CONSTRAINT fk_chunk_doc FOREIGN KEY (document_id) REFERENCES extracted_documents(id) ON DELETE CASCADE,
    CONSTRAINT fk_chunk_worker FOREIGN KEY (leased_by_worker_id) REFERENCES workers(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS document_chunks;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS extracted_documents;
-- +goose StatementEnd
-- +goose StatementBegin
DROP TABLE IF EXISTS processing_jobs;
-- +goose StatementEnd
