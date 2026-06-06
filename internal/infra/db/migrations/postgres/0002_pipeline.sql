-- +goose Up
-- +goose StatementBegin
CREATE TABLE processing_jobs (
    id                    BIGSERIAL PRIMARY KEY,
    lake_object_id        BIGINT NOT NULL REFERENCES lake_objects(id) ON DELETE CASCADE,
    processor             TEXT   NOT NULL,
    status                TEXT   NOT NULL DEFAULT 'queued',
    attempt_count         INTEGER NOT NULL DEFAULT 0,
    max_attempts          INTEGER NOT NULL DEFAULT 3,
    last_error            TEXT,
    started_at            TIMESTAMPTZ,
    finished_at           TIMESTAMPTZ,
    output_lake_object_id BIGINT REFERENCES lake_objects(id),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_proc_status ON processing_jobs(status, processor);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_proc_lake   ON processing_jobs(lake_object_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE extracted_documents (
    id                    BIGSERIAL PRIMARY KEY,
    source_lake_object_id BIGINT NOT NULL REFERENCES lake_objects(id) ON DELETE CASCADE,
    text                  TEXT   NOT NULL,
    language              TEXT,
    page_count            INTEGER,
    extracted_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(source_lake_object_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE document_chunks (
    id                  TEXT PRIMARY KEY,
    document_id         BIGINT NOT NULL REFERENCES extracted_documents(id) ON DELETE CASCADE,
    chunk_index         INTEGER NOT NULL,
    text                TEXT   NOT NULL,
    token_count         INTEGER NOT NULL,
    vector_id           TEXT,
    embed_status        TEXT   NOT NULL DEFAULT 'pending',
    leased_by_worker_id BIGINT REFERENCES workers(id),
    lease_token         BYTEA,
    lease_expires_at    TIMESTAMPTZ,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(document_id, chunk_index)
);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_chunks_embed ON document_chunks(embed_status);
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
