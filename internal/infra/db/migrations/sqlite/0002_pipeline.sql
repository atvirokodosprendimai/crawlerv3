-- +goose Up
-- +goose StatementBegin
CREATE TABLE processing_jobs (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    lake_object_id        INTEGER NOT NULL REFERENCES lake_objects(id) ON DELETE CASCADE,
    processor             TEXT    NOT NULL,
    status                TEXT    NOT NULL DEFAULT 'queued',
    attempt_count         INTEGER NOT NULL DEFAULT 0,
    max_attempts          INTEGER NOT NULL DEFAULT 3,
    last_error            TEXT,
    started_at            DATETIME,
    finished_at           DATETIME,
    output_lake_object_id INTEGER REFERENCES lake_objects(id),
    created_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_proc_status     ON processing_jobs(status, processor);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_proc_lake       ON processing_jobs(lake_object_id);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE extracted_documents (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    source_lake_object_id INTEGER NOT NULL REFERENCES lake_objects(id) ON DELETE CASCADE,
    text                  TEXT    NOT NULL,
    language              TEXT,
    page_count            INTEGER,
    extracted_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_lake_object_id)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE document_chunks (
    id                  TEXT    PRIMARY KEY,
    document_id         INTEGER NOT NULL REFERENCES extracted_documents(id) ON DELETE CASCADE,
    chunk_index         INTEGER NOT NULL,
    text                TEXT    NOT NULL,
    token_count         INTEGER NOT NULL,
    vector_id           TEXT,
    embed_status        TEXT    NOT NULL DEFAULT 'pending',
    leased_by_worker_id INTEGER REFERENCES workers(id),
    lease_token         BLOB,
    lease_expires_at    DATETIME,
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
