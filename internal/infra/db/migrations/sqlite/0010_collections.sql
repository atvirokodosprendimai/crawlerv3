-- +goose Up
-- +goose StatementBegin
CREATE TABLE collections (
    name          TEXT PRIMARY KEY,
    chunk_tokens  INTEGER NOT NULL DEFAULT 2800,
    overlap_prev  INTEGER NOT NULL DEFAULT 400,
    overlap_next  INTEGER NOT NULL DEFAULT 400,
    tokenizer     TEXT    NOT NULL DEFAULT 'cl100k_base',
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE collections;
-- +goose StatementEnd
