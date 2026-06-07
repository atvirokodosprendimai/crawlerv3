-- +goose Up
-- +goose StatementBegin
CREATE TABLE collections (
    name          TEXT PRIMARY KEY,
    chunk_tokens  INTEGER NOT NULL DEFAULT 2800,
    overlap_prev  INTEGER NOT NULL DEFAULT 400,
    overlap_next  INTEGER NOT NULL DEFAULT 400,
    tokenizer     TEXT NOT NULL DEFAULT 'cl100k_base',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE collections;
-- +goose StatementEnd
