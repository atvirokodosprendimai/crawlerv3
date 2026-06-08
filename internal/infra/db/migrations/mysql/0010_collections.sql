-- +goose Up
-- +goose StatementBegin
CREATE TABLE collections (
    name          VARCHAR(255) PRIMARY KEY,
    chunk_tokens  INT NOT NULL DEFAULT 2800,
    overlap_prev  INT NOT NULL DEFAULT 400,
    overlap_next  INT NOT NULL DEFAULT 400,
    tokenizer     VARCHAR(64) NOT NULL DEFAULT 'cl100k_base',
    created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE collections;
-- +goose StatementEnd
