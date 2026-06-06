-- +goose Up
-- +goose StatementBegin
ALTER TABLE workers
    ADD COLUMN capabilities   TEXT,
    ADD COLUMN max_concurrent INT NOT NULL DEFAULT 4,
    ADD COLUMN last_seen_at   DATETIME(6);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE workers
    DROP COLUMN last_seen_at,
    DROP COLUMN max_concurrent,
    DROP COLUMN capabilities;
-- +goose StatementEnd
