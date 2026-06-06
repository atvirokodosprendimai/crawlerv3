-- +goose Up
-- +goose StatementBegin
ALTER TABLE workers ADD COLUMN capabilities   TEXT;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE workers ADD COLUMN max_concurrent INTEGER NOT NULL DEFAULT 4;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE workers ADD COLUMN last_seen_at   TIMESTAMPTZ;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE workers DROP COLUMN last_seen_at;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE workers DROP COLUMN max_concurrent;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE workers DROP COLUMN capabilities;
-- +goose StatementEnd
