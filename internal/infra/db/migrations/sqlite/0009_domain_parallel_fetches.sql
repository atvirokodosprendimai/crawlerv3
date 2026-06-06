-- +goose Up
-- +goose StatementBegin
ALTER TABLE domains ADD COLUMN parallel_fetches INTEGER NOT NULL DEFAULT 1;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE domains DROP COLUMN parallel_fetches;
-- +goose StatementEnd
