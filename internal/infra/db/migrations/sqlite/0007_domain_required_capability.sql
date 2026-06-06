-- +goose Up
-- +goose StatementBegin
ALTER TABLE domains ADD COLUMN required_capability TEXT;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_domains_req_cap ON domains(required_capability);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_domains_req_cap;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE domains DROP COLUMN required_capability;
-- +goose StatementEnd
