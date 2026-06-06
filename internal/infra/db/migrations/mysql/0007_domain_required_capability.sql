-- +goose Up
-- +goose StatementBegin
ALTER TABLE domains
    ADD COLUMN required_capability VARCHAR(128),
    ADD KEY idx_domains_req_cap (required_capability);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE domains
    DROP KEY idx_domains_req_cap,
    DROP COLUMN required_capability;
-- +goose StatementEnd
