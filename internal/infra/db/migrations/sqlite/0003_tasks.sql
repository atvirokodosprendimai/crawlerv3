-- +goose Up
-- +goose StatementBegin
ALTER TABLE processing_jobs ADD COLUMN lease_token         BLOB;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE processing_jobs ADD COLUMN leased_by_worker_id INTEGER REFERENCES workers(id);
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE processing_jobs ADD COLUMN lease_expires_at    DATETIME;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_proc_lease ON processing_jobs(status, lease_expires_at);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_proc_lease;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE processing_jobs DROP COLUMN lease_expires_at;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE processing_jobs DROP COLUMN leased_by_worker_id;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE processing_jobs DROP COLUMN lease_token;
-- +goose StatementEnd
