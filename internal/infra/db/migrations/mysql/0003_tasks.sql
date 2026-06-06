-- +goose Up
-- +goose StatementBegin
ALTER TABLE processing_jobs
    ADD COLUMN lease_token         VARBINARY(64),
    ADD COLUMN leased_by_worker_id BIGINT,
    ADD COLUMN lease_expires_at    DATETIME(6),
    ADD KEY idx_proc_lease (status, lease_expires_at),
    ADD CONSTRAINT fk_proc_worker FOREIGN KEY (leased_by_worker_id) REFERENCES workers(id) ON DELETE SET NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE processing_jobs
    DROP FOREIGN KEY fk_proc_worker,
    DROP KEY idx_proc_lease,
    DROP COLUMN lease_expires_at,
    DROP COLUMN leased_by_worker_id,
    DROP COLUMN lease_token;
-- +goose StatementEnd
