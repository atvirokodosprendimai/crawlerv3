-- +goose Up
-- +goose StatementBegin
CREATE TABLE pipeline_triggers (
    id            BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    when_event    VARCHAR(64) NOT NULL,
    when_filter   TEXT,
    enqueue_kind  VARCHAR(64) NOT NULL,
    enabled       TINYINT(1) NOT NULL DEFAULT 1,
    created_at    DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    KEY idx_triggers_event (when_event, enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- +goose StatementEnd
-- +goose StatementBegin
INSERT INTO pipeline_triggers (when_event, when_filter, enqueue_kind) VALUES
('lake_object_inserted', '{"content_type_prefix":"text/html"}', 'html_strip'),
('lake_object_inserted', '{"content_type_prefix":"application/pdf"}', 'pdf_ocr'),
('lake_object_inserted', '{"content_type_prefix":"application/vnd.openxmlformats-officedocument.wordprocessingml.document"}', 'docx_to_pdf');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS pipeline_triggers;
-- +goose StatementEnd
