-- +goose Up
-- +goose StatementBegin
CREATE TABLE pipeline_triggers (
    id            BIGSERIAL PRIMARY KEY,
    when_event    TEXT NOT NULL,
    when_filter   TEXT,
    enqueue_kind  TEXT NOT NULL,
    enabled       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_triggers_event ON pipeline_triggers(when_event, enabled);
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
