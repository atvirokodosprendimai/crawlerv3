-- +goose Up
-- +goose StatementBegin
ALTER TABLE domains             ADD COLUMN embed_collection TEXT;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE extracted_documents ADD COLUMN collection       TEXT;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE INDEX idx_extracted_collection ON extracted_documents(collection);
-- +goose StatementEnd

-- Generalize the legacy docx_to_pdf trigger into a unified office_to_pdf path.
-- +goose StatementBegin
DELETE FROM pipeline_triggers
 WHERE when_event='lake_object_inserted' AND enqueue_kind='docx_to_pdf';
-- +goose StatementEnd

-- +goose StatementBegin
INSERT INTO pipeline_triggers (when_event, when_filter, enqueue_kind) VALUES
('lake_object_inserted', '{"content_type_prefix":"application/vnd.openxmlformats-officedocument.wordprocessingml.document"}', 'office_to_pdf'),
('lake_object_inserted', '{"content_type_prefix":"application/vnd.ms-excel"}', 'office_to_pdf'),
('lake_object_inserted', '{"content_type_prefix":"application/vnd.openxmlformats-officedocument.spreadsheetml"}', 'office_to_pdf'),
('lake_object_inserted', '{"content_type_prefix":"application/vnd.ms-powerpoint"}', 'office_to_pdf'),
('lake_object_inserted', '{"content_type_prefix":"application/vnd.openxmlformats-officedocument.presentationml"}', 'office_to_pdf'),
('lake_object_inserted', '{"content_type_prefix":"application/rtf"}', 'office_to_pdf'),
('lake_object_inserted', '{"content_type_prefix":"application/vnd.oasis.opendocument"}', 'office_to_pdf'),
('lake_object_inserted', '{"content_type_prefix":"text/plain"}', 'text_passthrough'),
('lake_object_inserted', '{"content_type_prefix":"text/csv"}', 'text_passthrough'),
('lake_object_inserted', '{"content_type_prefix":"application/json"}', 'text_passthrough'),
('lake_object_inserted', '{"content_type_prefix":"application/xml"}', 'text_passthrough'),
('lake_object_inserted', '{"content_type_prefix":"text/xml"}', 'text_passthrough');
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DELETE FROM pipeline_triggers
 WHERE enqueue_kind IN ('office_to_pdf','text_passthrough');
-- +goose StatementEnd
-- +goose StatementBegin
DROP INDEX IF EXISTS idx_extracted_collection;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE extracted_documents DROP COLUMN collection;
-- +goose StatementEnd
-- +goose StatementBegin
ALTER TABLE domains DROP COLUMN embed_collection;
-- +goose StatementEnd
