-- +goose Up
-- +goose StatementBegin
CREATE TABLE capabilities (
    name        TEXT      PRIMARY KEY,
    description TEXT,
    internal    BOOLEAN   NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd
-- +goose StatementBegin
INSERT INTO capabilities (name, description, internal) VALUES
('html_strip',       'Strip HTML to text; in-process worker',          TRUE),
('text_passthrough', 'Copy text-like bodies as extracted documents; in-process worker', TRUE),
('pdf_ocr',          'OCR PDF lake objects to extracted text',         FALSE),
('docx_to_pdf',      'Legacy: convert DOCX to PDF; use office_to_pdf', FALSE),
('office_to_pdf',    'Convert Office formats (docx/xlsx/pptx/odt) to PDF', FALSE),
('chunk',            'Chunk extracted documents for embedding',        FALSE);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS capabilities;
-- +goose StatementEnd
