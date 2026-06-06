-- +goose Up
-- +goose StatementBegin
CREATE TABLE capabilities (
    name        VARCHAR(128) PRIMARY KEY,
    description TEXT,
    internal    TINYINT(1)   NOT NULL DEFAULT 0,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);
-- +goose StatementEnd
-- +goose StatementBegin
INSERT INTO capabilities (name, description, internal) VALUES
('html_strip',       'Strip HTML to text; in-process worker',          1),
('text_passthrough', 'Copy text-like bodies as extracted documents; in-process worker', 1),
('pdf_ocr',          'OCR PDF lake objects to extracted text',         0),
('docx_to_pdf',      'Legacy: convert DOCX to PDF; use office_to_pdf', 0),
('office_to_pdf',    'Convert Office formats (docx/xlsx/pptx/odt) to PDF', 0),
('chunk',            'Chunk extracted documents for embedding',        0);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS capabilities;
-- +goose StatementEnd
