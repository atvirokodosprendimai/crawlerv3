// Package pdfproc is a placeholder PDF→text extractor.
//
// Real OCR (tesseract / Tika / unstructured.io) plugs in here later.
// For now Extract returns an empty doc and signals "skip" so the pipeline
// can move on without blocking the rest of the system.
package pdfproc

import (
	"errors"
	"io"
)

// ErrSkip indicates the processor intentionally did not extract text.
var ErrSkip = errors.New("pdf processor: OCR not yet wired")

// Extract reads PDF bytes from r and returns text + page count.
// Stub returns ErrSkip until a real engine is wired in.
func Extract(r io.Reader) (text string, pageCount int, err error) {
	_, _ = io.Copy(io.Discard, r)
	return "", 0, ErrSkip
}
