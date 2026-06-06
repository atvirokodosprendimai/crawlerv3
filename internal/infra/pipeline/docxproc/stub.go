// Package docxproc is a placeholder DOCX→PDF converter.
//
// Real conversion (libreoffice headless) plugs in here later.
package docxproc

import (
	"errors"
	"io"
)

// ErrSkip indicates the processor intentionally did not convert.
var ErrSkip = errors.New("docx processor: conversion not yet wired")

// ConvertToPDF reads DOCX bytes from r and writes PDF bytes to w.
// Stub returns ErrSkip until libreoffice is wired in.
func ConvertToPDF(r io.Reader, w io.Writer) error {
	_, _ = io.Copy(io.Discard, r)
	return ErrSkip
}
