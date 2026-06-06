// Package processing tracks per-asset extraction stages.
package processing

import "time"

// Processor identifies a pipeline stage.
type Processor string

const (
	ProcHTMLStrip       Processor = "html_strip"
	ProcPDFOCR          Processor = "pdf_ocr"
	ProcDOCXToPDF       Processor = "docx_to_pdf"   // legacy; use ProcOfficeToPDF
	ProcOfficeToPDF     Processor = "office_to_pdf"
	ProcTextPassthrough Processor = "text_passthrough"
	ProcChunk           Processor = "chunk"
)

// Status of a processing_jobs row.
type Status string

const (
	StatusQueued  Status = "queued"
	StatusRunning Status = "running"
	StatusDone    Status = "done"
	StatusFailed  Status = "failed"
	StatusSkipped Status = "skipped"
)

// Job is one stage of the pipeline.
type Job struct {
	ID                  int64
	LakeObjectID        int64
	Processor           Processor
	Status              Status
	AttemptCount        int
	MaxAttempts         int
	LastError           string
	StartedAt           *time.Time
	FinishedAt          *time.Time
	OutputLakeObjectID  *int64
}
