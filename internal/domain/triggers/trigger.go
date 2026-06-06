// Package triggers models declarative pipeline routing rules.
//
// A Trigger describes "when event X happens, enqueue processor Y for any row
// matching filter F". This replaces hardcoded MIME→processor mapping with a
// table operators can edit at runtime.
package triggers

import "time"

// Event names fired by the system.
type Event string

const (
	// EvtLakeObjectInserted fires after Service.AcceptResult stores a new blob
	// and indexes it. Payload carries lake_object_id + content_type.
	EvtLakeObjectInserted Event = "lake_object_inserted"

	// EvtBlobProduced fires after a task worker uploads an output blob (e.g.
	// docx_to_pdf produced a PDF). Payload carries the new lake_object_id,
	// content_type, and the producing processor name.
	EvtBlobProduced Event = "blob_produced"
)

// Trigger is one row of pipeline_triggers.
type Trigger struct {
	ID          int64
	WhenEvent   Event
	WhenFilter  string // JSON: { content_type_prefix, source_processor }
	EnqueueKind string // processing.Processor name
	Enabled     bool
	CreatedAt   time.Time
}
