package gormrepo

import "time"

// ProcessingJob row.
type ProcessingJob struct {
	ID                 int64      `gorm:"primaryKey;column:id"`
	LakeObjectID       int64      `gorm:"column:lake_object_id"`
	Processor          string     `gorm:"column:processor"`
	Status             string     `gorm:"column:status"`
	AttemptCount       int        `gorm:"column:attempt_count"`
	MaxAttempts        int        `gorm:"column:max_attempts"`
	LastError          *string    `gorm:"column:last_error"`
	StartedAt          *time.Time `gorm:"column:started_at"`
	FinishedAt         *time.Time `gorm:"column:finished_at"`
	OutputLakeObjectID *int64     `gorm:"column:output_lake_object_id"`
	CreatedAt          time.Time  `gorm:"column:created_at"`
	LeaseToken         []byte     `gorm:"column:lease_token"`
	LeasedByWorkerID   *int64     `gorm:"column:leased_by_worker_id"`
	LeaseExpiresAt     *time.Time `gorm:"column:lease_expires_at"`
}

func (ProcessingJob) TableName() string { return "processing_jobs" }

// ExtractedDocument row.
type ExtractedDocument struct {
	ID                 int64     `gorm:"primaryKey;column:id"`
	SourceLakeObjectID int64     `gorm:"column:source_lake_object_id;uniqueIndex"`
	Text               string    `gorm:"column:text"`
	Language           *string   `gorm:"column:language"`
	PageCount          *int      `gorm:"column:page_count"`
	ExtractedAt        time.Time `gorm:"column:extracted_at"`
	Collection         *string   `gorm:"column:collection"`
}

func (ExtractedDocument) TableName() string { return "extracted_documents" }

// DocumentChunk row.
type DocumentChunk struct {
	ID               string     `gorm:"primaryKey;column:id"`
	DocumentID       int64      `gorm:"column:document_id"`
	ChunkIndex       int        `gorm:"column:chunk_index"`
	Text             string     `gorm:"column:text"`
	TokenCount       int        `gorm:"column:token_count"`
	VectorID         *string    `gorm:"column:vector_id"`
	EmbedStatus      string     `gorm:"column:embed_status"`
	LeasedByWorkerID *int64     `gorm:"column:leased_by_worker_id"`
	LeaseToken       []byte     `gorm:"column:lease_token"`
	LeaseExpiresAt   *time.Time `gorm:"column:lease_expires_at"`
	AttemptCount     int        `gorm:"column:attempt_count"`
	CreatedAt        time.Time  `gorm:"column:created_at"`
}

func (DocumentChunk) TableName() string { return "document_chunks" }
