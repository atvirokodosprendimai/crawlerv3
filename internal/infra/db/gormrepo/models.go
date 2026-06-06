// Package gormrepo holds gorm-tagged models and Repository implementations.
package gormrepo

import "time"

// Domain row.
type Domain struct {
	ID                 int64      `gorm:"primaryKey;column:id"`
	Host               string     `gorm:"column:host;uniqueIndex"`
	Scheme             string     `gorm:"column:scheme"`
	IsActive           bool       `gorm:"column:is_active"`
	CrawlDelayMS       int        `gorm:"column:crawl_delay_ms"`
	ParallelFetches    int        `gorm:"column:parallel_fetches"`
	RobotsBody         *string    `gorm:"column:robots_body"`
	RobotsFetchedAt    *time.Time `gorm:"column:robots_fetched_at"`
	LastRequestAt      *time.Time `gorm:"column:last_request_at"`
	CreatedAt          time.Time  `gorm:"column:created_at"`
	EmbedCollection    *string    `gorm:"column:embed_collection"`
	RequiredCapability *string    `gorm:"column:required_capability"`
}

func (Domain) TableName() string { return "domains" }

// Worker row.
type Worker struct {
	ID              int64      `gorm:"primaryKey;column:id"`
	PATHash         []byte     `gorm:"column:pat_hash;uniqueIndex"`
	Label           string     `gorm:"column:label"`
	IPLast          *string    `gorm:"column:ip_last"`
	ReputationScore int        `gorm:"column:reputation_score"`
	BannedAt        *time.Time `gorm:"column:banned_at"`
	CreatedAt       time.Time  `gorm:"column:created_at"`
	Capabilities    *string    `gorm:"column:capabilities"`
	MaxConcurrent   int        `gorm:"column:max_concurrent"`
	LastSeenAt      *time.Time `gorm:"column:last_seen_at"`
}

func (Worker) TableName() string { return "workers" }

// Frontier row.
type Frontier struct {
	URLHash          []byte     `gorm:"primaryKey;column:url_hash"`
	URL              string     `gorm:"column:url"`
	CanonicalURL     string     `gorm:"column:canonical_url"`
	DomainID         int64      `gorm:"column:domain_id"`
	Depth            int        `gorm:"column:depth"`
	Priority         int        `gorm:"column:priority"`
	Status           string     `gorm:"column:status"`
	LeasedByWorkerID *int64     `gorm:"column:leased_by_worker_id"`
	LeaseToken       []byte     `gorm:"column:lease_token"`
	LeaseExpiresAt   *time.Time `gorm:"column:lease_expires_at"`
	AttemptCount     int        `gorm:"column:attempt_count"`
	MaxAttempts      int        `gorm:"column:max_attempts"`
	NextRetryAt      *time.Time `gorm:"column:next_retry_at"`
	ScheduledFor     time.Time  `gorm:"column:scheduled_for"`
	DiscoveredAt     time.Time  `gorm:"column:discovered_at"`
	CompletedAt      *time.Time `gorm:"column:completed_at"`
	ParentURLHash    []byte     `gorm:"column:parent_url_hash"`
	HTTPStatus       *int       `gorm:"column:http_status"`
	ErrorCode        *string    `gorm:"column:error_code"`
	ErrorMessage     *string    `gorm:"column:error_message"`
}

func (Frontier) TableName() string { return "crawl_frontier" }

// LakeObject row.
type LakeObject struct {
	ID             int64     `gorm:"primaryKey;column:id"`
	URLHash        []byte    `gorm:"column:url_hash"`
	StorageBackend string    `gorm:"column:storage_backend"`
	StorageKey     string    `gorm:"column:storage_key"`
	ContentType    *string   `gorm:"column:content_type"`
	ContentSHA256  []byte    `gorm:"column:content_sha256"`
	FileSizeBytes  int64     `gorm:"column:file_size_bytes"`
	ArchivedAt     time.Time `gorm:"column:archived_at"`
	MigratedFrom   *string   `gorm:"column:migrated_from"`
}

func (LakeObject) TableName() string { return "lake_objects" }
