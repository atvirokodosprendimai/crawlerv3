package frontier

import "context"

// DomainRow is a minimal projection of the domains table the frontier needs.
type DomainRow struct {
	ID                 int64
	Host               string
	Scheme             string
	IsActive           bool
	CrawlDelayMS       int
	ParallelFetches    int    // max URLs reserved per call from this domain; raise for cooperative hosts
	EmbedCollection    string // optional override for the vector-store collection
	RequiredCapability string // workers must have this capability to reserve URLs of this domain
}

// DomainRepo is the persistence port for crawl-target domains.
type DomainRepo interface {
	UpsertByHost(ctx context.Context, host, scheme string, crawlDelayMS int) (DomainRow, error)
	FindByHost(ctx context.Context, host string) (*DomainRow, error)
	GetByID(ctx context.Context, id int64) (*DomainRow, error)
	List(ctx context.Context) ([]DomainRow, error)
	SetActive(ctx context.Context, host string, active bool) error
	UpdateCrawlDelay(ctx context.Context, host string, ms int) error
	UpdateParallelFetches(ctx context.Context, host string, n int) error
	UpdateScheme(ctx context.Context, host, scheme string) error
	UpdateEmbedCollection(ctx context.Context, host, collection string) error
	UpdateRequiredCapability(ctx context.Context, host, capability string) error
}
