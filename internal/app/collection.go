package app

import (
	"context"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/frontier"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
)

// CollectionResolver maps a lake_object_id → its source domain's
// embed_collection (falling back to the domain host when no override is set).
// Returns "" when the chain cannot be resolved — callers must treat empty as
// "let the embed worker decide".
type CollectionResolver struct {
	Lake     lake.Repository
	Frontier frontier.Repository
	Domains  frontier.DomainRepo
}

// NewCollectionResolver wires a resolver.
func NewCollectionResolver(l lake.Repository, f frontier.Repository, d frontier.DomainRepo) *CollectionResolver {
	return &CollectionResolver{Lake: l, Frontier: f, Domains: d}
}

// ResolveForLakeObject walks lake_object → url_hash → frontier → domain to
// return the embed collection hint. Best-effort: any failure returns "".
func (c *CollectionResolver) ResolveForLakeObject(ctx context.Context, lakeID int64) string {
	if c == nil || c.Lake == nil || c.Frontier == nil || c.Domains == nil {
		return ""
	}
	o, err := c.Lake.GetByID(ctx, lakeID)
	if err != nil || o == nil {
		return ""
	}
	domainID, err := c.Frontier.DomainIDByURLHash(ctx, o.URLHash)
	if err != nil {
		return ""
	}
	dom, err := c.Domains.GetByID(ctx, domainID)
	if err != nil || dom == nil {
		return ""
	}
	if dom.EmbedCollection != "" {
		return dom.EmbedCollection
	}
	return dom.Host
}
