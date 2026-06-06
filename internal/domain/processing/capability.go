package processing

import (
	"context"
	"time"
)

// Capability is a row of the capabilities catalog. It documents which
// processor kinds the system knows about for `list-capabilities` and
// operator discoverability. It does NOT gate PAT issuance or task
// reservation — those accept any string (workers self-declare via PAT
// capabilities, routing is declarative via pipeline_triggers).
type Capability struct {
	Name        string
	Description string
	Internal    bool // true if served by the registry binary's in-process worker
	CreatedAt   time.Time
}

// CapabilityRepo is the persistence port for the capabilities catalog.
type CapabilityRepo interface {
	List(ctx context.Context) ([]Capability, error)
	Get(ctx context.Context, name string) (*Capability, error)
	Upsert(ctx context.Context, c Capability) error
	Delete(ctx context.Context, name string) error
}
