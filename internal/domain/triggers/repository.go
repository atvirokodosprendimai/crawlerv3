package triggers

import "context"

// Repository is the persistence port for pipeline triggers.
type Repository interface {
	Create(ctx context.Context, t Trigger) (int64, error)
	List(ctx context.Context) ([]Trigger, error)
	ListByEvent(ctx context.Context, e Event) ([]Trigger, error)
	SetEnabled(ctx context.Context, id int64, enabled bool) error
	Delete(ctx context.Context, id int64) error
}
