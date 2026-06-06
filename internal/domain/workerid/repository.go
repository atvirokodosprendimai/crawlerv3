package workerid

import "context"

// Repository is the persistence port for workers (PAT holders).
type Repository interface {
	Create(ctx context.Context, w Worker) (int64, error)
	FindByPATHash(ctx context.Context, patHash []byte) (*Worker, error)
	FindByID(ctx context.Context, id int64) (*Worker, error)
	TouchIP(ctx context.Context, id int64, ip string) error
	List(ctx context.Context) ([]Worker, error)
	UpdateCapabilities(ctx context.Context, id int64, caps []string) error
	UpdateMaxConcurrent(ctx context.Context, id int64, n int) error
	Ban(ctx context.Context, id int64) error
	Unban(ctx context.Context, id int64) error
	CountHeldLeases(ctx context.Context, id int64) (int64, error)
}
