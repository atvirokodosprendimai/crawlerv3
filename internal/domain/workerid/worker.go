// Package workerid models the human-facing worker identity (PAT holder).
// Named "workerid" to avoid clashing with go-routine "worker" concepts.
package workerid

import "time"

// Worker is a PAT-bound external participant.
type Worker struct {
	ID              int64
	PATHash         []byte
	Label           string
	IPLast          string
	ReputationScore int
	BannedAt        *time.Time
	CreatedAt       time.Time
	Capabilities    []string
	MaxConcurrent   int
	LastSeenAt      *time.Time
}

// IsBanned is true when BannedAt is set.
func (w Worker) IsBanned() bool { return w.BannedAt != nil }

// Can returns true when the worker is allowed to handle the given kind.
// An empty Capabilities list grants any kind (backward compat with legacy rows).
func (w Worker) Can(kind string) bool {
	if len(w.Capabilities) == 0 {
		return true
	}
	for _, c := range w.Capabilities {
		if c == kind {
			return true
		}
	}
	return false
}
