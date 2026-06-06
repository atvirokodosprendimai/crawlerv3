package app

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/processing"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/triggers"
)

// TriggerDispatcher fires events on writes and enqueues processing_jobs
// rows according to matching pipeline_triggers rows.
//
// The trigger set is cached for cacheTTL to avoid hitting the DB on every
// event. CLI mutations take effect within at most cacheTTL.
type TriggerDispatcher struct {
	Triggers   triggers.Repository
	Processing processing.Repository

	cacheTTL time.Duration
	mu       sync.RWMutex
	cache    map[triggers.Event][]triggers.Trigger
	expires  time.Time
}

// NewTriggerDispatcher constructs a dispatcher.
func NewTriggerDispatcher(t triggers.Repository, p processing.Repository) *TriggerDispatcher {
	return &TriggerDispatcher{Triggers: t, Processing: p, cacheTTL: 5 * time.Second}
}

// EventPayload carries fields used by trigger filters and dispatch.
type EventPayload struct {
	LakeObjectID    int64
	ContentType     string
	SourceProcessor string
}

// Fire evaluates all matching triggers for evt and enqueues processing rows.
// Best-effort: errors are swallowed (logged elsewhere) to avoid blocking the
// caller's write path.
func (d *TriggerDispatcher) Fire(ctx context.Context, evt triggers.Event, p EventPayload) {
	trs := d.lookup(ctx, evt)
	for _, t := range trs {
		if !matches(t, p) {
			continue
		}
		_, _ = d.Processing.Enqueue(ctx, p.LakeObjectID, processing.Processor(t.EnqueueKind))
	}
}

func (d *TriggerDispatcher) lookup(ctx context.Context, evt triggers.Event) []triggers.Trigger {
	d.mu.RLock()
	if !time.Now().After(d.expires) {
		ts, ok := d.cache[evt]
		d.mu.RUnlock()
		if ok {
			return ts
		}
	} else {
		d.mu.RUnlock()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if time.Now().After(d.expires) {
		all, err := d.Triggers.List(ctx)
		if err != nil {
			return nil
		}
		fresh := make(map[triggers.Event][]triggers.Trigger)
		for _, t := range all {
			if !t.Enabled {
				continue
			}
			fresh[t.WhenEvent] = append(fresh[t.WhenEvent], t)
		}
		d.cache = fresh
		d.expires = time.Now().Add(d.cacheTTL)
	}
	return d.cache[evt]
}

// matches evaluates a trigger's filter against an event payload.
func matches(t triggers.Trigger, p EventPayload) bool {
	if t.WhenFilter == "" {
		return true
	}
	var f struct {
		ContentTypePrefix string `json:"content_type_prefix"`
		SourceProcessor   string `json:"source_processor"`
	}
	if err := json.Unmarshal([]byte(t.WhenFilter), &f); err != nil {
		return false
	}
	if f.ContentTypePrefix != "" {
		if !strings.HasPrefix(strings.ToLower(p.ContentType), strings.ToLower(f.ContentTypePrefix)) {
			return false
		}
	}
	if f.SourceProcessor != "" && f.SourceProcessor != p.SourceProcessor {
		return false
	}
	return true
}
