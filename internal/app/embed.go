package app

import (
	"context"
	"fmt"
	"time"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/lease"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/qdrant"
)

// EmbedSvc is a small facade for chunk reservation + result acceptance.
//
// It is separate from Service so the registry can run with or without
// the embed feature enabled.
type EmbedSvc struct {
	Cfg    Config
	Chunks chunking.Repository
	Lease  *lease.Signer
	Qdrant *qdrant.Client // optional; when set, vector results are pushed to Qdrant
}

// NewEmbedSvc constructs an EmbedSvc.
func NewEmbedSvc(cfg Config, c chunking.Repository, s *lease.Signer) *EmbedSvc {
	return &EmbedSvc{Cfg: cfg, Chunks: c, Lease: s}
}

// SetQdrant wires an optional Qdrant client. With it set, AcceptVectorResult
// auto-creates collections and upserts points server-side.
func (e *EmbedSvc) SetQdrant(q *qdrant.Client) { e.Qdrant = q }

// Reserve leases up to batch pending chunks for an embed worker.
func (e *EmbedSvc) Reserve(ctx context.Context, workerID int64, batch int) ([]chunking.LeasedChunk, error) {
	if batch <= 0 {
		batch = 1000
	}
	sign := func(chunkUUID string, expires time.Time) (string, []byte) {
		return e.Lease.SignChunk(chunkUUID, workerID, expires)
	}
	return e.Chunks.ReserveBatch(ctx, workerID, batch, e.Cfg.LeaseTTL, sign)
}

// AcceptResult records a vector_id for a successfully-embedded chunk.
// Used by legacy embed workers that handle the vector store themselves.
func (e *EmbedSvc) AcceptResult(ctx context.Context, chunkID, vectorID, token string) error {
	if _, _, err := e.Lease.VerifyChunk(token, chunkID); err != nil {
		return fmt.Errorf("embed result: %w", err)
	}
	raw, err := lease.Raw(token)
	if err != nil {
		return err
	}
	return e.Chunks.MarkEmbedded(ctx, chunkID, vectorID, raw)
}

// AcceptVectorResult takes the embed vector itself, pushes it to Qdrant
// (auto-creating the collection if needed), and records the resulting
// canonical vector_id on the chunk.
//
// Fails fast (without marking the chunk done) when the Qdrant upsert fails,
// so the lease can expire and another embed worker can retry. When no Qdrant
// is configured, falls back to recording vector_id = "raw:{chunk_id}" — the
// vector is effectively dropped, useful only for tests.
func (e *EmbedSvc) AcceptVectorResult(ctx context.Context, chunkID, token string, vector []float32) error {
	if len(vector) == 0 {
		return fmt.Errorf("embed result: empty vector")
	}
	if _, _, err := e.Lease.VerifyChunk(token, chunkID); err != nil {
		return fmt.Errorf("embed result: %w", err)
	}
	raw, err := lease.Raw(token)
	if err != nil {
		return err
	}
	cc, err := e.Chunks.GetContext(ctx, chunkID)
	if err != nil {
		return fmt.Errorf("embed result: load context: %w", err)
	}
	if cc == nil {
		return fmt.Errorf("embed result: chunk %s not found", chunkID)
	}
	collection := cc.Collection
	if collection == "" {
		collection = "_default"
	}
	vectorID := "raw:" + chunkID
	if e.Qdrant != nil && e.Qdrant.Enabled() {
		if err := e.Qdrant.EnsureCollection(ctx, collection, len(vector)); err != nil {
			return fmt.Errorf("embed result: ensure collection: %w", err)
		}
		payload := map[string]any{
			"lake_object_id": cc.LakeObjectID,
			"document_id":    cc.DocumentID,
			"chunk_index":    cc.ChunkIndex,
			"text":           cc.Text,
			"url":            cc.CanonicalURL,
			"collection":     collection,
		}
		if err := e.Qdrant.Upsert(ctx, collection, []qdrant.Point{{
			ID: chunkID, Vector: vector, Payload: payload,
		}}); err != nil {
			return fmt.Errorf("embed result: qdrant upsert: %w", err)
		}
		vectorID = "qdrant:" + collection + ":" + chunkID
	}
	return e.Chunks.MarkEmbedded(ctx, chunkID, vectorID, raw)
}

// AcceptFailure marks a chunk as needing retry (or final fail).
func (e *EmbedSvc) AcceptFailure(ctx context.Context, chunkID, token, reason string) error {
	if _, _, err := e.Lease.VerifyChunk(token, chunkID); err != nil {
		return fmt.Errorf("embed fail: %w", err)
	}
	raw, err := lease.Raw(token)
	if err != nil {
		return err
	}
	return e.Chunks.MarkEmbedFailed(ctx, chunkID, raw, reason)
}

// SweepExpired re-queues stuck chunk leases.
func (e *EmbedSvc) SweepExpired(ctx context.Context) (int64, error) {
	return e.Chunks.SweepExpired(ctx, time.Now().UTC())
}
