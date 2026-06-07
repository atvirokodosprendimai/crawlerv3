package gormrepo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
)

// ChunkRepo implements chunking.Repository.
type ChunkRepo struct{ DB *rwdb.DB }

// NewChunkRepo wires a ChunkRepo to rwdb.
func NewChunkRepo(db *rwdb.DB) *ChunkRepo { return &ChunkRepo{DB: db} }

// InsertMany bulk-inserts chunks; ignores rows whose (document_id,chunk_index) is taken.
func (r *ChunkRepo) InsertMany(ctx context.Context, chunks []chunking.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	rows := make([]DocumentChunk, 0, len(chunks))
	for _, c := range chunks {
		rows = append(rows, DocumentChunk{
			ID:          c.ID,
			DocumentID:  c.DocumentID,
			ChunkIndex:  c.ChunkIndex,
			Text:        c.Text,
			TokenCount:  c.TokenCount,
			EmbedStatus: string(chunking.EmbedPending),
			CreatedAt:   time.Now().UTC(),
		})
	}
	return r.DB.W.WithContext(ctx).
		Session(&gorm.Session{CreateBatchSize: 500}).
		Create(&rows).Error
}

// ReplaceByDocument deletes every document_chunks row attached to documentID
// and inserts the provided fresh slice in one WriteTX. Returns the old chunk
// IDs in pre-delete order so the caller can drive a downstream Qdrant point
// delete after the DB commit lands.
//
// Used by the rechunk operator command. The DB transaction guarantees a
// crash mid-document leaves the row set in either fully-old or fully-new
// state, never a mix. If fresh is empty, the document ends up with no
// chunks (legal — document.Text might have been emptied externally).
func (r *ChunkRepo) ReplaceByDocument(ctx context.Context, documentID int64, fresh []chunking.Chunk) ([]string, error) {
	var oldIDs []string
	err := r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		// Collect old IDs for the Qdrant cleanup hand-off.
		var ids []string
		if err := tx.Model(&DocumentChunk{}).
			Where("document_id = ?", documentID).
			Order("chunk_index ASC").
			Pluck("id", &ids).Error; err != nil {
			return fmt.Errorf("rechunk: list old ids: %w", err)
		}
		// Delete in one go — same transaction so concurrent embed reserves
		// cannot pick up a chunk that is about to disappear.
		if err := tx.Where("document_id = ?", documentID).
			Delete(&DocumentChunk{}).Error; err != nil {
			return fmt.Errorf("rechunk: delete old: %w", err)
		}
		oldIDs = ids
		if len(fresh) == 0 {
			return nil
		}
		rows := make([]DocumentChunk, 0, len(fresh))
		now := time.Now().UTC()
		for _, c := range fresh {
			rows = append(rows, DocumentChunk{
				ID:          c.ID,
				DocumentID:  c.DocumentID,
				ChunkIndex:  c.ChunkIndex,
				Text:        c.Text,
				TokenCount:  c.TokenCount,
				EmbedStatus: string(chunking.EmbedPending),
				CreatedAt:   now,
			})
		}
		if err := tx.Session(&gorm.Session{CreateBatchSize: 500}).
			Create(&rows).Error; err != nil {
			return fmt.Errorf("rechunk: insert new: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return oldIDs, nil
}

// ReserveBatch leases up to batch chunks for the embed worker.
// Each returned chunk carries its source document's collection (from
// extracted_documents.collection); empty when no per-domain hint is set.
func (r *ChunkRepo) ReserveBatch(
	ctx context.Context,
	workerID int64,
	batch int,
	leaseTTL time.Duration,
	signLease func(chunkUUID string, expires time.Time) (string, []byte),
) ([]chunking.LeasedChunk, error) {
	if batch <= 0 {
		return nil, nil
	}
	now := time.Now().UTC()
	expires := now.Add(leaseTTL)
	var leased []chunking.LeasedChunk

	err := r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		var picks []DocumentChunk
		if err := tx.Where("embed_status = ?", string(chunking.EmbedPending)).
			Order("created_at ASC").Limit(batch).Find(&picks).Error; err != nil {
			return fmt.Errorf("reserve chunks: pick: %w", err)
		}
		if len(picks) == 0 {
			return nil
		}
		// Collect distinct document_ids to join collection in one read.
		docIDs := make([]int64, 0, len(picks))
		seen := map[int64]struct{}{}
		for _, c := range picks {
			if _, ok := seen[c.DocumentID]; ok {
				continue
			}
			seen[c.DocumentID] = struct{}{}
			docIDs = append(docIDs, c.DocumentID)
		}
		type docCol struct {
			ID         int64   `gorm:"column:id"`
			Collection *string `gorm:"column:collection"`
		}
		var docCols []docCol
		if err := tx.Table("extracted_documents").Select("id, collection").
			Where("id IN ?", docIDs).Scan(&docCols).Error; err != nil {
			return fmt.Errorf("reserve chunks: join collection: %w", err)
		}
		collectionByDoc := make(map[int64]string, len(docCols))
		for _, dc := range docCols {
			if dc.Collection != nil {
				collectionByDoc[dc.ID] = *dc.Collection
			}
		}
		for _, c := range picks {
			tok, raw := signLease(c.ID, expires)
			res := tx.Exec(`
UPDATE document_chunks
   SET embed_status = 'leased',
       leased_by_worker_id = ?,
       lease_token = ?,
       lease_expires_at = ?,
       attempt_count = attempt_count + 1
 WHERE id = ? AND embed_status = 'pending'`,
				workerID, raw, expires, c.ID)
			if res.Error != nil {
				return res.Error
			}
			if res.RowsAffected == 0 {
				continue
			}
			leased = append(leased, chunking.LeasedChunk{
				Chunk: chunking.Chunk{
					ID:          c.ID,
					DocumentID:  c.DocumentID,
					ChunkIndex:  c.ChunkIndex,
					Text:        c.Text,
					TokenCount:  c.TokenCount,
					EmbedStatus: chunking.EmbedLeased,
					Collection:  collectionByDoc[c.DocumentID],
				},
				Lease: chunking.Lease{
					ChunkID:   c.ID,
					Token:     tok,
					ExpiresAt: expires,
				},
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return leased, nil
}

// MarkEmbedded records a successful embed.
func (r *ChunkRepo) MarkEmbedded(ctx context.Context, chunkID, vectorID string, leaseToken []byte) error {
	res := r.DB.W.WithContext(ctx).Exec(`
UPDATE document_chunks
   SET embed_status = 'done',
       vector_id = ?,
       lease_token = NULL,
       lease_expires_at = NULL,
       leased_by_worker_id = NULL
 WHERE id = ? AND embed_status = 'leased' AND lease_token = ?`,
		vectorID, chunkID, leaseToken)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("mark embedded: lease not held")
	}
	return nil
}

// MarkEmbedFailed re-queues or fails based on attempt count.
func (r *ChunkRepo) MarkEmbedFailed(ctx context.Context, chunkID string, leaseToken []byte, reason string) error {
	return r.DB.WriteTX(ctx, func(tx *rwdb.Tx) error {
		var m DocumentChunk
		if err := tx.Where("id = ?", chunkID).First(&m).Error; err != nil {
			return err
		}
		if len(m.LeaseToken) == 0 || !equalBytes(m.LeaseToken, leaseToken) {
			return errors.New("mark failed: lease not held")
		}
		newStatus := string(chunking.EmbedPending)
		if m.AttemptCount >= 3 {
			newStatus = string(chunking.EmbedFailed)
		}
		updates := map[string]interface{}{
			"embed_status":        newStatus,
			"lease_token":         nil,
			"lease_expires_at":    nil,
			"leased_by_worker_id": nil,
		}
		return tx.Model(&DocumentChunk{}).Where("id = ?", chunkID).Updates(updates).Error
	})
}

// SweepExpired re-queues stuck chunk leases.
func (r *ChunkRepo) SweepExpired(ctx context.Context, now time.Time) (int64, error) {
	res := r.DB.W.WithContext(ctx).Exec(`
UPDATE document_chunks
   SET embed_status = 'pending',
       lease_token = NULL,
       lease_expires_at = NULL,
       leased_by_worker_id = NULL
 WHERE embed_status = 'leased' AND lease_expires_at < ?`, now)
	return res.RowsAffected, res.Error
}

// ListSince pages through chunks by created_at, optionally filtered by embed status.
// Cursor is created_at — pass time.Time{} on first call, then the last row's CreatedAt.
func (r *ChunkRepo) ListSince(ctx context.Context, embedStatus chunking.EmbedStatus, sinceCreatedAt time.Time, limit int) ([]chunking.Chunk, error) {
	if limit <= 0 {
		limit = 100
	}
	q := r.DB.R.WithContext(ctx).Model(&DocumentChunk{}).Where("created_at > ?", sinceCreatedAt)
	if embedStatus != "" {
		q = q.Where("embed_status = ?", string(embedStatus))
	}
	var ms []DocumentChunk
	if err := q.Order("created_at ASC, id ASC").Limit(limit).Find(&ms).Error; err != nil {
		return nil, err
	}
	out := make([]chunking.Chunk, 0, len(ms))
	for _, m := range ms {
		c := chunking.Chunk{
			ID:          m.ID,
			DocumentID:  m.DocumentID,
			ChunkIndex:  m.ChunkIndex,
			Text:        m.Text,
			TokenCount:  m.TokenCount,
			EmbedStatus: chunking.EmbedStatus(m.EmbedStatus),
		}
		if m.VectorID != nil {
			c.VectorID = *m.VectorID
		}
		out = append(out, c)
	}
	return out, nil
}

// RequeueByFilter flips matching rows back to 'pending' and clears their
// lease columns. Returns the number of rows updated.
//
// All fields are AND-ed. Empty Status means "no status constraint" — the CLI
// is responsible for requiring at least one filter to avoid mass-requeue
// accidents.
func (r *ChunkRepo) RequeueByFilter(ctx context.Context, f chunking.RequeueFilter) (int64, error) {
	q := r.DB.W.WithContext(ctx).Model(&DocumentChunk{})
	if f.Status != "" {
		q = q.Where("embed_status = ?", string(f.Status))
	}
	if f.WorkerID > 0 {
		q = q.Where("leased_by_worker_id = ?", f.WorkerID)
	}
	if f.DocumentID > 0 {
		q = q.Where("document_id = ?", f.DocumentID)
	}
	res := q.Updates(map[string]interface{}{
		"embed_status":        string(chunking.EmbedPending),
		"lease_token":         nil,
		"leased_by_worker_id": nil,
		"lease_expires_at":    nil,
	})
	return res.RowsAffected, res.Error
}

// StatusCounts returns a histogram of embed_status across all chunks.
func (r *ChunkRepo) StatusCounts(ctx context.Context) (map[string]int64, error) {
	type row struct {
		Status string `gorm:"column:s"`
		N      int64  `gorm:"column:n"`
	}
	var rows []row
	if err := r.DB.R.WithContext(ctx).Raw(`
SELECT embed_status AS s, COUNT(*) AS n FROM document_chunks GROUP BY embed_status`).Scan(&rows).Error; err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		out[r.Status] = r.N
	}
	return out, nil
}

// CountPending counts chunks awaiting embedding.
func (r *ChunkRepo) CountPending(ctx context.Context) (int64, error) {
	var n int64
	err := r.DB.R.WithContext(ctx).Model(&DocumentChunk{}).
		Where("embed_status = ?", string(chunking.EmbedPending)).Count(&n).Error
	return n, err
}

// GetContext joins the chain document_chunks → extracted_documents →
// lake_objects → crawl_frontier in a single read. Used by the embed-result
// path to build a Qdrant point payload.
func (r *ChunkRepo) GetContext(ctx context.Context, chunkID string) (*chunking.Context, error) {
	type row struct {
		ChunkID      string  `gorm:"column:chunk_id"`
		Text         string  `gorm:"column:text"`
		ChunkIndex   int     `gorm:"column:chunk_index"`
		DocumentID   int64   `gorm:"column:document_id"`
		Collection   *string `gorm:"column:collection"`
		LakeObjectID int64   `gorm:"column:lake_object_id"`
		CanonicalURL string  `gorm:"column:canonical_url"`
	}
	var rr row
	err := r.DB.R.WithContext(ctx).Raw(`
SELECT c.id   AS chunk_id,
       c.text AS text,
       c.chunk_index AS chunk_index,
       c.document_id AS document_id,
       d.collection  AS collection,
       d.source_lake_object_id AS lake_object_id,
       cf.canonical_url AS canonical_url
  FROM document_chunks    c
  JOIN extracted_documents d  ON d.id      = c.document_id
  JOIN lake_objects        lo ON lo.id     = d.source_lake_object_id
  JOIN crawl_frontier      cf ON cf.url_hash = lo.url_hash
 WHERE c.id = ?`, chunkID).Scan(&rr).Error
	if err != nil {
		return nil, err
	}
	if rr.ChunkID == "" {
		return nil, nil
	}
	c := &chunking.Context{
		ChunkID: rr.ChunkID, Text: rr.Text, ChunkIndex: rr.ChunkIndex,
		DocumentID: rr.DocumentID, LakeObjectID: rr.LakeObjectID,
		CanonicalURL: rr.CanonicalURL,
	}
	if rr.Collection != nil {
		c.Collection = *rr.Collection
	}
	return c, nil
}
