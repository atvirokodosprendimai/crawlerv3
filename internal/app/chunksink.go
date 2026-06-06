package app

import (
	"context"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
)

// ChunkRepoSink adapts a chunking.Repository to the internal chunkInserter
// interface used by TaskSvc. Keeps TaskSvc decoupled from the chunking domain.
type ChunkRepoSink struct{ Repo chunking.Repository }

// InsertMany converts chunkRow → chunking.Chunk and persists via Repo.
func (s *ChunkRepoSink) InsertMany(ctx context.Context, rows []chunkRow) error {
	if len(rows) == 0 {
		return nil
	}
	out := make([]chunking.Chunk, 0, len(rows))
	for _, r := range rows {
		out = append(out, chunking.Chunk{
			ID:         r.ID,
			DocumentID: r.DocumentID,
			ChunkIndex: r.ChunkIndex,
			Text:       r.Text,
			TokenCount: r.TokenCount,
		})
	}
	return s.Repo.InsertMany(ctx, out)
}
