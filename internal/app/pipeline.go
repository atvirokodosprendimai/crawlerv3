package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/extraction"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/processing"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/chunker"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/docxproc"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/htmlproc"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/pipeline/pdfproc"
)

// Pipeline orchestrates per-MIME processors after a result is accepted.
//
// InternalProcessors lists the kinds the in-process goroutine claims.
// Processors NOT in this list stay in 'queued' for external workers
// (see TaskSvc). Default: html_strip and text_passthrough run internally.
type Pipeline struct {
	Lake               lake.Repository
	Blobs              lake.BlobStore
	Processing         processing.Repository
	Extractions        extraction.Repository
	Chunks             chunking.Repository
	ChunkCfg           chunker.Config
	InternalProcessors []processing.Processor
	Resolver           *CollectionResolver // optional; resolves per-domain embed collection
	FTS                *FTSSvc             // optional; mirrors extracted text into Quickwit (Stanza-rewritten)
}

// NewPipeline wires a Pipeline. By default html_strip and text_passthrough
// run internally; pdf_ocr / office_to_pdf / docx_to_pdf are deferred to
// external task workers.
func NewPipeline(l lake.Repository, b lake.BlobStore, p processing.Repository, e extraction.Repository, c chunking.Repository) *Pipeline {
	return &Pipeline{
		Lake: l, Blobs: b, Processing: p, Extractions: e, Chunks: c,
		ChunkCfg: chunker.Defaults(),
		InternalProcessors: []processing.Processor{
			processing.ProcHTMLStrip,
			processing.ProcTextPassthrough,
		},
	}
}

// SetResolver wires the collection resolver for per-domain embed routing.
func (p *Pipeline) SetResolver(r *CollectionResolver) { p.Resolver = r }

// SetFTS attaches an FTSSvc; when set, the pipeline forwards extracted text
// (Stanza-rewritten) into Quickwit after every extraction upsert. Failures
// are logged inside FTSSvc and never block the pipeline.
func (p *Pipeline) SetFTS(f *FTSSvc) { p.FTS = f }

// Run polls for queued processing_jobs and dispatches them.
// Stops when ctx is cancelled.
func (p *Pipeline) Run(ctx context.Context, tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.drain(ctx)
		}
	}
}

func (p *Pipeline) drain(ctx context.Context) {
	for _, proc := range p.InternalProcessors {
		for {
			job, err := p.Processing.ClaimNext(ctx, proc)
			if err != nil || job == nil {
				break
			}
			p.exec(ctx, job)
		}
	}
}

func (p *Pipeline) exec(ctx context.Context, job *processing.Job) {
	switch job.Processor {
	case processing.ProcHTMLStrip:
		if err := p.execHTML(ctx, job); err != nil {
			_ = p.Processing.MarkFailed(ctx, job.ID, err.Error())
			return
		}
		_ = p.Processing.MarkDone(ctx, job.ID, nil)
	case processing.ProcTextPassthrough:
		if err := p.execTextPassthrough(ctx, job); err != nil {
			_ = p.Processing.MarkFailed(ctx, job.ID, err.Error())
			return
		}
		_ = p.Processing.MarkDone(ctx, job.ID, nil)
	case processing.ProcPDFOCR:
		if err := p.execPDF(ctx, job); err != nil {
			_ = p.Processing.MarkSkipped(ctx, job.ID, err.Error())
			return
		}
		_ = p.Processing.MarkDone(ctx, job.ID, nil)
	case processing.ProcDOCXToPDF, processing.ProcOfficeToPDF:
		_ = p.Processing.MarkSkipped(ctx, job.ID, docxproc.ErrSkip.Error())
	}
}

func (p *Pipeline) execHTML(ctx context.Context, job *processing.Job) error {
	o, err := p.lakeObjectByID(ctx, job.LakeObjectID)
	if err != nil {
		return err
	}
	rc, _, err := p.Blobs.Get(ctx, o.StorageKey)
	if err != nil {
		return fmt.Errorf("blob get: %w", err)
	}
	defer rc.Close()
	text, err := htmlproc.Strip(rc)
	if err != nil {
		return fmt.Errorf("html strip: %w", err)
	}
	if strings.TrimSpace(text) == "" {
		return nil
	}
	collection := p.resolveCollection(ctx, job.LakeObjectID)
	docID, err := p.Extractions.Upsert(ctx, extraction.Document{
		SourceLakeObjectID: job.LakeObjectID,
		Text:               text,
		Collection:         collection,
	})
	if err != nil {
		return fmt.Errorf("extracted upsert: %w", err)
	}
	if p.FTS != nil {
		p.FTS.OnExtracted(ctx, docID, job.LakeObjectID, collection, text)
	}
	return p.writeChunks(ctx, docID, text)
}

// execTextPassthrough treats the raw blob as UTF-8 text and writes it directly
// into extracted_documents. Used for text/plain, text/csv, application/json,
// application/xml, text/xml — anything where the body IS already the text.
func (p *Pipeline) execTextPassthrough(ctx context.Context, job *processing.Job) error {
	o, err := p.lakeObjectByID(ctx, job.LakeObjectID)
	if err != nil {
		return err
	}
	rc, _, err := p.Blobs.Get(ctx, o.StorageKey)
	if err != nil {
		return fmt.Errorf("blob get: %w", err)
	}
	defer rc.Close()
	body, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("blob read: %w", err)
	}
	text := string(body)
	if strings.TrimSpace(text) == "" {
		return nil
	}
	collection := p.resolveCollection(ctx, job.LakeObjectID)
	docID, err := p.Extractions.Upsert(ctx, extraction.Document{
		SourceLakeObjectID: job.LakeObjectID,
		Text:               text,
		Collection:         collection,
	})
	if err != nil {
		return fmt.Errorf("extracted upsert: %w", err)
	}
	if p.FTS != nil {
		p.FTS.OnExtracted(ctx, docID, job.LakeObjectID, collection, text)
	}
	return p.writeChunks(ctx, docID, text)
}

// resolveCollection returns the embed-collection hint for a lake object.
// Returns "" when no resolver is wired or any lookup fails.
func (p *Pipeline) resolveCollection(ctx context.Context, lakeID int64) string {
	if p.Resolver == nil {
		return ""
	}
	return p.Resolver.ResolveForLakeObject(ctx, lakeID)
}

func (p *Pipeline) execPDF(ctx context.Context, job *processing.Job) error {
	o, err := p.lakeObjectByID(ctx, job.LakeObjectID)
	if err != nil {
		return err
	}
	rc, _, err := p.Blobs.Get(ctx, o.StorageKey)
	if err != nil {
		return fmt.Errorf("blob get: %w", err)
	}
	defer rc.Close()
	text, pages, err := pdfproc.Extract(rc)
	if err != nil {
		return err // ErrSkip turns into MarkSkipped in caller
	}
	collection := p.resolveCollection(ctx, job.LakeObjectID)
	docID, err := p.Extractions.Upsert(ctx, extraction.Document{
		SourceLakeObjectID: job.LakeObjectID,
		Text:               text,
		PageCount:          pages,
		Collection:         collection,
	})
	if err != nil {
		return err
	}
	if p.FTS != nil {
		p.FTS.OnExtracted(ctx, docID, job.LakeObjectID, collection, text)
	}
	return p.writeChunks(ctx, docID, text)
}

func (p *Pipeline) writeChunks(ctx context.Context, docID int64, text string) error {
	pieces := chunker.Split(text, p.ChunkCfg)
	if len(pieces) == 0 {
		return nil
	}
	rows := make([]chunking.Chunk, 0, len(pieces))
	for _, c := range pieces {
		rows = append(rows, chunking.Chunk{
			ID:         newUUID(),
			DocumentID: docID,
			ChunkIndex: c.Index,
			Text:       c.Text,
			TokenCount: c.WordCount,
		})
	}
	return p.Chunks.InsertMany(ctx, rows)
}

func (p *Pipeline) lakeObjectByID(ctx context.Context, id int64) (*lake.Object, error) {
	o, err := p.Lake.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if o == nil {
		return nil, fmt.Errorf("lake object %d not found", id)
	}
	return o, nil
}

// newUUID returns a random 36-char UUID v4.
func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	out := make([]byte, 36)
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out)
}

