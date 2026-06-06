package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"

	"github.com/atvirokodosprendimai/crawlerv3/internal/app"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/extraction"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
)

// Router builds the chi router for the registry.
func Router(
	svc *app.Service,
	embed *app.EmbedSvc,
	tasks *app.TaskSvc,
	search *app.SearchSvc,
	workers workerid.Repository,
	lakeRepo lake.Repository,
	blobs lake.BlobStore,
	extractionsRepo extraction.Repository,
	chunksRepo chunking.Repository,
	maxBody int64,
) http.Handler {
	r := chi.NewRouter()
	r.Use(chimw.RequestID)
	r.Use(AccessLog)
	r.Use(chimw.Recoverer)
	r.Use(chimw.Timeout(60 * time.Second))

	h := NewJobsHandler(svc, workers, maxBody)

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	r.Route("/v1", func(r chi.Router) {
		r.Use(PATAuth(workers))
		r.Post("/jobs/reserve", h.Reserve)
		r.Post("/jobs/heartbeat", h.Heartbeat)
		r.Post("/jobs/result", h.Result)
		r.Post("/jobs/fail", h.Fail)
		r.Get("/workers/me", h.Me)

		if embed != nil {
			eh := NewEmbedHandler(embed, workers)
			r.Post("/embed/reserve", eh.Reserve)
			r.Post("/embed/result", eh.Result)
		}

		if tasks != nil {
			th := NewTasksHandler(tasks, workers)
			r.Post("/tasks/reserve", th.Reserve)
			r.Post("/tasks/heartbeat", th.Heartbeat)
			r.Post("/tasks/result", th.Result)
			r.Post("/tasks/fail", th.Fail)
		}

		if lakeRepo != nil && blobs != nil {
			bh := NewBlobsHandler(lakeRepo, blobs)
			r.Get("/blobs/{id}", bh.Get)
		}

		if lakeRepo != nil && extractionsRepo != nil && chunksRepo != nil {
			rh := NewReadsHandler(lakeRepo, extractionsRepo, chunksRepo)
			r.Get("/lake", rh.LakeList)
			r.Get("/extracted", rh.ExtractedList)
			r.Get("/extracted/{id}/text", rh.ExtractedText)
			r.Get("/chunks", rh.ChunksList)
		}

		if search != nil {
			sh := NewSearchHandler(search)
			r.Post("/search", sh.Search)
		}
	})
	return r
}
