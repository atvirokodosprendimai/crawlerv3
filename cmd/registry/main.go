// Registry is the central crawler control plane: queue, lease, blob index.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"sort"
	"strings"
	"syscall"
	"time"

	goose "github.com/pressly/goose/v3"
	cli "github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/crawlerv3/internal/app"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/chunking"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/frontier"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/processing"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/triggers"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/gormrepo"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/migrations"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
	httpapi "github.com/atvirokodosprendimai/crawlerv3/internal/infra/http"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/embedclient"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/lease"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/qdrant"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/store/local"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/urls"
)

func main() {
	cmd := &cli.Command{
		Name:  "registry",
		Usage: "crawler control plane (queue, lease, blob index)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "db-driver", Value: "sqlite", Sources: cli.EnvVars("DB_DRIVER")},
			&cli.StringFlag{Name: "db-dsn", Value: "crawler.db", Sources: cli.EnvVars("DB_DSN")},
			&cli.StringFlag{Name: "read-dsn", Sources: cli.EnvVars("READ_DSN")},
			&cli.StringFlag{Name: "blobs-root", Value: "./blobs", Sources: cli.EnvVars("BLOBS_ROOT")},
			&cli.StringFlag{Name: "lease-secret", Sources: cli.EnvVars("LEASE_SECRET"), Usage: "HMAC secret (base64, >=16 bytes raw)"},
			&cli.Int64Flag{Name: "max-body-bytes", Value: 200 * 1024 * 1024, Sources: cli.EnvVars("MAX_BODY_BYTES")},
			&cli.BoolFlag{Name: "debug", Sources: cli.EnvVars("DEBUG")},

			// Qdrant vector store (slice 10) — optional.
			&cli.StringFlag{Name: "qdrant-url", Sources: cli.EnvVars("QDRANT_URL"),
				Usage: "Qdrant base URL (e.g. http://localhost:6333). Empty disables vector push/search."},
			&cli.StringFlag{Name: "qdrant-api-key", Sources: cli.EnvVars("QDRANT_API_KEY")},
			&cli.IntFlag{Name: "qdrant-shards", Value: 9, Sources: cli.EnvVars("QDRANT_SHARDS"),
				Usage: "shard_number on collection auto-create"},
			&cli.StringFlag{Name: "qdrant-distance", Value: "Cosine", Sources: cli.EnvVars("QDRANT_DISTANCE"),
				Usage: "Cosine | Dot | Euclid"},

			// Optional Ollama-style query embed endpoint for /v1/search query_text.
			&cli.StringFlag{Name: "embed-url", Sources: cli.EnvVars("EMBED_URL"),
				Usage: "Ollama-style /api/embeddings server URL for query_text path"},
			&cli.StringFlag{Name: "embed-model", Value: "nomic-embed-text", Sources: cli.EnvVars("EMBED_MODEL")},
			&cli.StringFlag{Name: "embed-api-key", Sources: cli.EnvVars("EMBED_API_KEY")},
		},
		Commands: []*cli.Command{
			{Name: "serve", Usage: "run HTTP API", Flags: []cli.Flag{
				&cli.StringFlag{Name: "addr", Value: ":8080", Sources: cli.EnvVars("ADDR")},
				&cli.BoolFlag{Name: "allow-auto-domains", Sources: cli.EnvVars("ALLOW_AUTO_DOMAINS"),
					Usage: "auto-add any newly discovered host (default: drop external links)"},
				&cli.IntFlag{Name: "max-depth", Sources: cli.EnvVars("MAX_DEPTH"),
					Usage: "drop discovered links beyond this depth (0 = unlimited)"},
				&cli.DurationFlag{Name: "lease-ttl", Value: 10 * time.Minute, Sources: cli.EnvVars("LEASE_TTL"),
					Usage: "how long a reserved job/task lease is valid before it can be re-leased"},
				&cli.DurationFlag{Name: "heartbeat-extend", Value: 60 * time.Second, Sources: cli.EnvVars("HEARTBEAT_EXTEND"),
					Usage: "how much each successful heartbeat extends the lease"},
			}, Action: actionServe},
			{Name: "migrate", Usage: "run migrations", Commands: []*cli.Command{
				{Name: "up", Action: migrateAction("up")},
				{Name: "down", Action: migrateAction("down")},
				{Name: "status", Action: migrateAction("status")},
				{Name: "reset", Action: migrateAction("reset")},
			}},
			{Name: "create-worker", Usage: "issue a PAT", Flags: []cli.Flag{
				&cli.StringFlag{Name: "label", Required: true},
				&cli.StringSliceFlag{Name: "capabilities", Usage: "comma-separated kinds, e.g. crawl,pdf_ocr,embed"},
				&cli.IntFlag{Name: "max-concurrent", Value: 4},
			}, Action: actionCreateWorker},
			{Name: "list-workers", Usage: "show pool state", Action: actionListWorkers},
			{Name: "list-capabilities", Usage: "print core capabilities recognized by the registry", Action: actionListCapabilities},
			{Name: "update-worker", Usage: "change capabilities or concurrency", Flags: []cli.Flag{
				&cli.IntFlag{Name: "id", Required: true},
				&cli.StringSliceFlag{Name: "capabilities", Usage: "replace capabilities list"},
				&cli.IntFlag{Name: "max-concurrent", Value: -1, Usage: "-1 to leave unchanged"},
			}, Action: actionUpdateWorker},
			{Name: "ban-worker", Usage: "ban a worker (optionally release its held leases)", Flags: []cli.Flag{
				&cli.IntFlag{Name: "id", Required: true},
				&cli.BoolFlag{Name: "release", Usage: "also release all leases this worker holds across all 3 queues"},
			}, Action: actionBanWorker},
			{Name: "unban-worker", Usage: "unban a worker", Flags: []cli.Flag{
				&cli.IntFlag{Name: "id", Required: true},
			}, Action: actionUnbanWorker},
			{Name: "release-worker", Usage: "release all leases a worker holds (does not ban)", Flags: []cli.Flag{
				&cli.IntFlag{Name: "id", Required: true},
			}, Action: actionReleaseWorker},
			{Name: "queue-stats", Usage: "show per-queue status counts", Action: actionQueueStats},
			{Name: "requeue-chunks", Usage: "bulk-requeue document_chunks rows", Flags: []cli.Flag{
				&cli.StringFlag{Name: "status", Value: "", Usage: "embed_status filter (pending|leased|done|failed). Empty = any non-done"},
				&cli.IntFlag{Name: "worker", Value: 0, Usage: "only chunks held by this worker_id"},
				&cli.IntFlag{Name: "document", Value: 0, Usage: "only chunks of this document_id"},
			}, Action: actionRequeueChunks},
			{Name: "requeue-tasks", Usage: "bulk-requeue processing_jobs rows", Flags: []cli.Flag{
				&cli.StringFlag{Name: "status", Value: "", Usage: "queued|running|done|failed|skipped"},
				&cli.IntFlag{Name: "worker", Value: 0},
				&cli.StringFlag{Name: "processor", Value: "", Usage: "html_strip|pdf_ocr|office_to_pdf|..."},
			}, Action: actionRequeueTasks},
			{Name: "requeue-frontier", Usage: "bulk-requeue crawl_frontier rows", Flags: []cli.Flag{
				&cli.StringFlag{Name: "status", Value: "", Usage: "queued|leased|failed|dead"},
				&cli.IntFlag{Name: "worker", Value: 0},
				&cli.IntFlag{Name: "domain", Value: 0},
			}, Action: actionRequeueFrontier},
			{Name: "seed-domain", Usage: "register a crawl-target domain", Flags: []cli.Flag{
				&cli.StringFlag{Name: "host", Required: true},
				&cli.StringFlag{Name: "scheme", Value: "https"},
				&cli.IntFlag{Name: "crawl-delay-ms", Value: 1000},
			}, Action: actionSeedDomain},
			{Name: "list-domains", Usage: "show seeded crawl targets", Action: actionListDomains},
			{Name: "activate-domain", Usage: "set is_active=1 for a domain", Flags: []cli.Flag{
				&cli.StringFlag{Name: "host", Required: true},
			}, Action: actionActivateDomain(true)},
			{Name: "deactivate-domain", Usage: "set is_active=0 (drops it from reserve + discovery)", Flags: []cli.Flag{
				&cli.StringFlag{Name: "host", Required: true},
			}, Action: actionActivateDomain(false)},
			{Name: "update-domain", Usage: "change a domain's settings without restart", Flags: []cli.Flag{
				&cli.StringFlag{Name: "host", Required: true},
				&cli.IntFlag{Name: "crawl-delay-ms", Value: -1, Usage: "-1 = leave unchanged"},
				&cli.StringFlag{Name: "scheme", Value: "", Usage: "empty = leave unchanged"},
				&cli.StringFlag{Name: "embed-collection", Value: "", Usage: "vector-store collection hint; '-' to clear"},
				&cli.StringFlag{Name: "required-capability", Value: "",
					Usage: "bind this domain to workers with this capability (e.g. js_render, domain:foo.com); '-' to clear"},
			}, Action: actionUpdateDomain},
			{Name: "enqueue", Usage: "add a URL to the frontier", Flags: []cli.Flag{
				&cli.StringFlag{Name: "url", Required: true},
				&cli.IntFlag{Name: "depth", Value: 0},
				&cli.IntFlag{Name: "priority", Value: 0},
			}, Action: actionEnqueue},
			{Name: "reprocess", Usage: "bulk-enqueue processing tasks for existing lake objects", Flags: []cli.Flag{
				&cli.StringFlag{Name: "processor", Required: true, Usage: "pdf_ocr | docx_to_pdf | html_strip"},
				&cli.StringFlag{Name: "content-type-prefix", Value: "", Usage: "e.g. application/pdf"},
				&cli.IntFlag{Name: "limit", Value: 10000},
			}, Action: actionReprocess},
			{Name: "trigger-add", Usage: "create a pipeline trigger", Flags: []cli.Flag{
				&cli.StringFlag{Name: "on", Required: true, Usage: "lake_object_inserted | blob_produced"},
				&cli.StringFlag{Name: "content-type", Value: "", Usage: "filter: content_type prefix"},
				&cli.StringFlag{Name: "source-processor", Value: "", Usage: "filter: source processor (blob_produced)"},
				&cli.StringFlag{Name: "enqueue", Required: true, Usage: "processor name (e.g. pdf_ocr, quickwit_indexer)"},
			}, Action: actionTriggerAdd},
			{Name: "trigger-list", Usage: "list all pipeline triggers", Action: actionTriggerList},
			{Name: "trigger-enable", Usage: "enable a trigger", Flags: []cli.Flag{
				&cli.IntFlag{Name: "id", Required: true},
			}, Action: actionTriggerEnable},
			{Name: "trigger-disable", Usage: "disable a trigger", Flags: []cli.Flag{
				&cli.IntFlag{Name: "id", Required: true},
			}, Action: actionTriggerDisable},
			{Name: "trigger-delete", Usage: "delete a trigger", Flags: []cli.Flag{
				&cli.IntFlag{Name: "id", Required: true},
			}, Action: actionTriggerDelete},
			{Name: "capability-add", Usage: "register a capability in the catalog", Flags: []cli.Flag{
				&cli.StringFlag{Name: "name", Required: true, Usage: "e.g. video_processing"},
				&cli.StringFlag{Name: "description", Value: ""},
				&cli.BoolFlag{Name: "internal", Value: false, Usage: "served by registry binary's in-process worker"},
			}, Action: actionCapabilityAdd},
			{Name: "capability-rm", Usage: "delete a capability from the catalog", Flags: []cli.Flag{
				&cli.StringFlag{Name: "name", Required: true},
			}, Action: actionCapabilityRm},
		},
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, "registry:", err)
		os.Exit(1)
	}
}

// --- helpers --------------------------------------------------------------

func openDB(cmd *cli.Command) (*rwdb.DB, error) {
	return rwdb.New(rwdb.Config{
		Driver:  rwdb.Driver(cmd.String("db-driver")),
		DSN:     cmd.String("db-dsn"),
		ReadDSN: cmd.String("read-dsn"),
		Debug:   cmd.Bool("debug"),
	})
}

func leaseSecret(cmd *cli.Command) ([]byte, error) {
	raw := cmd.String("lease-secret")
	if raw == "" {
		return nil, errors.New("--lease-secret required (LEASE_SECRET env)")
	}
	b, err := base64.RawStdEncoding.DecodeString(raw)
	if err != nil {
		b, err = base64.StdEncoding.DecodeString(raw)
		if err != nil {
			return nil, fmt.Errorf("decode lease-secret: %w", err)
		}
	}
	if len(b) < 16 {
		return nil, errors.New("lease-secret too short, need >=16 bytes raw")
	}
	return b, nil
}

type registryBundle struct {
	Svc         *app.Service
	Embed       *app.EmbedSvc
	Tasks       *app.TaskSvc
	Search      *app.SearchSvc
	Pipeline    *app.Pipeline
	Dispatcher  *app.TriggerDispatcher
	Workers     workerid.Repository
	Lake        *gormrepo.LakeRepo
	Blobs       *local.Store
	Extractions *gormrepo.ExtractionRepo
	Chunks      *gormrepo.ChunkRepo
}

func buildService(cmd *cli.Command, db *rwdb.DB) (*registryBundle, error) {
	secret, err := leaseSecret(cmd)
	if err != nil {
		return nil, err
	}
	signer, err := lease.New(secret)
	if err != nil {
		return nil, err
	}
	blobs, err := local.New(cmd.String("blobs-root"))
	if err != nil {
		return nil, err
	}
	frepo := gormrepo.NewFrontierRepo(db)
	lrepo := gormrepo.NewLakeRepo(db)
	wrepo := gormrepo.NewWorkerRepo(db)
	prepo := gormrepo.NewProcessingRepo(db)
	erepo := gormrepo.NewExtractionRepo(db)
	crepo := gormrepo.NewChunkRepo(db)

	trepo := gormrepo.NewTriggersRepo(db)

	cfg := app.Defaults()
	cfg.AllowAutoDomains = cmd.Bool("allow-auto-domains")
	if md := cmd.Int("max-depth"); md > 0 {
		cfg.MaxDepth = md
	}
	if d := cmd.Duration("lease-ttl"); d > 0 {
		cfg.LeaseTTL = d
	}
	if d := cmd.Duration("heartbeat-extend"); d > 0 {
		cfg.HeartbeatExtend = d
	}
	svc := app.New(cfg, frepo, frepo, lrepo, blobs, wrepo, signer)
	pipe := app.NewPipeline(lrepo, blobs, prepo, erepo, crepo)
	svc.SetPipeline(pipe)
	disp := app.NewTriggerDispatcher(trepo, prepo)
	svc.SetDispatcher(disp)
	resolver := app.NewCollectionResolver(lrepo, frepo, frepo)
	pipe.SetResolver(resolver)
	embed := app.NewEmbedSvc(cfg, crepo, signer)
	tasks := app.NewTaskSvc(cfg, prepo, lrepo, blobs, erepo, signer)
	tasks.AttachChunkSink(&app.ChunkRepoSink{Repo: crepo})
	tasks.SetDispatcher(disp)
	tasks.SetResolver(resolver)

	// Qdrant + optional query-embed client (slice 10)
	qcli := qdrant.New(qdrant.Config{
		BaseURL:  cmd.String("qdrant-url"),
		APIKey:   cmd.String("qdrant-api-key"),
		Shards:   cmd.Int("qdrant-shards"),
		Distance: cmd.String("qdrant-distance"),
	})
	embed.SetQdrant(qcli)
	ecli := embedclient.New(embedclient.Config{
		BaseURL: cmd.String("embed-url"),
		Model:   cmd.String("embed-model"),
		APIKey:  cmd.String("embed-api-key"),
	})
	var searchSvc *app.SearchSvc
	if qcli.Enabled() {
		searchSvc = app.NewSearchSvc(qcli, ecli)
	}

	return &registryBundle{
		Svc: svc, Embed: embed, Tasks: tasks, Search: searchSvc,
		Pipeline: pipe, Dispatcher: disp,
		Workers: wrepo, Lake: lrepo, Blobs: blobs,
		Extractions: erepo, Chunks: crepo,
	}, nil
}

// --- actions --------------------------------------------------------------

func actionServe(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	b, err := buildService(cmd, db)
	if err != nil {
		return err
	}
	handler := httpapi.Router(b.Svc, b.Embed, b.Tasks, b.Search, b.Workers, b.Lake, b.Blobs, b.Extractions, b.Chunks, cmd.Int64("max-body-bytes"))
	srv := &http.Server{
		Addr:              cmd.String("addr"),
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	bgCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sweeper(bgCtx, b.Svc, b.Embed, b.Tasks)
	go b.Pipeline.Run(bgCtx, 2*time.Second)

	// graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		fmt.Println("registry: shutdown signal")
		shCtx, sCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer sCancel()
		_ = srv.Shutdown(shCtx)
	}()
	fmt.Printf("registry: listening on %s\n", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func sweeper(ctx context.Context, svc *app.Service, embed *app.EmbedSvc, tasks *app.TaskSvc) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := svc.SweepExpiredLeases(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "sweeper frontier:", err)
			} else if n > 0 {
				fmt.Printf("sweeper: requeued %d stuck frontier leases\n", n)
			}
			if embed != nil {
				if n, err := embed.SweepExpired(ctx); err != nil {
					fmt.Fprintln(os.Stderr, "sweeper chunks:", err)
				} else if n > 0 {
					fmt.Printf("sweeper: requeued %d stuck chunk leases\n", n)
				}
			}
			if tasks != nil {
				if n, err := tasks.SweepExpired(ctx); err != nil {
					fmt.Fprintln(os.Stderr, "sweeper tasks:", err)
				} else if n > 0 {
					fmt.Printf("sweeper: requeued %d stuck task leases\n", n)
				}
			}
		}
	}
}

func migrateAction(direction string) cli.ActionFunc {
	return func(ctx context.Context, cmd *cli.Command) error {
		db, err := openDB(cmd)
		if err != nil {
			return err
		}
		defer db.Close()
		switch rwdb.Driver(cmd.String("db-driver")) {
		case rwdb.DriverSQLite, "":
			goose.SetBaseFS(subFS(migrations.SQLite, "sqlite"))
			if err := goose.SetDialect("sqlite3"); err != nil {
				return err
			}
		case rwdb.DriverPostgres:
			goose.SetBaseFS(subFS(migrations.Postgres, "postgres"))
			if err := goose.SetDialect("postgres"); err != nil {
				return err
			}
		case rwdb.DriverMySQL:
			goose.SetBaseFS(subFS(migrations.MySQL, "mysql"))
			if err := goose.SetDialect("mysql"); err != nil {
				return err
			}
		default:
			return fmt.Errorf("migrations not yet packaged for driver %q", cmd.String("db-driver"))
		}
		sqlDB, err := db.W.DB()
		if err != nil {
			return err
		}
		switch direction {
		case "up":
			return goose.Up(sqlDB, ".")
		case "down":
			return goose.Down(sqlDB, ".")
		case "status":
			return goose.Status(sqlDB, ".")
		case "reset":
			return goose.Reset(sqlDB, ".")
		}
		return fmt.Errorf("unknown migrate direction %q", direction)
	}
}

// subFS returns a sub-FS rooted at `dir` so goose sees migration files at "./".
func subFS(efs fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(efs, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

func actionCreateWorker(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	wrepo := gormrepo.NewWorkerRepo(db)
	pat := make([]byte, 32)
	if _, err := rand.Read(pat); err != nil {
		return err
	}
	patStr := base64.RawURLEncoding.EncodeToString(pat)
	sum := sha256.Sum256([]byte(patStr))
	id, err := wrepo.Create(ctx, workerid.Worker{
		PATHash:         sum[:],
		Label:           cmd.String("label"),
		ReputationScore: 100,
		Capabilities:    cmd.StringSlice("capabilities"),
		MaxConcurrent:   cmd.Int("max-concurrent"),
	})
	if err != nil {
		return err
	}
	fmt.Printf("worker_id=%d\n", id)
	fmt.Printf("pat=%s\n", patStr)
	fmt.Println("(save the PAT — it is not stored, only its sha256 hash)")
	return nil
}

func actionListWorkers(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	wrepo := gormrepo.NewWorkerRepo(db)
	ws, err := wrepo.List(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%-4s  %-20s  %-30s  %4s  %4s  %-25s  %s\n",
		"ID", "LABEL", "CAPABILITIES", "MAX", "HELD", "LAST_SEEN", "BANNED")
	for _, w := range ws {
		held, _ := wrepo.CountHeldLeases(ctx, w.ID)
		caps := strings.Join(w.Capabilities, ",")
		if caps == "" {
			caps = "(any)"
		}
		seen := "-"
		if w.LastSeenAt != nil {
			seen = w.LastSeenAt.Format("2006-01-02T15:04:05Z")
		}
		banned := "no"
		if w.IsBanned() {
			banned = "yes"
		}
		fmt.Printf("%-4d  %-20s  %-30s  %4d  %4d  %-25s  %s\n",
			w.ID, w.Label, caps, w.MaxConcurrent, held, seen, banned)
	}
	return nil
}

func actionListCapabilities(ctx context.Context, cmd *cli.Command) error {
	fmt.Println("ENDPOINT-GATED (registry-defined):")
	fmt.Printf("  %-18s  %-8s  %s\n", "NAME", "GROUP", "DESCRIPTION")
	for _, c := range workerid.EndpointGatedCapabilities() {
		fmt.Printf("  %-18s  %-8s  %s\n", c.Name, c.Group, c.Description)
	}

	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()

	cat, err := gormrepo.NewCapabilityRepo(db).List(ctx)
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Println("CATALOG (capabilities table):")
	if len(cat) == 0 {
		fmt.Println("  (none)")
	} else {
		fmt.Printf("  %-20s  %-8s  %s\n", "NAME", "INTERNAL", "DESCRIPTION")
		for _, c := range cat {
			internal := "no"
			if c.Internal {
				internal = "yes"
			}
			fmt.Printf("  %-20s  %-8s  %s\n", c.Name, internal, c.Description)
		}
	}

	ws, err := gormrepo.NewWorkerRepo(db).List(ctx)
	if err != nil {
		return err
	}
	counts := map[string]int{}
	for _, w := range ws {
		for _, c := range w.Capabilities {
			counts[c]++
		}
	}
	for _, c := range workerid.EndpointGatedCapabilities() {
		delete(counts, c.Name)
	}
	for _, c := range cat {
		delete(counts, c.Name)
	}
	names := make([]string, 0, len(counts))
	for n := range counts {
		names = append(names, n)
	}
	sort.Strings(names)

	fmt.Println()
	fmt.Println("WORKER-DECLARED (free-form, not in catalog):")
	if len(names) == 0 {
		fmt.Println("  (none)")
		return nil
	}
	fmt.Printf("  %-20s  %s\n", "NAME", "WORKERS")
	for _, n := range names {
		fmt.Printf("  %-20s  %d\n", n, counts[n])
	}
	return nil
}

func actionUpdateWorker(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	wrepo := gormrepo.NewWorkerRepo(db)
	id := int64(cmd.Int("id"))
	if caps := cmd.StringSlice("capabilities"); len(caps) > 0 {
		if err := wrepo.UpdateCapabilities(ctx, id, caps); err != nil {
			return err
		}
		fmt.Printf("updated capabilities=%v on worker_id=%d\n", caps, id)
	}
	if n := cmd.Int("max-concurrent"); n >= 0 {
		if err := wrepo.UpdateMaxConcurrent(ctx, id, n); err != nil {
			return err
		}
		fmt.Printf("updated max_concurrent=%d on worker_id=%d\n", n, id)
	}
	return nil
}

func actionBanWorker(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	wid := int64(cmd.Int("id"))
	if err := gormrepo.NewWorkerRepo(db).Ban(ctx, wid); err != nil {
		return err
	}
	fmt.Printf("banned worker_id=%d\n", wid)
	if cmd.Bool("release") {
		nf, nt, nc, err := releaseWorkerLeases(ctx, db, wid)
		if err != nil {
			return err
		}
		fmt.Printf("released held leases: frontier=%d tasks=%d chunks=%d\n", nf, nt, nc)
	}
	return nil
}

func actionReleaseWorker(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	wid := int64(cmd.Int("id"))
	nf, nt, nc, err := releaseWorkerLeases(ctx, db, wid)
	if err != nil {
		return err
	}
	fmt.Printf("released worker_id=%d frontier=%d tasks=%d chunks=%d\n", wid, nf, nt, nc)
	return nil
}

// releaseWorkerLeases drops all leases held by a single worker across the
// three queues. Each repo runs a single UPDATE; no cross-table transaction.
func releaseWorkerLeases(ctx context.Context, db *rwdb.DB, wid int64) (int64, int64, int64, error) {
	frepo := gormrepo.NewFrontierRepo(db)
	prepo := gormrepo.NewProcessingRepo(db)
	crepo := gormrepo.NewChunkRepo(db)
	nf, err := frepo.RequeueByFilter(ctx, frontier.RequeueFilter{
		Status:   frontier.StatusLeased,
		WorkerID: wid,
	})
	if err != nil {
		return 0, 0, 0, err
	}
	nt, err := prepo.RequeueByFilter(ctx, processing.TaskRequeueFilter{
		Status:   processing.StatusRunning,
		WorkerID: wid,
	})
	if err != nil {
		return nf, 0, 0, err
	}
	nc, err := crepo.RequeueByFilter(ctx, chunking.RequeueFilter{
		Status:   chunking.EmbedLeased,
		WorkerID: wid,
	})
	if err != nil {
		return nf, nt, 0, err
	}
	return nf, nt, nc, nil
}

func actionQueueStats(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	frepo := gormrepo.NewFrontierRepo(db)
	prepo := gormrepo.NewProcessingRepo(db)
	crepo := gormrepo.NewChunkRepo(db)
	wrepo := gormrepo.NewWorkerRepo(db)

	fs, err := frepo.StatusCounts(ctx)
	if err != nil {
		return err
	}
	fmt.Println("CRAWL_FRONTIER")
	printCounts(fs, []string{"queued", "leased", "done", "failed", "dead"})

	ps, err := prepo.StatusCounts(ctx)
	if err != nil {
		return err
	}
	fmt.Println("PROCESSING_JOBS  (per processor)")
	procs := sortedKeys(ps)
	for _, p := range procs {
		fmt.Printf("  %s\n", p)
		printCounts(ps[p], []string{"queued", "running", "done", "failed", "skipped"})
	}

	cs, err := crepo.StatusCounts(ctx)
	if err != nil {
		return err
	}
	fmt.Println("DOCUMENT_CHUNKS")
	printCounts(cs, []string{"pending", "leased", "done", "failed"})

	ws, err := wrepo.List(ctx)
	if err != nil {
		return err
	}
	var banned, stale int64
	cutoff := time.Now().UTC().Add(-5 * time.Minute)
	for _, w := range ws {
		if w.IsBanned() {
			banned++
		}
		if w.LastSeenAt == nil || w.LastSeenAt.Before(cutoff) {
			stale++
		}
	}
	fmt.Printf("WORKERS\n  total %d  banned %d  stale(>5m) %d\n", len(ws), banned, stale)
	return nil
}

func printCounts(m map[string]int64, order []string) {
	parts := make([]string, 0, len(order))
	for _, k := range order {
		parts = append(parts, fmt.Sprintf("%s %d", k, m[k]))
	}
	// Surface any unknown keys at the tail.
	for k, v := range m {
		known := false
		for _, o := range order {
			if o == k {
				known = true
				break
			}
		}
		if !known {
			parts = append(parts, fmt.Sprintf("%s %d", k, v))
		}
	}
	fmt.Printf("  %s\n", strings.Join(parts, "  "))
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func actionRequeueChunks(ctx context.Context, cmd *cli.Command) error {
	if cmd.String("status") == "" && cmd.Int("worker") == 0 && cmd.Int("document") == 0 {
		return errors.New("requeue-chunks: at least one of --status / --worker / --document required")
	}
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	n, err := gormrepo.NewChunkRepo(db).RequeueByFilter(ctx, chunking.RequeueFilter{
		Status:     chunking.EmbedStatus(cmd.String("status")),
		WorkerID:   int64(cmd.Int("worker")),
		DocumentID: int64(cmd.Int("document")),
	})
	if err != nil {
		return err
	}
	fmt.Printf("requeue-chunks rows=%d\n", n)
	return nil
}

func actionRequeueTasks(ctx context.Context, cmd *cli.Command) error {
	if cmd.String("status") == "" && cmd.Int("worker") == 0 && cmd.String("processor") == "" {
		return errors.New("requeue-tasks: at least one of --status / --worker / --processor required")
	}
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	n, err := gormrepo.NewProcessingRepo(db).RequeueByFilter(ctx, processing.TaskRequeueFilter{
		Status:    processing.Status(cmd.String("status")),
		WorkerID:  int64(cmd.Int("worker")),
		Processor: processing.Processor(cmd.String("processor")),
	})
	if err != nil {
		return err
	}
	fmt.Printf("requeue-tasks rows=%d\n", n)
	return nil
}

func actionRequeueFrontier(ctx context.Context, cmd *cli.Command) error {
	if cmd.String("status") == "" && cmd.Int("worker") == 0 && cmd.Int("domain") == 0 {
		return errors.New("requeue-frontier: at least one of --status / --worker / --domain required")
	}
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	n, err := gormrepo.NewFrontierRepo(db).RequeueByFilter(ctx, frontier.RequeueFilter{
		Status:   frontier.Status(cmd.String("status")),
		WorkerID: int64(cmd.Int("worker")),
		DomainID: int64(cmd.Int("domain")),
	})
	if err != nil {
		return err
	}
	fmt.Printf("requeue-frontier rows=%d\n", n)
	return nil
}

func actionUnbanWorker(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	wrepo := gormrepo.NewWorkerRepo(db)
	if err := wrepo.Unban(ctx, int64(cmd.Int("id"))); err != nil {
		return err
	}
	fmt.Printf("unbanned worker_id=%d\n", cmd.Int("id"))
	return nil
}

func actionSeedDomain(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	frepo := gormrepo.NewFrontierRepo(db)
	d, err := frepo.UpsertByHost(ctx, cmd.String("host"), cmd.String("scheme"), cmd.Int("crawl-delay-ms"))
	if err != nil {
		return err
	}
	fmt.Printf("domain_id=%d host=%s scheme=%s delay=%dms\n", d.ID, d.Host, d.Scheme, d.CrawlDelayMS)
	return nil
}

func actionCapabilityAdd(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	c := processing.Capability{
		Name:        cmd.String("name"),
		Description: cmd.String("description"),
		Internal:    cmd.Bool("internal"),
	}
	if err := gormrepo.NewCapabilityRepo(db).Upsert(ctx, c); err != nil {
		return err
	}
	fmt.Printf("capability=%s internal=%v description=%q\n", c.Name, c.Internal, c.Description)
	return nil
}

func actionCapabilityRm(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	name := cmd.String("name")
	if err := gormrepo.NewCapabilityRepo(db).Delete(ctx, name); err != nil {
		return err
	}
	fmt.Printf("deleted capability=%s\n", name)
	return nil
}

func actionTriggerAdd(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	tr := gormrepo.NewTriggersRepo(db)
	filter := map[string]any{}
	if ct := cmd.String("content-type"); ct != "" {
		filter["content_type_prefix"] = ct
	}
	if sp := cmd.String("source-processor"); sp != "" {
		filter["source_processor"] = sp
	}
	filterStr := ""
	if len(filter) > 0 {
		b, _ := json.Marshal(filter)
		filterStr = string(b)
	}
	id, err := tr.Create(ctx, triggers.Trigger{
		WhenEvent:   triggers.Event(cmd.String("on")),
		WhenFilter:  filterStr,
		EnqueueKind: cmd.String("enqueue"),
		Enabled:     true,
	})
	if err != nil {
		return err
	}
	fmt.Printf("trigger_id=%d on=%s filter=%s enqueue=%s\n",
		id, cmd.String("on"), filterStr, cmd.String("enqueue"))
	return nil
}

func actionTriggerList(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	ts, err := gormrepo.NewTriggersRepo(db).List(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%-4s  %-25s  %-50s  %-20s  %s\n", "ID", "EVENT", "FILTER", "ENQUEUE", "ENABLED")
	for _, t := range ts {
		f := t.WhenFilter
		if f == "" {
			f = "(any)"
		}
		fmt.Printf("%-4d  %-25s  %-50s  %-20s  %v\n",
			t.ID, t.WhenEvent, f, t.EnqueueKind, t.Enabled)
	}
	return nil
}

func actionTriggerEnable(ctx context.Context, cmd *cli.Command) error {
	return triggerSetEnabled(ctx, cmd, true)
}
func actionTriggerDisable(ctx context.Context, cmd *cli.Command) error {
	return triggerSetEnabled(ctx, cmd, false)
}
func triggerSetEnabled(ctx context.Context, cmd *cli.Command, enabled bool) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := gormrepo.NewTriggersRepo(db).SetEnabled(ctx, int64(cmd.Int("id")), enabled); err != nil {
		return err
	}
	fmt.Printf("trigger_id=%d enabled=%v\n", cmd.Int("id"), enabled)
	return nil
}

func actionTriggerDelete(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := gormrepo.NewTriggersRepo(db).Delete(ctx, int64(cmd.Int("id"))); err != nil {
		return err
	}
	fmt.Printf("deleted trigger_id=%d\n", cmd.Int("id"))
	return nil
}

func actionReprocess(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	prepo := gormrepo.NewProcessingRepo(db)
	proc := processing.Processor(cmd.String("processor"))
	prefix := cmd.String("content-type-prefix")
	if prefix == "" {
		switch proc {
		case processing.ProcPDFOCR:
			prefix = "application/pdf"
		case processing.ProcDOCXToPDF:
			prefix = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
		case processing.ProcHTMLStrip:
			prefix = "text/html"
		}
	}
	if prefix == "" {
		return fmt.Errorf("reprocess: --content-type-prefix required for processor %q", proc)
	}
	n, err := prepo.BulkEnqueueByContentType(ctx, proc, prefix, 0, cmd.Int("limit"))
	if err != nil {
		return err
	}
	fmt.Printf("reprocess: enqueued=%d processor=%s prefix=%s\n", n, proc, prefix)
	return nil
}

func actionListDomains(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	ds, err := gormrepo.NewFrontierRepo(db).List(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%-4s  %-30s  %-8s  %8s  %-7s  %-20s  %s\n",
		"ID", "HOST", "SCHEME", "DELAY_MS", "ACTIVE", "REQ_CAP", "EMBED_COLLECTION")
	for _, d := range ds {
		active := "yes"
		if !d.IsActive {
			active = "no"
		}
		col := d.EmbedCollection
		if col == "" {
			col = "(host)"
		}
		req := d.RequiredCapability
		if req == "" {
			req = "(any)"
		}
		fmt.Printf("%-4d  %-30s  %-8s  %8d  %-7s  %-20s  %s\n",
			d.ID, d.Host, d.Scheme, d.CrawlDelayMS, active, req, col)
	}
	return nil
}

func actionUpdateDomain(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	r := gormrepo.NewFrontierRepo(db)
	host := cmd.String("host")
	changed := []string{}
	if v := cmd.Int("crawl-delay-ms"); v >= 0 {
		if err := r.UpdateCrawlDelay(ctx, host, v); err != nil {
			return err
		}
		changed = append(changed, fmt.Sprintf("crawl_delay_ms=%d", v))
	}
	if v := cmd.String("scheme"); v != "" {
		if err := r.UpdateScheme(ctx, host, v); err != nil {
			return err
		}
		changed = append(changed, fmt.Sprintf("scheme=%s", v))
	}
	if v := cmd.String("embed-collection"); v != "" {
		actual := v
		if v == "-" {
			actual = ""
		}
		if err := r.UpdateEmbedCollection(ctx, host, actual); err != nil {
			return err
		}
		if actual == "" {
			changed = append(changed, "embed_collection=(cleared)")
		} else {
			changed = append(changed, fmt.Sprintf("embed_collection=%s", actual))
		}
	}
	if v := cmd.String("required-capability"); v != "" {
		actual := v
		if v == "-" {
			actual = ""
		}
		if err := r.UpdateRequiredCapability(ctx, host, actual); err != nil {
			return err
		}
		if actual == "" {
			changed = append(changed, "required_capability=(cleared)")
		} else {
			changed = append(changed, fmt.Sprintf("required_capability=%s", actual))
		}
	}
	if len(changed) == 0 {
		fmt.Printf("update-domain host=%s (no fields changed)\n", host)
		return nil
	}
	fmt.Printf("update-domain host=%s %s\n", host, strings.Join(changed, " "))
	return nil
}

func actionActivateDomain(active bool) cli.ActionFunc {
	return func(ctx context.Context, cmd *cli.Command) error {
		db, err := openDB(cmd)
		if err != nil {
			return err
		}
		defer db.Close()
		if err := gormrepo.NewFrontierRepo(db).SetActive(ctx, cmd.String("host"), active); err != nil {
			return err
		}
		fmt.Printf("host=%s active=%v\n", cmd.String("host"), active)
		return nil
	}
}

func actionEnqueue(ctx context.Context, cmd *cli.Command) error {
	db, err := openDB(cmd)
	if err != nil {
		return err
	}
	defer db.Close()
	raw := cmd.String("url")
	canon, err := urls.Canonical(raw)
	if err != nil {
		return err
	}
	u, err := url.Parse(canon)
	if err != nil {
		return err
	}
	frepo := gormrepo.NewFrontierRepo(db)
	d, err := frepo.UpsertByHost(ctx, u.Host, u.Scheme, 1000)
	if err != nil {
		return err
	}
	hash := urls.Hash(canon)
	inserted, err := frepo.Enqueue(ctx, frontier.Job{
		URLHash:      hash,
		URL:          raw,
		CanonicalURL: canon,
		DomainID:     d.ID,
		Depth:        cmd.Int("depth"),
		Priority:     cmd.Int("priority"),
		MaxAttempts:  5,
	})
	if err != nil {
		return err
	}
	fmt.Printf("inserted=%v url=%s canonical=%s domain_id=%d\n", inserted, raw, canon, d.ID)
	_ = path.Join // keep "path" used if needed later
	return nil
}
