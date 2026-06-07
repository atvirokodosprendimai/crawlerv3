// litekoworker is the crawlerv3 worker for liteko.teismai.lt.
//
// Liteko renders listings via an ASP.NET WebForms RadDataPager — page 1 is a
// plain GET, pages 2..N are __doPostBack POSTs that echo VIEWSTATE back. The
// generic crawlerv3 worker treats each URL as one GET, so it can't reach past
// page 1 on its own. litekoworker fetches page 1, walks the pager inline, and
// reports every discovered case-detail URL in the result's discovered_links.
//
// Two subcommands:
//
//	seed --from YYYY-MM-DD --to YYYY-MM-DD
//	  Direct DB write. Upserts the liteko.teismai.lt domain row and enqueues
//	  one paieska.aspx?nuo=...&iki=... URL per day in the range. Idempotent
//	  (Enqueue dedups by URL hash).
//
//	run --registry URL --pat TOKEN
//	  Registry-backed worker loop. Reserves jobs over HTTP, dispatches by URL
//	  pattern (listing vs detail), uploads page-1 blob + discovered detail URLs.
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	cli "github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/logx"
)

func main() {
	cmd := &cli.Command{
		Name:  "litekoworker",
		Usage: "Liteko (liteko.teismai.lt) crawlerv3 worker",
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			logx.Init("litekoworker", c.String("log-level"))
			return ctx, nil
		},
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL"),
				Usage: "debug | info | warn | error"},
		},
		Commands: []*cli.Command{
			{
				Name:  "seed",
				Usage: "enqueue one per-day Liteko search URL for each day in [from, to]",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "db-driver", Value: "sqlite", Sources: cli.EnvVars("DB_DRIVER")},
					&cli.StringFlag{Name: "db-dsn", Value: "crawler.db", Sources: cli.EnvVars("DB_DSN")},
					&cli.StringFlag{Name: "from", Value: "2005-01-01", Usage: "YYYY-MM-DD (inclusive)"},
					&cli.StringFlag{Name: "to", Value: "", Usage: "YYYY-MM-DD (inclusive); empty = today"},
					&cli.IntFlag{Name: "crawl-delay-ms", Value: 1000,
						Usage: "per-domain crawl delay; only applied if domain row is being created"},
				},
				Action: runSeed,
			},
			{
				Name:  "run",
				Usage: "drain the frontier: reserve Liteko jobs, fetch + paginate, post results",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "registry", Required: true, Sources: cli.EnvVars("REGISTRY")},
					&cli.StringFlag{Name: "pat", Required: true, Sources: cli.EnvVars("PAT")},
					&cli.IntFlag{Name: "batch", Value: 4},
					&cli.IntFlag{Name: "concurrency", Value: 2,
						Usage: "max parallel jobs per reserved batch"},
					&cli.DurationFlag{Name: "idle-sleep", Value: 5 * time.Second},
					&cli.DurationFlag{Name: "fetch-timeout", Value: 60 * time.Second},
					&cli.DurationFlag{Name: "api-timeout", Value: 120 * time.Second,
						Usage: "registry API client timeout (reserve / result / fail / heartbeat). Raise when concurrency is high and the registry queues result POSTs behind the SQLite writer."},
					&cli.DurationFlag{Name: "page-delay", Value: 500 * time.Millisecond,
						Usage: "pause between RadDataPager POSTs within one listing job"},
					&cli.StringFlag{Name: "user-agent", Value: "crawlerv3-litekoworker/0.1"},
				},
				Action: runWorker,
			},
		},
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("litekoworker exit", "err", err)
		os.Exit(1)
	}
}
