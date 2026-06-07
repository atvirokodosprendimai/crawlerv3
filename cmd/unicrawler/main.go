// unicrawler is a YAML-configured crawlerv3 worker that drives a real
// headless browser over Selenium WebDriver.
//
// Why a browser: many target sites render listings via JavaScript (Liteko's
// ASP.NET WebForms RadDataPager, infinite-scroll feeds, single-page apps).
// A pure HTTP fetcher can't run __doPostBack, intersect-observer loaders, or
// React hydration. A real browser handles all of it; we only need YAML to
// describe what to look for and how to advance.
//
// Subcommands:
//
//	validate <config.yaml>
//	  Parse + lint the config without touching DB or browser.
//
//	seed <config.yaml>
//	  Direct DB write. Upsert the configured domain and enqueue seed URLs.
//	  seed.type = urls | date_range.
//
//	run <config.yaml>
//	  Registry-backed worker loop. Reserve jobs, match each URL to a page_type,
//	  drive Selenium per page_type (load → paginate → extract → repeat),
//	  upload the first-page HTML as blob and report every discovered link.
//	  Extracted structured fields are written as JSON sidecars to --sidecar-dir.
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
		Name:  "unicrawler",
		Usage: "YAML-configured Selenium-driven crawlerv3 worker",
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			logx.Init("unicrawler", c.String("log-level"))
			return ctx, nil
		},
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL"),
				Usage: "debug | info | warn | error"},
		},
		Commands: []*cli.Command{
			{
				Name:      "validate",
				Usage:     "parse + lint a YAML config",
				ArgsUsage: "<config.yaml>",
				Action:    runValidate,
			},
			{
				Name:      "seed",
				Usage:     "enqueue this site's seed URLs into the registry frontier",
				ArgsUsage: "<config.yaml>",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "db-driver", Value: "sqlite", Sources: cli.EnvVars("DB_DRIVER")},
					&cli.StringFlag{Name: "db-dsn", Value: "crawler.db", Sources: cli.EnvVars("DB_DSN")},
					&cli.StringFlag{Name: "to", Value: "",
						Usage: "override seed.to for date_range (YYYY-MM-DD); empty = config value or today"},
				},
				Action: runSeedCmd,
			},
			{
				Name:      "run",
				Usage:     "drain the frontier: reserve jobs, drive browser, post blob + links + fields",
				ArgsUsage: "<config.yaml>",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "registry", Required: true, Sources: cli.EnvVars("REGISTRY")},
					&cli.StringFlag{Name: "pat", Required: true, Sources: cli.EnvVars("PAT")},
					&cli.StringFlag{Name: "webdriver", Value: "http://localhost:4444/wd/hub",
						Sources: cli.EnvVars("WEBDRIVER_URL"),
						Usage:   "Selenium WebDriver remote URL"},
					&cli.StringFlag{Name: "browser", Value: "chrome", Sources: cli.EnvVars("BROWSER")},
					&cli.IntFlag{Name: "batch", Value: 2},
					&cli.IntFlag{Name: "concurrency", Value: 1,
						Usage: "parallel browser sessions; each consumes one slot in Selenium grid"},
					&cli.DurationFlag{Name: "idle-sleep", Value: 5 * time.Second},
					&cli.DurationFlag{Name: "page-load-timeout", Value: 60 * time.Second},
					&cli.DurationFlag{Name: "script-timeout", Value: 30 * time.Second},
					&cli.StringFlag{Name: "sidecar-dir", Value: "./sidecars",
						Usage: "directory for per-URL JSON field dumps; created if absent"},
				},
				Action: runRunCmd,
			},
		},
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("unicrawler exit", "err", err)
		os.Exit(1)
	}
}
