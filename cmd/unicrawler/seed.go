package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	cli "github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/frontier"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/gormrepo"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/urls"
)

const dateLayout = "2006-01-02"

// runSeedCmd implements `unicrawler seed <config.yaml>`. Opens the registry
// DB directly (no HTTP), upserts the domain, and enqueues the URLs the
// config's SeedSpec resolves to. Re-runs are idempotent (frontier dedups
// by URL hash).
func runSeedCmd(ctx context.Context, cmd *cli.Command) error {
	path := cmd.Args().First()
	if path == "" {
		return fmt.Errorf("usage: unicrawler seed <config.yaml>")
	}
	c, err := LoadConfig(path)
	if err != nil {
		return err
	}

	db, err := rwdb.New(rwdb.Config{
		Driver: rwdb.Driver(cmd.String("db-driver")),
		DSN:    cmd.String("db-dsn"),
	})
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	frepo := gormrepo.NewFrontierRepo(db)
	dom, err := frepo.UpsertByHost(ctx, c.Domain, c.Scheme, c.CrawlDelayMS)
	if err != nil {
		return fmt.Errorf("upsert domain: %w", err)
	}
	slog.Info("domain ready", "id", dom.ID, "host", dom.Host, "scheme", dom.Scheme, "delay_ms", dom.CrawlDelayMS)

	seedURLs, err := resolveSeed(c.Seed, cmd.String("to"))
	if err != nil {
		return fmt.Errorf("resolve seed: %w", err)
	}
	if len(seedURLs) == 0 {
		return fmt.Errorf("seed produced 0 URLs")
	}

	var inserted, dupes int
	for _, raw := range seedURLs {
		canon, err := urls.Canonical(raw)
		if err != nil {
			return fmt.Errorf("canonical %s: %w", raw, err)
		}
		ok, err := frepo.Enqueue(ctx, frontier.Job{
			URLHash:      urls.Hash(canon),
			URL:          raw,
			CanonicalURL: canon,
			DomainID:     dom.ID,
			Depth:        0,
			Priority:     0,
			MaxAttempts:  5,
		})
		if err != nil {
			return fmt.Errorf("enqueue %s: %w", raw, err)
		}
		if ok {
			inserted++
		} else {
			dupes++
		}
	}
	slog.Info("seed complete", "site", c.Name, "total", len(seedURLs), "inserted", inserted, "duplicate", dupes)
	return nil
}

// resolveSeed expands a SeedSpec into the concrete URL list. toOverride
// overrides SeedSpec.To when non-empty (lets ops freeze the upper bound
// without editing the config).
func resolveSeed(s SeedSpec, toOverride string) ([]string, error) {
	switch s.Type {
	case "urls":
		return s.URLs, nil
	case "date_range":
		from, err := time.Parse(dateLayout, s.From)
		if err != nil {
			return nil, fmt.Errorf("parse seed.from: %w", err)
		}
		toStr := toOverride
		if toStr == "" {
			toStr = s.To
		}
		var to time.Time
		if toStr == "" || toStr == "today" {
			to = time.Now().UTC().Truncate(24 * time.Hour)
		} else {
			to, err = time.Parse(dateLayout, toStr)
			if err != nil {
				return nil, fmt.Errorf("parse seed.to: %w", err)
			}
		}
		if to.Before(from) {
			return nil, fmt.Errorf("seed.to %s is before seed.from %s", to.Format(dateLayout), from.Format(dateLayout))
		}
		out := make([]string, 0, 1024)
		for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
			day := d.Format(dateLayout)
			out = append(out, strings.ReplaceAll(s.URLTemplate, "{day}", day))
		}
		return out, nil
	}
	return nil, fmt.Errorf("seed.type %q unknown", s.Type)
}
