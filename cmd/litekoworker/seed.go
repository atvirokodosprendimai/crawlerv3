package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"time"

	cli "github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/frontier"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/gormrepo"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/urls"
)

// litekoHost is the only host this worker ever crawls. Hard-coded because the
// seed URL shape (paieska.aspx?nuo=...&iki=...) and the pagination protocol
// are specific to this site.
const litekoHost = "liteko.teismai.lt"

// dateLayout is YYYY-MM-DD — the form accepted by --from/--to.
const dateLayout = "2006-01-02"

// runSeed opens the registry DB directly, upserts the liteko domain row, and
// enqueues one per-day search URL for each day in [from, to]. Splitting the
// range a day at a time keeps each listing under Liteko's result cap so the
// pager can reach every case (a single multi-year search silently drops the
// tail). The frontier's unique URL hash dedups re-runs.
func runSeed(ctx context.Context, cmd *cli.Command) error {
	from, err := time.Parse(dateLayout, cmd.String("from"))
	if err != nil {
		return fmt.Errorf("parse --from: %w", err)
	}
	toStr := cmd.String("to")
	var to time.Time
	if toStr == "" {
		to = time.Now().UTC().Truncate(24 * time.Hour)
	} else {
		to, err = time.Parse(dateLayout, toStr)
		if err != nil {
			return fmt.Errorf("parse --to: %w", err)
		}
	}
	if to.Before(from) {
		return fmt.Errorf("--to %s is before --from %s", to.Format(dateLayout), from.Format(dateLayout))
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
	dom, err := frepo.UpsertByHost(ctx, litekoHost, "https", cmd.Int("crawl-delay-ms"))
	if err != nil {
		return fmt.Errorf("upsert domain: %w", err)
	}
	slog.Info("domain ready", "id", dom.ID, "host", dom.Host, "scheme", dom.Scheme, "delay_ms", dom.CrawlDelayMS)

	var inserted, dupes int
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		raw := dayURL(d)
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
	slog.Info("seed complete",
		"from", from.Format(dateLayout), "to", to.Format(dateLayout),
		"inserted", inserted, "duplicate", dupes)
	return nil
}

// dayURL builds the Liteko search URL for a single calendar day:
// paieska.aspx?nuo=YYYY-MM-DD%2000:00:00&iki=YYYY-MM-DD%2023:59:59.
func dayURL(d time.Time) string {
	day := d.Format(dateLayout)
	nuo := url.QueryEscape(day + " 00:00:00")
	iki := url.QueryEscape(day + " 23:59:59")
	return fmt.Sprintf("%spaieska.aspx?nuo=%s&iki=%s", BaseURL, nuo, iki)
}
