// Migrator copies blobs between BlobStores and rewrites their lake_objects rows.
//
// Direction is determined by --from / --to. Default: local → s3.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	cli "github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/crawlerv3/internal/app"
	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/lake"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/gormrepo"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/db/rwdb"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/logx"
	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/store/local"
	s3store "github.com/atvirokodosprendimai/crawlerv3/internal/infra/store/s3"
)

func main() {
	cmd := &cli.Command{
		Name:  "migrator",
		Usage: "move lake blobs between storage backends",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "db-driver", Value: "sqlite", Sources: cli.EnvVars("DB_DRIVER")},
			&cli.StringFlag{Name: "db-dsn", Value: "crawler.db", Sources: cli.EnvVars("DB_DSN")},
			&cli.StringFlag{Name: "read-dsn", Sources: cli.EnvVars("READ_DSN")},

			&cli.StringFlag{Name: "from", Required: true, Usage: "source backend: local | s3"},
			&cli.StringFlag{Name: "to", Required: true, Usage: "destination backend: local | s3"},

			&cli.StringFlag{Name: "local-root", Value: "./blobs", Sources: cli.EnvVars("BLOBS_ROOT")},

			&cli.StringFlag{Name: "s3-bucket", Sources: cli.EnvVars("S3_BUCKET")},
			&cli.StringFlag{Name: "s3-region", Sources: cli.EnvVars("S3_REGION")},
			&cli.StringFlag{Name: "s3-endpoint", Sources: cli.EnvVars("S3_ENDPOINT")},
			&cli.StringFlag{Name: "s3-access-key", Sources: cli.EnvVars("S3_ACCESS_KEY")},
			&cli.StringFlag{Name: "s3-secret-key", Sources: cli.EnvVars("S3_SECRET_KEY")},
			&cli.BoolFlag{Name: "s3-path-style", Sources: cli.EnvVars("S3_PATH_STYLE")},

			&cli.IntFlag{Name: "batch", Value: 100},
			&cli.BoolFlag{Name: "delete-src", Usage: "delete source blob after successful copy"},
			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL"),
				Usage: "debug | info | warn | error"},
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			logx.Init("migrator", c.String("log-level"))
			return ctx, nil
		},
		Action: run,
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("migrator exit", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cmd *cli.Command) error {
	db, err := rwdb.New(rwdb.Config{
		Driver:  rwdb.Driver(cmd.String("db-driver")),
		DSN:     cmd.String("db-dsn"),
		ReadDSN: cmd.String("read-dsn"),
	})
	if err != nil {
		return err
	}
	defer db.Close()

	src, err := buildStore(ctx, cmd, cmd.String("from"))
	if err != nil {
		return fmt.Errorf("src: %w", err)
	}
	dst, err := buildStore(ctx, cmd, cmd.String("to"))
	if err != nil {
		return fmt.Errorf("dst: %w", err)
	}
	lrepo := gormrepo.NewLakeRepo(db)
	mover := &app.Mover{
		Lake:      lrepo,
		Src:       src,
		Dst:       dst,
		DeleteSrc: cmd.Bool("delete-src"),
		BatchSize: cmd.Int("batch"),
	}
	stats, err := mover.Run(ctx)
	if err != nil {
		return err
	}
	slog.Info("migration done",
		"scanned", stats.Scanned,
		"copied", stats.Copied,
		"skipped", stats.Skipped,
		"errors", stats.Errors)
	return nil
}

func buildStore(ctx context.Context, cmd *cli.Command, kind string) (lake.BlobStore, error) {
	switch kind {
	case "local":
		return local.New(cmd.String("local-root"))
	case "s3":
		return s3store.New(ctx, s3store.Config{
			Bucket:          cmd.String("s3-bucket"),
			Region:          cmd.String("s3-region"),
			Endpoint:        cmd.String("s3-endpoint"),
			AccessKeyID:     cmd.String("s3-access-key"),
			SecretAccessKey: cmd.String("s3-secret-key"),
			UsePathStyle:    cmd.Bool("s3-path-style"),
		})
	}
	return nil, fmt.Errorf("unknown backend %q (want local | s3)", kind)
}
