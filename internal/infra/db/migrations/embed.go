// Package migrations embeds goose SQL migrations for each dialect.
package migrations

import "embed"

//go:embed sqlite/*.sql
var SQLite embed.FS

//go:embed postgres/*.sql
var Postgres embed.FS

//go:embed mysql/*.sql
var MySQL embed.FS
