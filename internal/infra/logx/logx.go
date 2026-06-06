// Package logx initializes the process-wide slog logger.
//
// All cmd/* binaries call Init exactly once during startup. The resulting
// slog.Default() carries timestamps and a component tag for ops grep-ability.
package logx

import (
	"log/slog"
	"os"
	"strings"
)

// Init configures slog.Default with a text handler writing to stderr.
//
// level: "debug" | "info" | "warn" | "error" (case-insensitive). Unknown
// values fall back to info. component is added to every log record so
// `tail -F` on multiple binaries can be filtered by it.
func Init(component, level string) {
	lvl := ParseLevel(level)
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(h).With("component", component))
}

// ParseLevel maps a string to slog.Level. Unknown → info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
