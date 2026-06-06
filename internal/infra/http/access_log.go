package http

import (
	"log/slog"
	"net/http"
	"time"

	chimw "github.com/go-chi/chi/v5/middleware"
)

// AccessLog logs one slog.Info per request: method, path, status, bytes, dur_ms, request_id.
// Health checks are logged at Debug.
func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := chimw.NewWrapResponseWriter(w, r.ProtoMajor)
		start := time.Now()
		next.ServeHTTP(ww, r)
		dur := time.Since(start)

		lvl := slog.LevelInfo
		if r.URL.Path == "/healthz" {
			lvl = slog.LevelDebug
		} else if ww.Status() >= 500 {
			lvl = slog.LevelError
		} else if ww.Status() >= 400 {
			lvl = slog.LevelWarn
		}
		slog.Default().Log(r.Context(), lvl, "http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", ww.Status(),
			"bytes", ww.BytesWritten(),
			"dur_ms", dur.Milliseconds(),
			"request_id", chimw.GetReqID(r.Context()),
			"remote", r.RemoteAddr,
		)
	})
}
