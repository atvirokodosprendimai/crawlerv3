// Package http exposes the registry's chi router and middleware.
package http

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"

	"github.com/atvirokodosprendimai/crawlerv3/internal/domain/workerid"
)

type ctxKey int

const ctxKeyWorker ctxKey = 1

// WorkerFromCtx returns the authenticated worker, if any.
func WorkerFromCtx(ctx context.Context) (*workerid.Worker, bool) {
	w, ok := ctx.Value(ctxKeyWorker).(*workerid.Worker)
	return w, ok && w != nil
}

// PATAuth verifies the Bearer token by sha256(token) → workers.pat_hash lookup.
func PATAuth(repo workerid.Repository) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := bearer(r)
			if tok == "" {
				writeError(w, http.StatusUnauthorized, "missing_bearer", "Authorization: Bearer <PAT> required")
				return
			}
			sum := sha256.Sum256([]byte(tok))
			wk, err := repo.FindByPATHash(r.Context(), sum[:])
			if err != nil {
				writeError(w, http.StatusInternalServerError, "auth_lookup", err.Error())
				return
			}
			if wk == nil {
				writeError(w, http.StatusUnauthorized, "unknown_pat", "PAT not recognized")
				return
			}
			if wk.IsBanned() {
				writeError(w, http.StatusForbidden, "banned", "worker banned")
				return
			}
			ip := clientIP(r)
			_ = repo.TouchIP(r.Context(), wk.ID, ip)
			ctx := context.WithValue(r.Context(), ctxKeyWorker, wk)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const p = "Bearer "
	if !strings.HasPrefix(h, p) {
		return ""
	}
	return strings.TrimSpace(h[len(p):])
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.Index(xff, ","); i > 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if ra := r.RemoteAddr; ra != "" {
		if i := strings.LastIndex(ra, ":"); i > 0 {
			return ra[:i]
		}
		return ra
	}
	return ""
}
