// Package urls handles URL canonicalization and hashing.
package urls

import (
	"crypto/sha256"
	"net/url"
	"sort"
	"strings"
)

// Canonical produces a stable canonical form for dedup/keying.
// Rules: lowercase host, strip fragment, drop default port, sort query keys.
// Does NOT resolve <base href>; callers (workers) must resolve relative refs first.
func Canonical(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	u.Host = strings.ToLower(u.Host)
	u.Fragment = ""
	// drop default ports
	if (u.Scheme == "http" && strings.HasSuffix(u.Host, ":80")) ||
		(u.Scheme == "https" && strings.HasSuffix(u.Host, ":443")) {
		if i := strings.LastIndex(u.Host, ":"); i >= 0 {
			u.Host = u.Host[:i]
		}
	}
	// sort query
	if u.RawQuery != "" {
		q := u.Query()
		keys := make([]string, 0, len(q))
		for k := range q {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b strings.Builder
		for i, k := range keys {
			vs := q[k]
			sort.Strings(vs)
			for _, v := range vs {
				if b.Len() > 0 || i > 0 {
					b.WriteByte('&')
				}
				b.WriteString(url.QueryEscape(k))
				b.WriteByte('=')
				b.WriteString(url.QueryEscape(v))
			}
		}
		u.RawQuery = b.String()
	}
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), nil
}

// Hash returns sha256(canonical) as raw bytes.
func Hash(canonical string) []byte {
	sum := sha256.Sum256([]byte(canonical))
	return sum[:]
}
