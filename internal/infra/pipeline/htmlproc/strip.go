// Package htmlproc strips HTML to plain text.
package htmlproc

import (
	"bytes"
	"io"
	"strings"

	"golang.org/x/net/html"
)

// Strip reads HTML from r and returns visible text concatenated with single spaces.
// Inline <script>, <style>, <noscript> are dropped.
func Strip(r io.Reader) (string, error) {
	z := html.NewTokenizer(r)
	var b strings.Builder
	skipDepth := 0
	for {
		tt := z.Next()
		switch tt {
		case html.ErrorToken:
			if err := z.Err(); err != nil && err != io.EOF {
				return "", err
			}
			return collapse(b.String()), nil
		case html.StartTagToken, html.SelfClosingTagToken:
			name, _ := z.TagName()
			if isSkipTag(name) {
				skipDepth++
			}
		case html.EndTagToken:
			name, _ := z.TagName()
			if isSkipTag(name) && skipDepth > 0 {
				skipDepth--
			}
		case html.TextToken:
			if skipDepth > 0 {
				continue
			}
			t := bytes.TrimSpace(z.Text())
			if len(t) == 0 {
				continue
			}
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.Write(t)
		}
	}
}

func isSkipTag(name []byte) bool {
	switch string(name) {
	case "script", "style", "noscript", "template":
		return true
	}
	return false
}

func collapse(s string) string {
	var b strings.Builder
	prevSpace := false
	for _, r := range s {
		if r == ' ' || r == '\n' || r == '\t' || r == '\r' {
			if !prevSpace && b.Len() > 0 {
				b.WriteByte(' ')
			}
			prevSpace = true
			continue
		}
		prevSpace = false
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}
