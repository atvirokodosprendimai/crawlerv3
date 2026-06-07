package main

import (
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/tebeka/selenium"
)

// extractLinks runs every LinkSpec against the current page and returns the
// discovered URLs (deduped, absolute). pageURL is used to absolutize relative
// hrefs.
func extractLinks(wd selenium.WebDriver, pageURL string, specs []LinkSpec, seen map[string]struct{}) []discoveredLink {
	if seen == nil {
		seen = make(map[string]struct{})
	}
	base, err := url.Parse(pageURL)
	if err != nil {
		slog.Warn("extractLinks: bad pageURL", "url", pageURL, "err", err)
		return nil
	}
	out := make([]discoveredLink, 0, 64)
	for _, ls := range specs {
		sel := ls.Sel()
		els, err := wd.FindElements(by(sel), sel.Selector)
		if err != nil {
			slog.Debug("extractLinks: selector miss", "sel", sel.Selector, "err", err)
			continue
		}
		anchorAttr := ls.AnchorAttr
		if anchorAttr == "" {
			anchorAttr = "text"
		}
		newDepth := ls.NewDepth
		if newDepth == 0 {
			newDepth = 1
		}
		for _, el := range els {
			href, _ := el.GetAttribute("href")
			if href == "" {
				continue
			}
			ref, err := base.Parse(href)
			if err != nil {
				continue
			}
			if ref.Scheme != "http" && ref.Scheme != "https" {
				continue
			}
			full := ref.String()
			if _, dup := seen[full]; dup {
				continue
			}
			seen[full] = struct{}{}
			anchor := ""
			if anchorAttr == "text" {
				if t, err := el.Text(); err == nil {
					anchor = strings.TrimSpace(t)
				}
			} else {
				if v, err := el.GetAttribute(anchorAttr); err == nil {
					anchor = v
				}
			}
			out = append(out, discoveredLink{
				URL: full, Anchor: anchor, NewDepth: newDepth,
			})
		}
	}
	return out
}

// extractFields runs every FieldSpec and returns name → value. Values are:
//
//	text       → string
//	text_list  → []string
//	html       → string (outerHTML)
//	attribute  → string (named attribute value)
//	rows       → []map[string]string (one entry per matched row)
//
// A spec whose selector matches nothing is skipped silently.
func extractFields(wd selenium.WebDriver, specs []FieldSpec) map[string]any {
	out := make(map[string]any, len(specs))
	for _, fs := range specs {
		mode := fs.Mode
		if mode == "" {
			mode = "text"
		}
		sel := fs.Sel()
		switch mode {
		case "text":
			if v, ok := firstText(wd, sel); ok {
				out[fs.Name] = v
			}
		case "text_list":
			if vs, ok := allText(wd, sel); ok {
				out[fs.Name] = vs
			}
		case "html":
			if v, ok := firstHTML(wd, sel); ok {
				out[fs.Name] = v
			}
		case "attribute":
			if v, ok := firstAttr(wd, sel, fs.Attr); ok {
				out[fs.Name] = v
			}
		case "rows":
			rows := extractRows(wd, sel, fs.Columns)
			if len(rows) > 0 {
				out[fs.Name] = rows
			}
		default:
			slog.Warn("extractFields: unknown mode", "name", fs.Name, "mode", mode)
		}
	}
	return out
}

func firstText(wd selenium.WebDriver, sel Selector) (string, bool) {
	el, err := wd.FindElement(by(sel), sel.Selector)
	if err != nil {
		return "", false
	}
	t, err := el.Text()
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(t), true
}

func allText(wd selenium.WebDriver, sel Selector) ([]string, bool) {
	els, err := wd.FindElements(by(sel), sel.Selector)
	if err != nil || len(els) == 0 {
		return nil, false
	}
	out := make([]string, 0, len(els))
	for _, el := range els {
		t, err := el.Text()
		if err != nil {
			continue
		}
		out = append(out, strings.TrimSpace(t))
	}
	return out, len(out) > 0
}

func firstHTML(wd selenium.WebDriver, sel Selector) (string, bool) {
	el, err := wd.FindElement(by(sel), sel.Selector)
	if err != nil {
		return "", false
	}
	// outerHTML is not a W3C standard call; use getAttribute("outerHTML").
	v, err := el.GetAttribute("outerHTML")
	if err != nil || v == "" {
		return "", false
	}
	return v, true
}

func firstAttr(wd selenium.WebDriver, sel Selector, attr string) (string, bool) {
	el, err := wd.FindElement(by(sel), sel.Selector)
	if err != nil {
		return "", false
	}
	v, err := el.GetAttribute(attr)
	if err != nil {
		return "", false
	}
	return v, true
}

// extractRows: each matched row element's <td> children at the configured
// indices become named fields on the output row map.
func extractRows(wd selenium.WebDriver, sel Selector, cols []RowColumn) []map[string]string {
	rows, err := wd.FindElements(by(sel), sel.Selector)
	if err != nil || len(rows) == 0 {
		return nil
	}
	out := make([]map[string]string, 0, len(rows))
	for _, row := range rows {
		tds, err := row.FindElements(selenium.ByCSSSelector, "td")
		if err != nil || len(tds) == 0 {
			continue
		}
		m := make(map[string]string, len(cols))
		for _, col := range cols {
			if col.Index < 0 || col.Index >= len(tds) {
				continue
			}
			t, err := tds[col.Index].Text()
			if err != nil {
				continue
			}
			m[col.Name] = strings.TrimSpace(t)
		}
		if len(m) > 0 {
			out = append(out, m)
		}
	}
	return out
}

// keep fmt imported when no compile-time use
var _ = fmt.Sprintf
