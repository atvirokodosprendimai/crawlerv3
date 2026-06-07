package main

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

// Liteko-specific constants. Mirror the TS reference in tmp/liteko/examples
// and the Go port in tmp/liteko/internal/crawler.

// BaseURL is the Liteko viesasprendimupaieska root. Case detail hrefs in the
// listing TDs (e.g. "tekstas.aspx?id=<uuid>") are relative to this prefix.
const BaseURL = "https://liteko.teismai.lt/viesasprendimupaieska/"

// resultsPerPage is the RadDataPager page size on paieska.aspx.
const resultsPerPage = 50

// caseRowLabel marks the TDs in a Liteko listing that wrap a single case row.
const caseRowLabel = "Bylos numeris"

// listingCase is one row on a Liteko search-result page.
type listingCase struct {
	Href   string // relative — caller prepends BaseURL
	Anchor string // visible link text (file label)
}

// listingPage is everything one paieska.aspx page yields: the cases on it,
// the total result count (drives how many more pages to fetch), and the
// VIEWSTATE/VIEWSTATEGENERATOR pair the next __doPostBack POST must echo back.
type listingPage struct {
	Cases        []listingCase
	Total        int
	ViewState    string
	ViewStateGen string
}

// parseListing extracts cases, total, and viewstate from a Liteko listing.
func parseListing(body []byte) (*listingPage, error) {
	root, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	lp := &listingPage{}
	walk(root, func(n *html.Node) {
		switch {
		case n.Type == html.ElementNode && n.Data == "td":
			if c, ok := caseFromTD(n); ok {
				lp.Cases = append(lp.Cases, c)
			}
		case n.Type == html.ElementNode && n.Data == "span":
			id := attr(n, "id")
			if strings.HasSuffix(id, "_TotalItemsLabel") && lp.Total == 0 {
				if v, err := strconv.Atoi(strings.TrimSpace(text(n))); err == nil {
					lp.Total = v
				}
			}
		case n.Type == html.ElementNode && n.Data == "input":
			switch attr(n, "id") {
			case "__VIEWSTATE":
				lp.ViewState = attr(n, "value")
			case "__VIEWSTATEGENERATOR":
				lp.ViewStateGen = attr(n, "value")
			}
		}
	})
	return lp, nil
}

// caseFromTD returns the case-row payload from a TD, or ok=false when the TD
// isn't a case row. A TD is a case row when it contains a <b>Bylos numeris:</b>
// label and an <a href="...">.
func caseFromTD(td *html.Node) (listingCase, bool) {
	hasLabel := false
	walk(td, func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "b" {
			lbl := strings.TrimSuffix(strings.TrimSpace(text(n)), ":")
			if lbl == caseRowLabel {
				hasLabel = true
			}
		}
	})
	if !hasLabel {
		return listingCase{}, false
	}
	var href, anchor string
	walk(td, func(n *html.Node) {
		if href != "" || n.Type != html.ElementNode || n.Data != "a" {
			return
		}
		h := attr(n, "href")
		if h == "" {
			return
		}
		href = h
		anchor = strings.TrimSpace(text(n))
	})
	if href == "" {
		return listingCase{}, false
	}
	return listingCase{Href: href, Anchor: anchor}, true
}

// pageButton returns the RadDataPager ctlNN suffix for the i-th POST after the
// initial GET. Mirrors scrape.ts getPageButton(): i=1..10 → "01".."10", then
// subsequent blocks repeat "02".."11". i=0 is the initial GET (never POSTed).
func pageButton(i int) string {
	if i <= 0 {
		return ""
	}
	if i <= 10 {
		return fmt.Sprintf("%02d", i)
	}
	return fmt.Sprintf("%02d", ((i-11)%10)+2)
}

// --- html.Node helpers ----------------------------------------------------

func walk(n *html.Node, fn func(*html.Node)) {
	fn(n)
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		walk(c, fn)
	}
}

func attr(n *html.Node, key string) string {
	for _, a := range n.Attr {
		if a.Key == key {
			return a.Val
		}
	}
	return ""
}

func text(n *html.Node) string {
	var b strings.Builder
	walk(n, func(c *html.Node) {
		if c.Type == html.TextNode {
			b.WriteString(c.Data)
		}
	})
	return b.String()
}
