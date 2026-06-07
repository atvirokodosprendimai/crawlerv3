package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	cli "github.com/urfave/cli/v3"
	"gopkg.in/yaml.v3"
)

// Config is the root YAML schema for a single site. One config = one
// domain row + one or more page_types that drive per-URL behavior.
type Config struct {
	// Name is a human label, used in logs and the sidecar-dir path.
	Name string `yaml:"name"`

	// Domain is the host that gets upserted into the registry domains table.
	// All seed URLs and discovered links must resolve to this host or they
	// will be dropped by the registry's scope filter (unless the registry was
	// started with --allow-auto-domains).
	Domain string `yaml:"domain"`

	// Scheme is "https" or "http"; used during domain upsert.
	Scheme string `yaml:"scheme"`

	// CrawlDelayMS is the per-domain crawl delay applied at domain creation.
	CrawlDelayMS int `yaml:"crawl_delay_ms"`

	// Seed defines how to populate the frontier from this config.
	Seed SeedSpec `yaml:"seed"`

	// PageTypes is an ordered list of URL-pattern → behavior bindings.
	// The first matching PageType wins. URLs that match no PageType are
	// fetched as plain detail pages (no pagination, no field extraction).
	PageTypes []PageType `yaml:"page_types"`
}

// SeedSpec describes how to generate the initial URL set.
type SeedSpec struct {
	// Type is "urls" or "date_range".
	Type string `yaml:"type"`

	// URLs is the concrete list when Type=="urls".
	URLs []string `yaml:"urls"`

	// URLTemplate is the per-day template when Type=="date_range".
	// Supported substitutions: {day} (YYYY-MM-DD).
	URLTemplate string `yaml:"url_template"`

	// From is the inclusive start date for date_range, YYYY-MM-DD.
	From string `yaml:"from"`

	// To is the inclusive end date; empty = today (UTC).
	To string `yaml:"to"`
}

// PageType binds a URL pattern to pagination + extraction behavior.
type PageType struct {
	Name string `yaml:"name"`

	// Match is a regex (Go syntax) tested against the URL string. The first
	// PageType whose Match fires owns this URL.
	Match string `yaml:"match"`

	// Pagination strategy, or {type: none} for terminal pages.
	Pagination Pagination `yaml:"pagination"`

	// Extract pulls discovered links and structured fields off each page.
	Extract Extract `yaml:"extract"`

	matchRE *regexp.Regexp
}

// Pagination union: only the fields relevant to Type are read.
type Pagination struct {
	// Type: none | next_button | numbered_buttons | infinite_scroll | url_param
	Type string `yaml:"type"`

	// Common: pause after each page advance.
	DelayMS int `yaml:"delay_ms"`

	// MaxPages caps the loop (0 = unbounded).
	MaxPages int `yaml:"max_pages"`

	// --- next_button ------------------------------------------------------
	// NextSelector points at the "next page" link/button. Empty value or
	// missing element = pagination done.
	NextSelector Selector `yaml:"next_selector"`

	// --- numbered_buttons -------------------------------------------------
	// TotalSelector returns the element whose text is the total result count.
	TotalSelector Selector `yaml:"total_selector"`

	// PerPage is the results-per-page Liteko-style sites need to compute
	// extra_pages = total / per_page.
	PerPage int `yaml:"per_page"`

	// ButtonTemplate is a selector template with literal "{NN}" substituted
	// by the zero-padded page button index. The Pagination.NextSelector style
	// "find then click" is reused per iteration.
	ButtonTemplate Selector `yaml:"button_template"`

	// ButtonIndexFn shapes the {NN} index. "liteko" implements scrape.ts's
	// 01..10 then 02..11 repeat; "linear" emits "%02d" of i.
	ButtonIndexFn string `yaml:"button_index_fn"`

	// --- infinite_scroll -------------------------------------------------
	// ItemSelector counts page items; when 2+ consecutive scrolls don't
	// change the count, scrolling stops.
	ItemSelector Selector `yaml:"item_selector"`

	// MaxIdleRounds is the consecutive no-change scrolls before exit (default 2).
	MaxIdleRounds int `yaml:"max_idle_rounds"`

	// --- url_param --------------------------------------------------------
	// URLParam is the query parameter name to increment (e.g. "page").
	URLParam string `yaml:"url_param"`

	// StartIndex / Step shape the loop, defaults 2 / 1 (first param-bumped
	// page is &page=2, then 3, 4...).
	StartIndex int `yaml:"start_index"`
	Step       int `yaml:"step"`
}

// Extract describes per-page link discovery and field harvesting.
type Extract struct {
	// Links lists selector specs whose matched <a href> become discovered_links.
	Links []LinkSpec `yaml:"links"`

	// Fields lists structured fields written to the sidecar JSON.
	Fields []FieldSpec `yaml:"fields"`
}

// Selector identifies an element. SelectorType is "css" (default) or "xpath".
//
// At callsites where the field IS the selector (LinkSpec, FieldSpec), the
// YAML schema flattens to top-level `selector:` + `selector_type:` keys via
// the Sel() helpers. At callsites where a selector is one of several siblings
// (Pagination.TotalSelector etc.), users write the nested map form:
//
//	total_selector:
//	  selector: "..."
//	  selector_type: xpath
type Selector struct {
	Selector     string `yaml:"selector"`
	SelectorType string `yaml:"selector_type"`
}

// SelOf is a small constructor that defaults SelectorType to "css".
func SelOf(s, t string) Selector { return Selector{Selector: s, SelectorType: t} }

// LinkSpec is one source of <a href> URLs.
type LinkSpec struct {
	Selector     string `yaml:"selector"`
	SelectorType string `yaml:"selector_type"`
	// AnchorAttr defaults to "text"; can be "title", "aria-label", or any
	// attribute name.
	AnchorAttr string `yaml:"anchor_attr"`
	// NewDepth assigned to discovered links (default 1).
	NewDepth int `yaml:"new_depth"`
}

// Sel returns the link's selector pair.
func (l LinkSpec) Sel() Selector { return SelOf(l.Selector, l.SelectorType) }

// FieldSpec is one structured field.
type FieldSpec struct {
	Name         string `yaml:"name"`
	Selector     string `yaml:"selector"`
	SelectorType string `yaml:"selector_type"`
	// Mode: text | text_list | html | attribute | rows
	Mode string `yaml:"mode"`
	// Attr is the attribute name when Mode=="attribute".
	Attr string `yaml:"attr"`
	// Columns is the per-row TD layout when Mode=="rows".
	Columns []RowColumn `yaml:"columns"`
}

// Sel returns the field's selector pair.
func (f FieldSpec) Sel() Selector { return SelOf(f.Selector, f.SelectorType) }

// RowColumn maps one TD index to a named output field on a row.
type RowColumn struct {
	Name  string `yaml:"name"`
	Index int    `yaml:"index"`
}

// LoadConfig reads + validates a YAML file.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("validate %s: %w", path, err)
	}
	return &c, nil
}

// Validate checks the config and compiles regexes. Mutates PageTypes in
// place to attach the compiled matcher.
func (c *Config) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("name is required")
	}
	if c.Domain == "" {
		return fmt.Errorf("domain is required")
	}
	if c.Scheme == "" {
		c.Scheme = "https"
	}
	if c.CrawlDelayMS <= 0 {
		c.CrawlDelayMS = 1000
	}

	switch c.Seed.Type {
	case "urls":
		if len(c.Seed.URLs) == 0 {
			return fmt.Errorf("seed.urls is empty")
		}
	case "date_range":
		if c.Seed.URLTemplate == "" {
			return fmt.Errorf("seed.url_template is required for date_range")
		}
		if !strings.Contains(c.Seed.URLTemplate, "{day}") {
			return fmt.Errorf("seed.url_template must contain {day}")
		}
		if c.Seed.From == "" {
			return fmt.Errorf("seed.from is required for date_range")
		}
	case "":
		return fmt.Errorf("seed.type is required (urls | date_range)")
	default:
		return fmt.Errorf("seed.type %q unknown (urls | date_range)", c.Seed.Type)
	}

	for i := range c.PageTypes {
		pt := &c.PageTypes[i]
		if pt.Name == "" {
			return fmt.Errorf("page_types[%d].name required", i)
		}
		if pt.Match == "" {
			return fmt.Errorf("page_types[%d].match required", i)
		}
		re, err := regexp.Compile(pt.Match)
		if err != nil {
			return fmt.Errorf("page_types[%d].match: %w", i, err)
		}
		pt.matchRE = re

		if err := validatePagination(&pt.Pagination); err != nil {
			return fmt.Errorf("page_types[%d].pagination: %w", i, err)
		}
		for j, ls := range pt.Extract.Links {
			if ls.Selector == "" {
				return fmt.Errorf("page_types[%d].extract.links[%d].selector required", i, j)
			}
		}
		for j, fs := range pt.Extract.Fields {
			if fs.Name == "" || fs.Selector == "" {
				return fmt.Errorf("page_types[%d].extract.fields[%d]: name + selector required", i, j)
			}
			switch fs.Mode {
			case "", "text", "text_list", "html":
			case "attribute":
				if fs.Attr == "" {
					return fmt.Errorf("page_types[%d].extract.fields[%d].attr required for mode=attribute", i, j)
				}
			case "rows":
				if len(fs.Columns) == 0 {
					return fmt.Errorf("page_types[%d].extract.fields[%d].columns required for mode=rows", i, j)
				}
			default:
				return fmt.Errorf("page_types[%d].extract.fields[%d].mode %q unknown", i, j, fs.Mode)
			}
		}
	}
	return nil
}

func validatePagination(p *Pagination) error {
	switch p.Type {
	case "", "none":
		p.Type = "none"
	case "next_button":
		if p.NextSelector.Selector == "" {
			return fmt.Errorf("next_selector required for type=next_button")
		}
	case "numbered_buttons":
		if p.TotalSelector.Selector == "" {
			return fmt.Errorf("total_selector required for type=numbered_buttons")
		}
		if p.PerPage <= 0 {
			return fmt.Errorf("per_page must be > 0")
		}
		if p.ButtonTemplate.Selector == "" {
			return fmt.Errorf("button_template required for type=numbered_buttons")
		}
		if !strings.Contains(p.ButtonTemplate.Selector, "{NN}") {
			return fmt.Errorf("button_template must contain {NN}")
		}
		if p.ButtonIndexFn == "" {
			p.ButtonIndexFn = "liteko"
		}
	case "infinite_scroll":
		if p.ItemSelector.Selector == "" {
			return fmt.Errorf("item_selector required for type=infinite_scroll")
		}
		if p.MaxIdleRounds <= 0 {
			p.MaxIdleRounds = 2
		}
	case "url_param":
		if p.URLParam == "" {
			return fmt.Errorf("url_param required for type=url_param")
		}
		if p.StartIndex == 0 {
			p.StartIndex = 2
		}
		if p.Step == 0 {
			p.Step = 1
		}
	default:
		return fmt.Errorf("type %q unknown", p.Type)
	}
	return nil
}

// MatchPageType returns the first PageType whose Match regex fires on url,
// or nil if none match. Nil = "plain detail page" handling.
func (c *Config) MatchPageType(url string) *PageType {
	for i := range c.PageTypes {
		if c.PageTypes[i].matchRE.MatchString(url) {
			return &c.PageTypes[i]
		}
	}
	return nil
}

// runValidate is the `validate` subcommand entry point.
func runValidate(_ context.Context, cmd *cli.Command) error {
	path := cmd.Args().First()
	if path == "" {
		return fmt.Errorf("usage: unicrawler validate <config.yaml>")
	}
	c, err := LoadConfig(path)
	if err != nil {
		return err
	}
	fmt.Printf("ok: name=%s domain=%s seed=%s page_types=%d\n",
		c.Name, c.Domain, c.Seed.Type, len(c.PageTypes))
	for _, pt := range c.PageTypes {
		fmt.Printf("  - %s match=%q pagination=%s links=%d fields=%d\n",
			pt.Name, pt.Match, pt.Pagination.Type, len(pt.Extract.Links), len(pt.Extract.Fields))
	}
	return nil
}

