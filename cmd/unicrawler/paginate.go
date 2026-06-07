package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tebeka/selenium"
)

// paginate walks page 2..N according to pg.Type and calls onPage(html) for
// each page after the first. The caller is responsible for calling onPage on
// page 1 before invoking paginate. Returns when:
//   - the strategy says "done" (next button gone, scroll idle, max reached)
//   - ctx is cancelled
//   - any browser operation errors
func paginate(ctx context.Context, wd selenium.WebDriver, listingURL string, pg Pagination, onPage func(html string) error) error {
	switch pg.Type {
	case "none", "":
		return nil
	case "next_button":
		return paginateNext(ctx, wd, pg, onPage)
	case "numbered_buttons":
		return paginateNumbered(ctx, wd, pg, onPage)
	case "infinite_scroll":
		return paginateScroll(ctx, wd, pg, onPage)
	case "url_param":
		return paginateURLParam(ctx, wd, listingURL, pg, onPage)
	}
	return fmt.Errorf("unknown pagination type %q", pg.Type)
}

// --- next_button ----------------------------------------------------------

func paginateNext(ctx context.Context, wd selenium.WebDriver, pg Pagination, onPage func(string) error) error {
	for i := 1; pg.MaxPages == 0 || i <= pg.MaxPages; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		el, err := wd.FindElement(by(pg.NextSelector), pg.NextSelector.Selector)
		if err != nil {
			slog.Debug("next_button: selector missed, pagination done", "iter", i)
			return nil
		}
		// Treat aria-disabled / disabled / .disabled as "no more pages".
		if isDisabled(el) {
			slog.Debug("next_button: button disabled, pagination done", "iter", i)
			return nil
		}
		if err := el.Click(); err != nil {
			return fmt.Errorf("next_button click iter=%d: %w", i, err)
		}
		if err := sleepCtx(ctx, time.Duration(pg.DelayMS)*time.Millisecond); err != nil {
			return err
		}
		html, err := wd.PageSource()
		if err != nil {
			return fmt.Errorf("next_button page_source iter=%d: %w", i, err)
		}
		if err := onPage(html); err != nil {
			return err
		}
	}
	return nil
}

// --- numbered_buttons -----------------------------------------------------

func paginateNumbered(ctx context.Context, wd selenium.WebDriver, pg Pagination, onPage func(string) error) error {
	total, ok := readTotal(wd, pg.TotalSelector)
	if !ok {
		slog.Warn("numbered_buttons: total_selector missed; skipping pagination")
		return nil
	}
	extra := total / pg.PerPage
	if pg.MaxPages > 0 && extra > pg.MaxPages {
		extra = pg.MaxPages
	}
	slog.Info("numbered_buttons", "total", total, "per_page", pg.PerPage, "extra_pages", extra)

	for i := 1; i <= extra; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		nn := indexFn(pg.ButtonIndexFn, i)
		sel := Selector{
			Selector:     strings.ReplaceAll(pg.ButtonTemplate.Selector, "{NN}", nn),
			SelectorType: pg.ButtonTemplate.SelectorType,
		}
		el, err := wd.FindElement(by(sel), sel.Selector)
		if err != nil {
			return fmt.Errorf("numbered_buttons iter=%d find %q: %w", i, sel.Selector, err)
		}
		if err := el.Click(); err != nil {
			return fmt.Errorf("numbered_buttons iter=%d click: %w", i, err)
		}
		if err := sleepCtx(ctx, time.Duration(pg.DelayMS)*time.Millisecond); err != nil {
			return err
		}
		html, err := wd.PageSource()
		if err != nil {
			return fmt.Errorf("numbered_buttons iter=%d page_source: %w", i, err)
		}
		if err := onPage(html); err != nil {
			return err
		}
	}
	return nil
}

// readTotal pulls an integer out of the element matched by sel.
func readTotal(wd selenium.WebDriver, sel Selector) (int, bool) {
	el, err := wd.FindElement(by(sel), sel.Selector)
	if err != nil {
		return 0, false
	}
	t, err := el.Text()
	if err != nil {
		return 0, false
	}
	t = strings.TrimSpace(t)
	// Some sites render counts as "iš 1234" or "1,234 rezultatų". Pull the
	// first run of digits.
	var digits strings.Builder
	for _, r := range t {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		} else if digits.Len() > 0 {
			break
		}
	}
	if digits.Len() == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(digits.String())
	if err != nil {
		return 0, false
	}
	return n, true
}

// indexFn shapes the page-button index per Pagination.ButtonIndexFn.
//
//	"linear" → "%02d" of i
//	"liteko" → scrape.ts getPageButton: i<=10 → "%02d"; else (i-11)%10+2 → "%02d"
func indexFn(name string, i int) string {
	switch name {
	case "linear":
		return fmt.Sprintf("%02d", i)
	default: // liteko
		if i <= 10 {
			return fmt.Sprintf("%02d", i)
		}
		return fmt.Sprintf("%02d", ((i-11)%10)+2)
	}
}

// --- infinite_scroll ------------------------------------------------------

func paginateScroll(ctx context.Context, wd selenium.WebDriver, pg Pagination, onPage func(string) error) error {
	idle := 0
	prev := -1
	for i := 1; pg.MaxPages == 0 || i <= pg.MaxPages; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := wd.ExecuteScript("window.scrollTo(0, document.body.scrollHeight);", nil); err != nil {
			return fmt.Errorf("infinite_scroll iter=%d scroll: %w", i, err)
		}
		if err := sleepCtx(ctx, time.Duration(pg.DelayMS)*time.Millisecond); err != nil {
			return err
		}
		els, err := wd.FindElements(by(pg.ItemSelector), pg.ItemSelector.Selector)
		if err != nil {
			return fmt.Errorf("infinite_scroll iter=%d count: %w", i, err)
		}
		n := len(els)
		slog.Debug("infinite_scroll", "iter", i, "items", n, "prev", prev, "idle", idle)
		if n == prev {
			idle++
			if idle >= pg.MaxIdleRounds {
				return nil
			}
			continue
		}
		idle = 0
		prev = n
		html, err := wd.PageSource()
		if err != nil {
			return fmt.Errorf("infinite_scroll iter=%d page_source: %w", i, err)
		}
		if err := onPage(html); err != nil {
			return err
		}
	}
	return nil
}

// --- url_param ------------------------------------------------------------

func paginateURLParam(ctx context.Context, wd selenium.WebDriver, listingURL string, pg Pagination, onPage func(string) error) error {
	u, err := url.Parse(listingURL)
	if err != nil {
		return fmt.Errorf("parse listing URL: %w", err)
	}
	for k := 0; pg.MaxPages == 0 || k < pg.MaxPages; k++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		v := pg.StartIndex + k*pg.Step
		q := u.Query()
		q.Set(pg.URLParam, strconv.Itoa(v))
		u.RawQuery = q.Encode()
		next := u.String()
		if err := wd.Get(next); err != nil {
			// First miss = pagination done (404, server-side empty).
			slog.Debug("url_param: GET failed, treating as done", "url", next, "err", err)
			return nil
		}
		if err := sleepCtx(ctx, time.Duration(pg.DelayMS)*time.Millisecond); err != nil {
			return err
		}
		html, err := wd.PageSource()
		if err != nil {
			return fmt.Errorf("url_param iter=%d page_source: %w", k, err)
		}
		if err := onPage(html); err != nil {
			return err
		}
	}
	return nil
}

// --- helpers --------------------------------------------------------------

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func isDisabled(el selenium.WebElement) bool {
	if v, _ := el.GetAttribute("disabled"); v != "" && v != "false" {
		return true
	}
	if v, _ := el.GetAttribute("aria-disabled"); v == "true" {
		return true
	}
	if v, _ := el.GetAttribute("class"); strings.Contains(v, "disabled") {
		return true
	}
	if enabled, err := el.IsEnabled(); err == nil && !enabled {
		return true
	}
	return false
}
