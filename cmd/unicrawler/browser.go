package main

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/tebeka/selenium"
)

// Browser is one open Selenium session.
type Browser struct {
	WD selenium.WebDriver
	id int
}

// BrowserPool holds N pre-opened sessions. Workers Checkout()/Return() to
// reuse the same Chrome window across jobs (avoids ~1s per-job startup +
// keeps the Selenium grid slot occupied).
//
// Sessions leak page state (cookies, sessionStorage) across jobs. For sites
// that need clean state per job, set --concurrency to your job count and
// rely on grid recycling, or extend this type with WD.DeleteAllCookies().
type BrowserPool struct {
	ch chan *Browser
}

// NewBrowserPool dials remote and opens n sessions. The caller must Close()
// the returned pool to release sessions back to the grid.
func NewBrowserPool(remote, browser string, n int, pageLoad, script time.Duration) (*BrowserPool, error) {
	if n < 1 {
		n = 1
	}
	p := &BrowserPool{ch: make(chan *Browser, n)}
	for i := 0; i < n; i++ {
		wd, err := dial(remote, browser, pageLoad, script)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("session %d: %w", i, err)
		}
		p.ch <- &Browser{WD: wd, id: i}
	}
	slog.Info("browser pool ready", "n", n, "remote", remote, "browser", browser)
	return p, nil
}

// dial opens one WebDriver session against the remote URL.
func dial(remote, browser string, pageLoad, script time.Duration) (selenium.WebDriver, error) {
	caps := selenium.Capabilities{"browserName": browser}
	// Headless flags for chromium — docker-selenium images already run
	// headless, but setting the arg is harmless and works for bare chromium
	// containers too.
	if strings.HasPrefix(browser, "chrome") {
		caps["goog:chromeOptions"] = map[string]any{
			"args": []string{
				"--headless=new",
				"--no-sandbox",
				"--disable-dev-shm-usage",
				"--disable-gpu",
				"--window-size=1920,1080",
			},
		}
	}
	wd, err := selenium.NewRemote(caps, remote)
	if err != nil {
		return nil, err
	}
	if err := wd.SetPageLoadTimeout(pageLoad); err != nil {
		_ = wd.Quit()
		return nil, fmt.Errorf("set page load timeout: %w", err)
	}
	if err := wd.SetAsyncScriptTimeout(script); err != nil {
		_ = wd.Quit()
		return nil, fmt.Errorf("set script timeout: %w", err)
	}
	return wd, nil
}

// Checkout blocks until a session is free.
func (p *BrowserPool) Checkout() *Browser { return <-p.ch }

// Return puts the session back. Callers should check WD.Quit() errors only
// at Close time.
func (p *BrowserPool) Return(b *Browser) { p.ch <- b }

// Close drains the pool and quits every session.
func (p *BrowserPool) Close() {
	close(p.ch)
	for b := range p.ch {
		if err := b.WD.Quit(); err != nil {
			slog.Warn("browser quit", "id", b.id, "err", err)
		}
	}
}

// by maps a Selector's SelectorType to the Selenium "by" constant.
func by(s Selector) string {
	switch s.SelectorType {
	case "xpath":
		return selenium.ByXPATH
	default:
		return selenium.ByCSSSelector
	}
}
