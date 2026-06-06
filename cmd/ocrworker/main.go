// ocr-worker is a crawlerv3 task worker specialized for PDF OCR.
//
// It reserves pdf_ocr tasks from the registry, downloads each PDF blob, and
// runs a page-parallel pipeline:
//
//	mutool show {pdf} trailer/Root/Pages/Count   → page count
//	gs -sDEVICE=pnggray -r300 ...                → one PNG per page
//	tesseract -l {lang} page.png page txt        → one .txt per page
//
// Concatenated page text is posted to /v1/tasks/result as the extracted text.
//
// External tools required on PATH: mutool, gs, tesseract.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	cli "github.com/urfave/cli/v3"

	"github.com/atvirokodosprendimai/crawlerv3/internal/infra/logx"
)

const taskKind = "pdf_ocr"

func main() {
	cmd := &cli.Command{
		Name:  "ocr-worker",
		Usage: "PDF OCR task worker for crawlerv3 (mutool + gs + tesseract page-parallel)",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "registry", Required: true, Sources: cli.EnvVars("REGISTRY")},
			&cli.StringFlag{Name: "pat", Required: true, Sources: cli.EnvVars("PAT")},
			&cli.IntFlag{Name: "batch", Value: 4, Usage: "tasks reserved per /v1/tasks/reserve call"},
			&cli.IntFlag{Name: "concurrency", Value: 2, Usage: "max PDFs processed in parallel from one batch"},
			&cli.IntFlag{Name: "page-concurrency", Value: 4, Sources: cli.EnvVars("PAGE_CONCURRENCY"),
				Usage: "max pages OCR'd in parallel within one PDF"},
			&cli.StringFlag{Name: "tesseract-lang", Value: "eng+lit", Sources: cli.EnvVars("TESSERACT_LANG")},
			&cli.IntFlag{Name: "render-dpi", Value: 300, Usage: "gs rasterization DPI"},
			&cli.DurationFlag{Name: "idle-sleep", Value: 5 * time.Second},
			&cli.DurationFlag{Name: "max-runtime", Value: 0, Usage: "exit after this duration; 0 = run forever"},
			&cli.DurationFlag{Name: "exec-timeout", Value: 10 * time.Minute,
				Usage: "wall-clock budget for one PDF end-to-end"},
			&cli.DurationFlag{Name: "page-timeout", Value: 2 * time.Minute,
				Usage: "per-page (gs + tesseract) timeout"},
			&cli.DurationFlag{Name: "heartbeat-interval", Value: 30 * time.Second,
				Usage: "POST /v1/tasks/heartbeat every N to keep lease alive (0 disables)"},
			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL"),
				Usage: "debug | info | warn | error"},
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			logx.Init("ocr-worker", c.String("log-level"))
			return ctx, nil
		},
		Action: run,
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("ocr-worker exit", "err", err)
		os.Exit(1)
	}
}

type task struct {
	TaskID          int64  `json:"task_id"`
	Processor       string `json:"processor"`
	LakeObjectID    int64  `json:"lake_object_id"`
	BlobURL         string `json:"blob_url"`
	BlobContentType string `json:"blob_content_type"`
	BlobSizeBytes   int64  `json:"blob_size_bytes"`
	AttemptCount    int    `json:"attempt_count"`
	LeaseToken      string `json:"lease_token"`
	LeaseExpiresAt  int64  `json:"lease_expires_at"`
}

type reserveResp struct {
	Tasks []task `json:"tasks"`
}

type workerCfg struct {
	registry, pat        string
	batch, conc, pageCC  int
	tessLang             string
	renderDPI            int
	idle, execTO, pageTO time.Duration
	hbInterval           time.Duration
}

func run(ctx context.Context, cmd *cli.Command) error {
	if err := checkTools(); err != nil {
		return err
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		slog.Info("stopping")
		os.Exit(0)
	}()

	deadline := time.Time{}
	if d := cmd.Duration("max-runtime"); d > 0 {
		deadline = time.Now().Add(d)
	}

	cfg := workerCfg{
		registry:   strings.TrimRight(cmd.String("registry"), "/"),
		pat:        cmd.String("pat"),
		batch:      cmd.Int("batch"),
		conc:       max1(cmd.Int("concurrency")),
		pageCC:     max1(cmd.Int("page-concurrency")),
		tessLang:   cmd.String("tesseract-lang"),
		renderDPI:  cmd.Int("render-dpi"),
		idle:       cmd.Duration("idle-sleep"),
		execTO:     cmd.Duration("exec-timeout"),
		pageTO:     cmd.Duration("page-timeout"),
		hbInterval: cmd.Duration("heartbeat-interval"),
	}

	c := &http.Client{Timeout: 120 * time.Second}
	sem := make(chan struct{}, cfg.conc)

	slog.Info("ocr-worker started",
		"registry", cfg.registry, "kind", taskKind,
		"batch", cfg.batch, "concurrency", cfg.conc,
		"page_concurrency", cfg.pageCC, "tesseract_lang", cfg.tessLang,
		"render_dpi", cfg.renderDPI, "exec_timeout", cfg.execTO,
		"page_timeout", cfg.pageTO, "heartbeat_interval", cfg.hbInterval)

	for {
		if !deadline.IsZero() && time.Now().After(deadline) {
			slog.Info("max-runtime reached, exiting")
			return nil
		}
		tasks, err := reserve(ctx, c, cfg)
		if err != nil {
			slog.Error("reserve", "err", err)
			time.Sleep(cfg.idle)
			continue
		}
		if len(tasks) == 0 {
			slog.Debug("reserve empty, idling", "sleep", cfg.idle)
			time.Sleep(cfg.idle)
			continue
		}
		slog.Info("batch reserved", "n", len(tasks))
		var wg sync.WaitGroup
		for _, t := range tasks {
			wg.Add(1)
			sem <- struct{}{}
			go func(t task) {
				defer wg.Done()
				defer func() { <-sem }()
				workOne(ctx, c, cfg, t)
			}(t)
		}
		wg.Wait()
	}
}

func checkTools() error {
	for _, bin := range []string{"mutool", "gs", "tesseract"} {
		if _, err := exec.LookPath(bin); err != nil {
			return fmt.Errorf("required tool %q not found on PATH", bin)
		}
	}
	return nil
}

func reserve(ctx context.Context, c *http.Client, cfg workerCfg) ([]task, error) {
	body, _ := json.Marshal(map[string]any{"kinds": []string{taskKind}, "batch": cfg.batch})
	req, _ := http.NewRequestWithContext(ctx, "POST", cfg.registry+"/v1/tasks/reserve", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+cfg.pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("reserve: status=%d body=%s", resp.StatusCode, string(b))
	}
	var rr reserveResp
	if err := json.NewDecoder(resp.Body).Decode(&rr); err != nil {
		return nil, err
	}
	return rr.Tasks, nil
}

func workOne(ctx context.Context, c *http.Client, cfg workerCfg, t task) {
	log := slog.With("task_id", t.TaskID, "lake_object_id", t.LakeObjectID)
	start := time.Now()
	log.Info("task start", "blob", t.BlobURL, "size", t.BlobSizeBytes, "attempt", t.AttemptCount)

	scratch, err := os.MkdirTemp("", "ocrworker-*")
	if err != nil {
		postFail(ctx, c, cfg, t, "scratch_mkdir", err.Error(), true)
		return
	}
	defer os.RemoveAll(scratch)

	input := filepath.Join(scratch, "input.pdf")
	if err := downloadBlob(ctx, c, cfg, t.BlobURL, input); err != nil {
		postFail(ctx, c, cfg, t, "download", err.Error(), true)
		return
	}

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	if cfg.hbInterval > 0 {
		go heartbeatLoop(hbCtx, c, cfg, t.TaskID, t.LeaseToken)
	}

	execCtx, cancel := context.WithTimeout(ctx, cfg.execTO)
	defer cancel()

	pageCount, err := getPageCount(execCtx, input)
	if err != nil {
		postFail(ctx, c, cfg, t, "page_count", err.Error(), true)
		return
	}
	log.Info("page count", "pages", pageCount)
	if pageCount == 0 {
		postFail(ctx, c, cfg, t, "empty_pdf", "zero pages", false)
		return
	}

	texts, perr := ocrAllPages(execCtx, log, scratch, input, pageCount, cfg)
	if perr != nil {
		postFail(ctx, c, cfg, t, "ocr_pipeline", perr.Error(), true)
		return
	}

	joined := strings.Join(texts, "\n\n")
	if err := postResultText(ctx, c, cfg, t.TaskID, t.LeaseToken, joined); err != nil {
		log.Error("post text", "err", err)
		return
	}
	log.Info("task ok",
		"pages", pageCount, "text_bytes", len(joined),
		"dur_ms", time.Since(start).Milliseconds())
}

func getPageCount(ctx context.Context, pdfPath string) (int, error) {
	out, err := exec.CommandContext(ctx, "mutool", "show", pdfPath, "trailer/Root/Pages/Count").Output()
	if err != nil {
		return 0, fmt.Errorf("mutool show: %w", err)
	}
	var n int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &n); err != nil {
		return 0, fmt.Errorf("parse page count %q: %w", string(out), err)
	}
	return n, nil
}

func ocrAllPages(ctx context.Context, log *slog.Logger, scratch, pdfPath string, pageCount int, cfg workerCfg) ([]string, error) {
	pagesDir := filepath.Join(scratch, "pages")
	if err := os.MkdirAll(pagesDir, 0o755); err != nil {
		return nil, err
	}

	type pageErr struct {
		page int
		err  error
	}
	errCh := make(chan pageErr, pageCount)
	sem := make(chan struct{}, cfg.pageCC)
	var wg sync.WaitGroup

	for i := 1; i <= pageCount; i++ {
		wg.Add(1)
		go func(page int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			pageCtx, cancel := context.WithTimeout(ctx, cfg.pageTO)
			defer cancel()

			pngPath := filepath.Join(pagesDir, fmt.Sprintf("page-%04d.png", page))
			outBase := filepath.Join(pagesDir, fmt.Sprintf("page-%04d", page))

			if err := runGs(pageCtx, pagesDir, pdfPath, page, cfg.renderDPI); err != nil {
				errCh <- pageErr{page, fmt.Errorf("gs: %w", err)}
				return
			}
			if err := runTesseract(pageCtx, pngPath, outBase, cfg.tessLang); err != nil {
				errCh <- pageErr{page, fmt.Errorf("tesseract: %w", err)}
				return
			}
			_ = os.Remove(pngPath)
			log.Debug("page ok", "page", page)
		}(i)
	}
	wg.Wait()
	close(errCh)

	var firstErr error
	for pe := range errCh {
		log.Error("page failed", "page", pe.page, "err", pe.err)
		if firstErr == nil {
			firstErr = fmt.Errorf("page %d: %w", pe.page, pe.err)
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}

	texts := make([]string, pageCount)
	for i := 1; i <= pageCount; i++ {
		txtPath := filepath.Join(pagesDir, fmt.Sprintf("page-%04d.txt", i))
		b, err := os.ReadFile(txtPath)
		if err != nil {
			return nil, fmt.Errorf("read page %d text: %w", i, err)
		}
		texts[i-1] = string(b)
	}
	return texts, nil
}

func runGs(ctx context.Context, outDir, pdfPath string, page, dpi int) error {
	args := []string{
		"-dNOPAUSE", "-dBATCH", "-dQUIET", "-dSAFER",
		"-sDEVICE=pnggray",
		fmt.Sprintf("-r%d", dpi),
		fmt.Sprintf("-dFirstPage=%d", page),
		fmt.Sprintf("-dLastPage=%d", page),
		"-sstdout=%stderr",
		"-sOutputFile=" + filepath.Join(outDir, fmt.Sprintf("page-%04d.png", page)),
		"--", pdfPath,
	}
	cmd := exec.CommandContext(ctx, "gs", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, stderr.String())
	}
	return nil
}

func runTesseract(ctx context.Context, pngPath, outBase, lang string) error {
	cmd := exec.CommandContext(ctx, "tesseract", "-l", lang, pngPath, outBase, "txt")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, stderr.String())
	}
	return nil
}

func downloadBlob(ctx context.Context, c *http.Client, cfg workerCfg, blobURL, dst string) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", cfg.registry+blobURL, nil)
	req.Header.Set("Authorization", "Bearer "+cfg.pat)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("blob status=%d body=%s", resp.StatusCode, string(b))
	}
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, resp.Body)
	return err
}

func postResultText(ctx context.Context, c *http.Client, cfg workerCfg, taskID int64, lease, text string) error {
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	meta := map[string]any{
		"task_id":        taskID,
		"lease_token":    lease,
		"extracted_text": text,
	}
	mb, _ := json.Marshal(meta)
	_ = mw.WriteField("meta", string(mb))
	if err := mw.Close(); err != nil {
		return err
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", cfg.registry+"/v1/tasks/result", body)
	req.Header.Set("Authorization", "Bearer "+cfg.pat)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("result text status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func heartbeatLoop(ctx context.Context, c *http.Client, cfg workerCfg, taskID int64, leaseToken string) {
	tk := time.NewTicker(cfg.hbInterval)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			body, _ := json.Marshal(map[string]any{
				"task_id":     taskID,
				"lease_token": leaseToken,
			})
			req, _ := http.NewRequestWithContext(ctx, "POST", cfg.registry+"/v1/tasks/heartbeat", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+cfg.pat)
			req.Header.Set("Content-Type", "application/json")
			resp, err := c.Do(req)
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("heartbeat", "task_id", taskID, "err", err)
				}
				continue
			}
			if resp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(resp.Body)
				slog.Warn("heartbeat", "task_id", taskID, "status", resp.StatusCode, "body", string(b))
			}
			resp.Body.Close()
		}
	}
}

func postFail(ctx context.Context, c *http.Client, cfg workerCfg, t task, code, msg string, retryable bool) {
	body, _ := json.Marshal(map[string]any{
		"task_id":       t.TaskID,
		"lease_token":   t.LeaseToken,
		"error_code":    code,
		"error_message": msg,
		"retryable":     retryable,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", cfg.registry+"/v1/tasks/fail", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+cfg.pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		slog.Error("post fail", "task_id", t.TaskID, "code", code, "err", err)
		return
	}
	resp.Body.Close()
	slog.Warn("task failed", "task_id", t.TaskID, "code", code, "msg", msg, "retryable", retryable)
}

func max1(n int) int {
	if n < 1 {
		return 1
	}
	return n
}
