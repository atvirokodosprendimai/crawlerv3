// task-worker is the reference external worker for crawlerv3 processing tasks.
//
// It reserves tasks by kind (e.g. pdf_ocr, docx_to_pdf), downloads the source
// blob from the registry, shells out to a user-configured command for the
// real work, and pushes either extracted text or an output blob back.
//
// Two modes:
//   --mode text   command's stdout is the extracted text
//   --mode blob   command writes a file at {output}; we upload that file
//
// Examples:
//   task-worker --kind pdf_ocr      --mode text \
//     --extract-cmd "tesseract {input} - -l eng+lit"
//
//   task-worker --kind docx_to_pdf  --mode blob \
//     --extract-cmd "libreoffice --headless --convert-to pdf --outdir {outdir} {input}" \
//     --output-glob "{outdir}/*.pdf" --next-processor pdf_ocr \
//     --output-content-type application/pdf
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func main() {
	cmd := &cli.Command{
		Name:  "task-worker",
		Usage: "external processing-task worker for crawlerv3",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "registry", Required: true, Sources: cli.EnvVars("REGISTRY")},
			&cli.StringFlag{Name: "pat", Required: true, Sources: cli.EnvVars("PAT")},
			&cli.StringSliceFlag{Name: "kind", Required: true, Usage: "task kinds to claim (e.g. pdf_ocr); repeatable"},
			&cli.IntFlag{Name: "batch", Value: 4},
			&cli.IntFlag{Name: "concurrency", Value: 2,
				Usage: "max parallel tasks per reserved batch"},
			&cli.DurationFlag{Name: "idle-sleep", Value: 5 * time.Second},
			&cli.DurationFlag{Name: "max-runtime", Value: 0, Usage: "exit after this duration; 0 = run forever"},
			&cli.StringFlag{Name: "mode", Value: "text", Usage: "text | blob"},
			&cli.StringFlag{Name: "extract-cmd", Required: true,
				Usage: "shell command; {input} is downloaded blob path, {outdir} a scratch dir"},
			&cli.StringFlag{Name: "output-glob", Value: "{outdir}/output.*",
				Usage: "blob mode: glob pattern for produced file"},
			&cli.StringFlag{Name: "output-content-type", Value: "application/octet-stream"},
			&cli.StringFlag{Name: "next-processor", Value: "", Usage: "follow-up processor for blob mode (e.g. pdf_ocr)"},
			&cli.DurationFlag{Name: "exec-timeout", Value: 5 * time.Minute},
			&cli.DurationFlag{Name: "heartbeat-interval", Value: 30 * time.Second,
				Usage: "POST /v1/tasks/heartbeat every N to keep lease alive during long jobs (0 disables)"},
			&cli.StringFlag{Name: "log-level", Value: "info", Sources: cli.EnvVars("LOG_LEVEL"),
				Usage: "debug | info | warn | error"},
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			logx.Init("task-worker", c.String("log-level"))
			return ctx, nil
		},
		Action: run,
	}
	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("task-worker exit", "err", err)
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

func run(ctx context.Context, cmd *cli.Command) error {
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

	registry := strings.TrimRight(cmd.String("registry"), "/")
	pat := cmd.String("pat")
	kinds := cmd.StringSlice("kind")
	batch := cmd.Int("batch")
	conc := cmd.Int("concurrency")
	if conc < 1 {
		conc = 1
	}
	idle := cmd.Duration("idle-sleep")
	mode := cmd.String("mode")
	extractCmd := cmd.String("extract-cmd")
	outputGlob := cmd.String("output-glob")
	outputCT := cmd.String("output-content-type")
	nextProc := cmd.String("next-processor")
	execTO := cmd.Duration("exec-timeout")
	hbInterval := cmd.Duration("heartbeat-interval")

	c := &http.Client{Timeout: 60 * time.Second}

	sem := make(chan struct{}, conc)

	slog.Info("task-worker started",
		"registry", registry, "kinds", kinds, "batch", batch, "concurrency", conc,
		"exec_timeout", execTO.String(), "heartbeat_interval", hbInterval.String())

	for {
		if !deadline.IsZero() && time.Now().After(deadline) {
			slog.Info("max-runtime reached, exiting")
			return nil
		}
		tasks, err := reserve(ctx, c, registry, pat, kinds, batch)
		if err != nil {
			slog.Error("reserve", "err", err)
			time.Sleep(idle)
			continue
		}
		if len(tasks) == 0 {
			slog.Debug("reserve empty, idling", "sleep", idle.String())
			time.Sleep(idle)
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
				workOne(ctx, c, registry, pat, t, mode, extractCmd, outputGlob, outputCT, nextProc, execTO, hbInterval)
			}(t)
		}
		wg.Wait()
	}
}

func reserve(ctx context.Context, c *http.Client, registry, pat string, kinds []string, batch int) ([]task, error) {
	body, _ := json.Marshal(map[string]any{"kinds": kinds, "batch": batch})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/reserve", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
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

func workOne(ctx context.Context, c *http.Client, registry, pat string, t task, mode, extractCmd, outputGlob, outputCT, nextProc string, execTO, hbInterval time.Duration) {
	log := slog.With("task_id", t.TaskID, "processor", t.Processor)
	start := time.Now()
	log.Info("task start", "blob", t.BlobURL, "size", t.BlobSizeBytes, "attempt", t.AttemptCount)

	scratch, err := os.MkdirTemp("", "taskworker-*")
	if err != nil {
		postFail(ctx, c, registry, pat, t, "scratch_mkdir", err.Error(), true)
		return
	}
	defer os.RemoveAll(scratch)

	input := filepath.Join(scratch, "input"+extFor(t.BlobContentType))
	if err := downloadBlob(ctx, c, registry, pat, t.BlobURL, input); err != nil {
		postFail(ctx, c, registry, pat, t, "download", err.Error(), true)
		return
	}

	outdir := filepath.Join(scratch, "out")
	if err := os.MkdirAll(outdir, 0o755); err != nil {
		postFail(ctx, c, registry, pat, t, "outdir", err.Error(), true)
		return
	}

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	if hbInterval > 0 {
		go heartbeatLoop(hbCtx, c, registry, pat, t.TaskID, t.LeaseToken, hbInterval)
	}

	cmdStr := strings.NewReplacer("{input}", input, "{outdir}", outdir).Replace(extractCmd)
	execCtx, cancel := context.WithTimeout(ctx, execTO)
	defer cancel()
	c2 := exec.CommandContext(execCtx, "sh", "-c", cmdStr)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	c2.Stdout = &stdout
	c2.Stderr = &stderr
	if err := c2.Run(); err != nil {
		postFail(ctx, c, registry, pat, t, "extract_cmd",
			fmt.Sprintf("%v: %s", err, stderr.String()), true)
		return
	}

	switch mode {
	case "text":
		if err := postResultText(ctx, c, registry, pat, t.TaskID, t.LeaseToken, stdout.String()); err != nil {
			log.Error("post text", "err", err)
			return
		}
		log.Info("task ok", "mode", "text", "text_bytes", stdout.Len(), "dur_ms", time.Since(start).Milliseconds())
	case "blob":
		matches, _ := filepath.Glob(strings.ReplaceAll(outputGlob, "{outdir}", outdir))
		if len(matches) == 0 {
			postFail(ctx, c, registry, pat, t, "no_output", "extract-cmd produced no file matching glob", false)
			return
		}
		if err := postResultBlob(ctx, c, registry, pat, t.TaskID, t.LeaseToken, matches[0], outputCT, nextProc); err != nil {
			log.Error("post blob", "err", err)
			return
		}
		log.Info("task ok", "mode", "blob", "output", matches[0], "next_processor", nextProc, "dur_ms", time.Since(start).Milliseconds())
	default:
		postFail(ctx, c, registry, pat, t, "bad_mode", "mode must be text or blob", false)
	}
}

func downloadBlob(ctx context.Context, c *http.Client, registry, pat, blobURL, dst string) error {
	req, _ := http.NewRequestWithContext(ctx, "GET", registry+blobURL, nil)
	req.Header.Set("Authorization", "Bearer "+pat)
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

func postResultText(ctx context.Context, c *http.Client, registry, pat string, taskID int64, lease, text string) error {
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
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/result", body)
	req.Header.Set("Authorization", "Bearer "+pat)
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

func postResultBlob(ctx context.Context, c *http.Client, registry, pat string, taskID int64, lease, path, contentType, nextProc string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	hashed := sha256.New()
	buf := &bytes.Buffer{}
	if _, err := io.Copy(io.MultiWriter(hashed, buf), f); err != nil {
		return err
	}
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)
	meta := map[string]any{
		"task_id":                taskID,
		"lease_token":            lease,
		"output_content_type":    contentType,
		"output_content_sha256":  hex.EncodeToString(hashed.Sum(nil)),
		"next_processor":         nextProc,
	}
	mb, _ := json.Marshal(meta)
	_ = mw.WriteField("meta", string(mb))
	fw, err := mw.CreateFormFile("blob", filepath.Base(path))
	if err != nil {
		return err
	}
	if _, err := fw.Write(buf.Bytes()); err != nil {
		return err
	}
	if err := mw.Close(); err != nil {
		return err
	}
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/result", body)
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("result blob status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func heartbeatLoop(ctx context.Context, c *http.Client, registry, pat string, taskID int64, leaseToken string, interval time.Duration) {
	tk := time.NewTicker(interval)
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
			req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/heartbeat", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+pat)
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
			} else {
				slog.Debug("heartbeat ok", "task_id", taskID)
			}
			resp.Body.Close()
		}
	}
}

func postFail(ctx context.Context, c *http.Client, registry, pat string, t task, code, msg string, retryable bool) {
	body, _ := json.Marshal(map[string]any{
		"task_id":       t.TaskID,
		"lease_token":   t.LeaseToken,
		"error_code":    code,
		"error_message": msg,
		"retryable":     retryable,
	})
	req, _ := http.NewRequestWithContext(ctx, "POST", registry+"/v1/tasks/fail", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		slog.Error("post fail", "task_id", t.TaskID, "code", code, "err", err)
		return
	}
	resp.Body.Close()
	slog.Warn("task failed", "task_id", t.TaskID, "code", code, "msg", msg, "retryable", retryable)
}

func extFor(ct string) string {
	ct = strings.ToLower(strings.TrimSpace(ct))
	if i := strings.Index(ct, ";"); i > 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	switch ct {
	case "application/pdf":
		return ".pdf"
	case "text/html":
		return ".html"
	case "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
		return ".docx"
	}
	return ".bin"
}
