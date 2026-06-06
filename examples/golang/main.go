// Minimal native Go task worker for crawlerv3.
//
// Replace processHTMLToMarkdown with your own transform; everything
// else (reserve loop, blob download, multipart result, fail) stays.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Task struct {
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

type ReserveResp struct {
	Tasks []Task `json:"tasks"`
}

// Result returned by Process. Set ExtractedText for text mode, or
// OutputBlob + OutputContentType for blob mode.
type Result struct {
	ExtractedText string

	OutputBlob        []byte
	OutputContentType string
	NextProcessor     string
}

type Worker struct {
	registry string
	pat      string
	kinds    []string
	batch    int
	idle     time.Duration
	hc       *http.Client
	process  func(ctx context.Context, t Task, body []byte) (Result, error)
}

func main() {
	registry := flag.String("registry", os.Getenv("REGISTRY"), "registry base URL")
	pat := flag.String("pat", os.Getenv("PAT"), "personal access token")
	kindCSV := flag.String("kind", "html_to_markdown", "comma-separated processor kinds")
	batch := flag.Int("batch", 4, "max tasks per reserve")
	idle := flag.Duration("idle-sleep", 5*time.Second, "sleep when queue empty")
	flag.Parse()

	if *registry == "" || *pat == "" {
		fmt.Fprintln(os.Stderr, "registry and pat required (env REGISTRY, PAT or flags)")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	w := &Worker{
		registry: strings.TrimRight(*registry, "/"),
		pat:      *pat,
		kinds:    strings.Split(*kindCSV, ","),
		batch:    *batch,
		idle:     *idle,
		hc:       &http.Client{Timeout: 60 * time.Second},
		process:  processHTMLToMarkdown,
	}
	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

// processHTMLToMarkdown is the only function you replace.
// Return Result{ExtractedText: ...} for text mode, or
// Result{OutputBlob: ..., OutputContentType: "text/markdown"} for blob mode.
func processHTMLToMarkdown(_ context.Context, t Task, body []byte) (Result, error) {
	md := fmt.Sprintf("# Extracted from lake object %d\n\nSource bytes: %d\n", t.LakeObjectID, len(body))
	return Result{
		OutputBlob:        []byte(md),
		OutputContentType: "text/markdown; charset=utf-8",
	}, nil
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tasks, err := w.reserve(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, "reserve:", err)
			sleep(ctx, w.idle)
			continue
		}
		if len(tasks) == 0 {
			sleep(ctx, w.idle)
			continue
		}
		for _, t := range tasks {
			w.workOne(ctx, t)
		}
	}
}

func (w *Worker) workOne(ctx context.Context, t Task) {
	fmt.Printf("task=%d processor=%s blob=%s size=%d\n",
		t.TaskID, t.Processor, t.BlobURL, t.BlobSizeBytes)

	body, err := w.downloadBlob(ctx, t.BlobURL)
	if err != nil {
		w.postFail(ctx, t, "download", err.Error(), true)
		return
	}
	res, err := w.process(ctx, t, body)
	if err != nil {
		w.postFail(ctx, t, "process", err.Error(), true)
		return
	}
	if err := w.postResult(ctx, t, res); err != nil {
		fmt.Fprintln(os.Stderr, "post result:", err)
	}
}

func (w *Worker) reserve(ctx context.Context) ([]Task, error) {
	body, _ := json.Marshal(map[string]any{"kinds": w.kinds, "batch": w.batch})
	var rr ReserveResp
	if err := w.postJSON(ctx, "/v1/tasks/reserve", body, &rr); err != nil {
		return nil, err
	}
	return rr.Tasks, nil
}

func (w *Worker) downloadBlob(ctx context.Context, blobURL string) ([]byte, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", w.registry+blobURL, nil)
	req.Header.Set("Authorization", "Bearer "+w.pat)
	resp, err := w.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("blob status=%d body=%s", resp.StatusCode, string(b))
	}
	return io.ReadAll(resp.Body)
}

func (w *Worker) postResult(ctx context.Context, t Task, r Result) error {
	body := &bytes.Buffer{}
	mw := multipart.NewWriter(body)

	meta := map[string]any{
		"task_id":     t.TaskID,
		"lease_token": t.LeaseToken,
	}
	if r.ExtractedText != "" {
		meta["extracted_text"] = r.ExtractedText
	}
	if len(r.OutputBlob) > 0 {
		sum := sha256.Sum256(r.OutputBlob)
		meta["output_content_type"] = r.OutputContentType
		meta["output_content_sha256"] = hex.EncodeToString(sum[:])
		if r.NextProcessor != "" {
			meta["next_processor"] = r.NextProcessor
		}
	}
	mb, _ := json.Marshal(meta)
	_ = mw.WriteField("meta", string(mb))

	if len(r.OutputBlob) > 0 {
		fw, err := mw.CreateFormFile("blob", "output.bin")
		if err != nil {
			return err
		}
		if _, err := fw.Write(r.OutputBlob); err != nil {
			return err
		}
	}
	if err := mw.Close(); err != nil {
		return err
	}

	req, _ := http.NewRequestWithContext(ctx, "POST", w.registry+"/v1/tasks/result", body)
	req.Header.Set("Authorization", "Bearer "+w.pat)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := w.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("result status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

func (w *Worker) postFail(ctx context.Context, t Task, code, msg string, retryable bool) {
	body, _ := json.Marshal(map[string]any{
		"task_id":       t.TaskID,
		"lease_token":   t.LeaseToken,
		"error_code":    code,
		"error_message": msg,
		"retryable":     retryable,
	})
	if err := w.postJSON(ctx, "/v1/tasks/fail", body, nil); err != nil {
		fmt.Fprintln(os.Stderr, "post fail:", err)
	}
}

func (w *Worker) postJSON(ctx context.Context, path string, body []byte, out any) error {
	req, _ := http.NewRequestWithContext(ctx, "POST", w.registry+path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+w.pat)
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s status=%d body=%s", path, resp.StatusCode, string(b))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
