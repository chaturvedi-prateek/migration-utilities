package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Progress is the subset of the mongosync /api/v1/progress response we care about.
type Progress struct {
	State          string  `json:"state"`
	CanCommit      bool    `json:"canCommit"`
	CanWrite       bool    `json:"canWrite"`
	Info           string  `json:"info"`
	LagTimeSeconds float64 `json:"lagTimeSeconds"`
	CollectionCopy struct {
		EstimatedTotalBytes  int64 `json:"estimatedTotalBytes"`
		EstimatedCopiedBytes int64 `json:"estimatedCopiedBytes"`
	} `json:"collectionCopy"`
}

func (p Progress) percentCopied() float64 {
	if p.CollectionCopy.EstimatedTotalBytes == 0 {
		return 0
	}
	return 100 * float64(p.CollectionCopy.EstimatedCopiedBytes) / float64(p.CollectionCopy.EstimatedTotalBytes)
}

// instance is one running mongosync process managed by the orchestrator.
type instance struct {
	id   string
	port int
	cmd  *exec.Cmd
	cfg  *Config
}

func (in *instance) apiURL(path string) string {
	return fmt.Sprintf("http://localhost:%d/api/v1/%s", in.port, path)
}

// writeInstanceConfig writes the per-instance mongosync config file (cluster0 =
// source, cluster1 = destination). Each instance gets its own port and logPath.
func writeInstanceConfig(cfg *Config, id, source, dest string, port int) (string, error) {
	logPath := filepath.Join(cfg.LogDir, id)
	if err := os.MkdirAll(logPath, 0o755); err != nil {
		return "", err
	}
	body := map[string]any{
		"cluster0":         source,
		"cluster1":         dest,
		"logPath":          logPath,
		"verbosity":        "INFO",
		"port":             port,
		"acceptDisclaimer": true,
		"disableTelemetry": true,
	}
	b, _ := json.MarshalIndent(body, "", "  ")
	path := filepath.Join(cfg.LogDir, id+".mongosync.json")
	return path, os.WriteFile(path, b, 0o644)
}

// launch starts a mongosync process for the given source/destination pair and
// waits until its control API is responsive.
func launch(ctx context.Context, cfg *Config, id, source, dest string, port int) (*instance, error) {
	confPath, err := writeInstanceConfig(cfg, id, source, dest, port)
	if err != nil {
		return nil, err
	}
	out, err := os.Create(filepath.Join(cfg.LogDir, id+".out"))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(cfg.MongosyncBinary, "--config", confPath)
	cmd.Stdout = out
	cmd.Stderr = out
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start mongosync %q: %w", id, err)
	}
	in := &instance{id: id, port: port, cmd: cmd, cfg: cfg}
	if err := in.waitReady(ctx, 120*time.Second); err != nil {
		_ = in.stop()
		return nil, fmt.Errorf("mongosync %q never became ready: %w", id, err)
	}
	return in, nil
}

// waitReady blocks until the mongosync control API is up AND the process has
// finished initializing (state IDLE), which is when /start is accepted.
func (in *instance) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p, err := in.progress(ctx); err == nil && p.State == "IDLE" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("timed out after %s waiting for IDLE state", timeout)
}

func (in *instance) progress(ctx context.Context) (Progress, error) {
	var wrap struct {
		Progress Progress `json:"progress"`
		Success  bool     `json:"success"`
	}
	err := in.apiGET(ctx, "progress", &wrap)
	return wrap.Progress, err
}

// start issues the /start call. includeNamespaces and preExisting are optional.
// When embeddedVerify is true, mongosync's built-in verifier is enabled for this
// sync (needs extra oplog/resources; results surface on the /progress endpoint).
func (in *instance) start(ctx context.Context, ns []Namespace, preExisting, embeddedVerify bool) error {
	body := map[string]any{
		"source":       "cluster0",
		"destination":  "cluster1",
		"verification": map[string]any{"enabled": embeddedVerify},
	}
	if len(ns) > 0 {
		body["includeNamespaces"] = ns
	}
	if preExisting {
		body["preExistingDestinationData"] = true
	}
	return in.apiPOST(ctx, "start", body, nil)
}

func (in *instance) commit(ctx context.Context) error {
	return in.apiPOST(ctx, "commit", map[string]any{}, nil)
}

func (in *instance) pause(ctx context.Context) error {
	return in.apiPOST(ctx, "pause", map[string]any{}, nil)
}

func (in *instance) resume(ctx context.Context) error {
	return in.apiPOST(ctx, "resume", map[string]any{}, nil)
}

func (in *instance) stop() error {
	if in.cmd == nil || in.cmd.Process == nil {
		return nil
	}
	_ = in.cmd.Process.Signal(os.Interrupt)
	done := make(chan struct{})
	go func() { _ = in.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = in.cmd.Process.Kill()
	}
	return nil
}

func (in *instance) apiGET(ctx context.Context, path string, out any) error {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, in.apiURL(path), nil)
	return doAPI(req, out)
}

func (in *instance) apiPOST(ctx context.Context, path string, body any, out any) error {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, in.apiURL(path), bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	return doAPI(req, out)
}

var httpClient = &http.Client{Timeout: 30 * time.Second}

func doAPI(req *http.Request, out any) error {
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: %s: %s", req.Method, req.URL.Path, resp.Status, strings.TrimSpace(string(b)))
	}
	// mongosync returns {"success":false,"error":"..."} with 200 in some cases.
	var probe struct {
		Success *bool  `json:"success"`
		Error   string `json:"error"`
	}
	_ = json.Unmarshal(b, &probe)
	if probe.Success != nil && !*probe.Success && probe.Error != "" {
		return fmt.Errorf("%s %s: %s", req.Method, req.URL.Path, probe.Error)
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

// mongoshEval runs a JS snippet against a cluster via mongosh. Used for metadata
// cleanup between consolidation merges and for verification counts.
func mongoshEval(ctx context.Context, cfg *Config, uri, js string) (string, error) {
	cmd := exec.CommandContext(ctx, cfg.MongoshBinary, uri, "--quiet", "--eval", js)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	return strings.TrimSpace(buf.String()), err
}
