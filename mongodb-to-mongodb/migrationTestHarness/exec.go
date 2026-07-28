package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// logf prints a timestamped harness log line to stdout (also captured to the run log).
func logf(format string, args ...any) {
	fmt.Printf("%s  %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

// run executes a command, streaming stderr, failing on non-zero exit.
func run(dir, name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// output executes a command and returns trimmed stdout.
func output(dir, name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	return strings.TrimSpace(buf.String()), err
}

// mongoshEval runs a JS snippet against uri and returns trimmed stdout.
func mongoshEval(mongosh, uri, js string) (string, error) {
	cmd := exec.Command(mongosh, uri, "--quiet", "--eval", js)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	return strings.TrimSpace(buf.String()), err
}

// preflightKill terminates any mongosync/mongod processes left over from a prior
// run (matched by the harness base dir in their cmdline, plus any mongosync), so
// stale daemons don't hold API/mongod ports. Best-effort; safe on a dedicated test
// host. pkill exits non-zero when nothing matches — that's fine.
func preflightKill(base string) {
	// Match the daemon binary paths under this base dir only. These strings never
	// appear in the harness's own argv, so we never kill ourselves. (An earlier
	// version matched the bare base dir and SIGKILLed the harness via its
	// --base-dir argument.)
	patterns := []string{
		filepath.Join(base, "tools", "mongosync"),
		filepath.Join(base, "tools", "mongod"),
	}
	killed := false
	for _, p := range patterns {
		if err := exec.Command("pkill", "-9", "-f", p).Run(); err == nil {
			killed = true
		}
	}
	if killed {
		logf("preflight: killed leftover mongosync/mongod processes from a prior run")
		time.Sleep(2 * time.Second) // let ports free up
	} else {
		logf("preflight: no leftover processes found")
	}
}

// waitFor polls fn until it returns nil or the timeout elapses.
func waitFor(what string, timeout time.Duration, fn func() error) error {
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if last = fn(); last == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("timed out after %s waiting for %s: %v", timeout, what, last)
}
