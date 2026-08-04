package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// scaffoldTools are the extra binaries the scaffold driver needs: the scaffold
// generator itself, plus jq (the generated bash scripts depend on jq + curl).
type scaffoldTools struct {
	scaffold string
	jq       string
}

const jqDefaultURL = "https://github.com/jqlang/jq/releases/download/jq-1.7.1/jq-linux-amd64"

func resolveScaffoldTools(toolDir string, skipDownload bool) (*scaffoldTools, error) {
	st := &scaffoldTools{}
	// scaffold binary: PATH, toolDir, or next to the harness executable.
	if p, err := exec.LookPath("mongosyncScaffold"); err == nil {
		st.scaffold = p
	} else {
		cands := []string{filepath.Join(toolDir, "mongosyncScaffold")}
		if self, err := os.Executable(); err == nil {
			cands = append(cands, filepath.Join(filepath.Dir(self), "mongosyncScaffold"))
		}
		for _, c := range cands {
			if isExec(c) {
				st.scaffold = c
				break
			}
		}
	}
	if st.scaffold == "" {
		return nil, fmt.Errorf("mongosyncScaffold not found on PATH, in %s, or next to the harness", toolDir)
	}
	// jq: PATH or download a static binary into toolDir.
	if p, err := exec.LookPath("jq"); err == nil {
		st.jq = p
	} else {
		dst := filepath.Join(toolDir, "jq")
		if !isExec(dst) {
			if skipDownload {
				return nil, fmt.Errorf("jq not found and --skip-download set")
			}
			logf("downloading jq from %s", envOr("HARNESS_JQ_URL", jqDefaultURL))
			if err := run("", "curl", "-fsSL", "-o", dst, envOr("HARNESS_JQ_URL", jqDefaultURL)); err != nil {
				return nil, fmt.Errorf("download jq: %w", err)
			}
			_ = os.Chmod(dst, 0o755)
		}
		st.jq = dst
	}
	return st, nil
}

// scaffoldBasePort must match the basePort written into orchestrator.json.
const scaffoldBasePort = 27182

// runViaScaffold generates the migration artifacts with mongosyncScaffold, then runs
// the generated shell scripts in RUNBOOK order, polling the mongosync API between
// stages to decide when to commit. This exercises the real generated scripts.
func runViaScaffold(t *tools, sc *scaffoldTools, cs *clusterSet, srcCounts []int64, base, logDir, confPath string) (p1, p2 []verifyResult) {
	scaffoldOut := filepath.Join(base, "scaffold")
	// Environment for the generated scripts: point at the downloaded binaries and put
	// jq/curl on PATH.
	scriptEnv := append(os.Environ(),
		"MONGOSYNC="+t.mongosync,
		"MONGOSH="+t.mongosh,
		"PATH="+filepath.Dir(sc.jq)+":"+filepath.Dir(t.mongosync)+":"+os.Getenv("PATH"),
	)

	logf("=== SCAFFOLD: generate artifacts ===")
	if err := runLogged(logDir, "scaffold-gen", scriptEnv, sc.scaffold,
		"gen-all", "--config", confPath, "--out", scaffoldOut); err != nil {
		fatal("scaffold gen-all: %v", err)
	}

	liftDir := filepath.Join(scaffoldOut, "lift")
	conDir := filepath.Join(scaffoldOut, "consolidate")

	// ---- Phase 1 (lift) via generated scripts ----
	logf("=== PHASE 1 (scaffold): start-processes.sh ===")
	sh(scriptEnv, logDir, "phase1-start-processes", liftDir, "./start-processes.sh")
	logf("=== PHASE 1 (scaffold): start-api.sh ===")
	sh(scriptEnv, logDir, "phase1-start-api", liftDir, "./start-api.sh")

	logf("waiting for all lift jobs to drain (canCommit, low lag)...")
	for i := range cs.sources {
		waitCanCommit("lift/"+cs.sources[i].name, scaffoldBasePort+i, 30*time.Minute)
	}
	logf("=== PHASE 1 (scaffold): commit.sh ===")
	sh(scriptEnv, logDir, "phase1-commit", liftDir, "./commit.sh")
	p1 = verifyPhase1(t, cs, srcCounts)
	reportChecks("Phase 1 (scaffold)", p1)

	logf("=== PHASE 1 (scaffold): stop.sh (waits for COMMITTED) ===")
	sh(scriptEnv, logDir, "phase1-stop", liftDir, "./stop.sh")

	// ---- Phase 2 (consolidate) via generated scripts, one merge at a time ----
	logf("=== PHASE 2 (scaffold): sequential consolidation ===")
	for i := 1; i < len(cs.dests); i++ {
		id := "merge-" + cs.dests[i].name
		logf("--- %s ---", id)
		sh(scriptEnv, logDir, "phase2-"+id+"-cleanup", conDir, "./cleanup-metadata.sh", id)
		sh(scriptEnv, logDir, "phase2-"+id+"-start-processes", conDir, "./start-processes.sh", id)
		sh(scriptEnv, logDir, "phase2-"+id+"-start-api", conDir, "./start-api.sh", id)
		waitCanCommit(id, scaffoldBasePort, 30*time.Minute)
		sh(scriptEnv, logDir, "phase2-"+id+"-commit", conDir, "./commit.sh", id)
		sh(scriptEnv, logDir, "phase2-"+id+"-stop", conDir, "./stop.sh", id)
	}
	p2 = verifyHub(t, cs, srcCounts)
	reportChecks("Phase 2 (scaffold hub)", p2)
	return p1, p2
}

// sh runs a generated script (fatal on error), teeing output to <logDir>/<tag>.log.
func sh(env []string, logDir, tag, dir string, script string, args ...string) {
	if err := runLoggedDir(dir, logDir, tag, env, "bash", append([]string{script}, args...)...); err != nil {
		fatal("%s: %v", tag, err)
	}
}

// waitCanCommit polls the mongosync progress API until canCommit==true (lag<=10s).
func waitCanCommit(label string, port int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://localhost:%d/api/v1/progress", port)
	for time.Now().Before(deadline) {
		if p, err := fetchProgress(url); err == nil && p.CanCommit && p.LagTimeSeconds <= 10 {
			logf("  %s drained (state=%s lag=%.0f)", label, p.State, p.LagTimeSeconds)
			return
		}
		time.Sleep(10 * time.Second)
	}
	fatal("%s did not reach canCommit within %s", label, timeout)
}

type progressResp struct {
	Progress struct {
		State          string  `json:"state"`
		CanCommit      bool    `json:"canCommit"`
		LagTimeSeconds float64 `json:"lagTimeSeconds"`
	} `json:"progress"`
}

func fetchProgress(url string) (struct {
	State          string
	CanCommit      bool
	LagTimeSeconds float64
}, error) {
	var out struct {
		State          string
		CanCommit      bool
		LagTimeSeconds float64
	}
	resp, err := http.Get(url)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var pr progressResp
	if err := json.Unmarshal(b, &pr); err != nil {
		return out, err
	}
	out.State, out.CanCommit, out.LagTimeSeconds = pr.Progress.State, pr.Progress.CanCommit, pr.Progress.LagTimeSeconds
	return out, nil
}

// runLogged runs a command from the current dir; runLoggedDir runs it in `dir`. Both
// tee combined output to <logDir>/<tag>.log and the console.
func runLogged(logDir, tag string, env []string, name string, args ...string) error {
	return runLoggedDir("", logDir, tag, env, name, args...)
}

func runLoggedDir(dir, logDir, tag string, env []string, name string, args ...string) error {
	f, err := os.Create(filepath.Join(logDir, tag+".log"))
	if err != nil {
		return err
	}
	defer f.Close()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = env
	mw := multiWriter(os.Stdout, f)
	cmd.Stdout = mw
	cmd.Stderr = mw
	return cmd.Run()
}
