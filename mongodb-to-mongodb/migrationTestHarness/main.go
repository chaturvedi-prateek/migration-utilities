// migrationTestHarness stands up a full 10→10→1 migration test on a single
// linux/amd64 host: it provisions N source + N destination single-node replica sets,
// seeds disjoint data, generates orchestrator.json, then drives the real
// mongosyncOrchestrator through Phase 1 (parallel 1:1 sync) → coordinated commit →
// Phase 2 (sequential consolidation into one hub), verifying and logging every step.
//
// It exercises the actual playbook artifacts so real issues surface before production.
//
// Usage:
//
//	migrationTestHarness --clusters 10 --data-size-mb 2048 --base-dir /mnt/migtest
//	migrationTestHarness --keep            # leave clusters running after the test
//	migrationTestHarness --teardown-only   # stop clusters from a prior --keep run
package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	clusters := flag.Int("clusters", 10, "number of source (and destination) clusters")
	dataMB := flag.Int("data-size-mb", 2048, "approx data seeded per source cluster, in MB")
	baseDir := flag.String("base-dir", "./migtest", "working dir for tools, data, logs")
	skipDownload := flag.Bool("skip-download", false, "require tools on PATH/base-dir; do not download")
	keep := flag.Bool("keep", false, "do not tear down clusters after the test")
	teardownOnly := flag.Bool("teardown-only", false, "just stop clusters from a prior run and exit")
	emitConfig := flag.String("emit-config", "", "write the orchestrator.json the harness would use to this path and exit (no provisioning)")
	driver := flag.String("driver", "orchestrator", "migration driver: orchestrator | scaffold")
	flag.Parse()

	if *emitConfig != "" {
		cs := &clusterSet{t: &tools{mongosync: "mongosync", mongosh: "mongosh"}}
		for i := 0; i < *clusters; i++ {
			cs.sources = append(cs.sources, &node{name: fmt.Sprintf("src%02d", i+1), rs: fmt.Sprintf("srcrs%02d", i+1), port: srcBasePort + i, db: fmt.Sprintf("appdb%02d", i+1)})
			cs.dests = append(cs.dests, &node{name: fmt.Sprintf("dst%02d", i+1), rs: fmt.Sprintf("dstrs%02d", i+1), port: dstBasePort + i, db: fmt.Sprintf("appdb%02d", i+1), auth: true})
		}
		must(writeOrchestratorConfig(*emitConfig, cs.t, cs, "./logs"))
		fmt.Println("wrote", *emitConfig)
		return
	}

	base, _ := filepath.Abs(*baseDir)
	toolDir := filepath.Join(base, "tools")
	dataDir := filepath.Join(base, "data")
	logDir := filepath.Join(base, "logs")
	for _, d := range []string{base, dataDir, logDir} {
		must(os.MkdirAll(d, 0o755))
	}

	tee, closeLog := teeToFile(filepath.Join(logDir, "harness.log"))
	defer closeLog()
	_ = tee

	t, err := resolveTools(toolDir, *skipDownload)
	must(err)
	logf("tools: mongod=%s mongosh=%s mongosync=%s", t.mongod, t.mongosh, t.mongosync)

	if *teardownOnly {
		logf("teardown-only: stopping any clusters from a prior run")
		(&clusterSet{t: t, sources: nodesForRange(dataDir, srcBasePort, "src", "srcrs", *clusters, false),
			dests: nodesForRange(dataDir, dstBasePort, "dst", "dstrs", *clusters, true)}).teardown()
		return
	}

	// Preflight: kill any mongosync/mongod left running by a previous aborted run.
	// A wiped base-dir does NOT stop already-running daemons, and a stale mongosync
	// holding an API port makes the new run time out waiting for IDLE.
	preflightKill(base)

	start := time.Now()
	logf("=== provisioning %d source + %d destination replica sets ===", *clusters, *clusters)
	cs, err := provision(t, dataDir, *clusters)
	if err != nil {
		cs.teardown()
		fatal("provision: %v", err)
	}
	if !*keep {
		defer cs.teardown()
	}

	logf("=== seeding ~%d MB per source ===", *dataMB)
	srcCounts := make([]int64, len(cs.sources))
	for i, nd := range cs.sources {
		c, err := seedSource(t, nd, *dataMB)
		if err != nil {
			fatal("seed: %v", err)
		}
		srcCounts[i] = c
	}

	confPath := filepath.Join(base, "orchestrator.json")
	must(writeOrchestratorConfig(confPath, t, cs, logDir))
	logf("wrote migration config: %s", confPath)

	// ---- Drive the migration via the selected driver ----
	var p1, p2 []verifyResult
	switch *driver {
	case "orchestrator":
		orch, err := locateOrchestrator(toolDir)
		if err != nil {
			fatal("%v", err)
		}
		t.orchestrator = orch
		p1, p2 = runViaOrchestrator(t, cs, srcCounts, logDir, confPath)
	case "scaffold":
		sc, err := resolveScaffoldTools(toolDir, *skipDownload)
		must(err)
		p1, p2 = runViaScaffold(t, sc, cs, srcCounts, base, logDir, confPath)
	default:
		fatal("unknown --driver %q (want orchestrator | scaffold)", *driver)
	}

	// ---- Summary + log bundle ----
	all := append(append([]verifyResult{}, p1...), p2...)
	pass := 0
	for _, r := range all {
		if r.ok {
			pass++
		}
	}
	logf("=== RESULT: %d/%d checks passed in %s ===", pass, len(all), time.Since(start).Round(time.Second))

	bundle := filepath.Join(base, "migtest-logs.tgz")
	items := []string{"logs", "orchestrator.json"}
	if _, err := os.Stat(filepath.Join(base, "scaffold")); err == nil {
		items = append(items, "scaffold") // generated artifacts + their per-step logs
	}
	if err := run(base, "tar", append([]string{"-czf", bundle, "-C", base}, items...)...); err != nil {
		logf("WARN: could not bundle logs: %v", err)
	} else {
		logf("log bundle ready to share: %s", bundle)
	}
	if pass != len(all) {
		os.Exit(1)
	}
}

// runViaOrchestrator drives the migration with the automated mongosyncOrchestrator
// (the original path): sync -> commit -> stop -> consolidate.
func runViaOrchestrator(t *tools, cs *clusterSet, srcCounts []int64, logDir, confPath string) (p1, p2 []verifyResult) {
	logf("=== PHASE 1: sync (run to steady state) ===")
	if err := orchestrate(t, logDir, "phase1-sync", "sync", "--config", confPath, "--poll", "10", "--lag", "10"); err != nil {
		fatal("phase1 sync: %v", err)
	}
	logf("=== PHASE 1 CUTOVER: commit all syncs together ===")
	if err := orchestrate(t, logDir, "phase1-commit", "commit", "--config", confPath, "--lag", "10"); err != nil {
		fatal("phase1 commit: %v", err)
	}
	p1 = verifyPhase1(t, cs, srcCounts)
	reportChecks("Phase 1", p1)

	logf("=== PHASE 1 STOP: wait for COMMITTED, then stop syncs ===")
	if err := orchestrate(t, logDir, "phase1-stop", "stop", "--config", confPath); err != nil {
		fatal("phase1 stop: %v", err)
	}

	logf("=== PHASE 2: consolidate (10 → 1) ===")
	if err := orchestrate(t, logDir, "phase2-consolidate", "consolidate", "--config", confPath, "--verify", "--poll", "10", "--lag", "10"); err != nil {
		fatal("phase2 consolidate: %v", err)
	}
	p2 = verifyHub(t, cs, srcCounts)
	reportChecks("Phase 2 (hub)", p2)
	return p1, p2
}

// orchestrate runs the mongosyncOrchestrator binary with the given args, teeing its
// combined output to <logDir>/<tag>.log and the console.
func orchestrate(t *tools, logDir, tag string, args ...string) error {
	logPath := filepath.Join(logDir, tag+".log")
	f, err := os.Create(logPath)
	if err != nil {
		return err
	}
	defer f.Close()
	cmd := exec.Command(t.orchestrator, args...)
	mw := multiWriter(os.Stdout, f)
	cmd.Stdout = mw
	cmd.Stderr = mw
	logf("running: mongosyncOrchestrator %v  (log: %s)", args, logPath)
	err = cmd.Run()
	if err != nil {
		dumpMongosyncErrors(logDir)
	}
	return err
}

// dumpMongosyncErrors surfaces the errorDescription from each per-instance
// mongosync.log so failures are self-diagnosing without extra round-trips.
func dumpMongosyncErrors(logDir string) {
	entries, _ := os.ReadDir(logDir)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		lp := filepath.Join(logDir, e.Name(), "mongosync.log")
		b, err := os.ReadFile(lp)
		if err != nil {
			continue
		}
		var found []string
		for _, line := range strings.Split(string(b), "\n") {
			if i := strings.Index(line, "errorDescription"); i >= 0 {
				end := i + 300
				if end > len(line) {
					end = len(line)
				}
				found = append(found, line[i:end])
			}
		}
		if n := len(found); n > 0 {
			logf("--- mongosync error [%s] ---", e.Name())
			if n > 2 {
				found = found[n-2:]
			}
			for _, s := range found {
				logf("  %s", s)
			}
		}
	}
}

func reportChecks(phase string, rs []verifyResult) {
	for _, r := range rs {
		status := "PASS"
		if !r.ok {
			status = "FAIL"
		}
		logf("[%s] %-24s %s  (%s)", status, r.name, phase, r.msg)
	}
}

// nodesForRange rebuilds node structs for teardown-only mode (no state persisted).
func nodesForRange(dataDir string, basePort int, namePfx, rsPfx string, n int, auth bool) []*node {
	var out []*node
	for i := 0; i < n; i++ {
		out = append(out, &node{
			name: fmt.Sprintf("%s%02d", namePfx, i+1), rs: fmt.Sprintf("%s%02d", rsPfx, i+1),
			port: basePort + i, auth: auth,
			dbPath: filepath.Join(dataDir, fmt.Sprintf("%s%02d", namePfx, i+1)),
		})
	}
	return out
}

func must(err error) {
	if err != nil {
		fatal("%v", err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}
