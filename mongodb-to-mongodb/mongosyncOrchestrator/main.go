// mongosyncOrchestrator drives many mongosync instances from a single host using a
// generic plan of steps. Each step is a group of mongosync "jobs" run either in
// parallel (1:1 lift waves) or sequentially (fan-in / consolidation), with per-job
// namespace filtering, preExistingDestinationData, metadata cleanup, and verify.
//
// "10→10→1" is just a two-step plan: a parallel/hold step of 10 jobs, then a
// sequential/auto step of 9 preExisting+cleanup jobs. The legacy syncs[]/
// consolidation config is still accepted and translated into an equivalent plan.
//
// Usage:
//
//	mongosyncOrchestrator run    --config c.json --step <name> [--dry-run]
//	mongosyncOrchestrator commit --config c.json [--step <name>]
//	mongosyncOrchestrator stop   --config c.json [--step <name>] [--force]
//	mongosyncOrchestrator sync|consolidate|progress|pause|resume ...   (legacy)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	configPath := fs.String("config", "orchestrator.json", "path to orchestrator config JSON")
	step := fs.String("step", "", "plan step to act on (by name)")
	commit := fs.Bool("commit", false, "auto-commit each job once drained (parallel step)")
	verify := fs.Bool("verify", false, "run count verification after each job (sequential step)")
	embeddedVerify := fs.Bool("embedded-verify", false, "enable mongosync's built-in verifier on each job")
	dryRun := fs.Bool("dry-run", false, "print the plan without launching mongosync")
	force := fs.Bool("force", false, "skip readiness/COMMITTED waits (commit/stop)")
	pollSecs := fs.Int("poll", 15, "progress poll interval in seconds")
	lag := fs.Float64("lag", 5, "max lagTimeSeconds considered drained/ready to commit")
	_ = fs.Parse(os.Args[2:])

	if cmd == "help" || cmd == "-h" || cmd == "--help" {
		usage()
		return
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := stepOpts{poll: time.Duration(*pollSecs) * time.Second, lag: *lag, force: *force, embeddedVerify: *embeddedVerify}

	switch cmd {
	case "run":
		st, err := cfg.findStep(*step)
		if err != nil {
			fatal(err)
		}
		s := *st
		s.Verify = s.Verify || *verify
		if *commit && s.Mode == modeParallel {
			s.Commit = commitAuto
		}
		if *dryRun {
			dryRunStep(cfg, &s)
			return
		}
		if err := runStep(ctx, cfg, &s, opts); err != nil {
			fatal(err)
		}

	case "sync": // legacy: first parallel step
		if *dryRun {
			dryRunByMode(cfg, modeParallel)
			return
		}
		if err := runSync(ctx, cfg, *commit, *embeddedVerify, opts.poll, *lag); err != nil {
			fatal(err)
		}
	case "consolidate": // legacy: first sequential step
		if *dryRun {
			dryRunByMode(cfg, modeSequential)
			return
		}
		if err := runConsolidate(ctx, cfg, opts.poll, *lag, *verify, *embeddedVerify); err != nil {
			fatal(err)
		}
	case "commit":
		st, err := cfg.firstStepByMode(*step, modeParallel)
		if err != nil {
			fatal(err)
		}
		if err := commitStep(ctx, cfg, st, *lag, *force); err != nil {
			fatal(err)
		}
	case "stop":
		st, err := cfg.firstStepByMode(*step, modeParallel)
		if err != nil {
			fatal(err)
		}
		if err := stopStep(ctx, cfg, st, *force); err != nil {
			fatal(err)
		}
	case "progress":
		st, err := resolveStepAny(cfg, *step)
		if err != nil {
			fatal(err)
		}
		if err := progressStep(ctx, cfg, st); err != nil {
			fatal(err)
		}
	case "pause", "resume":
		st, err := resolveStepAny(cfg, *step)
		if err != nil {
			fatal(err)
		}
		if err := pauseResumeStep(ctx, cfg, st, cmd == "pause"); err != nil {
			fatal(err)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
}

// resolveStepAny returns the named step, or the first step in the plan if unnamed.
func resolveStepAny(cfg *Config, name string) (*Step, error) {
	if name != "" {
		return cfg.findStep(name)
	}
	return &cfg.plan().Steps[0], nil
}

func dryRunByMode(cfg *Config, mode string) {
	st, err := cfg.firstStepByMode("", mode)
	if err != nil {
		fmt.Println(err)
		return
	}
	dryRunStep(cfg, st)
}

func dryRunStep(cfg *Config, st *Step) {
	fmt.Printf("DRY RUN — step %q: mode=%s commit=%s verify=%v (%d jobs)\n",
		st.Name, st.Mode, st.Commit, st.Verify, len(st.Jobs))
	for i, j := range st.Jobs {
		port := cfg.portFor(i)
		if st.Mode == modeSequential {
			port = cfg.portFor(0)
		}
		fmt.Printf("  [%s] port=%d preExisting=%v cleanup=%v\n     source=%s\n     destination=%s\n     includeNamespaces=%v\n",
			j.ID, port, j.PreExistingDestinationData, j.CleanupMetadataOn,
			redact(j.Source), redact(j.Destination), j.IncludeNamespaces)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `mongosyncOrchestrator — drive many mongosync from one host via a plan of steps

Generic:
  run          execute one plan step (--step); parallel or sequential per its config
  commit       cutover a held parallel step: commit all jobs, wait canWrite (--step)
  stop         wait COMMITTED then stop a held step's jobs (--step, --force)
  progress     print an aggregated progress snapshot of a step's jobs (--step)
  pause|resume pause/resume a step's jobs (--step)

Legacy (map onto the first parallel / sequential step of the plan):
  sync         run the parallel step to steady state (--commit for all-in-one)
  consolidate  run the sequential step (fan-in)

Flags: --config --step --commit --verify --embedded-verify --force --dry-run --poll --lag

A step is: { name, mode: parallel|sequential, commit: hold|auto, verify, jobs[] }.
A job is:  { id, source, destination, includeNamespaces?, preExistingDestinationData?,
             cleanupMetadataOn?: [source|destination], embeddedVerify? }.
`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
