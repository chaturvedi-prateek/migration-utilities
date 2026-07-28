// mongosyncOrchestrator drives many mongosync instances from a single migration
// host: Phase 1 runs N 1:1 cluster syncs in parallel; Phase 2 consolidates them
// into one hub cluster sequentially (mongosync forbids many-to-one, so merges are
// one-at-a-time with the hub's mongosync metadata cleaned between runs).
//
// Usage:
//
//	mongosyncOrchestrator sync        --config c.json [--commit] [--dry-run]
//	mongosyncOrchestrator consolidate --config c.json [--verify]  [--dry-run]
//	mongosyncOrchestrator progress    --config c.json
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
	commit := fs.Bool("commit", false, "auto-commit each sync once drained (Phase 1)")
	verify := fs.Bool("verify", false, "run count verification after each merge (Phase 2)")
	embeddedVerify := fs.Bool("embedded-verify", false, "enable mongosync's built-in verifier on each sync")
	dryRun := fs.Bool("dry-run", false, "print the plan without launching mongosync")
	force := fs.Bool("force", false, "commit even if some syncs are not fully drained")
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

	poll := time.Duration(*pollSecs) * time.Second

	switch cmd {
	case "sync":
		if *dryRun {
			dryRunSync(cfg)
			return
		}
		if err := runSync(ctx, cfg, *commit, *embeddedVerify, poll, *lag); err != nil {
			fatal(err)
		}
	case "consolidate":
		if *dryRun {
			dryRunConsolidate(cfg)
			return
		}
		if err := runConsolidate(ctx, cfg, poll, *lag, *verify, *embeddedVerify); err != nil {
			fatal(err)
		}
	case "commit":
		if err := runCommit(ctx, cfg, *lag, *force); err != nil {
			fatal(err)
		}
	case "stop":
		if err := runStop(ctx, cfg, *force); err != nil {
			fatal(err)
		}
	case "progress":
		if err := progressOnce(ctx, cfg); err != nil {
			fatal(err)
		}
	case "pause", "resume":
		if err := pauseResume(ctx, cfg, cmd == "pause"); err != nil {
			fatal(err)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}
}

// progressOnce attaches to already-running instances (by their known ports) and
// prints a single aggregated snapshot.
func progressOnce(ctx context.Context, cfg *Config) error {
	instances := make([]*instance, len(cfg.Syncs))
	for i, s := range cfg.Syncs {
		instances[i] = &instance{id: s.ID, port: cfg.portFor(i), cfg: cfg}
	}
	renderTable(pollAll(ctx, instances))
	return nil
}

// pauseResume pauses or resumes every configured sync (by its known port). Useful
// for throttling source load or holding all syncs during a maintenance window.
func pauseResume(ctx context.Context, cfg *Config, doPause bool) error {
	action := "resume"
	if doPause {
		action = "pause"
	}
	var firstErr error
	for i, s := range cfg.Syncs {
		in := &instance{id: s.ID, port: cfg.portFor(i), cfg: cfg}
		var err error
		if doPause {
			err = in.pause(ctx)
		} else {
			err = in.resume(ctx)
		}
		if err != nil {
			fmt.Printf("[%s] %s: %v\n", s.ID, action, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		fmt.Printf("[%s] %sd\n", s.ID, action)
	}
	return firstErr
}

func dryRunSync(cfg *Config) {
	fmt.Printf("DRY RUN — Phase 1: %d parallel 1:1 syncs\n", len(cfg.Syncs))
	for i, s := range cfg.Syncs {
		fmt.Printf("  [%s] port=%d\n     cluster0(src)=%s\n     cluster1(dst)=%s\n     includeNamespaces=%v\n",
			s.ID, cfg.portFor(i), redact(s.Source), redact(s.Destination), s.IncludeNamespaces)
	}
}

func dryRunConsolidate(cfg *Config) {
	if cfg.Consolidation == nil {
		fmt.Println("no consolidation section in config")
		return
	}
	fmt.Printf("DRY RUN — Phase 2: %d sequential merges into hub\n  hub=%s\n",
		len(cfg.Consolidation.Merges), redact(cfg.Consolidation.Hub))
	for i, m := range cfg.Consolidation.Merges {
		fmt.Printf("  step %d [%s] port=%d preExistingDestinationData=true\n     source=%s\n     includeNamespaces=%v\n",
			i+1, m.ID, cfg.portFor(0), redact(m.Source), m.IncludeNamespaces)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `mongosyncOrchestrator — drive many mongosync from one host

  sync         Phase 1: launch all 1:1 syncs in parallel; without --commit,
               run to steady state and LEAVE them running for a coordinated commit
  commit       Phase 1 cutover: commit all syncs together, wait for canWrite
               (apps may cut over); does NOT stop mongosync
  stop         wait for COMMITTED (index builds done) then stop the syncs (--force
               to skip the wait); frees ports for consolidation
  consolidate  Phase 2: merge sources into the hub sequentially
  progress     print one aggregated progress snapshot of running syncs
  pause        pause all running syncs
  resume       resume all paused syncs

Flags: --config --commit --verify --embedded-verify --force --dry-run --poll --lag
`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
