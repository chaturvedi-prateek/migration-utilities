// mongosyncScaffold generates review-first migration artifacts from a plan config:
// per-mongosync config JSONs, /start request bodies, and shell scripts for every
// stage (start / progress / commit / stop / verify / cleanup / pause / resume) plus a
// RUNBOOK. Nothing connects to a cluster — the operator reviews each artifact and runs
// the generated scripts by hand. Same config file as mongosyncOrchestrator.
//
// Usage:
//
//	mongosyncScaffold gen-all      --config migration.json --out ./migration [--step <name>]
//	mongosyncScaffold gen-configs  --config migration.json --out ./migration
//	mongosyncScaffold gen-start    --config migration.json --out ./migration
//	mongosyncScaffold gen-progress|gen-commit|gen-stop|gen-verify|gen-cleanup|gen-pause-resume|gen-runbook ...
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	configPath := fs.String("config", "migration.json", "path to the plan config JSON")
	out := fs.String("out", "./migration", "output directory for generated artifacts")
	step := fs.String("step", "", "generate for this step only (default: all steps)")
	_ = fs.Parse(os.Args[2:])

	if cmd == "help" || cmd == "-h" || cmd == "--help" {
		usage()
		return
	}

	gens := map[string]func(*Config, *Step, string) error{
		"gen-configs":      genConfigs,
		"gen-start":        genStart,
		"gen-progress":     genProgress,
		"gen-commit":       genCommit,
		"gen-stop":         genStop,
		"gen-verify":       genVerify,
		"gen-cleanup":      genCleanup,
		"gen-pause-resume": genPauseResume,
		"gen-runbook":      genRunbook,
		"gen-all":          genAll,
	}
	fn, ok := gens[cmd]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		usage()
		os.Exit(2)
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fatal(err)
	}

	steps, err := targetSteps(cfg, *step)
	if err != nil {
		fatal(err)
	}
	for _, s := range steps {
		if err := fn(cfg, s, *out); err != nil {
			fatal(err)
		}
	}
	fmt.Printf("\nDone. Review the artifacts under %s/<step>/ then follow RUNBOOK.md.\n", *out)
}

func targetSteps(cfg *Config, name string) ([]*Step, error) {
	if name != "" {
		s, err := cfg.findStep(name)
		if err != nil {
			return nil, err
		}
		return []*Step{s}, nil
	}
	p := cfg.plan()
	out := make([]*Step, len(p.Steps))
	for i := range p.Steps {
		out[i] = &p.Steps[i]
	}
	return out, nil
}

func usage() {
	fmt.Fprint(os.Stderr, `mongosyncScaffold — generate review-first migration scripts from a plan config

  gen-all           generate every artifact for each step (configs, start bodies,
                    and all stage scripts + RUNBOOK)
  gen-configs       per-mongosync config JSONs + env.sh + log/pid dirs
  gen-start         /start request bodies + start-processes.sh + start-api.sh
  gen-progress      progress.sh (polls every 10s until Ctrl-C)
  gen-commit        commit.sh
  gen-stop          stop.sh (waits for COMMITTED, then stops)
  gen-verify        verify.sh (count-compare source vs destination)
  gen-cleanup       cleanup-metadata.sh (drop mongosync reserved DBs, both ends)
  gen-pause-resume  pause.sh / resume.sh
  gen-runbook       RUNBOOK.md (exact script order for the step)

Flags: --config <file>  --out <dir>  --step <name>

Everything is generated for review; no cluster is contacted by this tool. Edit the
generated configs / start bodies, then run the scripts per each step's RUNBOOK.md.
`)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
