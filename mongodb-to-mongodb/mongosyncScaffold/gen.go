package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// jsonIndent marshals with indentation and without HTML escaping, so `&` in
// connection URIs stays readable in the generated files.
func jsonIndent(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
	return strings.TrimRight(buf.String(), "\n")
}

// stepDir is the output directory for one step's artifacts.
func stepDir(out string, step *Step) string { return filepath.Join(out, step.Name) }

func writeFile(path, content string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		return err
	}
	fmt.Printf("  wrote %s\n", path)
	return nil
}

// genConfigs writes the per-job mongosync config JSONs, env.sh, and the log/pid dirs.
func genConfigs(cfg *Config, step *Step, out string) error {
	dir := stepDir(out, step)
	fmt.Printf("[%s] configs\n", step.Name)
	for i, j := range step.Jobs {
		conf := map[string]any{
			"cluster0":         j.Source,
			"cluster1":         j.Destination,
			"logPath":          "./logs/" + j.ID,
			"verbosity":        "INFO",
			"port":             cfg.portFor(step, i),
			"acceptDisclaimer": true,
			"disableTelemetry": true,
		}
		if err := writeFile(filepath.Join(dir, "configs", j.ID+".mongosync.json"), jsonIndent(conf)+"\n", 0o644); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, "pids"), 0o755); err != nil {
		return err
	}
	return writeFile(filepath.Join(dir, "env.sh"), envSh(cfg, step), 0o644)
}

// genStart writes the /start request bodies and the start-processes / start-api scripts.
func genStart(cfg *Config, step *Step, out string) error {
	dir := stepDir(out, step)
	fmt.Printf("[%s] start bodies + scripts\n", step.Name)
	for _, j := range step.Jobs {
		body := map[string]any{
			"source":       "cluster0",
			"destination":  "cluster1",
			"verification": map[string]any{"enabled": j.EmbeddedVerify},
		}
		if j.PreExistingDestinationData {
			body["preExistingDestinationData"] = true
		}
		if len(j.IncludeNamespaces) > 0 {
			body["includeNamespaces"] = j.IncludeNamespaces
		}
		if err := writeFile(filepath.Join(dir, "start-bodies", j.ID+".start.json"), jsonIndent(body)+"\n", 0o644); err != nil {
			return err
		}
	}
	if err := writeFile(filepath.Join(dir, "start-processes.sh"), startProcessesSh, 0o755); err != nil {
		return err
	}
	return writeFile(filepath.Join(dir, "start-api.sh"), startAPISh, 0o755)
}

func genProgress(cfg *Config, step *Step, out string) error {
	fmt.Printf("[%s] progress script\n", step.Name)
	return writeFile(filepath.Join(stepDir(out, step), "progress.sh"), progressSh, 0o755)
}

func genCommit(cfg *Config, step *Step, out string) error {
	fmt.Printf("[%s] commit script\n", step.Name)
	return writeFile(filepath.Join(stepDir(out, step), "commit.sh"), commitSh, 0o755)
}

func genStop(cfg *Config, step *Step, out string) error {
	fmt.Printf("[%s] stop script\n", step.Name)
	return writeFile(filepath.Join(stepDir(out, step), "stop.sh"), stopSh, 0o755)
}

func genVerify(cfg *Config, step *Step, out string) error {
	fmt.Printf("[%s] verify script\n", step.Name)
	return writeFile(filepath.Join(stepDir(out, step), "verify.sh"), verifySh, 0o755)
}

func genCleanup(cfg *Config, step *Step, out string) error {
	fmt.Printf("[%s] cleanup-metadata script\n", step.Name)
	return writeFile(filepath.Join(stepDir(out, step), "cleanup-metadata.sh"), cleanupSh, 0o755)
}

func genPauseResume(cfg *Config, step *Step, out string) error {
	fmt.Printf("[%s] pause/resume scripts\n", step.Name)
	if err := writeFile(filepath.Join(stepDir(out, step), "pause.sh"), pauseSh, 0o755); err != nil {
		return err
	}
	return writeFile(filepath.Join(stepDir(out, step), "resume.sh"), resumeSh, 0o755)
}

func genRunbook(cfg *Config, step *Step, out string) error {
	fmt.Printf("[%s] RUNBOOK.md\n", step.Name)
	return writeFile(filepath.Join(stepDir(out, step), "RUNBOOK.md"), runbookMd(step), 0o644)
}

// genAll emits every artifact for a step.
func genAll(cfg *Config, step *Step, out string) error {
	for _, fn := range []func(*Config, *Step, string) error{
		genConfigs, genStart, genProgress, genCommit, genStop, genVerify, genCleanup, genPauseResume, genRunbook,
	} {
		if err := fn(cfg, step, out); err != nil {
			return err
		}
	}
	return nil
}

// envSh builds the per-step env.sh sourced by every script. Connection strings live
// only in the mongosync config JSONs; sync parameters live only in the start bodies —
// env.sh derives from those (single source of truth per concern).
func envSh(cfg *Config, step *Step) string {
	var ids, ports strings.Builder
	for i, j := range step.Jobs {
		ids.WriteString(" " + shellWord(j.ID))
		fmt.Fprintf(&ports, "  [%s]=%d\n", j.ID, cfg.portFor(step, i))
	}
	seq := ""
	if step.Mode == modeSequential {
		seq = `
# NOTE: this is a SEQUENTIAL (fan-in) step. Run every script with a single instance id
# as the first argument, one instance at a time (see RUNBOOK.md). Do NOT start them all
# at once — they share the same port and only one may write to the hub at a time.`
	}
	return fmt.Sprintf(`#!/usr/bin/env bash
# Generated by mongosyncScaffold — step %q (mode: %s).
# Review before use. Requires: mongosync, mongosh, curl, jq on PATH.%s
set -uo pipefail

MONGOSYNC="${MONGOSYNC:-%s}"
MONGOSH="${MONGOSH:-%s}"
STEP=%q
MODE=%q

IDS=(%s )
declare -A PORT=(
%s)

# Connection strings come from the mongosync config JSONs (edit them there).
src() { jq -r '.cluster0' "configs/$1.mongosync.json"; }
dst() { jq -r '.cluster1' "configs/$1.mongosync.json"; }

# Databases for an instance come from its start body (empty output = whole cluster).
namespaces() { jq -r '.includeNamespaces[]?.database' "start-bodies/$1.start.json" 2>/dev/null; }

# targets: the id passed as $1, or all IDS when no id is given.
targets() { if [ "${1:-}" != "" ]; then echo "$1"; else printf '%%s\n' "${IDS[@]}"; fi; }
`, step.Name, step.Mode, seq, cfg.MongosyncBinary, cfg.MongoshBinary, step.Name, step.Mode,
		ids.String(), ports.String())
}

func shellWord(s string) string { return s } // ids are simple identifiers

func runbookMd(step *Step) string {
	if step.Mode == modeSequential {
		var rows strings.Builder
		for i, j := range step.Jobs {
			fmt.Fprintf(&rows, "%d. `%s`\n", i+1, j.ID)
		}
		return fmt.Sprintf("# RUNBOOK — step %q (sequential fan-in)\n\n"+
			"Run **one instance at a time**, passing its id to each script. Order:\n\n%s\n"+
			"For **each** instance `<id>` above, in order:\n\n"+
			"```shell\n"+
			"./cleanup-metadata.sh <id>     # drop mongosync metadata on source + destination\n"+
			"./start-processes.sh  <id>     # launch mongosync, wait for IDLE\n"+
			"#   review start-bodies/<id>.start.json first\n"+
			"./start-api.sh        <id>     # POST /start\n"+
			"./progress.sh         <id>     # watch until canCommit=true, low lag (Ctrl-C)\n"+
			"./commit.sh           <id>     # POST /commit\n"+
			"./progress.sh         <id>     # watch until state=COMMITTED\n"+
			"./stop.sh             <id>     # stop the mongosync process\n"+
			"./verify.sh           <id>     # count-compare source vs destination\n"+
			"#   then repoint this cluster's application to the hub\n"+
			"```\n\n"+
			"After the **last** instance, drop mongosync metadata left on the hub by the "+
			"final merge (run with no argument = all instances, which cleans the hub):\n\n"+
			"```shell\n./cleanup-metadata.sh\n```\n\n"+
			"Artifacts to review before running: `configs/<id>.mongosync.json`, "+
			"`start-bodies/<id>.start.json`.\n", step.Name, rows.String())
	}
	return fmt.Sprintf("# RUNBOOK — step %q (parallel lift)\n\n"+
		"All scripts act on **all** instances when run with no argument (pass an id to act "+
		"on just one). Order:\n\n"+
		"```shell\n"+
		"#   1. review configs/*.mongosync.json (connection strings, ports)\n"+
		"./start-processes.sh          # launch all mongosync, wait for IDLE\n"+
		"#   2. review start-bodies/*.start.json (namespaces, verification)\n"+
		"./start-api.sh                # POST /start to all\n"+
		"./progress.sh                 # watch all until canCommit=true, low lag (Ctrl-C)\n"+
		"#   --- coordinated cutover window ---\n"+
		"./commit.sh                   # POST /commit to all\n"+
		"./progress.sh                 # watch until COMMITTED / canWrite (repoint apps here)\n"+
		"./stop.sh                     # waits for COMMITTED, then stops all\n"+
		"./verify.sh                   # count-compare source vs destination\n"+
		"```\n\n"+
		"Helpers: `./pause.sh [id]`, `./resume.sh [id]`, `./cleanup-metadata.sh [id]`.\n", step.Name)
}
