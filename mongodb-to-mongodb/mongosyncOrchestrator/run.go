package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ---- Generic step executors ------------------------------------------------

// stepInstances builds instance handles (id + port) for a step's jobs, for
// attaching to already-running mongosync (commit/stop/progress/pause/resume).
func stepInstances(cfg *Config, step *Step) []*instance {
	ins := make([]*instance, len(step.Jobs))
	for i, j := range step.Jobs {
		ins[i] = &instance{id: j.ID, port: cfg.portFor(i), cfg: cfg}
	}
	return ins
}

// runParallelStep launches every job at once (ports basePort+i) and drives them to
// steady state. With commit=auto each job is committed once drained and stopped on
// exit; with commit=hold the processes are left running (pid file recorded) for a
// later coordinated commit.
func runParallelStep(ctx context.Context, cfg *Config, step *Step, o stepOpts) error {
	auto := step.Commit == commitAuto
	instances := make([]*instance, 0, len(step.Jobs))
	stopOnExit := true
	defer func() {
		if !stopOnExit {
			return
		}
		for _, in := range instances {
			_ = in.stop()
		}
	}()

	for i, j := range step.Jobs {
		if err := cleanupForJob(ctx, cfg, j); err != nil {
			return err
		}
		port := cfg.portFor(i)
		fmt.Printf("[%s] launching mongosync on port %d\n", j.ID, port)
		in, err := launch(ctx, cfg, j.ID, j.Source, j.Destination, port)
		if err != nil {
			return err
		}
		instances = append(instances, in)
		if err := in.start(ctx, j.IncludeNamespaces, j.PreExistingDestinationData, o.verifyFor(j)); err != nil {
			return fmt.Errorf("[%s] start: %w", j.ID, err)
		}
		fmt.Printf("[%s] started\n", j.ID)
	}

	committed := map[string]bool{}
	for {
		snaps := pollAll(ctx, instances)
		renderTable(snaps)

		allDone := true
		for i, in := range instances {
			p := snaps[i].prog
			ready := p.CanCommit && p.LagTimeSeconds <= o.lag
			if auto && ready && !committed[in.id] {
				fmt.Printf("[%s] canCommit && lag<=%.0fs -> committing\n", in.id, o.lag)
				if err := in.commit(ctx); err != nil {
					return fmt.Errorf("[%s] commit: %w", in.id, err)
				}
				committed[in.id] = true
			}
			done := committed[in.id] && (p.State == "COMMITTED" || p.CanWrite)
			if auto {
				if !done {
					allDone = false
				}
			} else if !ready {
				allDone = false
			}
		}
		if allDone {
			if auto {
				fmt.Printf("\nStep %q committed.\n", step.Name)
			} else {
				stopOnExit = false // leave running for a coordinated commit
				if err := writePidFile(cfg, step.Name, instances); err != nil {
					return fmt.Errorf("write pid file: %w", err)
				}
				fmt.Printf("\nStep %q at steady state (canCommit, low lag). mongosync left running.\n", step.Name)
				fmt.Printf("Run 'commit --step %s' during the cutover window, then 'stop --step %s'.\n", step.Name, step.Name)
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(o.poll):
		}
	}
}

// runSequentialStep runs one job at a time (reusing basePort): cleanup metadata,
// launch, start, wait for drain, commit, wait for terminal COMMITTED, stop, and
// optionally verify. This is the fan-in / consolidation primitive.
func runSequentialStep(ctx context.Context, cfg *Config, step *Step, o stepOpts) error {
	dests := map[string]bool{}
	for i, j := range step.Jobs {
		fmt.Printf("\n=== step %q: job %d/%d: %s ===\n", step.Name, i+1, len(step.Jobs), j.ID)
		if err := cleanupForJob(ctx, cfg, j); err != nil {
			return err
		}
		port := cfg.portFor(0)
		in, err := launch(ctx, cfg, j.ID, j.Source, j.Destination, port)
		if err != nil {
			return err
		}
		if err := in.start(ctx, j.IncludeNamespaces, j.PreExistingDestinationData, o.verifyFor(j)); err != nil {
			_ = in.stop()
			return fmt.Errorf("[%s] start: %w", j.ID, err)
		}
		fmt.Printf("[%s] started (preExistingDestinationData=%v)\n", j.ID, j.PreExistingDestinationData)

		if err := waitReadyToCommit(ctx, in, o.poll, o.lag); err != nil {
			_ = in.stop()
			return err
		}
		if err := in.commit(ctx); err != nil {
			_ = in.stop()
			return fmt.Errorf("[%s] commit: %w", j.ID, err)
		}
		if err := waitFullyCommitted(ctx, in, o.poll); err != nil {
			_ = in.stop()
			return err
		}
		_ = in.stop()
		fmt.Printf("[%s] committed\n", j.ID)

		if step.Verify {
			if err := verifyJob(ctx, cfg, j); err != nil {
				return fmt.Errorf("[%s] verify: %w", j.ID, err)
			}
		}
		dests[j.Destination] = true
	}
	// Leave each touched destination without mongosync metadata.
	for d := range dests {
		if err := cleanMetadata(ctx, cfg, d); err != nil {
			return fmt.Errorf("final clean metadata: %w", err)
		}
	}
	fmt.Printf("\nStep %q complete.\n", step.Name)
	return nil
}

// commitStep performs the coordinated cutover for a held parallel step: attach to
// the running jobs, confirm all drained (unless force), commit them together, and
// wait for canWrite (apps may cut over). Does NOT stop the processes.
func commitStep(ctx context.Context, cfg *Config, step *Step, lagThreshold float64, force bool) error {
	if step.Mode != modeParallel {
		return fmt.Errorf("commit applies to parallel steps; step %q is %s", step.Name, step.Mode)
	}
	instances := stepInstances(cfg, step)
	snaps := pollAll(ctx, instances)
	renderTable(snaps)

	var notReady []string
	for _, s := range snaps {
		if s.err != nil {
			notReady = append(notReady, s.id+"(unreachable)")
		} else if !(s.prog.CanCommit && s.prog.LagTimeSeconds <= lagThreshold) {
			notReady = append(notReady, s.id)
		}
	}
	if len(notReady) > 0 && !force {
		return fmt.Errorf("not all jobs are drained/ready: %v (wait, or use --force)", notReady)
	}

	for _, in := range instances {
		if err := in.commit(ctx); err != nil {
			return fmt.Errorf("[%s] commit: %w", in.id, err)
		}
		fmt.Printf("[%s] commit requested\n", in.id)
	}
	for _, in := range instances {
		if err := waitCanWrite(ctx, in, pollDefault); err != nil {
			return fmt.Errorf("[%s] waiting for canWrite: %w", in.id, err)
		}
		fmt.Printf("[%s] canWrite=true\n", in.id)
	}
	fmt.Println("\nCUTOVER READY: destinations are writable — repoint applications now.")
	fmt.Println("mongosync continues finalizing to COMMITTED (index builds) in the background.")
	fmt.Printf("When all are COMMITTED, run 'stop --step %s'.\n", step.Name)
	return nil
}

// stopStep waits for each held job to reach terminal COMMITTED (index builds done),
// then terminates the processes to free their ports. --force skips the wait.
func stopStep(ctx context.Context, cfg *Config, step *Step, force bool) error {
	if !force {
		for _, in := range stepInstances(cfg, step) {
			fmt.Printf("[%s] waiting for COMMITTED before stopping...\n", in.id)
			if err := waitFullyCommitted(ctx, in, pollDefault); err != nil {
				return fmt.Errorf("[%s] waiting for COMMITTED: %w", in.id, err)
			}
			fmt.Printf("[%s] COMMITTED\n", in.id)
		}
	}
	return stopByPidFile(cfg, step.Name)
}

// progressStep / pauseResumeStep operate on a step's running jobs.
func progressStep(ctx context.Context, cfg *Config, step *Step) error {
	renderTable(pollAll(ctx, stepInstances(cfg, step)))
	return nil
}

func pauseResumeStep(ctx context.Context, cfg *Config, step *Step, doPause bool) error {
	action := "resume"
	if doPause {
		action = "pause"
	}
	var firstErr error
	for _, in := range stepInstances(cfg, step) {
		var err error
		if doPause {
			err = in.pause(ctx)
		} else {
			err = in.resume(ctx)
		}
		if err != nil {
			fmt.Printf("[%s] %s: %v\n", in.id, action, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		fmt.Printf("[%s] %sd\n", in.id, action)
	}
	return firstErr
}

// ---- Legacy command adapters (map onto the generic plan) -------------------

func runSync(ctx context.Context, cfg *Config, commit, embeddedVerify bool, pollEvery time.Duration, lagThreshold float64) error {
	st, err := cfg.firstStepByMode("", modeParallel)
	if err != nil {
		return err
	}
	s := *st // copy so overriding commit policy doesn't mutate the plan
	s.Commit = commitHold
	if commit {
		s.Commit = commitAuto
	}
	return runParallelStep(ctx, cfg, &s, stepOpts{poll: pollEvery, lag: lagThreshold, embeddedVerify: embeddedVerify})
}

func runCommit(ctx context.Context, cfg *Config, lagThreshold float64, force bool) error {
	st, err := cfg.firstStepByMode("", modeParallel)
	if err != nil {
		return err
	}
	return commitStep(ctx, cfg, st, lagThreshold, force)
}

func runStop(ctx context.Context, cfg *Config, force bool) error {
	st, err := cfg.firstStepByMode("", modeParallel)
	if err != nil {
		return err
	}
	return stopStep(ctx, cfg, st, force)
}

func runConsolidate(ctx context.Context, cfg *Config, pollEvery time.Duration, lagThreshold float64, verify, embeddedVerify bool) error {
	st, err := cfg.firstStepByMode("", modeSequential)
	if err != nil {
		return err
	}
	s := *st
	s.Verify = s.Verify || verify
	return runSequentialStep(ctx, cfg, &s, stepOpts{poll: pollEvery, lag: lagThreshold, embeddedVerify: embeddedVerify})
}

func waitReadyToCommit(ctx context.Context, in *instance, pollEvery time.Duration, lagThreshold float64) error {
	for {
		p, err := in.progress(ctx)
		if err != nil {
			return err
		}
		renderTable([]snapshot{{id: in.id, prog: p}})
		if p.CanCommit && p.LagTimeSeconds <= lagThreshold {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

// pollDefault is the fallback poll interval for the post-commit waits.
const pollDefault = 5 * time.Second

// waitCanWrite returns once the destination is writable (canWrite), i.e. apps may
// cut over. COMMITTED also satisfies this.
func waitCanWrite(ctx context.Context, in *instance, pollEvery time.Duration) error {
	return waitUntil(ctx, in, pollEvery, func(p Progress) bool {
		return p.CanWrite || p.State == "COMMITTED"
	})
}

// waitFullyCommitted returns only once mongosync reaches the terminal COMMITTED
// state (final drain + index builds done). This can take a long time.
func waitFullyCommitted(ctx context.Context, in *instance, pollEvery time.Duration) error {
	return waitUntil(ctx, in, pollEvery, func(p Progress) bool {
		return p.State == "COMMITTED"
	})
}

func waitUntil(ctx context.Context, in *instance, pollEvery time.Duration, done func(Progress) bool) error {
	for {
		p, err := in.progress(ctx)
		if err != nil {
			return err
		}
		if done(p) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

// cleanMetadata drops the mongosync reserved databases from a cluster so a fresh
// merge can start. Cleaning both the hub and the source is what makes sequential
// fan-in possible.
func cleanMetadata(ctx context.Context, cfg *Config, hub string) error {
	js := `["mongosync_reserved_for_internal_use","__mdb_internal_mongosync"].forEach(function(d){try{db.getSiblingDB(d).dropDatabase();}catch(e){}});print("metadata-cleaned");`
	out, err := mongoshEval(ctx, cfg, hub, js)
	if err != nil {
		return fmt.Errorf("%v: %s", err, out)
	}
	return nil
}

// verifyJob compares per-database document counts between a job's source and
// destination for each included namespace.
func verifyJob(ctx context.Context, cfg *Config, j Job) error {
	for _, ns := range j.IncludeNamespaces {
		db := ns.Database
		js := fmt.Sprintf(`var t=0;db.getSiblingDB(%q).getCollectionNames().forEach(function(c){t+=db.getSiblingDB(%q)[c].countDocuments({});});print(t);`, db, db)
		src, err := mongoshEval(ctx, cfg, j.Source, js)
		if err != nil {
			return fmt.Errorf("source count %s: %v (%s)", db, err, src)
		}
		dst, err := mongoshEval(ctx, cfg, j.Destination, js)
		if err != nil {
			return fmt.Errorf("destination count %s: %v (%s)", db, err, dst)
		}
		status := "OK"
		if src != dst {
			status = "MISMATCH"
		}
		fmt.Printf("[%s] verify db=%s source=%s destination=%s -> %s\n", j.ID, db, src, dst, status)
		if src != dst {
			return fmt.Errorf("count mismatch for db %s: source=%s destination=%s", db, src, dst)
		}
	}
	return nil
}

type snapshot struct {
	id   string
	prog Progress
	err  error
}

func pollAll(ctx context.Context, instances []*instance) []snapshot {
	snaps := make([]snapshot, len(instances))
	for i, in := range instances {
		p, err := in.progress(ctx)
		snaps[i] = snapshot{id: in.id, prog: p, err: err}
	}
	return snaps
}

func renderTable(snaps []snapshot) {
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].id < snaps[j].id })
	fmt.Printf("\n%-16s %-12s %-24s %8s %10s %10s\n", "ID", "STATE", "INFO", "COPIED%", "LAG(s)", "CANCOMMIT")
	fmt.Println(strings.Repeat("-", 86))
	for _, s := range snaps {
		if s.err != nil {
			fmt.Printf("%-16s %-12s %s\n", s.id, "ERR", s.err)
			continue
		}
		p := s.prog
		fmt.Printf("%-16s %-12s %-24s %7.1f%% %10.0f %10v\n",
			s.id, p.State, truncate(p.Info, 24), p.percentCopied(), p.LagTimeSeconds, p.CanCommit)
	}
	fmt.Printf("%s  %s\n", "updated", time.Now().Format("15:04:05"))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
