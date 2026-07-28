package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// runSync runs Phase 1: launch every 1:1 sync in parallel on this host, start
// each, then poll aggregated progress until all reach CEA (canCommit). If commit
// is set, each job is committed once it is drained (canCommit && low lag).
func runSync(ctx context.Context, cfg *Config, commit, embeddedVerify bool, pollEvery time.Duration, lagThreshold float64) error {
	if len(cfg.Syncs) == 0 {
		return fmt.Errorf("no syncs defined in config")
	}
	instances := make([]*instance, 0, len(cfg.Syncs))
	// In steady-state mode (no --commit) we leave mongosync running on success so a
	// later coordinated `commit` can finalize all clusters together. On error or in
	// all-in-one --commit mode we stop the processes on exit.
	stopOnExit := true
	defer func() {
		if !stopOnExit {
			return
		}
		for _, in := range instances {
			_ = in.stop()
		}
	}()

	for i, s := range cfg.Syncs {
		port := cfg.portFor(i)
		fmt.Printf("[%s] launching mongosync on port %d\n", s.ID, port)
		in, err := launch(ctx, cfg, s.ID, s.Source, s.Destination, port)
		if err != nil {
			return err
		}
		instances = append(instances, in)
		if err := in.start(ctx, s.IncludeNamespaces, false, embeddedVerify); err != nil {
			return fmt.Errorf("[%s] start: %w", s.ID, err)
		}
		fmt.Printf("[%s] sync started\n", s.ID)
	}

	committed := map[string]bool{}
	for {
		snaps := pollAll(ctx, instances)
		renderTable(snaps)

		allDone := true
		for i, in := range instances {
			p := snaps[i].prog
			ready := p.CanCommit && p.LagTimeSeconds <= lagThreshold
			if commit && ready && !committed[in.id] {
				fmt.Printf("[%s] canCommit && lag<=%.0fs -> committing\n", in.id, lagThreshold)
				if err := in.commit(ctx); err != nil {
					return fmt.Errorf("[%s] commit: %w", in.id, err)
				}
				committed[in.id] = true
			}
			done := committed[in.id] && (p.State == "COMMITTED" || p.CanWrite)
			if commit {
				if !done {
					allDone = false
				}
			} else if !ready {
				allDone = false
			}
		}
		if allDone {
			if commit {
				fmt.Println("\nAll syncs committed. Phase 1 complete.")
			} else {
				stopOnExit = false // leave mongosync running for the coordinated commit
				if err := writePidFile(cfg, instances); err != nil {
					return fmt.Errorf("write pid file: %w", err)
				}
				fmt.Println("\nAll syncs at CEA and drained (canCommit, low lag).")
				fmt.Println("mongosync left running. Run 'commit' during the cutover window to finalize all clusters together.")
			}
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

// runCommit performs the single coordinated Phase-1 cutover: attach to the syncs
// left running by `sync`, confirm every one is drained, then commit them all
// together. Use --force to commit even if some are not fully drained.
func runCommit(ctx context.Context, cfg *Config, lagThreshold float64, force bool) error {
	if len(cfg.Syncs) == 0 {
		return fmt.Errorf("no syncs defined in config")
	}
	instances := make([]*instance, len(cfg.Syncs))
	for i, s := range cfg.Syncs {
		instances[i] = &instance{id: s.ID, port: cfg.portFor(i), cfg: cfg}
	}
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
		return fmt.Errorf("not all syncs are drained/ready: %v (wait, or use --force)", notReady)
	}

	for _, in := range instances {
		if err := in.commit(ctx); err != nil {
			return fmt.Errorf("[%s] commit: %w", in.id, err)
		}
		fmt.Printf("[%s] commit requested\n", in.id)
	}

	// Wait only for canWrite — the point at which the destination is writable and
	// applications may cut over. mongosync then continues finalizing to COMMITTED
	// (final drain + index builds) in the background, which can take a long time.
	// We deliberately do NOT stop the processes here: killing before COMMITTED could
	// leave index builds incomplete. Use 'stop' (which waits for COMMITTED) before
	// consolidation.
	for _, in := range instances {
		if err := waitCanWrite(ctx, in, pollDefault); err != nil {
			return fmt.Errorf("[%s] waiting for canWrite: %w", in.id, err)
		}
		fmt.Printf("[%s] canWrite=true\n", in.id)
	}
	fmt.Println("\nCUTOVER READY: destinations are writable — repoint all applications to Atlas now.")
	fmt.Println("mongosync continues finalizing to COMMITTED (index builds) in the background.")
	fmt.Println("When all are COMMITTED, run 'stop' (it waits for COMMITTED), then 'consolidate'.")
	return nil
}

// runStop waits for each still-running sync to reach COMMITTED (so index builds and
// finalization are complete), then terminates the processes to free their ports for
// consolidation. --force skips the wait and kills immediately.
func runStop(ctx context.Context, cfg *Config, force bool) error {
	if !force {
		instances := make([]*instance, len(cfg.Syncs))
		for i, s := range cfg.Syncs {
			instances[i] = &instance{id: s.ID, port: cfg.portFor(i), cfg: cfg}
		}
		for _, in := range instances {
			fmt.Printf("[%s] waiting for COMMITTED before stopping...\n", in.id)
			if err := waitFullyCommitted(ctx, in, pollDefault); err != nil {
				return fmt.Errorf("[%s] waiting for COMMITTED: %w", in.id, err)
			}
			fmt.Printf("[%s] COMMITTED\n", in.id)
		}
	}
	return stopByPidFile(cfg)
}

// runConsolidate runs Phase 2: sequentially merge each source cluster's disjoint
// namespaces into the shared hub. mongosync forbids many-to-one, so each merge is
// a fresh single mongosync run and the hub's mongosync metadata is dropped between
// runs. Only one instance ever writes to the hub at a time.
func runConsolidate(ctx context.Context, cfg *Config, pollEvery time.Duration, lagThreshold float64, verify, embeddedVerify bool) error {
	if cfg.Consolidation == nil || len(cfg.Consolidation.Merges) == 0 {
		return fmt.Errorf("no consolidation.merges defined in config")
	}
	hub := cfg.Consolidation.Hub
	for i, m := range cfg.Consolidation.Merges {
		fmt.Printf("\n=== merge %d/%d: %s ===\n", i+1, len(cfg.Consolidation.Merges), m.ID)

		// Clean mongosync metadata on BOTH ends: the hub so a fresh merge can
		// start, and the source in case it was itself a mongosync destination in
		// Phase 1 (leftover metadata there stalls initialization).
		if err := cleanMetadata(ctx, cfg, hub); err != nil {
			return fmt.Errorf("[%s] clean hub metadata: %w", m.ID, err)
		}
		if err := cleanMetadata(ctx, cfg, m.Source); err != nil {
			return fmt.Errorf("[%s] clean source metadata: %w", m.ID, err)
		}

		port := cfg.portFor(0) // sequential: always one instance, reuse base port
		in, err := launch(ctx, cfg, m.ID, m.Source, hub, port)
		if err != nil {
			return err
		}
		if err := in.start(ctx, m.IncludeNamespaces, true /*preExistingDestinationData*/, embeddedVerify); err != nil {
			_ = in.stop()
			return fmt.Errorf("[%s] start: %w", m.ID, err)
		}
		fmt.Printf("[%s] merge sync started (preExistingDestinationData=true)\n", m.ID)

		if err := waitReadyToCommit(ctx, in, pollEvery, lagThreshold); err != nil {
			_ = in.stop()
			return err
		}
		if err := in.commit(ctx); err != nil {
			_ = in.stop()
			return fmt.Errorf("[%s] commit: %w", m.ID, err)
		}
		// Wait for terminal COMMITTED (index builds complete) before stopping this
		// merge's mongosync and verifying counts.
		if err := waitFullyCommitted(ctx, in, pollEvery); err != nil {
			_ = in.stop()
			return err
		}
		_ = in.stop()
		fmt.Printf("[%s] merge committed\n", m.ID)

		if verify {
			if err := verifyMerge(ctx, cfg, m, hub); err != nil {
				return fmt.Errorf("[%s] verify: %w", m.ID, err)
			}
		}
	}
	// Final cleanup so the hub is left without mongosync metadata.
	if err := cleanMetadata(ctx, cfg, hub); err != nil {
		return fmt.Errorf("final clean hub metadata: %w", err)
	}
	fmt.Println("\nConsolidation complete. All sources merged into the hub.")
	return nil
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

func verifyMerge(ctx context.Context, cfg *Config, m Merge, hub string) error {
	for _, ns := range m.IncludeNamespaces {
		db := ns.Database
		js := fmt.Sprintf(`var t=0;db.getSiblingDB(%q).getCollectionNames().forEach(function(c){t+=db.getSiblingDB(%q)[c].countDocuments({});});print(t);`, db, db)
		src, err := mongoshEval(ctx, cfg, m.Source, js)
		if err != nil {
			return fmt.Errorf("source count %s: %v (%s)", db, err, src)
		}
		dst, err := mongoshEval(ctx, cfg, hub, js)
		if err != nil {
			return fmt.Errorf("hub count %s: %v (%s)", db, err, dst)
		}
		status := "OK"
		if src != dst {
			status = "MISMATCH"
		}
		fmt.Printf("[%s] verify db=%s source=%s hub=%s -> %s\n", m.ID, db, src, dst, status)
		if src != dst {
			return fmt.Errorf("count mismatch for db %s: source=%s hub=%s", db, src, dst)
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
