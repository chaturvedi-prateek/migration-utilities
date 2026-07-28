package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestWriteOrchestratorConfig checks the generated config has N parallel syncs and
// N-1 sequential merges into the first destination, with disjoint databases and
// distinct source/destination URIs — the invariants the orchestrator relies on.
func TestWriteOrchestratorConfig(t *testing.T) {
	const n = 10
	cs := &clusterSet{t: &tools{mongosync: "mongosync", mongosh: "mongosh"}}
	for i := 0; i < n; i++ {
		cs.sources = append(cs.sources, &node{
			name: srcName(i), rs: "srcrs", port: srcBasePort + i, db: appDB(i),
		})
		cs.dests = append(cs.dests, &node{
			name: dstName(i), rs: "dstrs", port: dstBasePort + i, db: appDB(i), auth: true,
		})
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "orchestrator.json")
	if err := writeOrchestratorConfig(path, cs.t, cs, dir); err != nil {
		t.Fatalf("write: %v", err)
	}
	b, _ := os.ReadFile(path)
	var cfg ocConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		t.Fatalf("generated config is not valid JSON: %v", err)
	}
	if len(cfg.Syncs) != n {
		t.Errorf("syncs = %d, want %d", len(cfg.Syncs), n)
	}
	if cfg.Consolidation == nil || len(cfg.Consolidation.Merges) != n-1 {
		t.Fatalf("merges = %v, want %d", cfg.Consolidation, n-1)
	}
	seenDB := map[string]bool{}
	for _, s := range cfg.Syncs {
		if len(s.IncludeNamespaces) != 1 {
			t.Errorf("sync %s: want exactly 1 namespace", s.ID)
		}
		db := s.IncludeNamespaces[0].Database
		if seenDB[db] {
			t.Errorf("duplicate (non-disjoint) database %s", db)
		}
		seenDB[db] = true
		if s.Source == s.Destination {
			t.Errorf("sync %s: source == destination", s.ID)
		}
	}
	// Hub must be the first destination; merges must be the other destinations.
	if cfg.Consolidation.Hub != cs.dests[0].destURI() {
		t.Errorf("hub = %s, want dst01 uri", cfg.Consolidation.Hub)
	}
	for i, m := range cfg.Consolidation.Merges {
		if m.Source == cfg.Consolidation.Hub {
			t.Errorf("merge %d sources the hub itself", i)
		}
	}
}

func srcName(i int) string { return fmt.Sprintf("src%02d", i+1) }
func dstName(i int) string { return fmt.Sprintf("dst%02d", i+1) }
func appDB(i int) string   { return fmt.Sprintf("appdb%02d", i+1) }
