package main

import "testing"

// TestLegacyShim confirms a legacy syncs[]/consolidation config translates into the
// expected generic plan: a parallel/hold "lift" step and a sequential/auto
// "consolidate" step whose jobs carry preExisting + metadata cleanup.
func TestLegacyShim(t *testing.T) {
	cfg := &Config{
		Syncs: []SyncJob{
			{ID: "c1", Source: "s1", Destination: "d1", IncludeNamespaces: []Namespace{{Database: "a"}}},
			{ID: "c2", Source: "s2", Destination: "d2", IncludeNamespaces: []Namespace{{Database: "b"}}},
		},
		Consolidation: &Consolidation{
			Hub: "d1",
			Merges: []Merge{
				{ID: "m2", Source: "d2", IncludeNamespaces: []Namespace{{Database: "b"}}},
			},
		},
	}
	p := cfg.plan()
	if len(p.Steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(p.Steps))
	}
	lift := p.Steps[0]
	if lift.Name != "lift" || lift.Mode != modeParallel || lift.Commit != commitHold || len(lift.Jobs) != 2 {
		t.Errorf("lift step wrong: %+v", lift)
	}
	con := p.Steps[1]
	if con.Mode != modeSequential || con.Commit != commitAuto || !con.Verify || len(con.Jobs) != 1 {
		t.Errorf("consolidate step wrong: %+v", con)
	}
	j := con.Jobs[0]
	if !j.PreExistingDestinationData || j.Destination != "d1" || len(j.CleanupMetadataOn) != 2 {
		t.Errorf("merge job wrong: %+v", j)
	}
	if err := p.validate(); err != nil {
		t.Errorf("shimmed plan should validate: %v", err)
	}
}

func TestPlanValidation(t *testing.T) {
	bad := []*Plan{
		{},
		{Steps: []Step{{Name: "x", Mode: "bogus", Commit: commitAuto, Jobs: []Job{{ID: "j", Source: "s", Destination: "d"}}}}},
		{Steps: []Step{{Name: "x", Mode: modeSequential, Commit: commitHold, Jobs: []Job{{ID: "j", Source: "s", Destination: "d"}}}}},
		{Steps: []Step{{Name: "x", Mode: modeParallel, Commit: commitAuto}}}, // no jobs
		{Steps: []Step{{Name: "x", Mode: modeParallel, Commit: commitAuto, Jobs: []Job{{ID: "j", Source: "s"}}}}}, // no dest
	}
	for i, p := range bad {
		if err := p.validate(); err == nil {
			t.Errorf("bad plan %d should have failed validation", i)
		}
	}
}
