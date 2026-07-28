package main

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// Job is the single generic primitive: one mongosync run. Every operation the tool
// performs (1:1 lift, fan-in merge, staged wave) is expressed as a Job.
type Job struct {
	ID                string      `json:"id"`
	Source            string      `json:"source"`
	Destination       string      `json:"destination"`
	IncludeNamespaces []Namespace `json:"includeNamespaces,omitempty"`
	// PreExistingDestinationData allows syncing into a destination that already holds
	// (disjoint) data — required for fan-in merges. It also makes mongosync disable
	// destination write blocking, so live traffic to other databases is unaffected.
	PreExistingDestinationData bool `json:"preExistingDestinationData,omitempty"`
	// CleanupMetadataOn lists which ends to drop mongosync metadata DBs on before the
	// run: any of "source", "destination". Needed when reusing clusters across runs.
	CleanupMetadataOn []string `json:"cleanupMetadataOn,omitempty"`
	// EmbeddedVerify turns on mongosync's built-in verifier for this job.
	EmbeddedVerify bool `json:"embeddedVerify,omitempty"`
}

// Step is a group of jobs executed together with one mode and commit policy.
//
//	mode:   "parallel"   — launch all jobs at once (ports basePort+i)
//	        "sequential" — one job at a time (fan-in / merges), reusing basePort
//	commit: "auto"       — commit each job once drained, then stop it
//	        "hold"       — run to steady state and LEAVE running for a later 'commit'
//	verify: run a per-job document-count check (source vs destination) after commit
type Step struct {
	Name   string `json:"name"`
	Mode   string `json:"mode"`
	Commit string `json:"commit"`
	Verify bool   `json:"verify,omitempty"`
	Jobs   []Job  `json:"jobs"`
}

// Plan is an ordered list of steps. "10→10→1" is just: a parallel/hold step of 10
// jobs, then a sequential/auto step of 9 preExisting+cleanup jobs.
type Plan struct {
	Steps []Step `json:"steps"`
}

const (
	modeParallel   = "parallel"
	modeSequential = "sequential"
	commitAuto     = "auto"
	commitHold     = "hold"
)

// plan returns the configured Plan, or synthesizes one from the legacy
// syncs[]/consolidation config so old configs keep working unchanged.
func (c *Config) plan() *Plan {
	if c.Plan != nil {
		return c.Plan
	}
	p := &Plan{}
	if len(c.Syncs) > 0 {
		st := Step{Name: "lift", Mode: modeParallel, Commit: commitHold}
		for _, s := range c.Syncs {
			st.Jobs = append(st.Jobs, Job{
				ID: s.ID, Source: s.Source, Destination: s.Destination,
				IncludeNamespaces: s.IncludeNamespaces,
			})
		}
		p.Steps = append(p.Steps, st)
	}
	if c.Consolidation != nil {
		st := Step{Name: "consolidate", Mode: modeSequential, Commit: commitAuto, Verify: true}
		for _, m := range c.Consolidation.Merges {
			st.Jobs = append(st.Jobs, Job{
				ID: m.ID, Source: m.Source, Destination: c.Consolidation.Hub,
				IncludeNamespaces:          m.IncludeNamespaces,
				PreExistingDestinationData: true,
				CleanupMetadataOn:          []string{"source", "destination"},
			})
		}
		p.Steps = append(p.Steps, st)
	}
	return p
}

func (p *Plan) validate() error {
	if len(p.Steps) == 0 {
		return fmt.Errorf("plan.steps is empty")
	}
	names := map[string]bool{}
	for i := range p.Steps {
		s := &p.Steps[i]
		if s.Name == "" {
			return fmt.Errorf("plan.steps[%d]: name is required", i)
		}
		if names[s.Name] {
			return fmt.Errorf("duplicate step name %q", s.Name)
		}
		names[s.Name] = true
		if s.Mode != modeParallel && s.Mode != modeSequential {
			return fmt.Errorf("step %q: mode must be %q or %q", s.Name, modeParallel, modeSequential)
		}
		if s.Commit != commitAuto && s.Commit != commitHold {
			return fmt.Errorf("step %q: commit must be %q or %q", s.Name, commitAuto, commitHold)
		}
		if s.Mode == modeSequential && s.Commit == commitHold {
			return fmt.Errorf("step %q: sequential steps cannot use commit=hold", s.Name)
		}
		if len(s.Jobs) == 0 {
			return fmt.Errorf("step %q: no jobs", s.Name)
		}
		jseen := map[string]bool{}
		for j := range s.Jobs {
			job := s.Jobs[j]
			if job.ID == "" {
				return fmt.Errorf("step %q jobs[%d]: id is required", s.Name, j)
			}
			if jseen[job.ID] {
				return fmt.Errorf("step %q: duplicate job id %q", s.Name, job.ID)
			}
			jseen[job.ID] = true
			if job.Source == "" || job.Destination == "" {
				return fmt.Errorf("job %q: source and destination are required", job.ID)
			}
			for _, t := range job.CleanupMetadataOn {
				if t != "source" && t != "destination" {
					return fmt.Errorf("job %q: cleanupMetadataOn entries must be \"source\" or \"destination\"", job.ID)
				}
			}
		}
	}
	return nil
}

func (c *Config) findStep(name string) (*Step, error) {
	p := c.plan()
	if name == "" {
		if len(p.Steps) == 1 {
			return &p.Steps[0], nil
		}
		return nil, fmt.Errorf("--step required; plan has %d steps: %s", len(p.Steps), strings.Join(p.stepNames(), ", "))
	}
	for i := range p.Steps {
		if p.Steps[i].Name == name {
			return &p.Steps[i], nil
		}
	}
	return nil, fmt.Errorf("no step named %q (have: %s)", name, strings.Join(p.stepNames(), ", "))
}

// firstStepByMode resolves the step for legacy commands: an explicit name if given,
// otherwise the first step of the requested mode.
func (c *Config) firstStepByMode(name, mode string) (*Step, error) {
	if name != "" {
		return c.findStep(name)
	}
	p := c.plan()
	for i := range p.Steps {
		if p.Steps[i].Mode == mode {
			return &p.Steps[i], nil
		}
	}
	return nil, fmt.Errorf("no %s step found in plan", mode)
}

func (p *Plan) stepNames() []string {
	out := make([]string, len(p.Steps))
	for i, s := range p.Steps {
		out[i] = s.Name
	}
	return out
}

type stepOpts struct {
	poll           time.Duration
	lag            float64
	force          bool
	embeddedVerify bool // OR'd with each job's own EmbeddedVerify
}

func (o stepOpts) verifyFor(j Job) bool { return j.EmbeddedVerify || o.embeddedVerify }

// runStep dispatches a step to the parallel or sequential executor.
func runStep(ctx context.Context, cfg *Config, step *Step, o stepOpts) error {
	switch step.Mode {
	case modeParallel:
		return runParallelStep(ctx, cfg, step, o)
	case modeSequential:
		return runSequentialStep(ctx, cfg, step, o)
	default:
		return fmt.Errorf("step %q: unknown mode %q", step.Name, step.Mode)
	}
}

// cleanupForJob drops mongosync metadata on the ends listed in job.CleanupMetadataOn.
func cleanupForJob(ctx context.Context, cfg *Config, j Job) error {
	for _, target := range j.CleanupMetadataOn {
		uri := j.Source
		if target == "destination" {
			uri = j.Destination
		}
		if err := cleanMetadata(ctx, cfg, uri); err != nil {
			return fmt.Errorf("[%s] clean %s metadata: %w", j.ID, target, err)
		}
	}
	return nil
}
