package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Namespace is a mongosync includeNamespaces entry (database-only = whole database).
type Namespace struct {
	Database    string   `json:"database"`
	Collections []string `json:"collections,omitempty"`
}

// Job is one mongosync run. Omitting IncludeNamespaces means whole-cluster sync.
type Job struct {
	ID                         string      `json:"id"`
	Source                     string      `json:"source"`
	Destination                string      `json:"destination"`
	IncludeNamespaces          []Namespace `json:"includeNamespaces,omitempty"`
	PreExistingDestinationData bool        `json:"preExistingDestinationData,omitempty"`
	CleanupMetadataOn          []string    `json:"cleanupMetadataOn,omitempty"` // "source","destination"
	EmbeddedVerify             bool        `json:"embeddedVerify,omitempty"`
}

// Step is a group of jobs: parallel (1:1 lift wave) or sequential (fan-in).
type Step struct {
	Name   string `json:"name"`
	Mode   string `json:"mode"`   // "parallel" | "sequential"
	Commit string `json:"commit"` // "hold" | "auto" (informational for scaffolding)
	Verify bool   `json:"verify,omitempty"`
	Jobs   []Job  `json:"jobs"`
}

// Plan is an ordered list of steps.
type Plan struct {
	Steps []Step `json:"steps"`
}

// Config is the scaffold input — identical to the orchestrator's, so the same file
// drives both tools. Legacy syncs[]/consolidation is translated via plan().
type Config struct {
	MongosyncBinary string         `json:"mongosyncBinary"`
	MongoshBinary   string         `json:"mongoshBinary"`
	BasePort        int            `json:"basePort"`
	Plan            *Plan          `json:"plan,omitempty"`
	Syncs           []SyncJob      `json:"syncs,omitempty"`
	Consolidation   *Consolidation `json:"consolidation,omitempty"`
}

type SyncJob struct {
	ID                string      `json:"id"`
	Source            string      `json:"source"`
	Destination       string      `json:"destination"`
	IncludeNamespaces []Namespace `json:"includeNamespaces,omitempty"`
}
type Merge struct {
	ID                string      `json:"id"`
	Source            string      `json:"source"`
	IncludeNamespaces []Namespace `json:"includeNamespaces,omitempty"`
}
type Consolidation struct {
	Hub    string  `json:"hub"`
	Merges []Merge `json:"merges"`
}

const (
	modeParallel   = "parallel"
	modeSequential = "sequential"
)

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(stripLineComments(b), &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if c.MongosyncBinary == "" {
		c.MongosyncBinary = "mongosync"
	}
	if c.MongoshBinary == "" {
		c.MongoshBinary = "mongosh"
	}
	if c.BasePort == 0 {
		c.BasePort = 27182
	}
	if err := c.plan().validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// plan returns the configured plan or synthesizes one from legacy config.
func (c *Config) plan() *Plan {
	if c.Plan != nil {
		return c.Plan
	}
	p := &Plan{}
	if len(c.Syncs) > 0 {
		st := Step{Name: "lift", Mode: modeParallel, Commit: "hold"}
		for _, s := range c.Syncs {
			st.Jobs = append(st.Jobs, Job{ID: s.ID, Source: s.Source, Destination: s.Destination, IncludeNamespaces: s.IncludeNamespaces})
		}
		p.Steps = append(p.Steps, st)
	}
	if c.Consolidation != nil {
		st := Step{Name: "consolidate", Mode: modeSequential, Commit: "auto", Verify: true}
		for _, m := range c.Consolidation.Merges {
			st.Jobs = append(st.Jobs, Job{
				ID: m.ID, Source: m.Source, Destination: c.Consolidation.Hub,
				IncludeNamespaces: m.IncludeNamespaces, PreExistingDestinationData: true,
				CleanupMetadataOn: []string{"source", "destination"},
			})
		}
		p.Steps = append(p.Steps, st)
	}
	return p
}

func (p *Plan) validate() error {
	if len(p.Steps) == 0 {
		return fmt.Errorf("config has no steps (define plan.steps[] or legacy syncs[]/consolidation)")
	}
	names := map[string]bool{}
	for _, s := range p.Steps {
		if s.Name == "" {
			return fmt.Errorf("a step is missing a name")
		}
		if names[s.Name] {
			return fmt.Errorf("duplicate step name %q", s.Name)
		}
		names[s.Name] = true
		if s.Mode != modeParallel && s.Mode != modeSequential {
			return fmt.Errorf("step %q: mode must be %q or %q", s.Name, modeParallel, modeSequential)
		}
		if len(s.Jobs) == 0 {
			return fmt.Errorf("step %q: no jobs", s.Name)
		}
		for _, j := range s.Jobs {
			if j.ID == "" || j.Source == "" || j.Destination == "" {
				return fmt.Errorf("step %q: every job needs id, source, destination", s.Name)
			}
		}
	}
	return nil
}

func (c *Config) findStep(name string) (*Step, error) {
	p := c.plan()
	for i := range p.Steps {
		if p.Steps[i].Name == name {
			return &p.Steps[i], nil
		}
	}
	var names []string
	for _, s := range p.Steps {
		names = append(names, s.Name)
	}
	return nil, fmt.Errorf("no step named %q (have: %s)", name, strings.Join(names, ", "))
}

// portFor: parallel jobs use basePort+index; sequential jobs share basePort (one at a time).
func (c *Config) portFor(step *Step, index int) int {
	if step.Mode == modeSequential {
		return c.BasePort
	}
	return c.BasePort + index
}

// stripLineComments removes // comments outside of strings so config files can be annotated.
func stripLineComments(b []byte) []byte {
	var out strings.Builder
	in := string(b)
	inStr, esc := false, false
	for i := 0; i < len(in); i++ {
		c := in[i]
		if inStr {
			out.WriteByte(c)
			if esc {
				esc = false
			} else if c == '\\' {
				esc = true
			} else if c == '"' {
				inStr = false
			}
			continue
		}
		if c == '"' {
			inStr = true
			out.WriteByte(c)
			continue
		}
		if c == '/' && i+1 < len(in) && in[i+1] == '/' {
			for i < len(in) && in[i] != '\n' {
				i++
			}
			if i < len(in) {
				out.WriteByte('\n')
			}
			continue
		}
		out.WriteByte(c)
	}
	return []byte(out.String())
}
