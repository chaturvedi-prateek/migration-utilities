package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Namespace is a mongosync includeNamespaces entry. A database-only entry
// selects the whole database; add Collection to scope to a single collection.
type Namespace struct {
	Database    string   `json:"database"`
	Collections []string `json:"collections,omitempty"`
}

// SyncJob is one 1:1 mongosync (Phase 1): one source cluster -> one dedicated
// destination cluster, run in parallel with the other jobs on the same host.
type SyncJob struct {
	ID                string      `json:"id"`
	Source            string      `json:"source"`
	Destination       string      `json:"destination"`
	IncludeNamespaces []Namespace `json:"includeNamespaces,omitempty"`
}

// Merge is one step of the sequential consolidation (Phase 2): a source cluster
// whose (disjoint) namespaces are merged into the shared hub cluster.
type Merge struct {
	ID                string      `json:"id"`
	Source            string      `json:"source"`
	IncludeNamespaces []Namespace `json:"includeNamespaces,omitempty"`
}

// Consolidation is the Phase 2 fan-in into a single hub. mongosync does not
// support many-to-one, so this is executed strictly one merge at a time, with
// the hub's mongosync metadata cleaned between runs (see run.go).
type Consolidation struct {
	Hub    string  `json:"hub"`
	Merges []Merge `json:"merges"`
}

// Config is the orchestrator config. Stdlib-only JSON so the tool stays a single
// static binary with no download dependencies in locked-down environments.
type Config struct {
	MongosyncBinary string         `json:"mongosyncBinary"` // path or name on PATH
	MongoshBinary   string         `json:"mongoshBinary"`   // used for metadata cleanup + verify
	LogDir          string         `json:"logDir"`
	BasePort        int            `json:"basePort"` // first mongosync API port; jobs use base+index
	Syncs           []SyncJob      `json:"syncs"`
	Consolidation   *Consolidation `json:"consolidation,omitempty"`
}

func loadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(stripLineComments(b), &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.MongosyncBinary == "" {
		c.MongosyncBinary = "mongosync"
	}
	if c.MongoshBinary == "" {
		c.MongoshBinary = "mongosh"
	}
	if c.LogDir == "" {
		c.LogDir = "./logs"
	}
	if c.BasePort == 0 {
		c.BasePort = 27182
	}
}

func (c *Config) validate() error {
	seen := map[string]bool{}
	for i, s := range c.Syncs {
		if s.ID == "" {
			return fmt.Errorf("syncs[%d]: id is required", i)
		}
		if seen[s.ID] {
			return fmt.Errorf("syncs[%d]: duplicate id %q", i, s.ID)
		}
		seen[s.ID] = true
		if s.Source == "" || s.Destination == "" {
			return fmt.Errorf("sync %q: source and destination are required", s.ID)
		}
	}
	if c.Consolidation != nil {
		if c.Consolidation.Hub == "" {
			return fmt.Errorf("consolidation.hub is required")
		}
		mseen := map[string]bool{}
		for i, m := range c.Consolidation.Merges {
			if m.ID == "" {
				return fmt.Errorf("consolidation.merges[%d]: id is required", i)
			}
			if mseen[m.ID] {
				return fmt.Errorf("consolidation.merges[%d]: duplicate id %q", i, m.ID)
			}
			mseen[m.ID] = true
			if m.Source == "" {
				return fmt.Errorf("merge %q: source is required", m.ID)
			}
			if len(m.IncludeNamespaces) == 0 {
				return fmt.Errorf("merge %q: includeNamespaces is required "+
					"(fan-in must be scoped to this cluster's disjoint namespaces)", m.ID)
			}
		}
	}
	return nil
}

func (c *Config) portFor(index int) int { return c.BasePort + index }
