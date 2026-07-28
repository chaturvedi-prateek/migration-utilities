package main

import (
	"bytes"
	"encoding/json"
	"os"
)

// These mirror the orchestrator's config shape so the generated file drops straight in.
type ocNamespace struct {
	Database string `json:"database"`
}
type ocSync struct {
	ID                string        `json:"id"`
	Source            string        `json:"source"`
	Destination       string        `json:"destination"`
	IncludeNamespaces []ocNamespace `json:"includeNamespaces"`
}
type ocMerge struct {
	ID                string        `json:"id"`
	Source            string        `json:"source"`
	IncludeNamespaces []ocNamespace `json:"includeNamespaces"`
}
type ocConsolidation struct {
	Hub    string    `json:"hub"`
	Merges []ocMerge `json:"merges"`
}
type ocConfig struct {
	MongosyncBinary string           `json:"mongosyncBinary"`
	MongoshBinary   string           `json:"mongoshBinary"`
	LogDir          string           `json:"logDir"`
	BasePort        int              `json:"basePort"`
	Syncs           []ocSync         `json:"syncs"`
	Consolidation   *ocConsolidation `json:"consolidation"`
}

// writeOrchestratorConfig builds orchestrator.json: one 1:1 sync per source->dest,
// and a consolidation that folds dst02..dstN into dst01 (the hub). Namespaces are
// disjoint by construction (each cluster owns appdbNN).
func writeOrchestratorConfig(path string, t *tools, cs *clusterSet, logDir string) error {
	cfg := ocConfig{
		MongosyncBinary: t.mongosync,
		MongoshBinary:   t.mongosh,
		LogDir:          logDir,
		BasePort:        27182,
	}
	for i := range cs.sources {
		cfg.Syncs = append(cfg.Syncs, ocSync{
			ID:                cs.sources[i].name,
			Source:            cs.sources[i].sourceURI(),
			Destination:       cs.dests[i].destURI(),
			IncludeNamespaces: []ocNamespace{{Database: cs.sources[i].db}},
		})
	}
	hub := cs.dests[0]
	con := &ocConsolidation{Hub: hub.destURI()}
	for i := 1; i < len(cs.dests); i++ {
		con.Merges = append(con.Merges, ocMerge{
			ID:                "merge-" + cs.dests[i].name,
			Source:            cs.dests[i].destURI(),
			IncludeNamespaces: []ocNamespace{{Database: cs.dests[i].db}},
		})
	}
	cfg.Consolidation = con

	// Disable HTML escaping so the `&` in connection URIs stays readable.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(cfg); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}
