# mongosyncOrchestrator

Drive many `mongosync` instances from a **single migration host** for a two-phase
consolidation migration:

- **Phase 1 — parallel 1:1 lift.** Launch one `mongosync` per source cluster (each
  on its own control-API port), start them together, and watch a single aggregated
  progress table until every job reaches change-event application and drains. Optionally
  auto-commit each job as it becomes ready.
- **Phase 2 — sequential fan-in.** Merge each Phase-1 destination into one **hub**
  cluster. `mongosync` does **not** support many-to-one, so merges run strictly one at
  a time; the tool drops the `mongosync` metadata databases on both ends between merges
  (using `preExistingDestinationData: true` and per-cluster `includeNamespaces`) so
  disjoint namespaces accumulate on the hub. A count check verifies each merge.

Single static Go binary, standard library only — nothing to download at runtime on a
locked-down host.

## Build

```shell
go build -o bin/mongosyncOrchestrator .
# or use the cross-compiled binaries already in bin/
```

## Usage

```shell
# Phase 1 — launch all syncs and run to steady state; without --commit they are
# LEFT RUNNING so all clusters can be cut over together later.
mongosyncOrchestrator sync        --config orchestrator.json
mongosyncOrchestrator sync        --config orchestrator.json --embedded-verify   # + built-in verifier
# Coordinated Phase-1 cutover — commit every running sync together (readiness-gated).
mongosyncOrchestrator commit      --config orchestrator.json --lag 5
mongosyncOrchestrator commit      --config orchestrator.json --force             # commit even if not fully drained

mongosyncOrchestrator consolidate --config orchestrator.json --verify            # Phase 2
mongosyncOrchestrator progress    --config orchestrator.json                     # snapshot
mongosyncOrchestrator pause       --config orchestrator.json                     # pause all syncs
mongosyncOrchestrator resume      --config orchestrator.json                     # resume all syncs
mongosyncOrchestrator sync        --config orchestrator.json --dry-run           # preview

# sync --commit still works as an all-in-one (launch + auto-commit when drained),
# for automated/unattended single-cluster runs.
```

Flags: `--config` `--commit` (Phase 1 all-in-one auto-commit) `--force` (commit even if
not fully drained) `--verify` (Phase 2 count check) `--embedded-verify` (enable
mongosync's built-in verifier per sync) `--dry-run` `--poll <sec>` (progress interval)
`--lag <sec>` (max lag considered drained).

`pause`/`resume` map to mongosync's `/pause` and `/resume` on every configured sync —
useful to relieve source-cluster load or hold all syncs during a maintenance window.
`--embedded-verify` turns on mongosync's own verifier (needs extra oplog/resources).

## Config

See `orchestrator.sample.json`. Shape:

- `syncs[]` — Phase 1 jobs: `{ id, source, destination, includeNamespaces[] }`
- `consolidation` — Phase 2: `{ hub, merges[] { id, source, includeNamespaces[] } }`
- `basePort` — job *i* uses `basePort + i`; consolidation reuses `basePort`.

`//` line comments are allowed in the config file.

## Requirements

- `mongosync` on the host (x86_64 Linux/macOS — no arm64 build is published).
- `mongosh` on PATH (used for metadata cleanup + verification in Phase 2).
- Source and destination must both be replica sets.
- The destination must have at least one user (mongosync enables destination write
  blocking when namespace filtering disables source write blocking).
- Consolidation requires **disjoint** namespaces across the merged clusters.

## Notes / caveats

- Fan-in is outside mongosync's officially supported topology. It is safe **only** for
  disjoint namespaces, one instance writing to the hub at a time, with metadata cleaned
  between runs — all of which this tool enforces. Validated end-to-end against real
  mongosync 1.21.
- Place the migration host on a low-latency path to both ends; host bandwidth/latency,
  not CPU/RAM, is the usual bottleneck when running many syncs at once.
