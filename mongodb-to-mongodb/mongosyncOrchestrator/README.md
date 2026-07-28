# mongosyncOrchestrator

Drive many `mongosync` instances from a **single host** using a generic **plan of
steps**. Every operation the tool performs is the same primitive — a mongosync
**job** (source → destination, with optional namespace filter, `preExistingDestinationData`,
metadata cleanup, and verify) — composed into ordered **steps** that run either in
parallel or sequentially.

- **`parallel` step** — launch all jobs at once (each on its own control-API port),
  watch one aggregated progress table until they drain. `commit: hold` leaves them
  running for a coordinated cutover; `commit: auto` commits + stops each when drained.
- **`sequential` step** — run jobs one at a time (fan-in / consolidation): cleanup
  metadata, launch, drain, commit, wait for terminal COMMITTED, stop, verify.

"10→10→1" is just a two-step plan: a parallel/hold **lift** step of 10 jobs, then a
sequential/auto **consolidate** step of 9 `preExisting`+`cleanup` jobs. Nothing about
consolidation is special-cased — it is ordinary configuration, so the tool is reusable
for any topology (1:1, N:N, fan-in, staged waves).

> **Fan-in note.** `mongosync` does not support many-to-one. Sequential steps make it
> safe *for disjoint namespaces only* by running one job at a time with
> `preExistingDestinationData: true` and dropping the `mongosync` metadata databases on
> both ends between jobs (`cleanupMetadataOn`).

The **legacy** `syncs[]` / `consolidation` config is still accepted and transparently
translated into an equivalent plan.

Single static Go binary, standard library only — nothing to download at runtime on a
locked-down host.

## Build

```shell
go build -o bin/mongosyncOrchestrator .
# or use the cross-compiled binaries already in bin/
```

## Usage (generic)

```shell
# Run a step by name (parallel or sequential, per its config).
mongosyncOrchestrator run    --config plan.json --step lift            # parallel/hold
mongosyncOrchestrator commit --config plan.json --step lift            # cutover: commit + canWrite
mongosyncOrchestrator stop   --config plan.json --step lift            # wait COMMITTED, then stop
mongosyncOrchestrator run    --config plan.json --step consolidate      # sequential fan-in

mongosyncOrchestrator run    --config plan.json --step lift --dry-run   # preview a step
mongosyncOrchestrator progress --config plan.json --step lift           # snapshot
mongosyncOrchestrator pause    --config plan.json --step lift           # pause/resume a step
```

`run` executes one step per its `mode`/`commit`. For a `parallel`+`hold` step it drives
to steady state and leaves mongosync running; `commit` then cuts over (commit + wait
`canWrite`) and `stop` waits for terminal `COMMITTED` (index builds) before freeing the
ports. A `sequential` step runs its jobs one at a time end-to-end.

## Usage (legacy — still supported)

```shell
mongosyncOrchestrator sync        --config orchestrator.json            # = run --step <first parallel>
mongosyncOrchestrator commit      --config orchestrator.json --lag 5
mongosyncOrchestrator stop        --config orchestrator.json
mongosyncOrchestrator consolidate --config orchestrator.json --verify   # = run --step <first sequential>
mongosyncOrchestrator sync        --config orchestrator.json --commit    # all-in-one (auto-commit)
```

Flags: `--config` `--step` `--commit` (parallel all-in-one auto-commit) `--force`
(skip readiness/COMMITTED waits) `--verify` (per-job count check) `--embedded-verify`
`--dry-run` `--poll <sec>` `--lag <sec>`.

## Config

Two accepted shapes (see `orchestrator.plan.sample.json` and `orchestrator.sample.json`):

**Generic plan (preferred):**

- `plan.steps[]` — `{ name, mode: parallel|sequential, commit: hold|auto, verify?, jobs[] }`
- `job` — `{ id, source, destination, includeNamespaces?, preExistingDestinationData?,
  cleanupMetadataOn?: [source|destination], embeddedVerify? }`

**Legacy (auto-translated to a plan):**

- `syncs[]` — `{ id, source, destination, includeNamespaces[] }` → a parallel/hold step
- `consolidation` — `{ hub, merges[] { id, source, includeNamespaces[] } }` → a
  sequential/auto step with `preExisting` + `cleanupMetadataOn: [source,destination]`

`basePort` — job *i* in a parallel step uses `basePort + i`; sequential steps reuse
`basePort`. `//` line comments are allowed.

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
