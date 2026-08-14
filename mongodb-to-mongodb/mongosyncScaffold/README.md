# mongosyncScaffold

A **review-first** migration driver. Instead of automating the migration, it *generates*
the artifacts — per-mongosync config JSONs, `/start` request bodies, and a shell script
for every stage — that the operator **reviews and runs by hand**. Nothing in this tool
connects to a cluster; it only writes files. This is the production-comfort alternative
to the fully automated `mongosyncOrchestrator` (both read the **same** plan config).

## Build

```shell
go build -o bin/mongosyncScaffold .
```

## Generate

```shell
# Everything for every step:
mongosyncScaffold gen-all --config migration.json --out ./migration

# Or stage by stage (review between stages):
mongosyncScaffold gen-configs  --config migration.json --out ./migration
mongosyncScaffold gen-start    --config migration.json --out ./migration
mongosyncScaffold gen-progress --config migration.json --out ./migration
mongosyncScaffold gen-commit   --config migration.json --out ./migration
mongosyncScaffold gen-stop     --config migration.json --out ./migration
# extras: gen-verify, gen-cleanup, gen-pause-resume, gen-runbook

# Limit to one step:
mongosyncScaffold gen-all --config migration.json --out ./migration --step lift
```

The config is the same plan file the orchestrator uses (`plan.steps[]` of jobs, or the
legacy `syncs[]`/`consolidation` shape). See `scaffold.plan.sample.json` for the
annotated plan format, or `scaffold.sample.json` for the legacy format.

## What it writes (per step, under `<out>/<step>/`)

| Artifact | Purpose |
| :---- | :---- |
| `configs/<id>.mongosync.json` | mongosync process config (cluster0/cluster1/port/log). **Edit connection strings here.** |
| `start-bodies/<id>.start.json` | the `/start` API body (`includeNamespaces`, `preExistingDestinationData`, `verification`). **Review before starting.** |
| `env.sh` | sourced by every script; instance ids + ports; derives URIs from the configs and namespaces from the start bodies |
| `start-processes.sh` | launch mongosync for the target(s), wait for `IDLE` |
| `start-api.sh` | POST each reviewed start body |
| `progress.sh` | poll `/progress` for all target(s) every 10s until Ctrl-C |
| `commit.sh` | POST `/commit` |
| `stop.sh` | wait for terminal `COMMITTED`, then stop (set `WAIT_COMMITTED=0` to skip) |
| `verify.sh` | count-compare source vs destination per database (whole cluster if unfiltered) |
| `cleanup-metadata.sh` | drop the mongosync reserved DBs on both ends (needed before fan-in merges) |
| `pause.sh` / `resume.sh` | mongosync `/pause` and `/resume` |
| `RUNBOOK.md` | the exact script order for the step |

Every script acts on **all** instances of the step when run with no argument, or on a
single instance when you pass its id (`./start-api.sh cluster03`).

## Parallel vs sequential steps

- **`parallel`** (the 1:1 lift): run each script once; it acts on all instances (distinct
  ports `basePort+i`). Follow `RUNBOOK.md`: start → review → start-api → progress →
  *cutover window* → commit → stop → verify.
- **`sequential`** (the fan-in consolidation): the jobs share `basePort` and only one may
  write to the hub at a time. Run every script with a single instance id, one instance at
  a time, per `RUNBOOK.md` (cleanup → start → start-api → progress → commit → stop →
  verify → repoint app → next instance).

## Requirements on the migration host

`mongosync`, `mongosh`, `curl`, `jq` on `PATH` (x86_64 Linux — mongosync ships x86_64 only).
The generated scripts are `bash`.
