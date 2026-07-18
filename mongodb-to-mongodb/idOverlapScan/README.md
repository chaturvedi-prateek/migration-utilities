# idOverlapScan

Go tool that **pre-flight checks for `_id` collisions** before a many-clusters →
one-Atlas merge. When same-name collections from several source clusters merge
into one target collection, two clusters can hold the same `_id`. This tool
reports, per merged namespace (a `db.collection` present in **≥2** clusters),
whether their `_id`s overlap — so you decide handling **before** starting the
Kafka backfill, instead of discovering it only via the sink DLQ.

Read-only. Single static binary — nothing to download at the customer site.

---

## Prebuilt binaries

In `bin/`:

| Platform | Binary |
| --- | --- |
| macOS (Apple Silicon) | `bin/idOverlapScan-darwin-arm64` |
| macOS (Intel) | `bin/idOverlapScan-darwin-amd64` |
| Linux (x86_64) | `bin/idOverlapScan-linux-amd64` |
| Linux (ARM64) | `bin/idOverlapScan-linux-arm64` |
| Windows (x86_64) | `bin/idOverlapScan-windows-amd64.exe` |

---

## Modes

| Mode | Speed | What it does |
| --- | --- | --- |
| `range` (default) | Fast (2 indexed queries/cluster) | Per cluster: `count` + `min` + `max` `_id`. Flags namespaces where two clusters' `[min,max]` `_id` intervals **intersect**. An intersecting interval is a *candidate* for overlap, not proof (e.g. `[1..5]` and `[4..8]` intersect). |
| `exact` | Thorough, streaming | Streams `_id` from every cluster in sorted order and does a **k-way merge** to find the **actual duplicate `_id`s** shared across clusters. Memory stays O(k) — one `_id` per cursor — so it scales to large collections. |

Recommended flow: run `range` first (cheap) to find candidates, then `exact` to
confirm and list the real duplicates.

---

## Output & resumability

- **Findings go to a results log file** (`--out`), not the shell. The console
  shows only per-namespace progress and the final summary; the full
  per-namespace blocks (including sample colliding `_id`s) are written to the
  file. Default path: `config.log_file`, else `overlap-report.<mode>.log`.
- **Resumable.** Progress is checkpointed to a JSON file (`--checkpoint`,
  default `<out>.checkpoint.json`) written atomically:
  - after **every namespace** (both modes), and
  - **within** a namespace in `exact` mode — periodically persisting an `_id`
    **watermark** (every ~100k merged `_id`s / ≥5 s).

  If the run is killed or dies, just re-run the same command: finished
  namespaces are skipped, and a half-scanned `exact` namespace continues from
  its watermark (cursors resume at `_id > watermark`) instead of restarting.
  The results file is appended to on resume (a `# resumed …` marker is added).
  Use `--restart` to ignore an existing checkpoint and start fresh.

  A checkpoint records its `mode`; resuming with a different `--mode` is
  rejected (use `--restart` or a separate `--checkpoint`).

---

## Setup

```bash
cp config.sample.json config.json
```

```json
{
  "sources": [
    { "label": "cluster01", "uri": "mongodb://user:pass@rs01-a:27017/?replicaSet=rs01&readPreference=secondary&readPreferenceTags=nodeType:hidden", "databases": ["db_a", "db_b"] },
    { "label": "cluster02", "uri": "mongodb://user:pass@rs02-a:27017/?replicaSet=rs02&readPreference=secondary&readPreferenceTags=nodeType:hidden", "databases": ["db_a", "db_c"] }
  ],
  "log_file": ""
}
```

| Field | Required | Description |
| --- | --- | --- |
| `sources` | Yes | One entry per source cluster (`label`, `uri`). Point reads at **hidden nodes** so the scan never touches production primaries. |
| `sources[].databases` | No | Limit which DBs are scanned. `[]` = all non-system DBs on that cluster. |
| `log_file` | No | Optional path; tees all output to the file **and** the console. |

Same `sources` shape as `../kafkaConnectors/sources.json` and
`../migrateIndexes` — reuse the inventory.

---

## Usage

```bash
# Fast candidate pass (findings -> overlap-report.range.log)
./bin/idOverlapScan-linux-amd64 --config config.json --mode=range

# Confirm real duplicate _ids (findings -> results.log; resumable)
./bin/idOverlapScan-linux-amd64 --config config.json --mode=exact --sample=50 --out results.log

# If it was killed, just run the SAME command again — it resumes from the checkpoint.
./bin/idOverlapScan-linux-amd64 --config config.json --mode=exact --sample=50 --out results.log

# Force a clean start, ignoring any checkpoint
./bin/idOverlapScan-linux-amd64 --config config.json --mode=exact --out results.log --restart
```

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--config` | `config.json` | Path to the JSON config file. |
| `--mode` | `range` | `range` \| `exact`. |
| `--sample` | `20` | Max duplicate `_id`s recorded per namespace (exact mode). |
| `--concurrency` | `4` | Namespaces scanned in parallel. |
| `--out` | `config.log_file` else `overlap-report.<mode>.log` | Results log file (findings go here, not the shell). |
| `--checkpoint` | `<out>.checkpoint.json` | Resume checkpoint file. |
| `--restart` | `false` | Ignore any existing checkpoint and start fresh. |

---

## Exit codes (usable as a cutover gate)

| Code | Meaning |
| --- | --- |
| `0` | PASS — no `_id` overlap across merged namespaces. |
| `5` | OVERLAP — `range`: intersecting intervals; `exact`: real shared `_id`s. Resolve before backfill. |
| `1` | Connection / runtime / config error. |

---

## Example output (exact)

```
  [OK      ] db_a.oid       (cluster01,cluster02) — no shared _ids
  [OVERLAP ] db_a.orders    (cluster01,cluster02) — 2 shared _id(s)
       4 in [cluster01,cluster02]
       5 in [cluster01,cluster02]
  [OK      ] db_a.disjoint  (cluster01,cluster02) — no shared _ids

  RESULT: OVERLAP found — real duplicate _ids exist across clusters.
```

---

## How it fits the migration

1. **idOverlapScan** (`exact`) → know the collision surface up front.
2. Resolve overlaps (remap `_id`, keep newest, drop, or split the namespace).
3. Start the Kafka backfill (`../kafkaConnectors`); the insert-only collision
   sink + DLQ remains the runtime safety net for anything missed.
4. Create indexes with `../migrateIndexes`.

---

## Notes

- Read-only: only `listDatabases`, `listCollections`, `count`, and `_id`-only
  `find` (indexed) are issued.
- `_id` comparison covers the common BSON `_id` types (numbers, string,
  ObjectId, binary/UUID, bool, date); mixed-type `_id`s fall back to BSON
  type-order then canonical string.
- System databases (`admin`, `config`, `local`) are skipped.

---

## Build from source

Requires Go 1.21+.

```bash
go build -o bin/idOverlapScan-darwin-arm64 .
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/idOverlapScan-linux-amd64 .
# ... other GOOS/GOARCH as needed
```
