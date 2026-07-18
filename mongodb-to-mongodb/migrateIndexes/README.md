# migrateIndexes (multi-source)

Go tool that migrates indexes from **many** self-managed MongoDB source clusters
to a **single** MongoDB Atlas destination, **merging** same-namespace collections.
It is the multi-source sibling of `docdb-to-mongodb/indexes/migrateIndexes`,
built for the 10-cluster → 1-Atlas consolidation where same-name collections
combine and therefore their indexes combine too.

Because collections merge, two sources can define the **same `db.collection`
index name with different keys/options**, or the **same key pattern under
different names** — definitions MongoDB will reject on the merged target. This
tool reads every source, **dedupes** identical index specs, and **detects and
reports conflicts** instead of blindly creating.

Driven entirely by a JSON config file. Runs `createIndex` / `collMod` operations
in parallel across a worker pool and prints active index builds every 5 s.

---

## Prebuilt binaries

In `bin/`:

| Platform | Binary |
| --- | --- |
| macOS (Apple Silicon) | `bin/migrateIndexes-darwin-arm64` |
| macOS (Intel) | `bin/migrateIndexes-darwin-amd64` |
| Linux (x86_64) | `bin/migrateIndexes-linux-amd64` |
| Linux (ARM64) | `bin/migrateIndexes-linux-arm64` |
| Windows (x86_64) | `bin/migrateIndexes-windows-amd64.exe` |

---

## Modes

| Mode | When | What it does |
| --- | --- | --- |
| `create` | Pre-cutover | Reads every non-`_id_` index from **all sources**, merges them, and creates each distinct index on the **destination** with TTL neutralized to `expireAfterSeconds = MAX_INT` and unique indexes created as `unique: false` (so the Kafka CDC sync can't hit uniqueness violations or premature expiry). Conflicts are **reported and skipped**; the run exits `3`. |
| `verify` | Any time | Read-only. Compares the merged source index set vs the destination and reports per-index status. Unresolved source conflicts also fail verify. |
| `rectify` | Post-cutover | Phase 1 (parallel): `prepareUnique` on formerly-unique indexes + restore original TTL. Phase 2 (parallel): `collMod unique:true`. Phases run strictly in order. |

Recommended flow, wired into the Kafka migration:

```
(pre)  create   →  verify (expect INCOMPLETE)          ← before starting the sink backfill
(sync) … Kafka source+sink backfill + CDC run …
(cut)  cutover apps to Atlas, freeze source writes
(post) rectify  →  verify (expect PASS)                ← enforce unique + real TTL
```

Neutralizing unique/TTL during the sync is what makes it safe to run indexes
**before** the CDC pipeline: replaces from CDC can't trip a unique constraint
mid-sync, and TTL won't delete freshly-synced docs. `rectify` restores the real
constraints once writes are frozen and data is verified.

> Note on `_id` collisions: this tool only handles **secondary** indexes. The
> cross-source `_id`-collision detection for the merge is handled by the Kafka
> **sink** (insert-only strategy → DLQ during backfill), not here.

---

## Setup

```bash
cp config.sample.json config.json
```

```json
{
  "sources": [
    { "label": "cluster01", "uri": "mongodb://user:pass@rs01-host:27017/?replicaSet=rs0&readPreference=secondary&readPreferenceTags=nodeType:hidden" },
    { "label": "cluster02", "uri": "mongodb://user:pass@rs02-host:27017/?replicaSet=rs0&readPreference=secondary&readPreferenceTags=nodeType:hidden" }
  ],
  "destination_uri": "mongodb+srv://user:pass@atlas-gcp.mongodb.net/?retryWrites=true&w=majority",
  "databases": [],
  "log_file": ""
}
```

| Field | Required | Description |
| --- | --- | --- |
| `sources` | Yes | One entry per self-managed source cluster. `label` is used in reports (auto-filled `sourceN` if omitted); `uri` is the connection string. Point reads at hidden nodes (`readPreference=secondary&readPreferenceTags=nodeType:hidden`) to avoid loading production primaries. |
| `destination_uri` | Yes | The single Atlas target. |
| `databases` | No | Databases to process. `[]` = auto-discover all non-system databases on each source. |
| `log_file` | No | Optional path; tees all output to the file **and** the console. |

---

## Usage

```bash
# Pre-cutover: dry run, then apply
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=true
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=false

# Any time: compare merged sources vs destination
./bin/migrateIndexes-linux-amd64 --config config.json --mode=verify

# Post-cutover: dry run, then apply
./bin/migrateIndexes-linux-amd64 --config config.json --mode=rectify --dry-run=true
./bin/migrateIndexes-linux-amd64 --config config.json --mode=rectify --dry-run=false
```

**Windows (PowerShell):** replace with `.\bin\migrateIndexes-windows-amd64.exe`.

---

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--config` | `config.json` | Path to the JSON config file. |
| `--mode` | `create` | `create` \| `verify` \| `rectify`. |
| `--dry-run` | `true` | Preview without writing. Set `false` to apply. Ignored by `verify`. |
| `--concurrency` | `8` | Index operations run in parallel on the destination (also sizes the connection pool / `maxConnecting`). |

---

## Conflict handling

During merge, two kinds of conflict are reported and **skipped** (create exits `3`):

| Kind | Meaning |
| --- | --- |
| `NAME` | Same `db.collection.indexName` across sources, but different keys / options / unique / TTL. |
| `KEYSPEC` | Same `db.collection` key pattern under **different** index names (MongoDB rejects this on one collection). |

Non-conflicting indexes are still created, so a run makes maximum safe progress.
Resolve each conflict (align the definition across sources, or decide which one
the merged collection should keep), then re-run `create`. `verify` also fails
(`exit 3`) while conflicts remain unresolved.

---

## Verify exit codes

| Code | Meaning |
| --- | --- |
| `0` | PASS — every merged index matches (keys, options, unique, TTL); no conflicts. |
| `4` | INCOMPLETE — present with matching keys, but still neutralized (`unique:false` / TTL `MAX_INT`). Run `--mode=rectify`. |
| `3` | FAIL — `MISMATCH` / `MISSING` / `EXTRA`, or unresolved source conflicts. |
| `1` | Connection / runtime / config error. |

---

## Build from source

Requires Go 1.21+.

```bash
go mod tidy
go build -o bin/migrateIndexes-darwin-arm64 .

# Cross-compile
CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-linux-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -ldflags="-s -w" -o bin/migrateIndexes-linux-arm64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-windows-amd64.exe .
CGO_ENABLED=0 GOOS=darwin  GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-darwin-amd64 .
```

---

## Notes

- The `_id_` index is always skipped.
- System databases (`admin`, `config`, `local`) are skipped.
- Identical index definitions across sources are created **once** (deduped); the report lists all contributing sources.
