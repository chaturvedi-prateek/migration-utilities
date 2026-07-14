# migrateIndexes

Go tool that migrates indexes from a source (Amazon DocumentDB) to a destination
(MongoDB Atlas) and validates them for cutover. Driven entirely by a JSON config
file — no code changes needed to target different clusters or databases.

It runs `createIndex` / `collMod` operations in parallel across a worker pool and
prints any active index builds on the destination every 5 seconds.

---

## Prebuilt binaries (run without building)

Prebuilt binaries are in `bin/`. Pick the one for your platform:

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
| `create` | Pre-cutover | Reads every non-`_id_` index from the **source** and creates it on the **destination** with TTL indexes neutralized to `expireAfterSeconds = MAX_INT` and unique indexes created as `unique: false` (so the sync can't hit uniqueness violations or premature expiry). |
| `verify` | Any time | Read-only. Compares source vs destination indexes and reports per-index status. Writes nothing. |
| `rectify` | Post-cutover | Phase 1 (parallel): `prepareUnique` on formerly-unique indexes + restore original TTL values. Phase 2 (parallel): `collMod unique:true`. Phases run strictly in order. |

Recommended flow:

```
create  →  verify (expect INCOMPLETE)  →  rectify  →  verify (expect PASS)
```

---

## Setup

### 1. Create your config file

```bash
cp config.sample.json config.json
```

Edit `config.json`:

```json
{
  "source_uri": "mongodb://user:pass@docdb-endpoint:27017/?tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred",
  "destination_uri": "mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority",
  "databases": [],
  "log_file": ""
}
```

| Field | Required | Description |
| --- | --- | --- |
| `source_uri` | Yes | Connection string for the source (DocumentDB). |
| `destination_uri` | Yes | Connection string for the destination (MongoDB Atlas). |
| `databases` | No | Databases to process. Use `[]` to auto-discover all non-system databases. |
| `log_file` | No | Optional path. When set, all output is tee'd to this file **and** the console. Omit or leave `""` for console only. |

---

## Usage

### Pre-cutover — create indexes

```bash
# Dry run first — prints what would be created
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=true

# Apply
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=false
```

### Verify — compare source vs destination

```bash
./bin/migrateIndexes-linux-amd64 --config config.json --mode=verify
```

### Post-cutover — rectify

```bash
# Dry run first
./bin/migrateIndexes-linux-amd64 --config config.json --mode=rectify --dry-run=true

# Apply
./bin/migrateIndexes-linux-amd64 --config config.json --mode=rectify --dry-run=false
```

**Windows (PowerShell):** replace `./bin/migrateIndexes-linux-amd64` with
`.\bin\migrateIndexes-windows-amd64.exe`.

---

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `--config` | `config.json` | Path to the JSON config file. |
| `--mode` | `create` | `create` \| `verify` \| `rectify`. |
| `--dry-run` | `true` | Preview without writing. Set `false` to apply. Ignored by `verify` (always read-only). |
| `--concurrency` | `8` | Number of index operations run in parallel on the destination (also sizes the connection pool). See [Tuning parallelism](#tuning-parallelism). |

---

## Tuning parallelism

`--concurrency` controls how many `createIndex` / `collMod` operations run at
once. The tool sizes the destination connection pool from this value, so all
workers can open connections simultaneously (it raises the driver's
`maxConnecting`, which otherwise defaults to **2** — the reason a run can appear
"stuck at 2 indexes at a time").

```bash
# More parallelism (large destination, e.g. Atlas M40+)
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=false --concurrency=16

# Less parallelism (small tier like M10/M20, or if you hit connection/timeout errors)
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=false --concurrency=2
```

Notes:
- MongoDB serializes multiple index builds **on the same collection** server-side,
  so indexes on one collection won't parallelize regardless of `--concurrency`;
  the setting speeds up builds spread **across many collections**.
- Higher concurrency increases load on the destination primary. If you see
  connection-pool / server-selection timeouts, lower `--concurrency`.
- The connection pool is sized to `--concurrency + 5` (headroom for the progress
  monitor); server-selection timeout is 5 minutes and transient errors are
  retried automatically.

---

## Verify output & exit codes

`verify` prints a per-index status table and a summary:

| Status | Meaning |
| --- | --- |
| `OK` | Present on destination with matching keys, options, `unique`, and TTL. |
| `PENDING` | Present with matching keys, but still in the migration-neutralized state (`unique:false` / TTL `MAX_INT`) — created but not yet rectified. |
| `MISMATCH` | Keys or options differ, or `unique`/TTL differ in an unexpected way. |
| `MISSING` | On source, not on destination. |
| `EXTRA` | On destination, not on source. |

Exit codes (useful as a cutover gate in CI):

| Code | Meaning |
| --- | --- |
| `0` | PASS — every index matches (keys, options, unique, TTL). |
| `4` | INCOMPLETE — all present with matching keys, but some still neutralized. Run `--mode=rectify`, then re-verify. |
| `3` | FAIL — discrepancies found (`MISMATCH` / `MISSING` / `EXTRA`). |
| `1` | Connection / runtime / config error. |

Example:

```
  STATUS    NAMESPACE.INDEX                                          DETAIL
  ====================================================================================================
  OK        mydb.orders.category_idx
  PENDING   mydb.orders.created_ttl                    TTL still MAX_INT (target 3600s)
  MISMATCH  mydb.orders.region_idx                     keys src={region:1} dst={region:-1}
  MISSING   mydb.orders.audit_idx                      not present on destination

  ── verify summary ──
  OK: 1  |  PENDING: 1  |  MISMATCH: 1  |  MISSING: 1  |  EXTRA: 0

  RESULT: FAIL — discrepancies found (see MISMATCH/MISSING/EXTRA above).
```

---

## Logging to a file

Set `log_file` in the config to tee all output to a file (the console still
shows everything):

```json
"log_file": "indexes-verify.log"
```

Or capture it from the shell. Note that piping through `tee` makes the shell's
`$?` reflect `tee`'s exit code, not the tool's — capture it explicitly if you
need the exit code for a cutover gate:

```bash
./bin/migrateIndexes-linux-amd64 --config config.json --mode=verify | tee indexes-verify.log
code=${PIPESTATUS[0]}   # zsh: ${pipestatus[1]}
echo "verify exit code: $code"
```

---

## Build from source

Requires Go 1.21+.

```bash
go mod tidy
go build -o migrateIndexes .

# Cross-compile all common platforms into bin/
CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -ldflags="-s -w" -o bin/migrateIndexes-darwin-arm64 .
CGO_ENABLED=0 GOOS=darwin  GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-darwin-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-linux-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -ldflags="-s -w" -o bin/migrateIndexes-linux-arm64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-windows-amd64.exe .
```

---

## Notes

- The `_id_` index is always skipped.
- System databases (`admin`, `config`, `local`) are skipped.
- `listCollections` is issued without a `type` filter because Amazon DocumentDB
  rejects that filter; views and `system.*` collections are excluded client-side.
- Change streams are unrelated to this tool; see `checkChangeStreams` for that.
</content>
</invoke>
