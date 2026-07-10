# fixIdTypes

Go binary that audits and fixes wrong `_id` BSON types on DocumentDB before migration to MongoDB Atlas.

Three modes — run them in order:

| Mode | What it does | Speed |
|---|---|---|
| `detect` | Scan databases — find collections with mixed `_id` types | Fast — 2 index seeks per collection, parallel |
| `count` | Count documents per `_id` type per namespace | Fast — covered index aggregation |
| `fix` | Three-phase batch fix: Move → Transform → Cleanup | Full collection scan (writes) |

---

## Why Go instead of mongosh

| | mongosh script | Go binary |
|---|---|---|
| Concurrency | Single-threaded | N worker goroutines per phase |
| Progress output | Count-based | Time-based (every 30 s) — output rate doesn't scale with throughput |
| Runtime | Requires mongosh + TLS cert on host | Static binary — no dependencies |
| Overhead | JS interpreter + BSON serialization | Compiled; BSON handled natively by Go driver |
| Throttle | N/A | `throttle_ms` delay between batches per worker |
| Log to file | Requires shell redirect | `log_file` in config — tees to stdout + file |

---

## Files

| File | Description |
|---|---|
| `bin/fixIdTypes-darwin-arm64` | macOS (Apple Silicon) static binary |
| `bin/fixIdTypes-darwin-amd64` | macOS (Intel) static binary |
| `bin/fixIdTypes-linux-amd64` | Linux x86-64 static binary |
| `bin/fixIdTypes-linux-arm64` | Linux ARM64 static binary |
| `bin/fixIdTypes-windows-amd64.exe` | Windows x86-64 static binary |
| `config.sample.json` | Config template (covers all three modes) |
| `main.go` | Source |

---

## Setup

### 1. Copy and edit config

```bash
cp config.sample.json config.json
```

The same `config.json` drives all three modes. Edit the relevant section(s) for what you want to run:

```json
{
  "source_uri": "mongodb://username:password@docdb-cluster.ap-south-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/path/to/ap-south-1-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred",

  "log_file": "fixIdTypes.log",

  "detect": {
    "databases": ["common-warranty-db", "svoi-db"],
    "output_json": "detect_results.json"
  },

  "count": {
    "namespaces": [
      "common-warranty-db.vm_warranty_approval_online",
      "svoi-db.consumer_did_mapping_log"
    ],
    "output_json": "count_results.json"
  },

  "fix": {
    "collections": [
      {
        "namespace": "common-warranty-db.vm_warranty_approval_online",
        "wrong_type": "string",
        "fix_strategy": "string_to_objectid",
        "new_id_on_failure": false
      }
    ],
    "batch_size": 1000,
    "workers": 4,
    "throttle_ms": 0,
    "drop_backup_on_success": false,
    "output_json": "fix_results.json"
  }
}
```

---

## Usage

```
fixIdTypes --mode <detect|count|fix> [--config config.json] [--dry-run=true]
```

### Mode: detect

```bash
# Linux
./bin/fixIdTypes-linux-amd64 --mode detect --config config.json

# Windows (PowerShell)
.\bin\fixIdTypes-windows-amd64.exe --mode detect --config config.json
```

Output: console summary + `detect_results.json` (if `detect.output_json` is set).

### Mode: count

```bash
# Linux
./bin/fixIdTypes-linux-amd64 --mode count --config config.json

# Windows (PowerShell)
.\bin\fixIdTypes-windows-amd64.exe --mode count --config config.json
```

Output: console table (type / count / %) per namespace + `count_results.json`.

### Mode: fix — dry run first

```bash
# Linux
./bin/fixIdTypes-linux-amd64 --mode fix --config config.json --dry-run=true

# Windows (PowerShell)
.\bin\fixIdTypes-windows-amd64.exe --mode fix --config config.json --dry-run=true
```

Review the output. Confirm Phase 1 `Would move` matches Phase 2 `Would insert` (minus expected skips).

### Mode: fix — apply

```bash
# Linux
./bin/fixIdTypes-linux-amd64 --mode fix --config config.json --dry-run=false

# Windows (PowerShell)
.\bin\fixIdTypes-windows-amd64.exe --mode fix --config config.json --dry-run=false
```

---

## Config reference

### Top-level

| Field | Required | Default | Description |
|---|---|---|---|
| `source_uri` | Yes | — | DocumentDB connection string |
| `log_file` | No | `""` | Path to log file. All output is tee'd to stdout **and** this file. Omit to stdout only. |

### `detect` section

| Field | Required | Default | Description |
|---|---|---|---|
| `databases` | No | all user DBs | Database names to scan. Use `[]` to scan all databases on the server. |
| `output_json` | No | `""` | Path for JSON report. Omit to skip. |

### `count` section

| Field | Required | Default | Description |
|---|---|---|---|
| `namespaces` | Yes | — | List of `"database.collection"` namespaces to count. |
| `output_json` | No | `""` | Path for JSON report. Omit to skip. |

### `fix` section

| Field | Required | Default | Description |
|---|---|---|---|
| `collections` | Yes | — | Collections to fix. See [Fix strategies](#fix-strategies). |
| `batch_size` | No | `1000` | Docs per `InsertMany` / `DeleteMany` batch. Lower for very large documents. |
| `workers` | No | `4` | Worker goroutines per phase. More workers = more throughput, more load. |
| `throttle_ms` | No | `0` | Sleep N ms per worker after each batch. Increase to reduce cluster load at the cost of throughput. `0` = no throttle. |
| `drop_backup_on_success` | No | `false` | Drop `<collection>_id_fix_backup` after a clean Phase 2. Keep `false` on first run. |
| `output_json` | No | `""` | Path for JSON summary. Omit to skip. |

---

## Fix strategies

| `fix_strategy` | Wrong `_id` | Correct `_id` |
|---|---|---|
| `string_to_objectid` | `"6374c4debb4e876fe62a36af"` (string) | `ObjectId("6374c4debb4e876fe62a36af")` |
| `nested_objectid` | `{ "_id": ObjectId("...") }` (nested doc) | `ObjectId("...")` (inner value) |
| `new_objectid` | anything | New random `ObjectId` |

**`new_id_on_failure`**: if `true`, falls back to a new random ObjectId when the configured strategy cannot derive a valid ObjectId from a specific document. Useful for collections where most docs have a recoverable wrong type but a small number have an unrecognisable structure.

---

## How fix works — three phases

| Phase | What happens |
|---|---|
| 1 — Move | Finds all wrong-type `_id` docs, batch-inserts them into `<collection>_id_fix_backup`, then batch-deletes them from source. Source is clean after this phase. |
| 2 — Transform | Reads backup, applies fix strategy to each doc, batch-inserts corrected docs back into source. |
| 3 — Cleanup | Drops backup (`drop_backup_on_success: true`) or retains it for verification. |

Each phase uses a reader goroutine → buffered channel → N worker goroutines. Workers run `InsertMany` / `DeleteMany` concurrently for maximum throughput.

**Restart-safe**: if interrupted and re-run, duplicate key errors in Phase 1 (already in backup) and Phase 2 (already in source) are handled gracefully.

---

## Throttling — reducing cluster load

If the DocumentDB cluster is under high load during the fix, increase `throttle_ms`:

```json
"throttle_ms": 100
```

With `batch_size: 1000`, `workers: 4`, and `throttle_ms: 100`:
- Each worker sleeps 100 ms after processing each 1,000-doc batch
- Effective cap: ~40,000 docs/sec from throttle alone (4 workers × 1,000 docs ÷ 0.1 s)

The tradeoff is linear: doubling `throttle_ms` halves throughput. Start at `0` (no throttle) and add delay only if you observe degradation in application latency on the cluster.

---

## JSON exports

### `detect_results.json`

```json
{
  "timestamp": "2026-06-23T10:00:00Z",
  "total_scanned": 20,
  "total_mixed": 2,
  "total_clean": 18,
  "total_empty": 0,
  "total_error": 0,
  "results": [
    { "namespace": "common-warranty-db.vm_warranty_approval_online", "status": "mixed", "min_type": "string", "max_type": "objectId" },
    { "namespace": "common-warranty-db.alerts", "status": "clean", "uniform_type": "objectId" }
  ]
}
```

### `count_results.json`

```json
{
  "timestamp": "2026-06-23T10:00:00Z",
  "results": [
    {
      "namespace": "common-warranty-db.vm_warranty_approval_online",
      "status": "mixed",
      "types": [
        { "bson_type": "objectId", "count": 237804, "percent": 97.69 },
        { "bson_type": "string",   "count": 5626,   "percent": 2.31  }
      ],
      "total": 243430
    }
  ]
}
```

### `fix_results.json`

```json
{
  "timestamp": "2026-06-23T10:01:28Z",
  "mode": "LIVE (changes WILL be applied)",
  "elapsed_seconds": 88.4,
  "results": [
    {
      "namespace": "common-warranty-db.vm_warranty_approval_online",
      "phase1": { "moved": 5626, "dupes": 0, "errors": 0 },
      "phase2": { "inserted": 5626, "skipped": 0, "errors": 0 }
    }
  ]
}
```

---

## Connection string

DocumentDB with TLS (Linux):
```
mongodb://user:pass@cluster.ap-south-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/path/to/ap-south-1-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred
```

DocumentDB with TLS (Windows — forward slashes in path):
```
mongodb://user:pass@cluster.ap-south-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=C:/certs/ap-south-1-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred
```

The binary sets `retryWrites=false` automatically — required for DocumentDB compatibility.

---

## Build from source

Requires Go 1.22+.

```bash
go mod tidy

# Current platform
go build -o fixIdTypes .

# Cross-compile all common platforms into bin/
CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -ldflags="-s -w" -o bin/fixIdTypes-darwin-arm64 .
CGO_ENABLED=0 GOOS=darwin  GOARCH=amd64 go build -ldflags="-s -w" -o bin/fixIdTypes-darwin-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -ldflags="-s -w" -o bin/fixIdTypes-linux-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -ldflags="-s -w" -o bin/fixIdTypes-linux-arm64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o bin/fixIdTypes-windows-amd64.exe .
```

---

## Notes

- Keep `config.json` out of version control — it contains credentials. Add it to `.gitignore`.
- Always run `--mode fix --dry-run=true` first. Confirm Phase 1 and Phase 2 counts match before applying.
- Use `detect` and `count` modes to audit before configuring the `fix` section.
