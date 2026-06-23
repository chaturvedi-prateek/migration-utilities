# copy-missing-docs

Syncs missing documents from a source (AWS DocumentDB) to a destination (MongoDB Atlas) by comparing `_id` sets per collection. Also removes documents from the destination that were deleted from the source.

Driven entirely by a JSON config file — no code changes needed to target different collections or clusters.

---

## Files

| File | Description |
|---|---|
| `copy-missing-docs-linux-amd64` | Linux x86-64 binary |
| `copy-missing-docs-windows-amd64.exe` | Windows x86-64 binary |
| `config.sample.json` | Config template — copy and fill in your values |
| `main.go` | Source code |

No installation required. No Python. No pip. No runtime dependencies.

---

## Setup

### 1. Create your config file

```bash
cp config.sample.json config.json
```

Edit `config.json`:

```json
{
  "source_uri": "mongodb://username:password@docdb-endpoint:27017/?tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred",
  "destination_uri": "mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority",
  "namespaces": [
    "database1.collection1",
    "database1.collection2",
    "database2.collection1"
  ]
}
```

Namespaces must be in `database.collection` format. Add as many as needed across any number of databases.

### 2. Dry run first

Always do a dry run before applying changes. Output is printed to the terminal and written to a timestamped log file simultaneously.

**Linux:**
```bash
chmod +x copy-missing-docs-linux-amd64
./copy-missing-docs-linux-amd64 --config config.json 2>&1 | tee copy-missing-docs-dryrun-$(date +%Y%m%d-%H%M%S).log
```

**Windows (PowerShell):**
```powershell
.\copy-missing-docs-windows-amd64.exe --config config.json 2>&1 | Tee-Object -FilePath copy-missing-docs-dryrun.log
```

**Windows (CMD):**
```cmd
copy-missing-docs-windows-amd64.exe --config config.json > copy-missing-docs-dryrun.log 2>&1
type copy-missing-docs-dryrun.log
```

Review the output. Confirm INSERT and DELETE counts match expectations before applying.

### 3. Apply changes

**Linux:**
```bash
./copy-missing-docs-linux-amd64 --config config.json --dry-run=false 2>&1 | tee copy-missing-docs-live-$(date +%Y%m%d-%H%M%S).log
```

**Windows (PowerShell):**
```powershell
.\copy-missing-docs-windows-amd64.exe --config config.json --dry-run=false 2>&1 | Tee-Object -FilePath copy-missing-docs-live.log
```

**Windows (CMD):**
```cmd
copy-missing-docs-windows-amd64.exe --config config.json --dry-run=false > copy-missing-docs-live.log 2>&1
type copy-missing-docs-live.log
```

---

## Flags

| Flag | Default | Description |
|---|---|---|
| `--config` | `config.json` | Path to the JSON config file |
| `--dry-run` | `true` | Preview changes without writing anything. Set to `false` to apply. |

---

## Config File Reference

| Field | Required | Description |
|---|---|---|
| `source_uri` | Yes | Connection string for the source (DocumentDB) |
| `destination_uri` | Yes | Connection string for the destination (MongoDB Atlas) |
| `namespaces` | Yes | List of `"database.collection"` namespaces to sync |

### Connection string examples

**DocumentDB with TLS — Linux:**
```
mongodb://user:pass@docdb-cluster.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred
```

**DocumentDB with TLS — Windows (use forward slashes in path):**
```
mongodb://user:pass@docdb-cluster.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=C:/certs/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred
```

**MongoDB Atlas:**
```
mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
```

---

## Sample Output

```
============================================================
  copy-missing-docs  |  DRY RUN — no changes will be written
  Config: config.json  |  Namespaces: 3
============================================================

[DRY RUN] ── database1.collection1 ──
  Fetching _ids from source... 8155 docs
  Fetching _ids from destination... 8031 docs
  To INSERT:        124 docs
  To DELETE:          0 docs
  [DRY RUN] Would INSERT _id=ObjectID("6a1f3c...")
  [DRY RUN] Would INSERT _id=ObjectID("6a1f3d...")
  [DRY RUN] Would INSERT _id=ObjectID("6a1f3e...")
  [DRY RUN] ... and 121 more
  Would INSERT 124 | DELETE 0 | Errors 0

[DRY RUN] ── database2.collection1 ──
  Fetching _ids from source... 0 docs
  Fetching _ids from destination... 1 docs
  To INSERT:          0 docs
  To DELETE:          1 docs
  WARNING: 1 doc(s) exist in destination but NOT in source
    Extra _id=abc123...
  [DRY RUN] Would DELETE 1 doc(s) from destination
  Would INSERT 0 | DELETE 1 | Errors 0

============================================================
  TOTAL  INSERT: 124  |  DELETE: 1  |  Errors: 0
============================================================

  Re-run with --dry-run=false to apply changes.
```

---

## Build from Source

Requires Go 1.21+.

```bash
# Download dependencies
go mod tidy

# Build for current platform
go build -o copy-missing-docs .

# Cross-compile for Linux (run from macOS or Windows)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o copy-missing-docs-linux-amd64 .

# Cross-compile for Windows (run from macOS or Linux)
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o copy-missing-docs-windows-amd64.exe .
```

---

## Notes

- Matching is by `_id` only. Documents that exist on both sides but differ in field values (missed updates) are not detected or fixed. Run `dsync --verify` after this tool to catch those.
- Namespaces are processed sequentially in the order listed in the config file.
- Keep `config.json` out of version control — it contains database credentials. Add it to `.gitignore`.
