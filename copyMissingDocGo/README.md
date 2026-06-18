# copy-missing-docs

Copies documents that exist in the source (AWS DocumentDB) but are missing from the destination (MongoDB Atlas), and deletes documents from the destination that have been deleted from the source.

Driven entirely by a JSON config file — no code changes needed to target different collections or clusters.

## Binaries

| File | Platform |
|---|---|
| `copy-missing-docs-linux-amd64` | Linux x86-64 (EC2, Ubuntu, RHEL, Amazon Linux) |
| `copy-missing-docs-windows-amd64.exe` | Windows x86-64 |

No installation required. No Python. No pip. No runtime dependencies.

---

## Quick Start

### 1. Create a config file

Copy the sample and fill in your values:

```bash
cp config.sample.json config.json
```

Edit `config.json`:

```json
{
  "source_uri": "mongodb://username:password@docdb-endpoint:27017/?tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred",
  "destination_uri": "mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority",
  "namespaces": [
    "db1.collection1",
    "db2.collection2",
    "db3.collection3"
  ]
}
```

Namespaces must be in `database.collection` format. Add as many as needed.

### 2. Dry run (always run this first)

**Linux:**
```bash
chmod +x copy-missing-docs-linux-amd64
./copy-missing-docs-linux-amd64 --config config.json 2>&1 | tee copy-missing-docs-dryrun-$(date +%Y%m%d-%H%M%S).log
```

**Windows (CMD):**
```cmd
copy-missing-docs-windows-amd64.exe --config config.json > copy-missing-docs-dryrun.log 2>&1
type copy-missing-docs-dryrun.log
```

**Windows (PowerShell):**
```powershell
.\copy-missing-docs-windows-amd64.exe --config config.json 2>&1 | Tee-Object -FilePath copy-missing-docs-dryrun.log
```

Review the log output. Confirm INSERT and DELETE counts are as expected before proceeding.

### 3. Apply changes

**Linux:**
```bash
./copy-missing-docs-linux-amd64 --config config.json --dry-run=false 2>&1 | tee copy-missing-docs-live-$(date +%Y%m%d-%H%M%S).log
```

**Windows (CMD):**
```cmd
copy-missing-docs-windows-amd64.exe --config config.json --dry-run=false > copy-missing-docs-live.log 2>&1
type copy-missing-docs-live.log
```

**Windows (PowerShell):**
```powershell
.\copy-missing-docs-windows-amd64.exe --config config.json --dry-run=false 2>&1 | Tee-Object -FilePath copy-missing-docs-live.log
```

---

## Flags

| Flag | Default | Description |
|---|---|---|
| `--config` | `config.json` | Path to the JSON config file |
| `--dry-run` | `true` | Print what would be done without writing. Set to `false` to apply. |

---

## Config File Reference

| Field | Required | Description |
|---|---|---|
| `source_uri` | Yes | MongoDB connection string for the source (DocumentDB) |
| `destination_uri` | Yes | MongoDB connection string for the destination (Atlas) |
| `namespaces` | Yes | Array of `"database.collection"` strings to sync |

### DocumentDB TLS (Linux)
```json
"source_uri": "mongodb://user:pass@docdb-endpoint:27017/?tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred"
```

### DocumentDB TLS (Windows — use forward slashes or escaped backslashes)
```json
"source_uri": "mongodb://user:pass@docdb-endpoint:27017/?tls=true&tlsCAFile=C:/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred"
```

### MongoDB Atlas
```json
"destination_uri": "mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority"
```

---

## Sample Output

```
============================================================
  copy-missing-docs  |  DRY RUN — no changes will be written
  Config: config.json  |  Namespaces: 5
============================================================

[DRY RUN] ── svoc-db.sync_job_log ──
  Fetching _ids from source... 8155 docs
  Fetching _ids from destination... 8031 docs
  To INSERT:        124 docs
  To DELETE:          0 docs
  [DRY RUN] Would INSERT _id=ObjectID("6a1f3c...")
  [DRY RUN] Would INSERT _id=ObjectID("6a1f3d...")
  [DRY RUN] Would INSERT _id=ObjectID("6a1f3e...")
  [DRY RUN] ... and 121 more
  Would INSERT 124 | DELETE 0 | Errors 0

[DRY RUN] ── ebook-delegate-db.token ──
  Fetching _ids from source... 0 docs
  Fetching _ids from destination... 1 docs
  To INSERT:          0 docs
  To DELETE:          1 docs
  WARNING: 1 doc(s) exist in destination but NOT in source
    Extra _id=abc123...
  [DRY RUN] Would DELETE 1 doc(s) from destination
  Would INSERT 0 | DELETE 1 | Errors 0

============================================================
  TOTAL  INSERT: 134  |  DELETE: 1  |  Errors: 0
============================================================

  Re-run with --dry-run=false to apply changes.
```

---

## Build from Source

Requires Go 1.21+.

```bash
# Install dependencies
go mod tidy

# Build for current platform
go build -o copy-missing-docs .

# Cross-compile for Linux (from macOS or Windows)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o copy-missing-docs-linux-amd64 .

# Cross-compile for Windows (from macOS or Linux)
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o copy-missing-docs-windows-amd64.exe .
```

---

## Notes

- Documents are matched by `_id` only. Documents that exist on both sides but have different field values (missed updates) are not detected. Run `dsync --verify` after this tool to catch those.
- The tool processes namespaces sequentially in the order listed in the config file.
- Keep `config.json` out of version control — it contains database credentials.
