# copy-missing-docs

Copies documents that exist in the source (AWS DocumentDB) but are missing from the destination (MongoDB Atlas), and deletes documents from the destination that have been deleted from the source. Purpose-built for the MSIL CDC stall recovery — use after a dsync migration where CDC failed to replicate all changes.

## Binaries

| File | Platform |
|---|---|
| `copy-missing-docs-linux-amd64` | Linux x86-64 (EC2, Ubuntu, RHEL, Amazon Linux) |
| `copy-missing-docs-windows-amd64.exe` | Windows x86-64 |

No installation required. No Python. No pip. No runtime dependencies.

## Collections Targeted

| Database | Collection | Expected issue |
|---|---|---|
| `dms-trv-db` | `st_tv_stock_log` | 7 docs missing |
| `dms-trv-db` | `sh_tv_stock` | 1 doc missing |
| `ebook-delegate-db` | `token` | 1 extra doc in destination (source deleted) |
| `ebook-usersession-db` | `usersession` | 2 docs missing |
| `svoc-db` | `sync_job_log` | 124 docs missing |

## Usage

### Step 1 — Set connection strings

**Linux / macOS:**
```bash
export DOCDB_SRC="mongodb://username:password@docdb-endpoint:27017/?tls=true&tlsCAFile=/path/to/rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred"
export MDB_DEST="mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority"
```

**Windows (CMD):**
```cmd
set DOCDB_SRC=mongodb://username:password@docdb-endpoint:27017/?tls=true&tlsCAFile=C:\path\to\rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred
set MDB_DEST=mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority
```

**Windows (PowerShell):**
```powershell
$env:DOCDB_SRC = "mongodb://username:password@docdb-endpoint:27017/?tls=true&tlsCAFile=C:\path\to\rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred"
$env:MDB_DEST  = "mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority"
```

### Step 2 — Dry run (always run this first)

**Linux:**
```bash
chmod +x copy-missing-docs-linux-amd64
./copy-missing-docs-linux-amd64 2>&1 | tee copy-missing-docs-dryrun-$(date +%Y%m%d-%H%M%S).log
```

**Windows (CMD):**
```cmd
copy-missing-docs-windows-amd64.exe > copy-missing-docs-dryrun.log 2>&1
type copy-missing-docs-dryrun.log
```

**Windows (PowerShell):**
```powershell
.\copy-missing-docs-windows-amd64.exe 2>&1 | Tee-Object -FilePath copy-missing-docs-dryrun.log
```

Review the log. Confirm the INSERT and DELETE counts match what you expect before proceeding.

### Step 3 — Apply changes

**Linux:**
```bash
./copy-missing-docs-linux-amd64 --dry-run=false 2>&1 | tee copy-missing-docs-live-$(date +%Y%m%d-%H%M%S).log
```

**Windows (CMD):**
```cmd
copy-missing-docs-windows-amd64.exe --dry-run=false > copy-missing-docs-live.log 2>&1
type copy-missing-docs-live.log
```

**Windows (PowerShell):**
```powershell
.\copy-missing-docs-windows-amd64.exe --dry-run=false 2>&1 | Tee-Object -FilePath copy-missing-docs-live.log
```

## Flags

| Flag | Default | Description |
|---|---|---|
| `--dry-run` | `true` | Print what would be done without writing anything. Set to `false` to apply. |

## Sample Output

```
============================================================
  copy-missing-docs  |  DRY RUN — no changes will be written
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

============================================================
  TOTAL  INSERT: 134  |  DELETE: 1  |  Errors: 0
============================================================

  Re-run with --dry-run=false to apply changes.
```

## Build from Source

Requires Go 1.21+.

```bash
go mod tidy
go build -o copy-missing-docs-linux-amd64 .

# Cross-compile for Windows from Linux/macOS
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o copy-missing-docs-windows-amd64.exe .

# Cross-compile for Linux from macOS/Windows
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o copy-missing-docs-linux-amd64 .
```

## Notes

- The tool compares documents by `_id` only. It does not detect documents that exist on both sides but have different field values (updates that CDC missed). Run `dsync --verify` after this tool to catch those.
- Run the count verification script (`full-count-verify.sh`) after applying changes to confirm all collections are in sync.
- `ebook-usersession-db.usersession` is an active live collection — the diff may change between dry-run and live run due to in-flight writes. This is expected.
