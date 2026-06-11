# fix_id_types.js

A `mongosh` script that identifies documents with incorrect `_id` BSON types in specified collections, backs them up, re-inserts them with the correct `ObjectId` `_id`, and deletes the originals.

---

## Prerequisites

- `mongosh` 1.0+
- TLS certificate (`global-bundle.pem`) for DocumentDB connections

```bash
# Download DocumentDB CA bundle if not already available
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

---

## Supported Wrong `_id` Patterns

| Wrong `_id` type | Example | Fix |
|---|---|---|
| `string` | `"6374c4debb4e876fe62a36af"` | Converts string to `ObjectId` |
| `object` (nested) | `{ "_id": ObjectId("...") }` | Extracts inner `ObjectId` |

---

## Configuration

Open `fix_id_types.js` and update the two sections at the top:

### 1. `COLLECTIONS` — one entry per collection to fix

```javascript
const COLLECTIONS = [
  {
    dbName:    "your-database",            // database name
    collName:  "your-collection",          // collection name
    wrongType: "string",                   // BSON type of the wrong _id
    fixFn: (doc) => ObjectId(doc._id)      // how to derive the correct ObjectId
  },
  {
    dbName:    "another-database",
    collName:  "another-collection",
    wrongType: "object",
    fixFn: (doc) => doc._id._id            // for nested { _id: ObjectId(...) } pattern
  }
];
```

**`fixFn` reference:**

| Wrong `_id` | `fixFn` |
|---|---|
| String that looks like an ObjectId | `(doc) => ObjectId(doc._id)` |
| Nested object `{ _id: ObjectId(...) }` | `(doc) => doc._id._id` |

### 2. `DRY_RUN` flag

```javascript
const DRY_RUN = true;   // true = preview only, false = apply changes
```

Always run with `true` first to verify the output before making any changes.

---

## How to Run

### Step 1 — Dry run (preview)

**Windows (PowerShell):**
```powershell
mongosh "mongodb://user:pass@your-endpoint:27017" `
  --tls `
  --tlsCAFile global-bundle.pem `
  --quiet `
  --file fix_id_types.js | Out-File -Encoding utf8 fix_id_types_dryrun.log
```

**macOS / Linux:**
```bash
mongosh "mongodb://user:pass@your-endpoint:27017" \
  --tls \
  --tlsCAFile global-bundle.pem \
  --quiet \
  --file fix_id_types.js > fix_id_types_dryrun.log
```

Review `fix_id_types_dryrun.log` and confirm the old and new `_id` values look correct.

### Step 2 — Live run (apply changes)

Set `DRY_RUN = false` in the script, then re-run:

**Windows (PowerShell):**
```powershell
mongosh "mongodb://user:pass@your-endpoint:27017" `
  --tls `
  --tlsCAFile global-bundle.pem `
  --quiet `
  --file fix_id_types.js | Out-File -Encoding utf8 fix_id_types_live.log
```

### DocumentDB connection string

```powershell
mongosh "mongodb://user:pass@docdb-cluster.cluster-abc123.us-east-1.docdb.amazonaws.com:27017" `
  --tls `
  --tlsCAFile global-bundle.pem `
  --replicaSet rs0 `
  --readPreference secondaryPreferred `
  --quiet `
  --file fix_id_types.js | Out-File -Encoding utf8 fix_id_types_live.log
```

---

## What the Script Does Per Document

For each document with a wrong `_id` type:

| Step | Action |
|---|---|
| 1 | Backs up the original document (with wrong `_id`) to `<collection>_id_fix_backup` |
| 2 | Inserts a new document with the corrected `ObjectId` `_id` |
| 3 | Deletes the original wrong document |

If step 1 or step 2 fails, the document is skipped to prevent data loss.

---

## Backup Collections

Originals are backed up to a collection in the same database:

```
<database>.<collection>_id_fix_backup
```

Example:
```
common-warranty-db.vm_warranty_approval_online_id_fix_backup
```

Drop backup collections only after verifying the fix was successful.

---

## Log Levels

Every operation is timestamped in the output log:

| Level | Meaning |
|---|---|
| `INFO` | General progress |
| `STEP` | Individual operation (backup / insert / delete) |
| `OK` | Step completed successfully |
| `DRY` | What would happen in dry run mode |
| `WARN` | Non-fatal issue (e.g. backup already exists, duplicate key) |
| `SKIP` | Document skipped — could not derive new `_id` |
| `ERROR` | Operation failed — document was not modified |

### Sample output

```
════════════════════════════════════════════════════════════
[2026-06-11T10:00:00.000Z] [INFO ] Script start
[2026-06-11T10:00:00.001Z] [INFO ] Mode     : DRY RUN (no changes will be made)
[2026-06-11T10:00:00.001Z] [INFO ] Collections to process : 2
────────────────────────────────────────────────────────────
[2026-06-11T10:00:00.002Z] [INFO ] [1/2] Starting collection : common-warranty-db.vm_warranty_approval_online
[2026-06-11T10:00:00.003Z] [STEP ] Querying for documents with _id type "string"...
[2026-06-11T10:00:01.100Z] [INFO ] Found 5626 document(s) with wrong _id type
[2026-06-11T10:00:01.101Z] [INFO ] [doc 1/5626] Processing _id : "6374c4debb4e876fe62a36af"
[2026-06-11T10:00:01.101Z] [INFO ] [doc 1/5626] Derived new _id : ObjectId("6374c4debb4e876fe62a36af")
[2026-06-11T10:00:01.101Z] [DRY  ] [doc 1/5626] Would backup  → common-warranty-db.vm_warranty_approval_online_id_fix_backup
[2026-06-11T10:00:01.101Z] [DRY  ] [doc 1/5626] Would insert  → common-warranty-db.vm_warranty_approval_online  with _id: ObjectId("6374c4...")
[2026-06-11T10:00:01.101Z] [DRY  ] [doc 1/5626] Would delete  → common-warranty-db.vm_warranty_approval_online  old _id: "6374c4..."
────────────────────────────────────────────────────────────
[2026-06-11T10:00:05.300Z] [INFO ] Collection summary : common-warranty-db.vm_warranty_approval_online
[2026-06-11T10:00:05.300Z] [INFO ]   Total found  : 5626
[2026-06-11T10:00:05.300Z] [INFO ]   Fixed        : 5626
[2026-06-11T10:00:05.300Z] [INFO ]   Skipped      : 0
[2026-06-11T10:00:05.300Z] [INFO ]   Errors       : 0
[2026-06-11T10:00:05.300Z] [INFO ]   Elapsed      : 5.3s
════════════════════════════════════════════════════════════
[2026-06-11T10:00:05.301Z] [INFO ] GLOBAL SUMMARY
[2026-06-11T10:00:05.301Z] [INFO ] common-warranty-db.vm_warranty_approval_online
[2026-06-11T10:00:05.301Z] [INFO ]   Found: 5626  Fixed: 5626  Skipped: 0  Errors: 0
[2026-06-11T10:00:05.302Z] [INFO ] Mode : DRY RUN — no changes were made
════════════════════════════════════════════════════════════
```
