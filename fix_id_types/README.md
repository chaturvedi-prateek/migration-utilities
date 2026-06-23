# _id Type Tooling

Three `mongosh` scripts for auditing and fixing wrong `_id` BSON types before migration. Run them in order:

| Script | Purpose | Speed |
|---|---|---|
| `detect_mixed_ids.js` | Scan databases — find collections with mixed `_id` types | Fast — 2 index seeks per collection |
| `count_id_types.js` | For flagged namespaces — count documents per `_id` type | Fast — covered index scan |
| `fix_id_types.js` | Fix wrong `_id` types (backup → insert corrected → delete original) | Full collection scan (writes) |

---

## Prerequisites

- `mongosh` 1.0+
- TLS certificate for DocumentDB connections (use the regional bundle, e.g. `ap-south-1-bundle.pem`)

---

## Step 1 — Detect: `detect_mixed_ids.js`

Scans one or more databases using 2 index probes per collection:

- Sort `_id` ASC → captures the lowest BSON-order type
- Sort `_id` DESC → captures the highest BSON-order type
- If the two types differ → mixed types exist

MongoDB sorts mixed BSON types by BSON comparison order:
`numbers < string < object < array < binData < objectId < bool < date`

So a collection with both `string` and `objectId` `_id` values will surface immediately — no full scan needed.

### Configuration

Open `detect_mixed_ids.js` and set `DATABASES`:

```javascript
const DATABASES = [
  "common-warranty-db",
  "svoi-db",
  // Use [] to scan ALL user databases on the server
];
```

### Run

**Windows (PowerShell):**
```powershell
mongosh "mongodb://user:pass@docdb-cluster.ap-south-1.docdb.amazonaws.com:27017" `
  --tls `
  --tlsCAFile ap-south-1-bundle.pem `
  --replicaSet rs0 `
  --readPreference secondaryPreferred `
  --quiet `
  --file detect_mixed_ids.js | Out-File -Encoding utf8 detect_mixed_ids.log
```

**macOS / Linux:**
```bash
mongosh "mongodb://user:pass@docdb-cluster.ap-south-1.docdb.amazonaws.com:27017" \
  --tls \
  --tlsCAFile ap-south-1-bundle.pem \
  --replicaSet rs0 \
  --readPreference secondaryPreferred \
  --quiet \
  --file detect_mixed_ids.js > detect_mixed_ids.log
```

### Sample output

```
════════════════════════════════════════════════════════════
[INFO ] detect_mixed_ids — fast _id type scan via ASC / DESC index probes
[INFO ] Databases to scan : 2
────────────────────────────────────────────────────────────
[INFO ] [common-warranty-db] 12 collection(s)
[CLEAN]   alerts — objectId
[MIXED]   vm_warranty_approval_online — ASC type: string  |  DESC type: objectId
[CLEAN]   vm_warranty_data — objectId
[INFO ] [svoi-db] 8 collection(s)
[MIXED]   consumer_did_mapping_log — ASC type: object  |  DESC type: objectId
[CLEAN]   svoi_requests — objectId
════════════════════════════════════════════════════════════
[INFO ] SUMMARY
[INFO ]   Collections scanned         : 20
[INFO ]   Collections with mixed _ids : 2
────────────────────────────────────────────────────────────
[INFO ] Collections requiring attention:
[INFO ]   common-warranty-db.vm_warranty_approval_online  (min type: string, max type: objectId)
[INFO ]   svoi-db.consumer_did_mapping_log  (min type: object, max type: objectId)
[INFO ] Next steps:
[INFO ]   1. Run count_id_types.js on the above namespaces for exact counts per type
[INFO ]   2. Add the namespaces to fix_id_types.js COLLECTIONS array and run to fix
[INFO ]   Elapsed : 1.4s
════════════════════════════════════════════════════════════
```

---

## Step 2 — Count: `count_id_types.js`

For each namespace, runs a covered aggregation on the `_id` index to count documents by type. Use this to understand the scope before running the fix.

### Configuration

Open `count_id_types.js` and set `NAMESPACES` (paste the mixed namespaces from the detect output):

```javascript
const NAMESPACES = [
  "common-warranty-db.vm_warranty_approval_online",
  "svoi-db.consumer_did_mapping_log",
];
```

### Run

**Windows (PowerShell):**
```powershell
mongosh "mongodb://user:pass@docdb-cluster.ap-south-1.docdb.amazonaws.com:27017" `
  --tls `
  --tlsCAFile ap-south-1-bundle.pem `
  --replicaSet rs0 `
  --readPreference secondaryPreferred `
  --quiet `
  --file count_id_types.js | Out-File -Encoding utf8 count_id_types.log
```

**macOS / Linux:**
```bash
mongosh "mongodb://user:pass@docdb-cluster.ap-south-1.docdb.amazonaws.com:27017" \
  --tls \
  --tlsCAFile ap-south-1-bundle.pem \
  --replicaSet rs0 \
  --readPreference secondaryPreferred \
  --quiet \
  --file count_id_types.js > count_id_types.log
```

### Sample output

```
════════════════════════════════════════════════════════════
[INFO ] count_id_types — _id type distribution for 2 namespace(s)
────────────────────────────────────────────────────────────
[INFO ] common-warranty-db.vm_warranty_approval_online
[INFO ]   string                  5626 docs     2.31%
[INFO ]   objectId              237804 docs    97.69%
[INFO ]   total                 243430 docs   100.00%
[INFO ]   STATUS : MIXED — configure fix_id_types.js and run to fix before migration
────────────────────────────────────────────────────────────
[INFO ] svoi-db.consumer_did_mapping_log
[INFO ]   object                   312 docs     0.13%
[INFO ]   objectId              241188 docs    99.87%
[INFO ]   total                 241500 docs   100.00%
[INFO ]   STATUS : MIXED — configure fix_id_types.js and run to fix before migration
────────────────────────────────────────────────────────────
[INFO ] Elapsed : 3.2s
════════════════════════════════════════════════════════════
```

---

## Step 3 — Fix: `fix_id_types.js`

Three-phase batch operation. No upfront count queries — streams and processes directly.

| Phase | Action |
|---|---|
| 1 — Move | Batch-inserts wrong-type docs into `<collection>_id_fix_backup`, then batch-deletes them from source. Source is clean after this phase. |
| 2 — Transform | Reads from backup, applies `fixFn`, batch-inserts corrected docs into source. |
| 3 — Cleanup | Drops backup collection (only if `DROP_BACKUP_ON_SUCCESS = true` and Phase 2 had zero errors). |

### Supported wrong `_id` patterns

| Wrong `_id` type | Example | Fix |
|---|---|---|
| `string` | `"6374c4debb4e876fe62a36af"` | Converts string to `ObjectId` |
| `object` (nested) | `{ "_id": ObjectId("...") }` | Extracts inner `ObjectId` |
| `object` (compound) | `{ mobilenumber: 343566, did: 324432 }` | Generates a brand new `ObjectId` |

### Configuration

Open `fix_id_types.js` and update `COLLECTIONS`. The script is pre-configured for two known MSIL collections:

```javascript
const COLLECTIONS = [
  {
    namespace:      "common-warranty-db.vm_warranty_approval_online",
    wrongType:      "string",
    fixFn:          (doc) => ObjectId(doc._id),   // string → ObjectId
    newIdOnFailure: false,   // skip doc if fixFn fails
  },
  {
    namespace:      "svoi-db.consumer_did_mapping_log",
    wrongType:      "object",
    fixFn:          (doc) => doc._id._id,          // nested { _id: ObjectId } → inner ObjectId
    newIdOnFailure: true,    // assign new ObjectId() if fixFn fails (e.g. compound object)
  },
];

const DRY_RUN                = true;        // ← set to false when ready to apply
const BATCH_SIZE             = 1000;        // lower for very large documents (>16 KB)
const LOG_EVERY              = 1_000_000;   // progress line every N docs — keeps output sane on billion-scale collections
const MAX_INLINE_WARN        = 20;          // max individual WARN lines per phase; excess counted silently
const MAX_INLINE_SKIP        = 20;          // max individual SKIP lines per phase; excess counted silently
const DROP_BACKUP_ON_SUCCESS = false;       // set true to drop backup after a clean fix
```

**`fixFn` reference:**

| Wrong `_id` | `fixFn` |
|---|---|
| String that looks like an ObjectId | `(doc) => ObjectId(doc._id)` |
| Nested object `{ _id: ObjectId("...") }` | `(doc) => doc._id._id` |
| Compound object with no embedded ObjectId | `(doc) => new ObjectId()` |

**`newIdOnFailure`:** set to `true` on collections where some wrong-type `_id` values cannot be mapped to a specific ObjectId (e.g. compound keys). The document is still migrated; it just gets a new random ObjectId.

### Run (dry run first)

**Windows (PowerShell):**
```powershell
mongosh "mongodb://user:pass@docdb-cluster.ap-south-1.docdb.amazonaws.com:27017" `
  --tls `
  --tlsCAFile ap-south-1-bundle.pem `
  --replicaSet rs0 `
  --readPreference secondaryPreferred `
  --quiet `
  --file fix_id_types.js | Out-File -Encoding utf8 fix_id_types_dryrun.log
```

**macOS / Linux:**
```bash
mongosh "mongodb://user:pass@docdb-cluster.ap-south-1.docdb.amazonaws.com:27017" \
  --tls \
  --tlsCAFile ap-south-1-bundle.pem \
  --replicaSet rs0 \
  --readPreference secondaryPreferred \
  --quiet \
  --file fix_id_types.js > fix_id_types_dryrun.log
```

Review the dry-run log. Confirm Phase 2 `inserted` count matches Phase 1 `moved` count (minus any expected skips). Then set `DRY_RUN = false` and re-run.

### Restart safety

The script is idempotent. If it is interrupted and re-run:
- Phase 1 handles docs already in backup (duplicate key errors are treated as already-moved).
- Phase 2 handles corrected docs already in source (duplicate key errors are treated as already-inserted).

### Log levels

| Level | Meaning |
|---|---|
| `INFO` | General progress |
| `STEP` | Phase start |
| `OK` | Phase completed |
| `DRY` | What would happen (dry run) |
| `WARN` | Non-fatal issue (duplicate, newIdOnFailure assignment) |
| `SKIP` | Document skipped — fixFn failed and `newIdOnFailure = false` |
| `ERROR` | Operation failed |
