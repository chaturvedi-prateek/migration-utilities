# Oplog Resume Replay

Resume a `mongorestore --oplogReplay` that stopped midway, using only the existing oplog dump file (no access to source cluster required).

---

## Prerequisites

### Local machine
- Python 3.x
- `pymongo` library:
  ```bash
  pip install pymongo
  ```
- The original `oplog.bson` dump file

### Server (Atlas connectivity, no internet)
- `mongosh`
- `mongorestore`
- The original `oplog.bson` dump file
- SSH / SCP access from local machine

---

## Architecture

```
LOCAL MACHINE                          SERVER
─────────────────────────────────      ──────────────────────────────────────
oplog_resume_local.py
  reads oplog.bson
  embeds last 500 ts values
  generates find_resume.js      ──scp──▶  find_resume.js
                                           mongosh runs against Atlas
                                           outputs RESUME_T / RESUME_I
                                 ◀──────  note down RESUME_T and RESUME_I

oplog_resume_local.py
  filters oplog.bson
  writes resume_dir/oplog.bson  ──scp──▶  resume_dir/oplog.bson
                                           mongorestore --oplogReplay
```

---

## Step-by-Step

### Step 1 — Generate the mongosh query script (Local)

```bash
python3 oplog_resume_local.py \
  --dump  /path/to/oplog.bson \
  --generate \
  --js    find_resume.js
```

Expected output:
```
Reading dump: /path/to/oplog.bson
  84,321 entries  |  Timestamp(1718400000, 1) → Timestamp(1718500999, 3)
Generated: find_resume.js
  Embedded last 500 ts values from dump (84321 total)
```

---

### Step 2 — Copy the generated script to the server

```bash
scp find_resume.js user@server:/path/to/working/dir/
```

---

### Step 3 — Find the resume point (Server)

```bash
mongosh "mongodb+srv://user:pass@cluster.mongodb.net" \
  --file find_resume.js \
  --quiet
```

Possible outputs:

| Output | Meaning |
|--------|---------|
| `STATUS: COMPLETE` | Replay already fully applied — nothing to do |
| `STATUS: FOUND` | Resume point found — note the RESUME_T and RESUME_I values |
| `STATUS: NOT_FOUND` | Target oplog rolled over — see Troubleshooting |

Example success output:
```
STATUS: FOUND
RESUME_T: 1718500123
RESUME_I: 4
```

---

### Step 4 — Filter the dump (Local)

Use the `RESUME_T` and `RESUME_I` values from Step 3:

```bash
python3 oplog_resume_local.py \
  --dump  /path/to/oplog.bson \
  --filter \
  --t     1718500123 \
  --i     4 \
  --out   ./resume_dir
```

Expected output:
```
Filtered dump: 61,203 entries skipped, 23,118 entries written
Output: ./resume_dir/oplog.bson
```

---

### Step 5 — Copy filtered dump to server

```bash
scp ./resume_dir/oplog.bson user@server:/path/to/resume_dir/
```

---

### Step 6 — Run mongorestore (Server)

```bash
mongorestore \
  --uri="mongodb+srv://user:pass@cluster.mongodb.net" \
  --oplogReplay \
  --dir=/path/to/resume_dir \
  > restore_resume.log 2>&1 &

# Monitor progress
tail -f restore_resume.log
```

---

## Verifying Completion

### Check mongorestore log for final status line
```bash
tail -20 restore_resume.log
# Look for: X document(s) restored successfully. 0 document(s) failed.
```

### Confirm last ts from dump exists on target
Get last ts from dump (Local):
```bash
bsondump /path/to/oplog.bson | tail -1 | python3 -c "
import sys, json
doc = json.load(sys.stdin)
ts = doc['ts']['\$timestamp']
print('t:', ts['t'], ' i:', ts['i'])
"
```

Check it on target (Server):
```js
// In mongosh
db.getSiblingDB("local").oplog.rs.find(
  { ts: Timestamp(<t>, <i>) },
  { ts: 1, _id: 0 }
)
```
If a document is returned, the replay reached that point.

---

## Troubleshooting

### STATUS: NOT_FOUND — target oplog rolled over
The target cluster's oplog no longer contains the replayed entries (they were overwritten by newer operations). You cannot determine the resume point from the oplog alone.

Options:
1. Compare document counts per collection between source and target to estimate what is missing
2. Re-run the full replay from the beginning if data consistency is critical

### mongorestore exits without error or completion message
```bash
# Check exit code
echo $?   # 0 = success

# Check for errors in log
grep -i "error\|fail" restore_resume.log

# Check last applied ts on target vs last ts in dump
```

### find_resume.js returns wrong resume point
The script checks the last 500 entries of the dump. If the replay stopped very early (more than 500 entries from the end), the script may not detect the exact boundary. Re-run with a larger tail by increasing the `tail = entries[-500:]` value in `oplog_resume_local.py` line 58.

---

## Quick Reference — All Commands

```bash
# LOCAL — generate query script
python3 oplog_resume_local.py --dump oplog.bson --generate --js find_resume.js

# LOCAL → SERVER — copy query script
scp find_resume.js user@server:/path/

# SERVER — find resume point
mongosh "mongodb+srv://..." --file find_resume.js --quiet

# LOCAL — filter dump (use RESUME_T and RESUME_I from above)
python3 oplog_resume_local.py --dump oplog.bson --filter --t <T> --i <I> --out ./resume_dir

# LOCAL → SERVER — copy filtered dump
scp ./resume_dir/oplog.bson user@server:/path/to/resume_dir/

# SERVER — run mongorestore
mongorestore --uri="mongodb+srv://..." --oplogReplay --dir=/path/to/resume_dir > restore_resume.log 2>&1
```
