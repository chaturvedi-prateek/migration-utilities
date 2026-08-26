# DocumentDB → Atlas Migration Runbook (dsync Enterprise + Temporal)

Consolidated from the POC run against `maruti-poc-docdb-cluster`. Every command
here was actually executed and validated during that run — including the
mistakes, which is why the pre-flight section is non-negotiable.

Fill in placeholders (`<...>`) before running anything. Treat this file as
confidential once filled in — it will contain connection strings/credentials.

**Structure:** Part One is the normal, start-to-finish happy path — follow it
in order for a standard migration. Part Two is troubleshooting, tuning, and
reference material — go there only when something in Part One doesn't behave
as described, or you need a deeper explanation/full flag list.

---

# PART ONE

## 1. One-time host setup

Already done if you followed `install-docdb-migration-toolkit.sh`. Confirms
Docker, dsynct, and helper tools are present:

```bash
which dsynct mongosh jq fixIdTypes migrateIndexes checkChangeStreams copyMissingDocs fullCountVerify.sh
docker compose version
docker images | grep -E "dsynct|temporal"
```

---

## 2. Pre-flight on the DocumentDB source — DO NOT SKIP

**This is the step that broke the POC run for over an hour.** Change streams
were never enabled, dsync gave no error, CDC silently did nothing while
looking perfectly healthy. Confirm all of this *before* starting any sync.

### 2.1 Enable change streams cluster-wide

```bash
export DOCDB_SRC="mongodb://<user>:<password>@<cluster>.cluster-<id>.<region>.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/opt/docdb-migration/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"

# NOTE: `use admin` inside mongosh --eval does NOT reliably surface command
# results. Always call db.getSiblingDB("admin").runCommand(...) directly and
# wrap in printjson() so failures/`{ok:1}` are actually visible.
mongosh "$DOCDB_SRC" --eval '
printjson(db.getSiblingDB("admin").runCommand({ modifyChangeStreams: 1, database: "", collection: "", enable: true }))
'
# Expect: { ok: 1, ... }
```

### 2.2 Set change-stream retention to 7 days (covers a multi-day initial sync)

```bash
aws docdb describe-db-cluster-parameters \
  --db-cluster-parameter-group-name <your-param-group-name> \
  --query "Parameters[?ParameterName=='change_stream_log_retention_duration']"

# If not 604800:
aws docdb modify-db-cluster-parameter-group \
  --db-cluster-parameter-group-name <your-param-group-name> \
  --parameters ParameterName=change_stream_log_retention_duration,ParameterValue=604800,ApplyMethod=immediate
```

Note: `db.adminCommand({getParameter:1, changeStreamLogRetentionDuration:1})`
from the base OSS plan does **not** work on DocumentDB (`Feature not
supported: getParameter`) — always check retention via the AWS CLI instead.

### 2.3 Prove change streams actually work (don't trust step 2.1's `{ok:1}` alone)

Terminal A:
```bash
mongosh "$DOCDB_SRC" --eval '
  const cs = db.getSiblingDB("<dbname>").<collection>.watch();
  print("watching...");
  while (!cs.hasNext()) { sleep(500); }
  printjson(cs.next());
'
```
Terminal B (while A is running):
```bash
mongosh "$DOCDB_SRC" --eval 'db.getSiblingDB("<dbname>").<collection>.insertOne({probe: new Date()})'
```
Terminal A must print the change event and exit. If it errors with
`modifyChangeStreams has not been run...`, step 2.1 did not actually take —
go back and confirm with `printjson()` wrapping, not bare `use admin`.

### 2.4 Confirm `_id` types are clean (mandatory — dsync fails the whole flow plan on mixed types)

```bash
fixIdTypes --mode detect --config /opt/docdb-migration/configs/fixIdTypes.config.sample.json
# every collection must report [CLEAN] before proceeding
```

### 2.5 Confirm change-stream config via the toolkit's own checker

```bash
checkChangeStreams
```

---

## 3. Configure the migration

```bash
cd /opt/docdb-migration/compose
cp .env.sample .env
```

Edit `.env`:
```bash
DOCDB_SRC=mongodb://<user>:<password>@<cluster>.cluster-<id>.<region>.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false
MDB_DEST=mongodb+srv://<user>:<password>@<atlas-cluster>.mongodb.net/?retryWrites=true

QUEUE=dsync-prod-<name>
WORKFLOW_ID=<name>
# NAMESPACE intentionally omitted below — see note

WORKER_COUNT=3
CONCURRENT_ACTIVITIES=4
SYNC_WRITER_WORKERS=8
PER_STREAM_WORKERS=4
DOC_PARTITION=500000
```

**Note on `tlsCAFile`:** the compose file's dsynct containers use
`/certs/global-bundle.pem` — the path *baked into the `dsynct:enterprise`
image*, not a host path. Leave it as-is in `.env`.

### Where `WORKFLOW_ID` comes from

It's not generated externally — it's a name **you choose** in this `.env`
file, used consistently by every `temporal workflow ...` command later in
this runbook (`describe`, `terminate`, `list`, etc.). Pick something
meaningful and stable per migration run rather than reusing a generic name
like `prod` across multiple databases/clusters, since you'll likely have
several running at once (SVOC, DPS, MI, Common Services Audit) and need to
tell them apart in the Temporal UI — e.g. `WORKFLOW_ID=svoc-prod`,
`WORKFLOW_ID=common-crm-prod`.

If you forget what you set it to on a given host:
```bash
cat /opt/docdb-migration/compose/.env | grep WORKFLOW_ID
# or, ask Temporal directly what workflows it knows about:
docker compose exec temporal temporal workflow list
```

### Ports — dashboards, security groups, and remapping

Three ports matter, all defined in `compose/docker-compose.yml`:

| Port | Service | Purpose | Needs external access? |
|------|---------|---------|------------------------|
| `7233` | `temporal` | gRPC — workers/runner connect here | No — stays inside the Docker network on a single host. Only needed externally if you run worker containers on a **separate** EC2 host from `temporal` (multi-host scaling). |
| `8233` | `temporal` | Temporal Web UI | Only if you want to browse workflow history/activity state directly. |
| `8080` | `runner` | dsynct progress dashboard | Only if you want to view the React dashboard in a browser (note: it has no simple curl-able API — see §5, use `temporal workflow describe` for scripted monitoring instead). |

**Recommended: don't open these in the security group at all — use an SSH tunnel instead.** These ports expose migration control-plane access (workflow termination, activity pausing, live data-in-flight visibility), so treat them the same as you would a database admin port:
```bash
ssh -i <key.pem> -L 8233:localhost:8233 -L 8080:localhost:8080 ec2-user@<host> -N
```
Then browse to `http://localhost:8233` and `http://localhost:8080` on your own machine — nothing needs to be opened on the EC2 security group at all.

**If you do need direct browser access** (e.g. multiple people on a team without SSH access to the box), open inbound rules scoped to specific known IPs only, never `0.0.0.0/0`:
```bash
aws ec2 authorize-security-group-ingress \
  --group-id <sg-id> \
  --protocol tcp --port 8233 --cidr <your-ip>/32
aws ec2 authorize-security-group-ingress \
  --group-id <sg-id> \
  --protocol tcp --port 8080 --cidr <your-ip>/32
```

**If a port is already in use on the host** (e.g. something else already binds `8080`), remap the **host** side in `docker-compose.yml` — leave the container-internal port unchanged:
```yaml
  runner:
    ports:
      - "18080:8080"   # host:container — dashboard now at http://<host>:18080
```
```yaml
  temporal:
    ports:
      - "17233:7233"
      - "18233:8233"
```
After changing a mapping, recreate just that service:
```bash
docker compose up -d temporal   # or: runner
```
Note: if you remap `7233` (Temporal's gRPC port), and workers run in
**separate containers on this same host** via `docker compose`, nothing else
needs to change — Compose's internal DNS (`temporal:7233`) is unaffected by
host-side port remaps, since that's a container-to-container connection, not
going through the host's mapped port at all.

### To migrate ALL databases (not just one)

`dsynct run --namespace` is optional and repeatable — omitting it entirely
syncs every database on the cluster. **Verified empirically**: with zero
`--namespace` flags, both initial sync and live CDC correctly covered every
database, not just one.

The shipped `docker-compose.yml` requires `NAMESPACE` via
`${NAMESPACE:?set NAMESPACE in .env}`. For an all-databases run, strip that
requirement from the compose file once:
```bash
sed -i '/--namespace=\${NAMESPACE/d' docker-compose.yml
grep -A3 "^  runner:" docker-compose.yml   # confirm the line is gone
```
Leave `NAMESPACE` unset/deleted in `.env` after this edit.

**Confirm Atlas network access before continuing:**
- Real database user created (not the `<db_username>`/`<db_password>` template placeholders)
- EC2 host IP whitelisted in Atlas → Network Access
- All destination databases are empty (dsync requirement)

---

## 4. Bring up the topology

```bash
docker compose up -d temporal
docker compose ps          # confirm temporal is Up and NOT restarting before continuing
```

If `temporal` crash-loops with `SQLite ... unable to open database file`
(error code 14, `SQLITE_CANTOPEN`), it's a volume-permission mismatch, not
disk space — the `temporalio/temporal` image runs as a non-root `temporal`
user, and Docker creates the named volume owned by `root`:
```bash
docker inspect --format '{{.Config.User}}' temporalio/temporal:1.8.2   # confirm non-root
docker compose down
sudo chmod 777 /var/lib/docker/volumes/compose_temporal-data/_data
docker compose up -d temporal
docker compose logs -f temporal   # should show "Temporal Server: 0.0.0.0:7233" cleanly
```

Then bring up workers and the runner:
```bash
docker compose up -d --scale worker=3 worker
docker compose up -d runner
docker compose ps                 # all should be Up, none Restarting
docker compose logs worker --tail=20   # each should show "Started Worker ... TaskQueue=..."
docker compose logs runner --tail=20   # should show "Started workflow workflow-id=..."
```

---

## 5. Monitor progress

The dsynct web dashboard (`:8080`) is a full React SPA with no simple
REST/SSE endpoint — `curl`-ing `/progress` just returns the HTML shell
regardless of path. **Use Temporal directly instead:**

```bash
# Workflow status + currently pending activities (most useful single command)
docker compose exec temporal temporal workflow describe --workflow-id <WORKFLOW_ID>

# Confirm work is actually spread across workers
docker compose exec temporal temporal workflow show --workflow-id <WORKFLOW_ID> -o json \
  | grep -oE '"identity": *"[^"]*"' | sort | uniq -c | sort -rn
```

Reading `workflow describe` output:
- `Pending Activities` of type `dsync_InitialSync` → still copying bulk data
- `Pending Activities` of type `dsync_StreamChanges` / `dsync_StreamLSN` → in CDC mode
- `LastHeartbeatDetails` on `dsync_StreamChanges` shows `EventsRead`/`EventsWritten` and a `Cursor` —
  **if `Cursor` stays byte-identical across repeated checks while writes are
  happening on the source, CDC is silently stalled** (dead/expired resume
  token — see Part Two §T6). This is the same silent-stall failure the base
  plan warns about; it produces zero error output.
- **Positive confirmation CDC is genuinely healthy:** run `workflow describe`
  twice, ~30s apart, and compare. `EventsRead`/`EventsWritten` climbing and
  the `Cursor` value changing between the two checks means CDC is actively
  processing live writes — this is the real proof, not just the absence of a
  frozen cursor.

To watch visually instead: SSH-tunnel port 8080 to your laptop and use a real
browser (the dashboard's JS makes its own internal calls that `curl` can't
replicate):
```bash
ssh -i <key.pem> -L 8080:localhost:8080 ec2-user@<host> -N
# then open http://localhost:8080
```
Temporal's own Web UI at `:8233` works normally over the same kind of tunnel
or directly if the security group allows it.

For a deeper read on CDC architecture, throughput tuning, and what each
dashboard column actually measures, see Part Two §T1.

---

## 6. Scaling workers live

Add workers — new replicas immediately start pulling pending partition tasks,
no restart of runner/other workers needed:
```bash
docker compose up -d --scale worker=<N> worker
```

Remove a worker permanently (not auto-restarted):
```bash
docker compose stop compose-worker-<N>
```

Simulate a crash (Docker's `restart: unless-stopped` policy auto-restarts it
within seconds — this tests self-healing, not permanent removal):
```bash
docker kill compose-worker-<N>
```

In-flight partitions on a removed/crashed worker get reassigned to survivors
automatically once the activity's heartbeat timeout expires (~1 min default) —
**unless `--pause-on-error` triggers first**, in which case see Part Two §T2.

**Note:** scaling worker *count* only helps during initial sync (parallelizable
across many doc partitions). It does nothing for change-stream/CDC throughput
once initial sync is complete — see Part Two §T1 for what actually moves CDC
throughput and why.

---

## 7. Verification

`verify` runs in `simple` mode (no Temporal) and, by default, **runs
indefinitely** — it verifies the initial-sync data and then keeps tailing and
verifying CDC forever unless told otherwise.

**Run this only once initial sync is confirmed complete.** Running `verify`
while dsync's own initial sync is still copying data produces enormous
false-positive mismatch counts (validated live: 30-50 million "mismatches"
during an active copy, decreasing over time as the destination caught up) —
those aren't real, just a timing artifact of comparing against a
still-changing destination. Confirm the workflow is in `dsync_StreamChanges`
(§5) before trusting any verify output.

**Avoid `--report-all` — use `--report-limit` instead.** `--report-all`
tries to hold/print every mismatch found each report interval; if the run
happens to catch a large mismatch count (e.g. started too early, per above),
that's real memory pressure. `--report-limit N` caps it to a bounded,
representative sample per interval instead (default is `5`, a bit thin —
`50` gives a better sample without the unbounded-memory risk):

**One-time check before cutover (recommended — exits cleanly when done):**
```bash
set -a; source /opt/docdb-migration/compose/.env; set +a

# DSYNCT_MODE=simple is required — without it the image only recognizes the
# Temporal-mode commands (app/temporal/worker/run), not verify/sync/etc.
#
# Flag order matters: verify's own flags (--report-limit, --parallelism,
# --skip-change-stream, --namespace) must come BEFORE the source/destination
# URIs. Anything placed after a URI gets parsed as an option for THAT
# connector instead and fails with "flag provided but not defined".
docker run --rm --network compose_default -e DSYNCT_MODE=simple dsynct:enterprise \
  verify \
  --report-limit 50 --parallelism 8 --skip-change-stream \
  "$DOCDB_SRC" \
  "$MDB_DEST"
```

If memory is still a concern on a very large dataset (verify's Merkle-tree
comparison loads items from both sides, so it's inherently memory-hungry at
scale), lower `--parallelism` (fewer concurrent workers held in memory) or
split the run across `--total-partitions`/`--partition` instead of one full
pass:
```bash
--total-partitions 4 --partition 0   # run this range, then repeat for 1,2,3
```

**This is not a theoretical concern — see Part Two §T8 for a real incident**
where `verify` OOM-killed `sshd` twice on a shared host and locked out SSH
access, plus a lighter-weight Enterprise-native alternative
(`sample-ids`/`verify-ids`) for very large datasets.

Omit `--namespace` for all-databases verification, matching the sync scope.

**Background/continuous run (includes CDC verification, runs forever):**
```bash
docker run -d --name docdb-verify --network compose_default -e DSYNCT_MODE=simple dsynct:enterprise \
  verify \
  --report-limit 50 --parallelism 8 --report-interval 30s \
  "$DOCDB_SRC" \
  "$MDB_DEST"
```
This container has **no restart policy** — it will not survive a host
reboot (see Part Two §T5) and must be re-launched manually if it exits.
Check on it with `docker logs --tail 50 docdb-verify` / `docker ps -a | grep docdb-verify`.

---

## 8. Create indexes on the destination (once CDC backlog is ~0, before cutover)

Once the change-stream backlog has converged to ~0 (per Part Two §T1's
"cutover-ready signature" — `Events Read ≈ Events Written`, `Latency: 0`,
`Last Event` matching wall-clock now), create the destination indexes using
the `migrateIndexes` utility. It is already staged as part of this toolkit
(`HELPER_TOOLS` in the bundle) — no separate copy step needed.

Binary and sample config location (installed by
`install-docdb-migration-toolkit.sh`):
```
${PREFIX}/libexec/migrateIndexes                          # e.g. /opt/docdb-migration/libexec/migrateIndexes
${PREFIX}/configs/migrateIndexes.config.sample.json
```

**Why now, not earlier:** `create` mode deliberately neutralizes `unique`
indexes to `unique:false` and TTL indexes to `expireAfterSeconds = MAX_INT`
when building them on the destination. Building real `unique`/TTL indexes
while CDC is still actively replaying inserts would risk a uniqueness
violation on a replayed/duplicate write, or a document expiring before it's
even been verified. Waiting until the backlog is ~0 (but still before
cutover/stopping source writes) minimizes that window while still getting
the index builds — usually the slowest part of a migration — done ahead of
time instead of adding it to the cutover critical path.

```bash
cd ${PREFIX}/configs
cp migrateIndexes.config.sample.json migrateIndexes.config.json
```

Edit `migrateIndexes.config.json` — reuse the same connection strings as
`.env`'s `DOCDB_SRC`/`MDB_DEST`:
```json
{
  "source_uri": "mongodb://<user>:<password>@<docdb-cluster-endpoint>:27017/?tls=true&tlsCAFile=${PREFIX}/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false",
  "destination_uri": "mongodb+srv://<user>:<password>@<atlas-cluster>.mongodb.net/?retryWrites=true",
  "databases": [],
  "log_file": "migrateIndexes-create.log"
}
```
`databases: []` auto-discovers every non-system database — leave empty
unless scoping to specific DBs.

Dry run first — always:
```bash
${PREFIX}/libexec/migrateIndexes --config migrateIndexes.config.json --mode=create --dry-run=true
```
Review the printed list of indexes it would create (unique/TTL shown
neutralized), then apply:
```bash
${PREFIX}/libexec/migrateIndexes --config migrateIndexes.config.json --mode=create --dry-run=false --concurrency=8
```
(`--concurrency` default 8; raise on a bigger Atlas tier with headroom,
lower if you see connection-pool/server-selection timeouts. MongoDB
serializes multiple index builds on the *same* collection regardless of
this setting — it only parallelizes across different collections.)

Verify (read-only, safe to run anytime):
```bash
${PREFIX}/libexec/migrateIndexes --config migrateIndexes.config.json --mode=verify
```
At this stage expect `PENDING` on the unique/TTL indexes — that's the
neutralized state and is correct, not a failure. Exit code `4` (INCOMPLETE)
is expected right now; a real problem is exit code `3`
(`MISMATCH`/`MISSING`/`EXTRA`).

**Post-cutover** (after source writes have genuinely stopped and CDC has
fully drained for real, not just momentarily): run `--mode=rectify` to
restore real `unique:true` and original TTL values, then `verify` again
expecting `PASS` (exit code `0`). See §9 for where this fits in the overall
cutover sequence.

---

## 9. Cutover sequence

A full `verify` pass over a large collection takes hours, not minutes — it
does not fit inside the actual cutover window. **Start it well before
cutover is planned** (§7), let it run to completion in the background, and
only schedule/execute cutover once it has finished with zero mismatches.
Don't start a fresh `verify` run as part of the cutover steps themselves.

1. Start (or confirm already running) the background `verify` from §7, with
   enough lead time before the planned cutover for a full pass to complete.
2. Once both `src` and `dst` show `Tasks=X/X` (fully scanned — see §5/§7 for
   why partial-pass output isn't trustworthy) and `mismatchCount` has
   settled at `0`, the data is confirmed consistent. This is your green
   light to schedule the actual cutover.
3. Create destination indexes now if not already done (§8) — do this
   *before* stopping writes, while CDC backlog is ~0, so index builds don't
   sit on the cutover critical path:
   ```bash
   ${PREFIX}/libexec/migrateIndexes --config migrateIndexes.config.json --mode=create --dry-run=false
   ```
4. **Stop writes on DocumentDB** (app-level maintenance mode, or block at the
   security-group level). This is the actual start of the cutover window.
5. Confirm CDC lag has drained to 0 — check `workflow describe`'s
   `dsync_StreamChanges` heartbeat repeatedly; `EventsRead` should stop
   climbing and the cursor should stabilize once no new writes occur (same
   signature as Part Two §T1's cutover-ready check, but now against a
   genuinely static source).
6. **Compare document counts and indexes on both sides** now that the
   source is frozen — this is the final correctness gate before switching
   traffic:
   ```bash
   # Document counts, every DB/collection, source vs destination
   DOCDB_SRC="$DOCDB_SRC" MDB_DEST="$MDB_DEST" fullCountVerify.sh
   # (or, if not symlinked onto PATH: ${PREFIX}/libexec/fullCountVerify.sh)

   # Index parity, including that unique/TTL are still neutralized
   # (expected PENDING/INCOMPLETE until step 7 below)
   migrateIndexes --config migrateIndexes.config.json --mode=verify
   ```
   `fullCountVerify.sh` (staged as part of this toolkit — reuses the same
   `$DOCDB_SRC`/`$MDB_DEST` env vars as everything else) walks every
   non-system database/collection on the source, runs
   `estimatedDocumentCount()` against the matching collection on the
   destination, and prints a `PASS`/`FAIL` line per collection plus a
   summary count. Any `FAIL` here means don't proceed — investigate before
   cutting over, don't just retry the comparison.
7. Restore unique indexes and correct TTL settings:
   ```bash
   migrateIndexes --config migrateIndexes.config.json --mode=rectify --dry-run=false
   ```
   Then re-run `verify` and expect `PASS` (exit code `0`).
8. Switch the application's connection string to Atlas.
9. Cancel/terminate the Temporal workflow once cutover is confirmed stable.
10. Re-enable Atlas backups (disabled during migration per the target-prep checklist).

---

## 10. Rollback safety net — reverse CDC (Atlas → DocumentDB)

Same toolkit, same topology, opposite direction: after cutover, run a
**CDC-only** flow that replicates every new write landing on Atlas back to
DocumentDB, so a rollback decision doesn't lose data written after cutover.
Since both sides already hold identical data at cutover, this is
change-stream-only — **no initial bulk copy**.

**Validated live** on the POC run. If the reverse stream misbehaves after
following these steps, go to Part Two §T7 for the diagnostic checklist —
several failure modes here look alike but have very different causes.

### 1. Stop the forward topology
The forward workflow should already be terminated (§9 step 9). Bring the
whole compose stack down before reconfiguring — `temporal`, `worker`, and
`runner` all need to restart with the new `.env`/command args:
```bash
sudo docker compose down
```
This removes containers but preserves the `temporal-data` volume (workflow
history, including the terminated forward run) — do **not** add `-v`.

### 2. Edit `.env` — swap source/destination, use a NEW queue and workflow ID
Never reuse the forward run's `QUEUE`/`WORKFLOW_ID`, even though it's
terminated:
```bash
sudo cp .env .env.forward.bak   # keep the forward config for reference
sudo nano .env
```
```bash
# swap these two — same variable names as the forward config, reversed values
MDB_DEST=mongodb://docdbadmin:<password>@<docdb-cluster-endpoint>:27017/?tls=true&tlsCAFile=/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false
DOCDB_SRC=mongodb+srv://<user>:<password>@<atlas-cluster>.mongodb.net/?retryWrites=true

QUEUE=dsync-<name>-reverse
WORKFLOW_ID=<name>-reverse
```
`retryWrites=false` on the DocumentDB URI is still required regardless of
direction (DocumentDB doesn't support retryable writes) — it's now on the
*destination* side of this config.

**Note:** always re-derive these values by reading the `.env` file directly
when running manual `mongosh` checks later — don't trust a shell's already-
exported `$DOCDB_SRC`/`$MDB_DEST`, which can silently go stale after an edit
like this (see Part Two §T7 item 4).

### 3. Add `--skip-initial-sync` to the `runner` service's command
Not parameterized in the generated template by default — add it manually to
`docker-compose.yml`, in the `run` leg of the chain (before `temporal`):
```yaml
  runner:
    command:
      - run
      - "--workflow-id=${WORKFLOW_ID:?set WORKFLOW_ID in .env}"
      - "--queue-name=${QUEUE:?set QUEUE in .env}"
      - "--namespace=${NAMESPACE:?set NAMESPACE in .env}"
      - --skip-initial-sync   # <-- CDC only, no bulk copy — data is already identical
      - temporal
      - --host-port=temporal:7233
      - app
      - --host-port=0.0.0.0:8080
      - --persist
```

### 4. Bring the stack back up
```bash
sudo docker compose up -d temporal
sudo docker compose up -d --scale worker=<N> worker
sudo docker compose up -d runner
```

### 5. Confirm it started in CDC-only mode, in the right direction
```bash
docker compose exec temporal temporal workflow describe --workflow-id <name>-reverse
```
**Pending Activities should show only `dsync_StreamChanges` /
`dsync_StreamLSN` — no `dsync_InitialSync`.** If you see an `InitialSync`
activity, `--skip-initial-sync` didn't take effect — check the compose file
edit and re-recreate the `runner`.

The dashboard's own **Summary** panel will show `Documents Synced: 0` and
`Namespaces Synced: 0/0` for the entire lifetime of this run — that's
expected and benign for a `--skip-initial-sync` (CDC-only) flow, not a sign
anything is wrong. Those fields track initial-sync progress specifically,
which never runs here; watch the Change Stream Progress table instead (§T7
item 3 for how to read it).

### 6. Live-fire sanity check — confirm direction and delivery
Write to Atlas (now the source) and confirm it lands on DocumentDB (now the
destination). Use a small test collection (e.g. `coll_cdctest`), not a
large production one, so the check stays fast:
```bash
mongosh "<Atlas URI>" --eval 'db.getSiblingDB("<db>").coll_cdctest.insertOne({reverseMarker: "check1", ts: new Date()})'
# wait, then poll:
mongosh "<DocumentDB URI>" --eval 'db.getSiblingDB("<db>").coll_cdctest.findOne({reverseMarker: "check1"})'
```
`workflow describe` should show `EventsRead`/`EventsWritten` climb from 0
and `LastEventTime` update to a real timestamp once the write is processed.
If the marker doesn't show up after a reasonable wait, or the numbers look
inconsistent, go to Part Two §T7 for the full diagnostic checklist before
assuming it's broken — several of these symptoms are misleading.

### When to tear this down
Once the rollback window has passed and you're confident in the forward
cutover, terminate this workflow and bring the stack down the same way as
§9 step 9 — there's no ongoing need for it past the rollback decision point.

---

## Appendix — full command index (copy-paste order for a fresh production run)

```bash
# --- Pre-flight (source) ---
mongosh "$DOCDB_SRC" --eval 'printjson(db.getSiblingDB("admin").runCommand({ modifyChangeStreams: 1, database: "", collection: "", enable: true }))'
aws docdb modify-db-cluster-parameter-group --db-cluster-parameter-group-name <pg> --parameters ParameterName=change_stream_log_retention_duration,ParameterValue=604800,ApplyMethod=immediate
fixIdTypes --mode detect --config /opt/docdb-migration/configs/fixIdTypes.config.sample.json
checkChangeStreams

# --- Configure ---
cd /opt/docdb-migration/compose
cp .env.sample .env   # edit DOCDB_SRC, MDB_DEST, QUEUE, WORKFLOW_ID
sed -i '/--namespace=\${NAMESPACE/d' docker-compose.yml   # only if migrating ALL databases

# --- Launch ---
docker compose up -d temporal
docker compose up -d --scale worker=3 worker
docker compose up -d runner

# --- Monitor (repeat throughout) ---
docker compose exec temporal temporal workflow describe --workflow-id <WORKFLOW_ID>

# --- Create destination indexes (once CDC backlog ~0, BEFORE stopping writes — §8) ---
migrateIndexes --config migrateIndexes.config.json --mode=create --dry-run=false

# --- Cutover ---
# (stop source writes externally)
docker run --rm --network compose_default -e DSYNCT_MODE=simple dsynct:enterprise verify --report-limit 50 --parallelism 8 --skip-change-stream "$DOCDB_SRC" "$MDB_DEST"
fullCountVerify.sh                                                          # count parity, source vs destination
migrateIndexes --config migrateIndexes.config.json --mode=verify           # index parity (expect PENDING pre-rectify)
migrateIndexes --config migrateIndexes.config.json --mode=rectify --dry-run=false
migrateIndexes --config migrateIndexes.config.json --mode=verify           # expect PASS now
# (switch app to Atlas)
docker compose exec temporal temporal workflow terminate --workflow-id <WORKFLOW_ID> --reason "cutover complete"
```

---

# PART TWO — Troubleshooting, Tuning & Reference

Nothing in this part is a required step in a normal run — come here when
Part One doesn't behave as described, you need to tune throughput, or you
want the full CLI flag reference.

## T1. CDC architecture and tuning — lessons learned (validated live on the POC)

### A single change stream is pinned to ONE worker container — scaling worker
### *count* does not speed up CDC once initial sync is done

The dashboard's Change Stream Progress table showed a single row
(`Index 0, Namespaces: All`) — Temporal assigns that one `dsync_StreamChanges`
activity to exactly one worker process. Confirmed via `docker stats`: with 6
worker replicas running, 5 sat at **0.00% CPU** while the one holding the
activity ran at **300%+ CPU** (3 cores). Scaling `docker compose up -d --scale
worker=N` only helps during initial sync (parallelizable across many doc
partitions); it does **nothing** for change-stream throughput once initial
sync is 100% complete. Don't waste time adding more worker replicas to fix a
CDC bottleneck — check the CPU on the specific container actually running the
stream activity instead (`docker stats`, cross-referenced against
`temporal workflow describe` to see which `identity`/worker owns
`dsync_StreamChanges`).

### What actually moves CDC throughput

Levers that scale the *writer* side of a single change stream (edit in
`.env` / the `worker` service's `command:`, then
`docker compose up -d --force-recreate worker` — this resumes from the
stored heartbeat/cursor, it does not restart from zero):
```
--per-stream-workers=<N>              # default 4 — writer pool for this stream
--stream-writer-max-batch-size=<N>    # not in the generated compose template by
                                       # default — add it manually; default 0 (unbounded)
--stream-buffer-size=<N>              # default 0
--stream-worker-buffer-size=<N>       # default 100
```
**Rule of thumb for `--per-stream-workers`:** don't exceed the host's core
count (`nproc`). Oversubscribing threads on an already CPU-bound process adds
scheduling overhead without adding real parallelism — check `docker stats`
for the owning container's CPU% first. On the POC host (16 cores), the
change-stream worker was only using ~3 cores (300% CPU) even at
`--per-stream-workers=12`, meaning host CPU was NOT the bottleneck — see
below for where the real ceiling was.

### If tuning worker-side flags doesn't move throughput, look downstream —
### the bottleneck may be Atlas write capacity or DocumentDB read capacity,
### not dsync configuration at all

Live example: bumping `--per-stream-workers` 4→12 and adding
`--stream-writer-max-batch-size=1000` did NOT increase throughput (stayed
flat around 18k ops/sec, down slightly from an earlier 36k ops/sec peak)
despite the owning worker container having 13 idle cores of headroom. When
host CPU is not saturated but throughput won't climb, check:
- **Atlas Metrics** (source of the actual write ceiling): `Opcounters`
  (insert/update rate), `Normalized System CPU`, disk IOPS — if these are
  flat/saturated regardless of dsync-side tuning, you need a bigger Atlas
  tier, not more dsync tuning.
- **DocumentDB Console → Monitoring**: `CPUUtilization`, `ReadIOPS` — a busy
  source can throttle change-stream read-ahead independent of anything on
  the dsync/EC2 side.
- **Network**: `iftop`/`nload` on the migration host, in case the EC2
  instance's NIC is saturated between the host and both clusters.

### Reading the Change Stream Progress table correctly

| Column | Meaning |
|---|---|
| `Events Read` − `Events Written` | current backlog. Growing = destination writer falling behind; shrinking/stable = healthy. |
| `Latency` | gap between an event being read and being written — watch it trend down, not just its absolute value. |
| `Throughput` | ops/sec **for the current interval only** — 0 ops/sec when caught up simply means no new source writes are landing right now, not that the stream died. |
| `Last Event` | wall-clock timestamp of the most recently processed source event — compare to `date -u` on the host. Within a few seconds = live and caught up. Stuck/old while source is writing = silently stalled resume token (see §2), OR see §T7 item 3 for another common cause of an apparently-frozen `Last Event`. |

**Cutover-ready signature, observed live:** `Events Read == Events Written`
(zero backlog), `Latency: 0`, `Throughput: 0 ops/sec` (nothing new to
process), `Last Event` timestamp matching wall-clock now. Confirm this isn't
a false "quiet" reading by checking whether the load generator/application
writes have actually stopped (if using Locust for a load test, check the
process is still alive — a plateaued dashboard with 0 ops/sec can equally
mean the source-side write generator itself has stopped or errored out).

### CDC writes may show as "update" ops downstream even though the source is
### 100% inserts — this is expected, not a correctness problem

CDC replication tools commonly apply destination writes as **upsert**
(`update` with `upsert: true`) rather than raw `insert`, because change
streams deliver at-least-once — a redelivered event must be idempotent, and
upsert absorbs a duplicate delivery without a duplicate-key error. This means
Atlas's `Opcounters`/Real Time metrics can show `UPDATE` ops climbing even
when the source workload (e.g. a Locust `insert_many` load generator) is
doing nothing but inserts with fresh `ObjectId()`s.

**Confirmed live via `currentOp` on the Atlas destination** — filter by
`appName: "dsync"` to isolate dsync's own connections from application
traffic:
```bash
mongosh "$MDB_DEST" --eval 'db.getSiblingDB("admin").currentOp({"appName": "dsync"})'
```
Sample real output captured during active CDC: `appName: "dsync"`,
`driver.name: "mongo-go-driver"`, `ns: "load_test_db.$cmd"`,
`query.update: "load_test_coll"`, `query.ordered: false` — i.e. **during
CDC, dsync is issuing a batch `update` command (unordered) against the
collection, not raw `insert`.** This is expected/idempotent-upsert behavior,
not a correctness problem — seeing `UPDATE` climb in Atlas `Opcounters`
while the source is 100% inserts is normal during CDC.

---

## T2. Handling paused activities

Worker command includes `--pause-on-error`, and `--attempts-before-pause`
defaults to `0` — **the first error on any activity pauses it immediately,
with no auto-retry.** This is intentional (stop-and-inspect on real errors),
but it also fires on routine worker restarts/kills (`context canceled`).

Check what's currently paused (don't chase stale IDs from old log lines —
they may already be rescheduled under new IDs):
```bash
docker compose exec temporal temporal activity list --query 'TemporalPauseInfo IS NOT NULL'
```

Clear all currently-paused activities in one shot:
```bash
docker compose exec temporal temporal activity unpause \
  --query 'TemporalPauseInfo IS NOT NULL' --yes \
  --reset-attempts --reset-heartbeats
```

Or target a single activity by ID (get the current ID from `activity list`
above first — don't reuse an ID from an old log line, it may have been
rescheduled):
```bash
docker compose exec temporal temporal activity unpause \
  --workflow-id <WORKFLOW_ID> --activity-id <ID> \
  --reset-attempts --reset-heartbeats
```

### Silent pause via the dsynct web dashboard (`:8080`) — validated live, cost significant debugging time

The dsynct progress dashboard's **Actions** menu on each Change Stream
Progress row includes a Pause action. Clicking it (even accidentally, or
from an earlier session) sets `TemporalPauseInfo` on that activity and
**freezes `Events Read`/`Events Written`/`Last Event` completely** — but the
workflow still shows `State: Running` in the dashboard summary and produces
**no error**. `Read Ahead`/`Latency` (driven by the separate `dsync_StreamLSN`
polling activity) keep climbing normally the whole time, which makes a
paused `dsync_StreamChanges` activity look deceptively like a slow
backlog-drain in progress rather than a fully stopped pipeline.

**How to tell the difference:** compare two `workflow describe` snapshots a
few minutes apart.
- **Genuinely slow (not paused):** `EventsRead`/`EventsWritten` keep
  climbing, even slowly.
- **Silently paused:** `EventsRead`/`EventsWritten` are byte-for-byte
  identical across snapshots, `LastHeartbeatTime` stops advancing, and
  `SearchAttributes` in `workflow describe`'s top section shows
  `TemporalPauseInfo` populated (not `null`) — this is the definitive tell,
  check it before spending time on oplog-size/OOM/backlog theories:
  ```bash
  docker compose exec temporal temporal workflow describe --workflow-id <WORKFLOW_ID> \
    | grep -A2 TemporalPauseInfo
  ```
Fix is the same `activity unpause` command above. Also check
`docker compose logs worker` for a `RecordActivityHeartbeat with error ...
Error="activity paused"` line — that pinpoints the exact moment and activity
ID the pause took effect.

---

## T3. Enabling debug logs (chained "app" mode)

`worker` and `runner` run in dsynct's **chained "app" mode** (no
`DSYNCT_MODE=simple`) — this exposes a completely different CLI surface than
the "simple" mode used for `verify`/`sync`/etc. in §7. In chained mode,
`--log-level` is NOT a top-level global flag — it belongs to the `app`
sub-command specifically, which appears at the *end* of the chain in both
`worker` and `runner`'s `command:` list. Applies identically whether you're
running the forward config or the reverse-CDC config from §10 — same
mechanism either way.

**Wrong** (fails with `flag provided but not defined: -log-level`):
```yaml
command:
  - --log-level=DEBUG   # top-level position — rejected
  - worker
  ...
```

**Right** — add it to the `app` leg of the chain:
```yaml
  worker:
    command:
      - worker
      - "--queue-name=${QUEUE:?set QUEUE in .env}"
      - "--concurrent-activities=${CONCURRENT_ACTIVITIES:-4}"
      - "--sync-writer-workers=${SYNC_WRITER_WORKERS:-8}"
      - "--per-stream-workers=${PER_STREAM_WORKERS:-4}"
      - --pause-on-error
      - "${DOCDB_SRC:?set DOCDB_SRC in .env}"
      - "--doc-partition=${DOC_PARTITION:-500000}"
      - "--namespace-fanout=${NAMESPACE_FANOUT:-100}"
      - "--documentdb-sampling-fanout=${DOCUMENTDB_SAMPLING_FANOUT:-100}"
      - "${MDB_DEST:?set MDB_DEST in .env}"
      - temporal
      - --host-port=temporal:7233
      - app
      - --log-level=DEBUG   # <-- here, after `app`
      - --no-progress

  runner:
    command:
      - run
      - "--workflow-id=${WORKFLOW_ID:?set WORKFLOW_ID in .env}"
      - "--queue-name=${QUEUE:?set QUEUE in .env}"
      - "--namespace=${NAMESPACE:?set NAMESPACE in .env}"
      - temporal
      - --host-port=temporal:7233
      - app
      - --log-level=DEBUG   # <-- here, after `app`
      - --host-port=0.0.0.0:8080
      - --persist
```
Apply and tail:
```bash
docker compose up -d --force-recreate worker runner
docker compose logs -f worker
```

`DEBUG` surfaces useful internals not shown at `INFO`, e.g. the live change-stream
namespace filter regex:
```
DEBUG Change stream namespace filter: {"$and":[{"ns.db":{"$regex":{"$regularExpression":{"pattern":"^(?!local$|config$|admin$|adiom-internal$)","options":""}}}},{"ns.coll":{"$regex":{"$regularExpression":{"pattern":"^(?!system.)","options":""}}}}]}
```
It does **not**, however, surface per-write operation type or a stalled/paused
state any more clearly than `workflow describe` does — for diagnosing a
stuck/paused stream, the check in §T2 (`TemporalPauseInfo` +
`"activity paused"` in the logs) is the one that actually matters.

Docker's own on-disk log file backing `docker compose logs` (useful for `grep`/
long-term retention regardless of dsynct's own verbosity):
```bash
sudo cat /var/lib/docker/containers/$(docker compose ps -q worker)/*-json.log | jq -r '.log'
```

---

## T4. Full CLI reference — chained "app" mode (`worker` / `runner` / `temporal` / `app`)

This is the mode actually used by the running topology (no `DSYNCT_MODE=simple`).
Captured live via `docker run --rm dsynct:enterprise <subcommand> --help`.

### `worker` — registers source→destination sync flow workers with Temporal
```
USAGE: dsynct worker [command options] source [source options] destination [destination options] [transformer] [transformer options]

--queue-name value                       Temporal queue name (default: "dsync")
--transform                              set if a transformer follows source+destination (default: false)
--concurrent-activities value            concurrent Temporal activities for this worker (default: 4)
--sync-transform-workers value           workers for transformer during initial sync (default: 1)
--sync-writer-workers value              workers writing to destination during initial sync (default: 1)
--per-stream-workers value               workers writing to destination per change stream (default: 1)
--stream-writer-max-batch-size value     max batch size for change stream worker (default: 0)
--buffer-size value                      reader -> writer/transformer buffer size, initial sync (default: 0)
--transformer-buffer-size value          transformer -> writer buffer size (default: 0)
--stream-buffer-size value               change stream buffer size (default: 0)
--stream-worker-buffer-size value        per-stream-worker buffer size (default: 100)
--heartbeat-interval value               heartbeat/progress interval to Temporal (default: 20s)
--stream-update-interval value           stream cursor progress update interval (default: 15s)
--src-data-type value                    source data type, inferred if unset
--dst-data-type value                    destination data type, inferred if unset
--namespace-mapping value [...]          fully-qualified namespace mapping src -> dst, applied before transform
--ignore-write-failures                  ignore write failures instead of failing the activity (default: false)
--pause-on-error                         pause the Temporal activity on error instead of auto-retry (default: false)
--attempts-before-pause value            attempts before pausing, if --pause-on-error set (default: 0)
--mapping-delimiter value                delimiter for namespace mappings (default: ":")
```

### `run` — submits a flow execution to Temporal and monitors progress (the `runner` service)
```
USAGE: dsynct run [command options]

--workflow-id value                identifies a unique resumable flow execution
--queue-name value                 Temporal queue (default: picks up worker's queue if unset; else "dsync")
--initial-sync-queue-name value    override queue for initial-sync tasks
--stream-queue-name value          override queue for streaming activities
--namespace value [...]            namespaces to execute the flow for (repeatable)
--skip-initial-sync                (default: false)
--skip-change-stream               (default: false)
--heartbeat-timeout value          Temporal heartbeat timeout — must exceed worker's --heartbeat-interval (default: 1m0s)
--terminate-existing               terminate any existing execution with same workflow-id, then start fresh (default: false)
--cancel-existing                  cancel any existing execution with same workflow-id, then start fresh (default: false)
--progress-check-interval value    interval to poll Temporal for heartbeat progress (default: 9s)
--progress-publish-interval value  interval to push collected progress (default: 10s)
--max-partitions value             split initial sync into multiple workflows above this partition count (default: 0, unlimited)
```

### `app` — configure app runtime (progress server, logging, otel) — appears once per chain, after `temporal`
```
USAGE: dsynct app [command options]

--log-level value               (default: "INFO")   <-- set to DEBUG here for verbose logs
--otel                           export logs/metrics to Otel; needs OTEL_EXPORTER_OTLP_ENDPOINT env (default: false)
--otel-metric-interval value    push interval if --otel set (default: 0s)
--otel-service-name value       service name for otel (default: "dsynct")
--pprof-host-port value         host:port to expose pprof
--host-port value                address for the web progress dashboard (default: "localhost:8080")
--progress-refresh value        interval between progress pushes (default: 1s)
--graceful-exit-timeout value   time allowed for graceful shutdown (default: 15s)
--persist                        keep progress server running after runs complete (default: false) — used by runner
--no-progress                    do not run a progress server at all (default: false) — used by worker, since the runner already serves :8080
```

### `temporal` — configure the Temporal connection (once per chain)
```
USAGE: dsynct temporal [command options]

--host-port value   address of the Temporal instance
--namespace value   Temporal namespace to use (default: "default")
```

Note: this chained-mode CLI is distinct from the **"simple" mode**
(`-e DSYNCT_MODE=simple`) used for one-shot commands like `verify`,
`sync`, `sample-ids`, `verify-ids`, `connectors` — see §7 for those, and note
that in simple mode `--log-level` IS a genuine top-level global flag (it sits
alongside `--otel`, `--host-port`, `--pprof-host-port` at the root, not nested
under `app`).

---

## T5. Recovering from an EC2 host reboot/restart

**Validated live** on the POC run — an unplanned host reboot mid-migration
recovered on its own with no manual steps:

```bash
docker compose ps   # confirm everything came back Up (not Restarting)
docker compose exec temporal temporal workflow describe --workflow-id <WORKFLOW_ID>
```

Expect to see `LastFailure: activity Heartbeat timeout` on `Attempt 2` (or
higher) for the in-flight activities — this is the *expected* consequence of
the whole host going down mid-heartbeat, not a new bug. Both
`dsync_StreamChanges` and `dsync_StreamLSN` resumed automatically once
Docker's `restart: unless-stopped` policy brought the containers back,
continuing from their prior `EventsRead`/`EventsWritten` counts rather than
restarting from zero.

If anything *doesn't* come back automatically:
- `temporal` / `worker` services have `restart: unless-stopped` — should
  self-recover once the Docker daemon restarts (which itself normally
  auto-starts on boot).
- `runner` has `restart: on-failure` — may or may not restart depending on
  whether the container's exit was recorded as clean or a failure during
  shutdown. If it's not `Up`, just: `docker compose up -d runner`.
- Any one-off container started with plain `docker run` (no compose service,
  no restart policy) — e.g. the `docdb-verify` container from §7 — will
  **not** come back on its own. Re-run it manually.
- If `temporal` crash-loops post-reboot with the same
  `SQLITE_CANTOPEN`/permissions error from §4, the volume permission fix
  should have persisted across the reboot (it's a host filesystem change,
  not container state) — but re-check if it recurs:
  ```bash
  sudo ls -la /var/lib/docker/volumes/compose_temporal-data/_data
  ```

Also worth checking after any host-level interruption: if you can't
reconnect via SSH at all afterward, don't assume the instance is down —
`ping`/`nc` failing with **"Network is unreachable"** (as opposed to a
timeout) points to your own client-side routing/network, not the EC2 host.
Confirmed once by pinging `8.8.8.8`/`google.com` successfully while the EC2
IP specifically failed. If the instance's public IP genuinely changed after
a stop/start cycle (no Elastic IP attached), check with:
```bash
aws ec2 describe-instances --instance-ids <instance-id> \
  --query "Reservations[0].Instances[0].{State:State.Name,PublicIP:PublicIpAddress}"
```

---

## T6. Restarting cleanly (stale/dead CDC token, or any full restart)

```bash
# 1. Terminate the existing workflow (only if still Running; a Completed
#    workflow needs no termination step)
docker compose exec temporal temporal workflow terminate \
  --workflow-id <WORKFLOW_ID> --reason "<reason>"

# 2. Drop dsync's internal metadata/progress DB on the DESTINATION — clears
#    any stored resume token so the new run starts genuinely fresh
mongosh "$MDB_DEST" --eval "db.getSiblingDB('adiom-internal').dropDatabase()"

# 3. Resubmit
docker compose restart runner
docker compose logs -f runner
```

Redoing initial sync is always safe — dsync upserts by `_id`, so a restart
never creates duplicates, just re-matches/overwrites what's already there.

**Terminating a workflow does NOT automatically submit a new run — validated
live.** After `workflow terminate`, the `runner` container can sit `Up` for
many minutes with no new workflow started; `run` only submits once per
process lifetime, it doesn't watch for termination and resubmit on its own.
You must explicitly restart/recreate it (step 3 above) to trigger a fresh
submission. Confirm it actually happened by checking for a **new `RunId`**
in `workflow describe`, not just that the container shows `Up`.

**`docker compose restart` vs `docker compose up -d --force-recreate`:**
- `restart` re-runs the existing container's entrypoint as-is — sufficient
  to resubmit after a termination when nothing in `.env`/`docker-compose.yml`
  changed.
- If you changed `.env` or the compose file (new tuning flags, a new
  `--skip-initial-sync`, etc.) in between, `restart` will **not** pick up
  those changes — it doesn't reread either file. Use
  `docker compose up -d --force-recreate <service>` instead, which rebuilds
  the container from the current `.env`/compose file state.

---

## T7. Reverse CDC (§10) — known issues and diagnostic checklist (validated live)

Debugging a stalled/misbehaving reverse stream is easy to get wrong because
several symptoms look identical on the dashboard but have completely
different causes and fixes. **Check these in this order** — the first one is
by far the most common and cheapest to rule out.

### 1. Check for a silent pause FIRST, before any backlog/OOM/oplog theory

The dsynct web dashboard's **Actions** menu on each Change Stream Progress
row includes a Pause action. Clicking it (even accidentally, from an
earlier session, or muscle memory from clearing a *different* paused
activity) sets `TemporalPauseInfo` on that activity and **freezes `Events
Read`/`Events Written`/`Last Event` completely** — but the workflow still
shows `State: Running` in the dashboard summary and produces **no error**.
Meanwhile `Read Ahead`/`Latency` (driven by the separate `dsync_StreamLSN`
polling activity, which keeps running independently) continue climbing
normally the whole time. This makes a fully-stopped `dsync_StreamChanges`
activity look exactly like a slow-but-alive backlog drain — the two are
easy to confuse and this one cost significant debugging time chasing
oplog-size/OOM theories before the real cause (a stale pause) was found.

**The definitive check:**
```bash
docker compose exec temporal temporal workflow describe --workflow-id <WORKFLOW_ID> \
  | grep -A2 TemporalPauseInfo
```
If it shows a populated value (not `null`/empty) instead of
`data:"null"`, the activity is paused. Also grep worker logs for the exact
moment and activity it happened to:
```bash
docker compose logs worker | grep "activity paused"
```
Fix — unpause and let it resume from where it left off (same command as §T2):
```bash
docker compose exec temporal temporal activity unpause \
  --workflow-id <WORKFLOW_ID> --activity-id <ID> \
  --reset-attempts --reset-heartbeats
```

### 2. Fresh workflow restarts can still hit CappedPositionLost/ChangeStreamHistoryLost repeatedly

Not just a stale-token problem (§T6) — if the source's oplog contains a very
large *recent* volume (e.g., right after a heavy bulk-load burst), even a
**brand-new** workflow run can fail immediately on `dsync_StreamLSN`/
`dsync_StreamChanges` with the same errors, because the position it tries
to resume from is already being evicted from the capped oplog by the sheer
turnover rate. Symptom in worker logs: repeated `Resuming from heartbeat
details` → `Failed to open change stream: (ChangeStreamHistoryLost)...`
across many attempts, sometimes 10-30+ in a row, before either succeeding
or exhausting into a paused state.
- **Fix that worked live:** terminate and restart (`docker compose exec
  temporal temporal workflow terminate ...` then
  `sudo docker compose up -d --force-recreate runner`). It sometimes takes
  more than one restart attempt before it lands on a resume position that
  survives long enough to establish a stable stream.
- If this recurs repeatedly, increase the source's oplog size/window
  (Atlas UI → Cluster → Configuration → Additional Settings) — a bigger
  oplog gives more slack before a slow-to-establish stream's starting
  position gets evicted.

### 3. Understand what each Change Stream Progress column actually measures — don't extrapolate the wrong one

Confirmed against Adiom's own docs (`dsynct.read_ahead_gauge`,
`dsynct.last_event_time`, `dsynct.written`):

| Column | What it actually means | How to (mis)read it |
|---|---|---|
| `Read Ahead` | The **source's own current LSN** (log position), reported by a separate `dsync_StreamLSN` polling activity. Not a cumulative "events scanned" counter, and independent of whether the reader/writer is stuck, alive, or paused. | A climbing `Read Ahead` alone proves NOTHING about whether data is actually being written — don't use it to conclude the pipeline is healthy. |
| `Events Read` / `Events Written` | The real, trustworthy progress counters. | **This is the one to trust.** Compare across two `workflow describe` snapshots a few minutes apart — both climbing = genuinely alive, regardless of what any other column shows. |
| `Throughput` | An instantaneous rate at one heartbeat/dashboard poll, not a cumulative average. | Can legitimately read `0` for one snapshot even on a healthy, actively-writing stream. Never conclude "stalled" from a single `0` reading — diff `EventsWritten` across two checks first. |
| `Last Event` | Timestamp of the *specific event currently being processed* — not wall-clock now. | Can appear **frozen for a long time** (observed: stuck within the same ~4-second window for over an hour, while `EventsWritten` climbed past a million) if the source has an extremely dense cluster of same-timestamp events, typical after a high-concurrency bulk-load burst. Not stuck — that one burst is just enormous. Use `Read Ahead`'s growth *rate* slowing/plateauing as the "approaching real-time" signal instead. |
| `TemporalPauseInfo` (in `workflow describe`'s `SearchAttributes`, not the dashboard table) | Whether the activity has been paused via the dashboard's Actions menu or CLI. | **Check this first, before any of the above** — see item 1. `null`/empty = not paused. |

### 4. Don't trust `opcounters`/shell env vars without re-verifying the connection

`db.serverStatus().opcounters` compared across two `mongosh` calls that
*look* like they hit the same cluster can report wildly different absolute
numbers if the two commands actually connected to different clusters
(stale/incorrectly-exported `$DOCDB_SRC`/`$MDB_DEST` in the shell,
unrelated to what's actually in `.env` — see §10 step 2's note on always
re-deriving from the file). **The decisive, unambiguous test is a
tag-and-poll**, not opcounters:
```bash
# 1. Insert a uniquely tagged marker into the SOURCE (paste the literal
#    connection string, don't rely on a shell variable you haven't just
#    re-verified against the .env file)
mongosh "<source URI>" --eval 'db.getSiblingDB("<db>").<small-coll>.insertOne({reverseMarker: "check1", ts: new Date()})'

# 2. Poll the DESTINATION directly for it
watch -n 15 'mongosh "<dest URI>" --quiet --eval "db.getSiblingDB(\"<db>\").<small-coll>.findOne({reverseMarker: \"check1\"})"'
```
Use a small test collection (e.g. `coll_cdctest`), not a multi-hundred-
million-document production collection, so the poll query itself stays fast.

---

## T8. `verify` memory pressure — real incident, and a lighter-weight alternative

### The incident

On a shared `m5.4xlarge` host running `verify` against a ~1TB collection
**alongside 10 active dsync worker containers**, `verify` OOM-killed
`sshd` — **twice** — locking out SSH access to the host both times (once
with `--report-all`, and again after switching to the supposedly lighter
`--report-limit`). Atlas-side `iowait` also climbed to ~30% during the run.

**Root cause:** `verify`'s Merkle Search Tree comparison holds a hash+ID
entry per document across the **entire dataset**, regardless of
`--report-limit`/`--report-all` — those flags only control how many
*mismatches* get printed, not the underlying tree's memory footprint. On a
1TB/hundreds-of-millions-of-document collection, that tree competes directly
with Temporal + the worker fleet for host memory, and can starve/kill
unrelated host processes like `sshd`.

### Remediations, in order of effectiveness

1. **Run `verify` on a separate, dedicated host** — not the same instance
   running the live migration workers. This is the most reliable fix; it
   removes the resource contention entirely.
2. **Cgroup-limit the container** if it must share a host:
   ```bash
   docker run --rm --memory=8g --memory-swap=8g --network compose_default \
     -e DSYNCT_MODE=simple dsynct:enterprise verify ...
   ```
3. **Chunk the workload** with `--total-partitions`/`--partition` (§7) —
   bounds memory per run, at the cost of needing multiple sequential runs.
4. **Lower `--parallelism`** — fewer concurrent comparison workers held in
   memory at once.

### Is there a count-only/quick-check mode to avoid this entirely?

**Confirmed live (via direct `--help` inspection of the real Enterprise
`dsynct` binary's `verify`, `sync`, and `connectors` subcommands): no.**
`--verify-quick-count` exists on the **OSS** `dsync` tool but is **not**
present on Enterprise `dsynct` at all. Don't spend time looking for it on
Enterprise.

### Enterprise-native bounded-memory alternative: `sample-ids` + `verify-ids`

Instead of a full Merkle-tree comparison, reservoir-sample a fixed number of
IDs from the source, then verify only those — memory is bounded by the
sample size, not the collection size:
```bash
# 1. Sample N random IDs from the source
docker run --rm --network compose_default -e DSYNCT_MODE=simple \
  -v "$(pwd):/out" dsynct:enterprise \
  sample-ids --namespace <db>.<coll> --count 10000 --output /out/sample-ids.jsonl \
  "$DOCDB_SRC"

# 2. Verify just those sampled IDs match on both sides
docker run --rm --network compose_default -e DSYNCT_MODE=simple \
  -v "$(pwd):/out" dsynct:enterprise \
  verify-ids --namespace <db>.<coll> --id-file /out/sample-ids.jsonl \
  "$DOCDB_SRC" "$MDB_DEST"
```
This trades exhaustiveness for safety — it won't catch every possible
mismatch the way a full `verify` pass does, but it's a reasonable
lower-risk sanity check on a host that can't safely run a full pass, or as
a quick spot-check between full `verify` runs.
</content>
