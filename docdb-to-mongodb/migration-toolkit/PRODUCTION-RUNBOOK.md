# DocumentDB → Atlas Migration Runbook (dsync Enterprise + Temporal)

Consolidated from the POC run against `maruti-poc-docdb-cluster`. Every command
here was actually executed and validated during that run — including the
mistakes, which is why the pre-flight section is non-negotiable.

Fill in placeholders (`<...>`) before running anything. Treat this file as
confidential once filled in — it will contain connection strings/credentials.

---

## 0. One-time host setup

Already done if you followed `install-docdb-migration-toolkit.sh`. Confirms
Docker, dsynct, and helper tools are present:

```bash
which dsynct mongosh jq fixIdTypes migrateIndexes checkChangeStreams copyMissingDocs
docker compose version
docker images | grep -E "dsynct|temporal"
```

---

## 1. Pre-flight on the DocumentDB source — DO NOT SKIP

**This is the step that broke the POC run for over an hour.** Change streams
were never enabled, dsync gave no error, CDC silently did nothing while
looking perfectly healthy. Confirm all of this *before* starting any sync.

### 1.1 Enable change streams cluster-wide

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

### 1.2 Set change-stream retention to 7 days (covers a multi-day initial sync)

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

### 1.3 Prove change streams actually work (don't trust step 1.1's `{ok:1}` alone)

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
`modifyChangeStreams has not been run...`, step 1.1 did not actually take —
go back and confirm with `printjson()` wrapping, not bare `use admin`.

### 1.4 Confirm `_id` types are clean (mandatory — dsync fails the whole flow plan on mixed types)

```bash
fixIdTypes --mode detect --config /opt/docdb-migration/configs/fixIdTypes.config.sample.json
# every collection must report [CLEAN] before proceeding
```

### 1.5 Confirm change-stream config via the toolkit's own checker

```bash
checkChangeStreams
```

---

## 2. Configure the migration

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
| `8080` | `runner` | dsynct progress dashboard | Only if you want to view the React dashboard in a browser (note: it has no simple curl-able API — see §4, use `temporal workflow describe` for scripted monitoring instead). |

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

## 3. Bring up the topology

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

## 4. Monitor progress

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
  token — see §1). This is the same silent-stall failure the base plan warns
  about; it produces zero error output.
- **Positive confirmation CDC is genuinely healthy:** run `workflow describe`
  twice, ~30s apart, and compare. `EventsRead`/`EventsWritten` climbing and
  the `Cursor` value changing between the two checks means CDC is actively
  processing live writes — this is the real proof, not just the absence of a
  frozen cursor. (Validated live: after an unplanned EC2 reboot, `Attempt 2`
  with `LastFailure: activity Heartbeat timeout` on both `dsync_StreamChanges`
  and `dsync_StreamLSN` appeared — expected, not a bug, since the whole host
  went down mid-heartbeat. Both auto-resumed on their own once Docker's
  `restart: unless-stopped` policy brought the containers back, continuing
  from their prior `EventsRead`/`EventsWritten` counts rather than restarting
  from zero.)

To watch visually instead: SSH-tunnel port 8080 to your laptop and use a real
browser (the dashboard's JS makes its own internal calls that `curl` can't
replicate):
```bash
ssh -i <key.pem> -L 8080:localhost:8080 ec2-user@<host> -N
# then open http://localhost:8080
```
Temporal's own Web UI at `:8233` works normally over the same kind of tunnel
or directly if the security group allows it.

---

## 5. Handling paused activities

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
**unless `--pause-on-error` triggers first**, in which case see §5.

---

## 6.1 Recovering from an EC2 host reboot/restart

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
  no restart policy) — e.g. the `docdb-verify` container from §8 — will
  **not** come back on its own. Re-run it manually.
- If `temporal` crash-loops post-reboot with the same
  `SQLITE_CANTOPEN`/permissions error from §3, the volume permission fix
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

## 7. Restarting cleanly (stale/dead CDC token, or any full restart)

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

---

## 8. Verification

`verify` runs in `simple` mode (no Temporal) and, by default, **runs
indefinitely** — it verifies the initial-sync data and then keeps tailing and
verifying CDC forever unless told otherwise.

**Run this only once initial sync is confirmed complete.** Running `verify`
while dsync's own initial sync is still copying data produces enormous
false-positive mismatch counts (validated live: 30-50 million "mismatches"
during an active copy, decreasing over time as the destination caught up) —
those aren't real, just a timing artifact of comparing against a
still-changing destination. Confirm the workflow is in `dsync_StreamChanges`
(§4) before trusting any verify output.

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
reboot (see §6.1) and must be re-launched manually if it exits. Check on it
with `docker logs --tail 50 docdb-verify` / `docker ps -a | grep docdb-verify`.

---

## 9. Cutover sequence

A full `verify` pass over a large collection takes hours, not minutes — it
does not fit inside the actual cutover window. **Start it well before
cutover is planned** (§8), let it run to completion in the background, and
only schedule/execute cutover once it has finished with zero mismatches.
Don't start a fresh `verify` run as part of the cutover steps themselves.

1. Start (or confirm already running) the background `verify` from §8, with
   enough lead time before the planned cutover for a full pass to complete.
2. Once both `src` and `dst` show `Tasks=X/X` (fully scanned — see §4/§8 for
   why partial-pass output isn't trustworthy) and `mismatchCount` has
   settled at `0`, the data is confirmed consistent. This is your green
   light to schedule the actual cutover.
3. Stop writes on DocumentDB (app-level maintenance mode, or block at the
   security-group level).
4. Confirm CDC lag has drained to 0 — check `workflow describe`'s
   `dsync_StreamChanges` heartbeat repeatedly; `EventsRead` should stop
   climbing and the cursor should stabilize once no new writes occur.
5. Restore unique indexes and correct TTL settings:
   ```bash
   migrateIndexes --config <config.json> --mode=rectify --dry-run=false
   ```
6. Switch the application's connection string to Atlas.
7. Cancel/terminate the Temporal workflow once cutover is confirmed stable.
8. Re-enable Atlas backups (disabled during migration per the target-prep checklist).

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

# --- Cutover ---
# (stop source writes externally)
docker run --rm --network compose_default -e DSYNCT_MODE=simple dsynct:enterprise verify --report-limit 50 --parallelism 8 --skip-change-stream "$DOCDB_SRC" "$MDB_DEST"
migrateIndexes --config <config.json> --mode=rectify --dry-run=false
# (switch app to Atlas)
docker compose exec temporal temporal workflow terminate --workflow-id <WORKFLOW_ID> --reason "cutover complete"
```
