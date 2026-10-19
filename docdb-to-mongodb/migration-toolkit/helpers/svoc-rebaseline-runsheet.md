# SVOC re-baseline runsheet — two flows (hot per-namespace + cold shared)

> INTERNAL ONLY (git-ignored). Based on the T0 / T2 / pause test results
> (`dsync-svoc-learnings.md` §15.4e–§15.4h) and `CDC-TOPOLOGY-PLAYBOOK.md` §4–§8.
> Host `/opt/docdb-migration`, run everything as **root**, and strip CRLF from
> every file you edit on Windows: `sed -i 's/\r$//' <file>`.
> This procedure does not require `svoc-rebase.sh` or `svoc-flows.sh`. It still
> uses the installed `dsync-progress.sh` and `dsync-find-pauses.sh` helpers.
> For this run, the destination database drop has already happened: §2.3 only
> verifies that state and contains no drop command.

## Layout

| Compose project | Dir / env file | Role | Queue | Size |
|---|---|---|---|---|
| `compose` | `compose/.env` | **temporal** (shared) + **hot runner** + **hot sync pool** | `dsync-svoc-hot-sync` | 3 workers × 4 slots |
| `hot-streams` | `compose/.env.hot-streams` | **hot stream pool** (workers only) | `dsync-svoc-hot-streams` | 4 workers × 20 slots (≥ 2 × hot namespace count) |
| `flow-cold` | `compose/.env.cold` | **cold runner + cold pool** (1 shared stream + its initial sync) | `dsync-svoc-cold` | 2 workers × 5 slots (2 for the stream, 8 for sync) |

Workflows: **`svoc-hot`** and **`svoc-cold`** (new IDs, never reuse `prod-svoc`). Dashboards: hot on 8081; cold on `127.0.0.1:8083` (SSH tunnel required).

---

## 0. Pre-checks (before anything is stopped)

```bash
cd /opt/docdb-migration
cat manifest.env 2>/dev/null | grep -E 'DSYNCT_(IMAGE|VERSION|IMAGE_DIGEST)'   # new bundle installed?
grep -c 'PER_NAMESPACE_STREAMS' compose/docker-compose.yml                      # must be > 0 (new compose)
docker run --rm dsynct:enterprise run --help | grep -E 'stream-queue-name|initial-sync-queue-name'
```
- If the compose file is the old one, copy `compose/docker-compose.yml` from the new bundle first. The old `prod-svoc` containers keep running on their already-created config.
- Copy the latest `helpers/` (`dsync-find-pauses.sh` must default to `--reset-attempts` **only**).
- Tell the app teams: **no new collections** from now until cutover (dsync doesn't support collections created during the run).
- Tear down the test: `cd /opt/docdb-migration/cdctest && ./run-cdc-test.sh down`, then drop the scratch database.

## 1. Fresh namespace lists and exact partition (cluster-wide)

```bash
set -euo pipefail
mkdir -p ~/cdc-plan && cd ~/cdc-plan
cd /opt/docdb-migration/compose
DOCDB_SRC_HOST="$(grep '^DOCDB_SRC=' .env | tail -n1 | cut -d= -f2- | sed 's|tlsCAFile=/certs/|tlsCAFile=/opt/docdb-migration/certs/|')"
[[ -n "$DOCDB_SRC_HOST" ]] || { echo 'DOCDB_SRC missing from compose/.env' >&2; exit 1; }
cd ~/cdc-plan
mongosh "$DOCDB_SRC_HOST" --quiet --eval 'db.adminCommand({listDatabases:1}).databases
  .filter(d=>!["admin","local","config"].includes(d.name))
  .forEach(d=>db.getSiblingDB(d.name).getCollectionNames()
  .forEach(c=>print(d.name+"."+c)))' | tr -d '\r' | sort -u > all.txt
[[ -s all.txt ]] || { echo 'source inventory is empty; stop' >&2; exit 1; }
wc -l all.txt                                        # every collection in every DB

# hot = the 30 collections proven in T2. Reuse the verified T2 list already
# in this directory; if it is not that list, copy it from the recorded T2 file.
[[ -s hot.txt ]] || { echo 'copy the verified T2 hot.txt here before continuing' >&2; exit 1; }
# exclusions (only with sign-off, e.g. stale backups) — one per line, may be empty
[[ -e exclude.txt ]] || : > exclude.txt
for f in all.txt hot.txt exclude.txt; do sed -i 's/\r$//' "$f"; done
for f in all.txt hot.txt; do
  grep -q . "$f" || { echo "$f is empty; stop" >&2; exit 1; }
done
for f in all.txt hot.txt exclude.txt; do
  awk 'NF && ($0 !~ /\./ || $0 ~ /[[:space:],]/) { print "invalid namespace: " $0 > "/dev/stderr"; bad=1 } END { exit bad }' "$f" || exit 1
  sort "$f" | uniq -d | grep -q . && { echo "duplicate entries in $f" >&2; exit 1; }
done
sort -u all.txt > all.sorted.txt
sort -u hot.txt > hot.sorted.txt
sort -u exclude.txt > exclude.sorted.txt
comm -23 all.sorted.txt <(cat hot.sorted.txt exclude.sorted.txt | sort -u) > cold.txt
sort -u cold.txt -o cold.txt

comm -12 <(sort -u hot.txt) <(sort -u cold.txt)       # MUST print nothing (no overlap)
sort -u hot.txt cold.txt exclude.txt > covered.txt
diff -u all.sorted.txt covered.txt && echo "COVERAGE OK"
[[ "$(grep -c '\.customer' hot.txt)" -eq 12 ]]       # the customer* group stays together
printf 'hot=%s cold=%s excluded=%s all=%s\n' \
  "$(wc -l < hot.sorted.txt | tr -d ' ')" "$(wc -l < cold.txt | tr -d ' ')" \
  "$(wc -l < exclude.sorted.txt | tr -d ' ')" "$(wc -l < all.sorted.txt | tr -d ' ')"
```

Do not proceed if `diff` reports a mismatch or the overlap command prints a
namespace. `exclude.txt` must contain only explicitly signed-off collections;
it is not a way to hide an unexplained gap. The T2 hot list must still be current
and contain the 12 `customer*` collections as a group.

## 2. Retire `prod-svoc` — POINT OF NO RETURN after step 2.3

```bash
set -euo pipefail
cd /opt/docdb-migration/compose
# 2.1 record the final state
docker compose exec -T temporal temporal workflow describe --workflow-id prod-svoc > ~/cdc-plan/prod-svoc-final.txt
/opt/docdb-migration/helpers/dsync-progress.sh --compose-dir "$PWD" --workflow-id prod-svoc > ~/cdc-plan/prod-svoc-progress.txt || true
# 2.2 query status explicitly; query failure is not proof that the workflow stopped
RUNNING="$(docker compose exec -T temporal temporal workflow list \
  --query "WorkflowId='prod-svoc' AND ExecutionStatus='Running'")"
if grep -Fq 'prod-svoc' <<<"$RUNNING"; then
  docker compose exec -T temporal temporal workflow terminate --workflow-id prod-svoc \
    --reason "re-baseline: hot/cold per-namespace streams"
fi
for _ in $(seq 1 30); do
  RUNNING="$(docker compose exec -T temporal temporal workflow list \
    --query "WorkflowId='prod-svoc' AND ExecutionStatus='Running'")"
  if ! grep -Fq 'prod-svoc' <<<"$RUNNING"; then break; fi
  sleep 2
done
! grep -Fq 'prod-svoc' <<<"$RUNNING" || { echo 'prod-svoc still Running; stop' >&2; exit 1; }
docker compose stop runner worker          # temporal stays up (shared)
```

**2.3 Destination state check (the database drop is already complete).**
This step is read-only. It verifies that every database from `all.txt` and
`adiom-internal` are absent on the destination. Do not run `dropDatabase` for
this re-baseline. If any expected database remains, stop and resolve the state
with the migration owner before proceeding.

```bash
set -euo pipefail
cd /opt/docdb-migration/compose
DEST="$(grep '^MDB_DEST=' .env | tail -n1 | cut -d= -f2-)"
[[ -n "$DEST" ]] || { echo 'MDB_DEST missing; stop' >&2; exit 1; }
DBS_JSON="$(cut -d. -f1 ~/cdc-plan/all.txt | sort -u | jq -Rsc 'split("\n")[:-1]')"
[[ "$DBS_JSON" != '[]' ]] || { echo 'all.txt has no databases; stop' >&2; exit 1; }
mongosh "$DEST" --quiet --eval "
  const want=${DBS_JSON}.concat(['adiom-internal']);
  const have=new Set(db.adminCommand({listDatabases:1,nameOnly:true}).databases.map(x=>x.name));
  const left=want.filter(d=>have.has(d));
  printjson({inScopeStillPresent:left});
  if (left.length) quit(2);"
```

Expected: `inScopeStillPresent` is empty and `mongosh` exits successfully. A
connection/auth/query failure or any listed database is a hard stop. Never
print `DEST` or the URI.

## 3. Pre-flight on the source (runbook §2.1–§2.5)

Run this after the database-empty check and before bringing up workers. It
creates and drops only the temporary `svoc_rebase_probe` source database; do
not run it if that database is an application namespace.

```bash
set -euo pipefail
cd /opt/docdb-migration/compose
DOCDB_SRC_HOST="$(grep '^DOCDB_SRC=' .env | tail -n1 | cut -d= -f2- | sed 's|tlsCAFile=/certs/|tlsCAFile=/opt/docdb-migration/certs/|')"
[[ -n "$DOCDB_SRC_HOST" ]] || { echo 'DOCDB_SRC missing; stop' >&2; exit 1; }

# 3.1 change streams enabled (require ok:1)
mongosh "$DOCDB_SRC_HOST" --quiet --eval '
  printjson(db.getSiblingDB("admin").runCommand(
    {modifyChangeStreams:1,database:"",collection:"",enable:true}))' | tee ~/cdc-plan/change-streams-enable.txt
grep -Eq '"ok" *: *1|ok: 1' ~/cdc-plan/change-streams-enable.txt

# 3.2 verify retention. Set PARAM_GROUP to the actual DocumentDB cluster parameter group.
: "${PARAM_GROUP:?export PARAM_GROUP to the DocumentDB cluster parameter group name}"
RETENTION="$(aws docdb describe-db-cluster-parameters \
  --db-cluster-parameter-group-name "$PARAM_GROUP" \
  --query "Parameters[?ParameterName=='change_stream_log_retention_duration'].ParameterValue | [0]" --output text)"
[[ "$RETENTION" == 604800 ]] || { echo "retention=$RETENTION, expected 604800; stop" >&2; exit 1; }

# 3.3 live stream probe
mongosh "$DOCDB_SRC_HOST" --quiet --eval '
  const p=db.getSiblingDB("svoc_rebase_probe").probe;
  const cs=p.watch(); p.insertOne({probe:new Date()});
  let ev=null, t=Date.now();
  while (!ev && Date.now()-t<30000) { ev=cs.tryNext(); if (!ev) sleep(250); }
  if (!ev) { db.getSiblingDB("svoc_rebase_probe").dropDatabase(); quit(2); }
  print("PROBE OK " + ev.operationType); cs.close();
  db.getSiblingDB("svoc_rebase_probe").dropDatabase();'

# 3.4 _id and change-stream compatibility checks
fixIdTypes --mode detect --config /opt/docdb-migration/configs/fixIdTypes.config.sample.json \
  | tee ~/cdc-plan/fixidtypes-detect.txt
CLEAN="$(grep -c '\[CLEAN\]' ~/cdc-plan/fixidtypes-detect.txt || true)"
TOTAL="$(grep -c . ~/cdc-plan/all.txt)"
[[ "$CLEAN" -eq "$TOTAL" ]] || { echo "fixIdTypes CLEAN=$CLEAN expected=$TOTAL; stop" >&2; exit 1; }
checkChangeStreams

# 3.5 re-list; any inventory change means refresh hot/cold and repeat this pre-flight
mongosh "$DOCDB_SRC_HOST" --quiet --eval 'db.adminCommand({listDatabases:1}).databases
  .filter(d=>!["admin","local","config"].includes(d.name))
  .forEach(d=>db.getSiblingDB(d.name).getCollectionNames()
  .forEach(c=>print(d.name+"."+c)))' | tr -d '\r' | sort -u > ~/cdc-plan/all-recheck.txt
diff -u <(sort -u ~/cdc-plan/all.txt) ~/cdc-plan/all-recheck.txt
printf '%s %s\n' "$(date -u +%s)" \
  "$(cd ~/cdc-plan && sha256sum all.txt hot.txt cold.txt exclude.txt | sha256sum | awk '{print $1}')" \
  > ~/cdc-plan/.preflight-ok
chmod 600 ~/cdc-plan/.preflight-ok
```

`PARAM_GROUP` is required. This block does not change the parameter group; it
stops unless the current value is exactly `604800`. A non-zero command or a
non-empty inventory diff is a hard stop.

## 4. Compose file: runner flags (once, shared by both runners)

```bash
set -euo pipefail
cd /opt/docdb-migration/compose
[[ -f docker-compose.yml ]] || { echo 'docker-compose.yml missing' >&2; exit 1; }
[[ -f docker-compose.yml.bak ]] || command cp -p docker-compose.yml docker-compose.yml.bak
grep -q -- '--max-partitions=500' docker-compose.yml || \
  sed -i '/--namespace=\${NAMESPACE/a\      - --max-partitions=500' docker-compose.yml
grep -q -- '--stream-queue-name' docker-compose.yml || \
  sed -i '/--namespace=\${NAMESPACE/a\      - "--stream-queue-name=${STREAM_QUEUE:?set STREAM_QUEUE in .env}"' docker-compose.yml
RUNNER_ARGS="$(awk '/^  runner:/{p=1;next} /^  [[:alnum:]_-]+:/{p=0} p' docker-compose.yml)"
[[ "$(grep -c -- '--max-partitions=500' <<<"$RUNNER_ARGS" || true)" -eq 1 ]]
[[ "$(grep -c -- '--stream-queue-name' <<<"$RUNNER_ARGS" || true)" -eq 1 ]]
grep -A14 '^  runner:' docker-compose.yml
```

The backup is one-time; never overwrite it after editing the compose file.
Both flags must occur exactly once under the runner's `run` arguments.

## 5. Env files

Write `NAMESPACE` with `printf` (the T2 failure was `.` instead of `,` separators):

```bash
set -euo pipefail
cd /opt/docdb-migration/compose
umask 077
[[ -f .env ]] || { echo '.env missing' >&2; exit 1; }
if [[ ! -f .env.prod-svoc.bak ]]; then
  grep -qx 'WORKFLOW_ID=prod-svoc' .env || { echo 'no backup and .env is not clearly prod-svoc; stop' >&2; exit 1; }
  command cp -p .env .env.prod-svoc.bak
fi
grep -qx 'WORKFLOW_ID=prod-svoc' .env.prod-svoc.bak || { echo 'backup is not original prod-svoc config; stop' >&2; exit 1; }
chmod 600 .env.prod-svoc.bak
command cp -f .env.prod-svoc.bak .env
chmod 600 .env
rm -f .env.tmp
HOT="$(sed '/^[[:space:]]*$/d' ~/cdc-plan/hot.txt | paste -sd ',' -)"
COLD="$(sed '/^[[:space:]]*$/d' ~/cdc-plan/cold.txt | paste -sd ',' -)"
[[ -n "$HOT" && -n "$COLD" ]] || { echo 'hot/cold list is empty; stop' >&2; exit 1; }

setenv() {
  local f="$1" kv k t rc
  shift
  for kv in "$@"; do
    k="${kv%%=*}"
    t="$(mktemp "${f}.tmp.XXXXXX")"
    chmod 600 "$t"
    if grep -v "^${k}=" "$f" > "$t"; then :; else
      rc=$?; [[ "$rc" -le 1 ]] || { rm -f "$t"; return "$rc"; }
    fi
    printf '%s\n' "$kv" >> "$t"
    chmod 600 "$t"
    command mv -f "$t" "$f" || { rm -f "$t"; return 1; }
  done
  chmod 600 "$f"
}

# HOT flow: runner + sync pool (main project)
setenv .env WORKFLOW_ID=svoc-hot QUEUE=dsync-svoc-hot-sync STREAM_QUEUE=dsync-svoc-hot-streams \
  "NAMESPACE=$HOT" PER_NAMESPACE_STREAMS=true CONCURRENT_ACTIVITIES=4 PER_STREAM_WORKERS=4 \
  ATTEMPTS_BEFORE_PAUSE=3 SKIP_INITIAL_SYNC=false DASHBOARD_PORT=8081

# HOT stream pool (workers only, same source/dest/connector options)
command cp -f .env .env.hot-streams
chmod 600 .env.hot-streams
setenv .env.hot-streams QUEUE=dsync-svoc-hot-streams CONCURRENT_ACTIVITIES=20 TEMPORAL_ADDR=host.docker.internal:7233

# COLD flow: one pool does its single stream + its initial sync
command cp -f .env .env.cold
chmod 600 .env.cold
setenv .env.cold WORKFLOW_ID=svoc-cold QUEUE=dsync-svoc-cold STREAM_QUEUE=dsync-svoc-cold \
  "NAMESPACE=$COLD" PER_NAMESPACE_STREAMS=false CONCURRENT_ACTIVITIES=5 \
  TEMPORAL_ADDR=host.docker.internal:7233 DASHBOARD_PORT=127.0.0.1:8083

sed -i 's/\r$//' .env .env.hot-streams .env.cold
chmod 600 .env .env.hot-streams .env.cold .env.prod-svoc.bak
for f in .env .env.hot-streams .env.cold .env.prod-svoc.bak; do
  [[ "$(stat -c '%a' "$f")" == 600 ]] || { echo "$f must be mode 600" >&2; exit 1; }
done
ns_count() { grep '^NAMESPACE=' "$1" | tail -n1 | cut -d= -f2- | tr ',' '\n' | grep -c .; }
HOT_N="$(grep -c . ~/cdc-plan/hot.txt)"; COLD_N="$(grep -c . ~/cdc-plan/cold.txt)"
[[ "$(ns_count .env)" -eq "$HOT_N" && "$(ns_count .env.hot-streams)" -eq "$HOT_N" ]]
[[ "$(ns_count .env.cold)" -eq "$COLD_N" ]]
docker compose --env-file .env config >/dev/null
docker compose -p hot-streams --env-file .env.hot-streams config >/dev/null
docker compose -p flow-cold --env-file .env.cold config >/dev/null
docker compose up -d temporal
echo "COMPOSE OK; env files mode 600; hot=$HOT_N cold=$COLD_N"
```
Expected namespace counts: `.env` and `.env.hot-streams` = hot count (30).
`.env.cold` = cold count. Never `cat` or `source` these files; they contain credentials.

## 6. Final state gate and bring up

```bash
set -euo pipefail
cd /opt/docdb-migration/compose

# Re-check exact list coverage immediately before launch.
comm -12 <(sort -u ~/cdc-plan/hot.txt) <(sort -u ~/cdc-plan/cold.txt) > ~/cdc-plan/overlap.txt
[[ ! -s ~/cdc-plan/overlap.txt ]] || { cat ~/cdc-plan/overlap.txt; echo 'hot/cold overlap; stop' >&2; exit 1; }
sort -u ~/cdc-plan/hot.txt ~/cdc-plan/cold.txt ~/cdc-plan/exclude.txt > ~/cdc-plan/covered.txt
diff -u <(sort -u ~/cdc-plan/all.txt) ~/cdc-plan/covered.txt
EXPECTED_PREFLIGHT="$(cd ~/cdc-plan && sha256sum all.txt hot.txt cold.txt exclude.txt | sha256sum | awk '{print $1}')"
[[ -s ~/cdc-plan/.preflight-ok ]] || { echo 'pre-flight marker missing; stop' >&2; exit 1; }
read -r PREFLIGHT_AT PREFLIGHT_HASH < ~/cdc-plan/.preflight-ok
NOW="$(date -u +%s)"
[[ "$PREFLIGHT_AT" =~ ^[0-9]+$ && "$PREFLIGHT_HASH" == "$EXPECTED_PREFLIGHT" ]] \
  || { echo 'pre-flight marker is malformed or not for these exact lists; stop' >&2; exit 1; }
(( NOW >= PREFLIGHT_AT && NOW - PREFLIGHT_AT <= 86400 )) \
  || { echo 'pre-flight marker is older than 24 hours; rerun pre-flight' >&2; exit 1; }

# No secrets in output; all env files must be private and all projects parse.
for f in .env .env.hot-streams .env.cold .env.prod-svoc.bak; do
  [[ -f "$f" && "$(stat -c '%a' "$f")" == 600 ]] || { echo "$f missing or not mode 600; stop" >&2; exit 1; }
done
docker compose --env-file .env config >/dev/null
docker compose -p hot-streams --env-file .env.hot-streams config >/dev/null
docker compose -p flow-cold --env-file .env.cold config >/dev/null

# Confirm runner flags in the runner section and adequate permanent stream slots.
RUNNER_ARGS="$(awk '/^  runner:/{p=1;next} /^  [[:alnum:]_-]+:/{p=0} p' docker-compose.yml)"
[[ "$(grep -c -- '--max-partitions=500' <<<"$RUNNER_ARGS" || true)" -eq 1 ]]
[[ "$(grep -c -- '--stream-queue-name' <<<"$RUNNER_ARGS" || true)" -eq 1 ]]
HOT_N="$(grep -c . ~/cdc-plan/hot.txt)"
(( 4 * 20 >= 2 * HOT_N )) || { echo "hot stream slots insufficient for $HOT_N namespaces; stop" >&2; exit 1; }

# Wait for Temporal, then confirm prod-svoc is not Running. Any query error stops here.
HEALTHY=false
for _ in $(seq 1 30); do
  if docker compose exec -T temporal temporal operator cluster health >/dev/null 2>&1; then HEALTHY=true; break; fi
  sleep 2
done
[[ "$HEALTHY" == true ]] || { echo 'Temporal unhealthy; no workers started' >&2; exit 1; }
RUNNING="$(docker compose exec -T temporal temporal workflow list \
  --query "WorkflowId='prod-svoc' AND ExecutionStatus='Running'")"
! grep -Fq 'prod-svoc' <<<"$RUNNING" || { echo 'prod-svoc is still Running; stop' >&2; exit 1; }
for wf in svoc-hot svoc-cold; do
  EXISTING="$(docker compose exec -T temporal temporal workflow list --query "WorkflowId='$wf'")"
  ! grep -Fq "$wf" <<<"$EXISTING" || { echo "$wf already exists; inspect it, do not launch duplicate" >&2; exit 1; }
done

# Read-only proof that the already-dropped destination is still empty.
DEST="$(grep '^MDB_DEST=' .env | tail -n1 | cut -d= -f2-)"
DBS_JSON="$(cut -d. -f1 ~/cdc-plan/all.txt | sort -u | jq -Rsc 'split("\n")[:-1]')"
[[ -n "$DEST" && "$DBS_JSON" != '[]' ]]
mongosh "$DEST" --quiet --eval "
  const want=${DBS_JSON}.concat(['adiom-internal']);
  const have=new Set(db.adminCommand({listDatabases:1,nameOnly:true}).databases.map(x=>x.name));
  const left=want.filter(d=>have.has(d)); left.forEach(d=>print(d));
  if (left.length) quit(2);"

read -r -p 'Confirm source pre-flight passed and destination is empty. Type READY: ' REPLY
[[ "$REPLY" == READY ]] || { echo 'aborted' >&2; exit 1; }

# Temporal is shared and healthy; start worker pools before their runners.
docker compose -p hot-streams --env-file .env.hot-streams up -d --no-deps --scale worker=4 worker
docker compose up -d --no-deps --force-recreate --scale worker=3 worker
docker compose -p flow-cold --env-file .env.cold up -d --no-deps --scale worker=2 worker
docker compose up -d --no-deps --force-recreate runner
docker compose -p flow-cold --env-file .env.cold up -d --no-deps runner

# Confirm both new runs entered Running; on partial startup, inspect, do not retry blindly.
STARTED=false
for _ in $(seq 1 30); do
  HOT_RUNNING="$(docker compose exec -T temporal temporal workflow list --query "WorkflowId='svoc-hot' AND ExecutionStatus='Running'")"
  COLD_RUNNING="$(docker compose exec -T temporal temporal workflow list --query "WorkflowId='svoc-cold' AND ExecutionStatus='Running'")"
  if grep -Fq svoc-hot <<<"$HOT_RUNNING" && grep -Fq svoc-cold <<<"$COLD_RUNNING"; then STARTED=true; break; fi
  sleep 2
done
[[ "$STARTED" == true ]] || { echo 'partial start; inspect Temporal and containers; do not rerun blindly' >&2; exit 1; }
echo 'svoc-hot and svoc-cold are Running'
```

## 7. First 15 minutes — confirm

```bash
cd /opt/docdb-migration/compose
docker compose exec -T temporal temporal workflow list | grep -E 'svoc-hot|svoc-cold'
/opt/docdb-migration/helpers/dsync-progress.sh --compose-dir "$PWD" --workflow-id svoc-hot
/opt/docdb-migration/helpers/dsync-progress.sh --compose-dir "$PWD" --workflow-id svoc-cold
/opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir "$PWD" --workflow-id svoc-hot
/opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir "$PWD" --workflow-id svoc-cold
docker compose logs runner --tail=30
docker compose -p flow-cold --env-file .env.cold logs runner --tail=30
docker stats --no-stream
```
- Both flows are planning or copying, with `bulk-N` children appearing.
- `svoc-hot`: one `StreamLSN` per hot collection (from the start), all on the hot-streams workers. `svoc-cold`: 1.
- No "namespace too long" or "unsupported connector" errors, and no slot starvation.
- Replica CPU, Atlas CPU/disk and worker memory look sane. Scale the sync pools live if needed (runbook §6): `docker compose up -d --scale worker=N worker` for hot sync, `-p flow-cold ...` for cold.

## 8. During initial sync (~1.5 days)
- Every few hours, per flow: `/opt/docdb-migration/helpers/dsync-progress.sh --compose-dir /opt/docdb-migration/compose --workflow-id svoc-hot` (and `svoc-cold`), `/opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir /opt/docdb-migration/compose --workflow-id svoc-hot` (and `svoc-cold`), `docker stats`, replica and Atlas metrics.
- On a pause, inspect the failure and resume with the helper's `--unpause` mode, which uses **`--reset-attempts` only**. Never use `--reset-heartbeats`.
- Restarts are safe (activities resume from their heartbeat).

## 9. After initial sync — CDC catch-up
- Expect the hot streams to drain several times faster than live traffic (pause test: about 3× on the top stream), and the cold stream to keep up easily (~230–350 ev/s).
- During active source writes, judge lag trend over hours and a peak window; last-event age is meaningful only while writes continue.

## 10. Then (playbook §8 / runbook)
1. §7 verify, per flow.
2. §7.5 empty collections.
3. §8 indexes, once both flows are at lag ~0.
4. **Re-list source collections**: anything not in `hot.txt`/`cold.txt`/`exclude.txt` needs its own flow.
5. §9 cutover: re-list source collections and stop if the inventory differs from the planned set; stop source writes and confirm with the app teams; then run:

   ```bash
   set -euo pipefail
   cd /opt/docdb-migration/compose
   DOCDB_SRC_HOST="$(grep '^DOCDB_SRC=' .env | tail -n1 | cut -d= -f2- | sed 's|tlsCAFile=/certs/|tlsCAFile=/opt/docdb-migration/certs/|')"
   mongosh "$DOCDB_SRC_HOST" --quiet --eval 'db.adminCommand({listDatabases:1}).databases
     .filter(d=>!["admin","local","config"].includes(d.name))
     .forEach(d=>db.getSiblingDB(d.name).getCollectionNames()
     .forEach(c=>print(d.name+"."+c)))' | tr -d '\r' | sort -u > ~/cdc-plan/all-cutover.txt
   diff -u <(sort -u ~/cdc-plan/all.txt) ~/cdc-plan/all-cutover.txt
   /opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir "$PWD" --workflow-id svoc-hot
   /opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir "$PWD" --workflow-id svoc-cold
   /opt/docdb-migration/helpers/dsync-progress.sh --compose-dir "$PWD" --workflow-id svoc-hot --interval 30
   /opt/docdb-migration/helpers/dsync-progress.sh --compose-dir "$PWD" --workflow-id svoc-cold --interval 30
   ```

   Require the expected stream counts (hot = `wc -l hot.txt`; cold = 1), no
   paused activities, every partition `gap == 0`, `lsn-read <= 0`, and unchanged
   read/written/LSN counters across the 30-second sample. A positive
   `lsn-read` is outstanding source backlog. If the LSN heartbeat trails the
   main stream (`lsn-read` negative), that is not backlog. Do not use age of
   `LastEventTime` as the post-stop backlog measure. Then verify and terminate
   both workflows.

   Only after the checks and application verification pass, terminate the two
   new workflows:

   ```bash
   docker compose exec -T temporal temporal workflow terminate --workflow-id svoc-hot --reason "cutover complete"
   docker compose exec -T temporal temporal workflow terminate --workflow-id svoc-cold --reason "cutover complete"
   ```
6. §10 reverse CDC: mirror hot/cold (Atlas source, `SKIP_INITIAL_SYNC=true`, new queues and IDs), started **before** app writes to Atlas.

## Rollback notes
- The destination drop was completed before this runsheet was resumed. There is no rollback to the previous destination data or to `prod-svoc`.
- If a new flow misbehaves early: terminate it, stop its project, drop only **its** namespaces on the destination, fix, and restart it with a **new** workflow ID. The other flow keeps running, because flows don't overlap.
