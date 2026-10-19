# SVOC re-baseline runsheet — two flows (hot per-namespace + cold shared)

> INTERNAL ONLY (git-ignored). Based on the T0 / T2 / pause test results
> (`dsync-svoc-learnings.md` §15.4e–§15.4h) and `CDC-TOPOLOGY-PLAYBOOK.md` §4–§8.
> Host `/opt/docdb-migration`, run everything as **root**, and strip CRLF from
> every file you edit on Windows: `sed -i 's/\r$//' <file>`.

## Layout

| Compose project | Dir / env file | Role | Queue | Size |
|---|---|---|---|---|
| `compose` | `compose/.env` | **temporal** (shared) + **hot runner** + **hot sync pool** | `dsync-svoc-hot-sync` | 3 workers × 4 slots |
| `hot-streams` | `compose/.env.hot-streams` | **hot stream pool** (workers only) | `dsync-svoc-hot-streams` | 4 workers × 20 slots (≥ 2 × 30 streams, as proven in T2) |
| `flow-cold` | `compose/.env.cold` | **cold runner + cold pool** (1 shared stream + its initial sync) | `dsync-svoc-cold` | 2 workers × 5 slots (2 for the stream, 8 for sync) |

Workflows: **`svoc-hot`** and **`svoc-cold`** (new IDs, never reuse `prod-svoc`). Dashboards: hot on 8081, cold on 8083.

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

## 1. Fresh namespace lists (cluster-wide, all databases)

```bash
mkdir -p ~/cdc-plan && cd ~/cdc-plan
export DOCDB_SRC_HOST='<DOCDB_SRC with tlsCAFile=/opt/docdb-migration/certs/global-bundle.pem>'
mongosh "$DOCDB_SRC_HOST" --quiet --eval 'db.adminCommand({listDatabases:1}).databases
  .filter(d=>!["admin","local","config"].includes(d.name))
  .forEach(d=>db.getSiblingDB(d.name).getCollectionNames()
  .forEach(c=>print(d.name+"."+c)))' | tr -d '\r' | sort -u > all.txt
wc -l all.txt                                        # every collection in every DB (svoc-db, ebook-*, enquiry-db, ...)

# hot = the 30 collections proven in T2 (reuse that hot.txt)
cp /path/to/T2/hot.txt hot.txt                       # or re-run split-hot-cold.sh --max-cold-rate 250
# exclusions (only with sign-off, e.g. stale backups) — one per line, may be empty
: > exclude.txt
comm -23 all.txt <(sort -u hot.txt exclude.txt) > cold.txt

comm -12 <(sort hot.txt) <(sort cold.txt)            # MUST print nothing (no overlap)
sort -u hot.txt cold.txt exclude.txt | diff - all.txt && echo "COVERAGE OK"
grep -c '\.customer' hot.txt                         # 12 — the customer* group stays together
wc -l hot.txt cold.txt
```

## 2. Retire `prod-svoc` — POINT OF NO RETURN after step 2.3

```bash
cd /opt/docdb-migration/compose
# 2.1 record the final state
docker compose exec -T temporal temporal workflow describe --workflow-id prod-svoc > ~/cdc-plan/prod-svoc-final.txt
dsync-progress.sh --workflow-id prod-svoc > ~/cdc-plan/prod-svoc-progress.txt
# 2.2 stop it writing
docker compose exec temporal temporal workflow terminate --workflow-id prod-svoc --reason "re-baseline: hot/cold per-namespace streams"
docker compose stop runner worker          # temporal stays up (shared)
```

**2.3 Empty the destination.** dsync requires empty destination DBs.
- Use `MDB_DEST` **read from `compose/.env`**, not a shell variable.
- Drop **only** the in-scope databases (every DB in `all.txt`), plus `adiom-internal` if it exists.

```bash
DBS=$(cut -d. -f1 ~/cdc-plan/all.txt | sort -u | sed 's/.*/"&"/' | paste -sd, -); echo "$DBS"
mongosh "$(grep '^MDB_DEST=' /opt/docdb-migration/compose/.env | cut -d= -f2-)" --quiet --eval "
  const dbs=[${DBS},'adiom-internal'];
  dbs.forEach(d=>printjson({drop:d,res:db.getSiblingDB(d).dropDatabase()}));
  printjson(db.adminCommand({listDatabases:1,nameOnly:true}).databases.map(x=>x.name));"
```

## 3. Pre-flight on the source (runbook §2.1–§2.5)

Check that change streams are enabled and retention is `604800`, prove a live change stream works, run `fixIdTypes --mode detect` (every collection `[CLEAN]`), and run `checkChangeStreams`.

## 4. Compose file: runner flags (once, shared by both runners)

```bash
cd /opt/docdb-migration/compose
cp docker-compose.yml docker-compose.yml.bak
sed -i '/--namespace=\${NAMESPACE/a\      - --max-partitions=500' docker-compose.yml
sed -i '/--namespace=\${NAMESPACE/a\      - "--stream-queue-name=${STREAM_QUEUE:?set STREAM_QUEUE in .env}"' docker-compose.yml
grep -A14 '^  runner:' docker-compose.yml           # both lines under the run leg, same indent as --namespace
```

## 5. Env files

Write `NAMESPACE` with `printf` (the T2 failure was `.` instead of `,` separators):

```bash
cd /opt/docdb-migration/compose
cp .env .env.prod-svoc.bak
HOT=$(sed '/^$/d' ~/cdc-plan/hot.txt  | paste -sd ',' -)
COLD=$(sed '/^$/d' ~/cdc-plan/cold.txt | paste -sd ',' -)

setenv() { f=$1; shift; for kv in "$@"; do k=${kv%%=*}; grep -v "^${k}=" "$f" > "$f.tmp"; printf '%s\n' "$kv" >> "$f.tmp"; mv "$f.tmp" "$f"; done; }

# HOT flow: runner + sync pool (main project)
setenv .env WORKFLOW_ID=svoc-hot QUEUE=dsync-svoc-hot-sync STREAM_QUEUE=dsync-svoc-hot-streams \
  "NAMESPACE=$HOT" PER_NAMESPACE_STREAMS=true CONCURRENT_ACTIVITIES=4 PER_STREAM_WORKERS=4 \
  ATTEMPTS_BEFORE_PAUSE=3 SKIP_INITIAL_SYNC=false DASHBOARD_PORT=8081

# HOT stream pool (workers only, same source/dest/connector options)
cp .env .env.hot-streams
setenv .env.hot-streams QUEUE=dsync-svoc-hot-streams CONCURRENT_ACTIVITIES=20 TEMPORAL_ADDR=host.docker.internal:7233

# COLD flow: one pool does its single stream + its initial sync
cp .env .env.cold
setenv .env.cold WORKFLOW_ID=svoc-cold QUEUE=dsync-svoc-cold STREAM_QUEUE=dsync-svoc-cold \
  "NAMESPACE=$COLD" PER_NAMESPACE_STREAMS=false CONCURRENT_ACTIVITIES=5 \
  TEMPORAL_ADDR=host.docker.internal:7233 DASHBOARD_PORT=8083

sed -i 's/\r$//' .env .env.hot-streams .env.cold
for f in .env .env.hot-streams .env.cold; do echo "== $f"; grep -E '^(WORKFLOW_ID|QUEUE|STREAM_QUEUE|PER_NAMESPACE_STREAMS|CONCURRENT_ACTIVITIES|TEMPORAL_ADDR|DASHBOARD_PORT)=' $f; grep '^NAMESPACE=' $f | tr ',' '\n' | wc -l; done
docker compose --env-file .env config >/dev/null && docker compose --env-file .env.cold config >/dev/null && echo "COMPOSE OK"
```
Expected namespace counts: `.env` and `.env.hot-streams` = hot count (30). `.env.cold` = cold count.

## 6. Bring up (Temporal is already running)

```bash
cd /opt/docdb-migration/compose
docker compose up -d temporal                                                             # no-op if running
docker compose -p hot-streams --env-file .env.hot-streams up -d --no-deps --scale worker=4 worker
docker compose up -d --force-recreate --scale worker=3 worker                             # hot sync pool
docker compose -p flow-cold --env-file .env.cold up -d --no-deps --scale worker=2 worker
docker compose up -d --force-recreate runner                                              # starts svoc-hot
docker compose -p flow-cold --env-file .env.cold up -d --no-deps runner                   # starts svoc-cold
```

## 7. First 15 minutes — confirm

```bash
docker compose exec temporal temporal workflow list | head                     # svoc-hot + svoc-cold Running
dsync-progress.sh --workflow-id svoc-hot ; dsync-progress.sh --workflow-id svoc-cold
dsync-find-pauses.sh --workflow-id svoc-hot ; dsync-find-pauses.sh --workflow-id svoc-cold
docker compose logs runner --tail=30 ; docker compose -p flow-cold logs runner --tail=30
docker stats --no-stream
```
- Both flows are planning or copying, with `bulk-N` children appearing.
- `svoc-hot`: **30 StreamLSN activities on the hot-streams workers** (from the start), one per hot collection. `svoc-cold`: 1.
- No "namespace too long" or "unsupported connector" errors, and no slot starvation.
- Replica CPU, Atlas CPU/disk and worker memory look sane. Scale the sync pools live if needed (runbook §6): `docker compose up -d --scale worker=N worker` for hot sync, `-p flow-cold ...` for cold.

## 8. During initial sync (~1.5 days)
- Every few hours, per flow: `dsync-progress.sh`, `dsync-find-pauses.sh`, `docker stats`, replica and Atlas metrics.
- On a pause: unpause with **`--reset-attempts` only**. Never use `--reset-heartbeats`.
- Restarts are safe (activities resume from their heartbeat).

## 9. After initial sync — CDC catch-up
- Expect the hot streams to drain several times faster than live traffic (pause test: about 3× on the top stream), and the cold stream to keep up easily (~230–350 ev/s).
- Done when **both** flows show bounded lag (< ~1 min) across a peak window.

## 10. Then (playbook §8 / runbook)
1. §7 verify, per flow.
2. §7.5 empty collections.
3. §8 indexes, once both flows are at lag ~0.
4. **Re-list source collections**: anything not in `hot.txt`/`cold.txt` needs its own flow.
5. §9 cutover: stop writes → both flows `EventsRead == EventsWritten`, lag 0 → verify → terminate both.
6. §10 reverse CDC: mirror hot/cold (Atlas source, `SKIP_INITIAL_SYNC=true`, new queues and IDs), started **before** app writes to Atlas.

## Rollback notes
- Before step 2.3, the old run's data is still on Atlas. After 2.3 there's no going back to `prod-svoc`, but it couldn't converge anyway.
- If a new flow misbehaves early: terminate it, stop its project, drop only **its** namespaces on the destination, fix, and restart it with a **new** workflow ID. The other flow keeps running, because flows don't overlap.
