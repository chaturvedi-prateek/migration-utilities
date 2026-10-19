# CDC Topology Playbook — test, then re-baseline with per-namespace streams

Use this playbook when a dsynct DocumentDB → Atlas run **cannot keep up in CDC**
(lag keeps growing) and you need to (1) prove the fix with stream-only tests,
then (2) start a new, correctly partitioned run. It is also the checklist for
choosing the stream topology **before** any large migration.

It complements, not replaces, [PRODUCTION-RUNBOOK.md](PRODUCTION-RUNBOOK.md):
§6 below lists exactly which runbook sections to reuse, change or skip.

Every rule in §0 was learned the hard way on a live production migration
(~36 TB, ~140 collections, ~3.8k change events/s at peak, a replace-heavy
workload of ~1 KB documents).

---

## 0. Golden rules — read before touching anything

1. **One shared stream cannot keep up with many busy collections.** By default
   dsynct reads all namespaces through ONE change stream (`Index:0,
   Namespaces:null` in the heartbeat). Its writer sends documents to
   `--per-stream-workers` by a hash of `_id`, but **flushes whenever the next
   event belongs to a different namespace**, and there is an ordering barrier
   between flushes. With ~140 interleaved collections the average batch is
   ~1 event, so CDC capped at **~1.6–1.9k events/s**, while the source wrote
   ~3.8k/s at peak.
2. **None of these help a shared multi-namespace stream:**
   `--per-stream-workers`, `--stream-writer-max-batch-size`, more worker
   containers (the stream is pinned to one worker), a bigger Atlas tier,
   `--namespace-fanout` (that one only affects planning).
3. **The fix is `--per-namespace-streams`** (connector option; compose:
   `PER_NAMESPACE_STREAMS=true`), optionally with the low-traffic tail on a
   shared stream in a **separate, non-overlapping** flow.
4. **Topology is fixed at workflow start.** A running workflow cannot be
   re-partitioned, and a new flow cannot start CDC from a past timestamp or
   resume token. Getting it wrong means a full re-baseline. **Decide before the
   initial sync.**
5. **Flows must never overlap in namespaces.** Two flows writing the same
   collection apply changes out of order, so an older version can overwrite a
   newer one. Each flow gets its own `QUEUE`, `WORKFLOW_ID`, workers and
   dashboard port.
6. **Slots:** every stream partition holds **2** `--concurrent-activities`
   slots (`dsync_StreamChanges` + `dsync_StreamLSN`) for its whole life.
   `CONCURRENT_ACTIVITIES × workers ≥ 2 × streams` + the initial-sync
   concurrency you want. Otherwise some streams never start, silently.
   Adiom confirmed this is expected, and recommends **separate worker pools**:
   one pool for streams and one for initial sync, each on its own
   `--queue-name`, targeted from `run` with `--stream-queue-name` /
   `--initial-sync-queue-name` (§5.4). A pool is just dsynct workers on one
   queue; the queue flags decide which activity types are sent there. Stream
   activities never complete, so each one holds its worker slots
   permanently. If its worker stops, the stream resumes on another worker
   from its last heartbeat.
7. **Pauses:** `--pause-on-error` with the old default of 0 attempts paused an
   initial-sync activity on a **routine container restart** (a restart counts
   as an error). The dashboard froze at 99% with 0 ops/s and **no error in any
   log**. (Seen on the production image build. In a local test the current
   public image, 2026-10, tolerated restarts even with 0 attempts. Keep the
   setting anyway as insurance.) Keep `ATTEMPTS_BEFORE_PAUSE=3`. A paused activity is visible only in
   `workflow describe` of the **child** workflow (`<wf>/bulk-N`), so use
   `dsync-find-pauses.sh`. Always unpause with `--reset-attempts` **only**.
   **Never add `--reset-heartbeats`.** Per Adiom it restarts the activity from
   its *original* cursor: a CDC stream replays from the start of the run, and
   an initial-sync partition is re-copied. dsynct also ships a
   `temporaltools` mode for pausing and unpausing a whole workflow:
   `docker run --rm -e DSYNCT_MODE=temporaltools dsynct:enterprise unpause --workflow-id=<wf> --dry-run`
   (and `pause`). Run its `--help` first to see how it reaches Temporal (from
   the host, use `--network <compose-project>_default` or the published 7233
   port), and drop `--dry-run` only after checking the output.
8. **Read the heartbeat correctly:**
   - A **constant non-zero** `EventsRead − EventsWritten` (it sat at ~16.4k)
     means the buffer is full and the **writer** is the bottleneck.
   - **Catch-up ratio** = Δ`LastEventTime` ÷ Δwall-clock. Below 1.0 means
     falling behind. Measure it over **hours**, not minutes, because write
     traffic follows a daily cycle. It is only meaningful once the reader is
     at the head of the stream (`lsn-read` ≈ 0). While a backlog drains, the
     ratio swings widely (5.2 one minute, 0.46 the next in testing), so in
     short or stream-only tests read the **lag**, not the ratio.
   - `LSN − EventsRead` ≈ events still to read (Adiom: both count from the
     same point). The dashboard's "behind" figure is exactly this: events the
     main stream has not yet read compared with the LSN stream. Its "read"
     value is the main stream, not the LSN stream.
9. **Low Atlas / DocumentDB CPU does not prove they aren't involved.**
   Thousands of serial 1-event writes leave both idle and waiting.
10. **`dsync_StreamLSN` opens a second change stream** on the source (inbound
    traffic ≈ 2–3.4× outbound), and there is no switch to turn it off. Many
    per-namespace streams multiply read load on the source instance, so watch
    replica CPU. The main stream uses `fullDocument: updateLookup` (dsynct
    writes full snapshots for updates), so `update`-heavy workloads cost an
    extra lookup per event on the source. Insert and replace events already
    carry the document. The LSN stream does not use `updateLookup`. There is
    no change-stream read batch-size option.
11. **Fast tests that don't match production are misleading.** Initial-sync
    benchmarks and insert-only load on a few collections never exercise an
    interleaved many-namespace CDC stream. Test with **real source traffic at
    peak** (§2).
12. **Never point a test at the production destination.** Namespaces are
    identical, so the test would overwrite production data.
    `run-cdc-test.sh` refuses to start if the hosts match.
13. **DDL is not replicated, and collections created during the run are not
    supported** (Adiom). Empty collections, indexes and views are out-of-band
    steps (runbook §7.5, §8, §9). Ask the application teams to **freeze
    collection creation** from the initial sync until cutover. With
    per-namespace streams, a collection created mid-run has **no stream at
    all**, because streams are created up front during planning, so its
    events are skipped. Re-list source
    collections before cutover (§8). If a new one appeared, it has no data on
    the destination: sync it with a separate, non-overlapping flow (its own
    initial sync) before cutting over. Confirm this remedy with Adiom.
14. **`docker compose restart` does not re-read compose / `.env` changes.** Use
    `up -d --force-recreate`. A recreated worker resumes CDC from the
    heartbeat cursor (`Attempt +1`, `LastFailure` = heartbeat timeout:
    expected and harmless).
15. **Screenshots and pasted URIs leak passwords.** Redact before sharing, and
    keep engagement notes out of public repos.

---

## 1. Prerequisites

- [ ] New toolkit bundle built and installed on the migration host (README §1–2).
      The new compose supports `PER_NAMESPACE_STREAMS`,
      `ATTEMPTS_BEFORE_PAUSE`, `SKIP_INITIAL_SYNC`, OTel, `TEMPORAL_ADDR` and
      `DASHBOARD_PORT`, and the bundle includes `helpers/` and `cdctest/`.
- [ ] dsynct licence confirmed with Adiom for production use, if the bundle
      was built from the Docker Hub image (`<prefix>/dsynct-LICENSE.md`).
- [ ] A **scratch** Atlas cluster in the same project/region as the
      destination (same network path, so the existing peering applies).
      M30–M50 is enough. It must not hold production data.
- [ ] A peak-traffic window identified (the tests and the profile must run
      at peak).
- [ ] Optional: OTel collector + SigNoz on the host (`DSYNCT_OTEL=true`,
      `OTEL_EXPORTER_OTLP_ENDPOINT=http://<collector>:4317`). Adiom's SigNoz
      dashboards are at
      <https://github.com/adiom-data/public/tree/main/kubernetes/system/signoz_dashboards>.
      Key metrics: `dsynct.since_last_event_time` (lag), `dsynct.write_updates`
      (ms per CDC write), `dsynct.stream_read_gauge` /
      `dsynct.stream_written_gauge` (buffer), `dsynct.read_ahead_gauge` (LSN).
- [ ] If an old run is still going: **leave it running** as a fallback until
      §4 says otherwise.

Host-side tools (`mongosh`) need the **host** CA path in the URI,
`tlsCAFile=<prefix>/certs/global-bundle.pem`. Below, `$DOCDB_SRC_HOST` means
`DOCDB_SRC` with that path. Containers use `/certs/global-bundle.pem`.

---

## 2. Profile the source (at peak traffic)

```bash
cd ~ && mkdir -p cdc-plan && cd cdc-plan

# 1. write rate per namespace / operation for 5 minutes (longer is better)
CS_SECONDS=300 CS_OUT=cs-profile.json \
  mongosh "$DOCDB_SRC_HOST" --quiet --file <prefix>/helpers/cs-rate-profile.js

# 2. every namespace in scope (quiet ones are absent from the profile)
mongosh "$DOCDB_SRC_HOST" --quiet --eval 'db.adminCommand({listDatabases:1}).databases
  .filter(d=>!["admin","local","config"].includes(d.name))
  .forEach(d=>db.getSiblingDB(d.name).getCollectionNames()
  .forEach(c=>print(d.name+"."+c)))' > all.txt

# 3. hot (per-namespace streams) vs cold (one shared stream)
split-hot-cold.sh cs-profile.json --max-cold-rate 500 --all-namespaces all.txt
#   -> hot.txt, cold.txt, and ready-to-paste --namespace lists
```

Record: total events/s, the operation mix, the top namespaces and the hot
count. The split is greedy: **cold is only the quiet tail** whose combined
rate fits under `--max-cold-rate`, and everything else goes hot, including
collections with ~1% of traffic. A large hot set is expected. Size the slots
for it (§5.4). Sanity checks:
- **Cold remainder ≤ ~500 events/s.** The shared-stream ceiling is ~1.6–1.9k/s,
  so keep a wide margin. In the production case the top 10 collections were
  only ~66% of traffic, so the hot set has to be larger than the top 10.
- **No single namespace above ~1.5k/s.** One per-namespace stream is still one
  serial stream. If one is, raise it with Adiom before going further.
- Exclude collections that should not migrate (for example stale `*_back_up`
  copies) from **both** lists, deliberately and with sign-off.
- Run the profile on more than one day/time if traffic varies a lot.

---

## 3. Stream-only tests (Adiom's recommendation)

**Scratch target options.** Prefer a separate scratch cluster, because it
doesn't add test load to the production destination. If you must test
against the production *cluster*, redirect writes to differently named
databases:
- **Exact namespaces:** the worker option
  `--namespace-mapping appdb.t1:appdb_test.t1test,appdb.t2:appdb_test.t2test`
  (source:destination pairs, no wildcards).
- **Patterns:** a transformer config with a top-level CEL `namespacemapper`,
  for example `namespacemapper: '"appdb_test." + self.split(".")[1]'`.

`run-cdc-test.sh` refuses a scratch URI whose host matches production, so
this case needs the scratch cluster or a manual run.

The kit's `docker-compose.yml` is **generated by the fetch script** (it is
not in the repo's `helpers/cdctest/`), so use the installed copy at
`<prefix>/cdctest`. To add networks or port remaps, put them in
`<prefix>/cdctest/docker-compose.override.yml`; `run-cdc-test.sh` loads it.

**Temporal volume permissions.** `temporalio/temporal` runs as a non-root
user, but Docker creates the named volume owned by root. Temporal then
restart-loops with `unable to open database file: out of memory (14)`, which
is SQLite's CANTOPEN error, not memory. The test project's volume is
`cdctest_temporal-data`, **not** `compose_temporal-data` (the runbook §4 fix
targets the main project). `run-cdc-test.sh` fixes this itself on every start
(it needs `sudo` on Docker hosts). If you start the project by hand:
`docker compose down && sudo chmod 777 /var/lib/docker/volumes/cdctest_temporal-data/_data && docker compose up -d temporal`.

All tests run in their own compose project (`cdctest`) with their own Temporal
(host ports 17233 / 18233, dashboard 8082), `SKIP_INITIAL_SYNC` forced on.
Each test starts reading **from now**, so "keeps up" simply means the lag
stays near 0. Run one test at a time, at peak, for **≥ 30 minutes** each.

```bash
cd <prefix>/cdctest
cp .env.sample .env
#   DOCDB_SRC         = same source URI as production (container CA path)
#   SCRATCH_MDB_DEST  = the SCRATCH cluster — never production
#   NAMESPACE         = what to stream (see the table)
#   WORKER_COUNT / CONCURRENT_ACTIVITIES / PER_STREAM_WORKERS = as planned for prod
#   ATTEMPTS_BEFORE_PAUSE=3
# If you edited .env on Windows, strip CRLF first, or every value gets a
# trailing \r ("strconv.Atoi: parsing "3\r"") — same for compose/.env files:
sed -i 's/\r$//' .env
```

| Test | `NAMESPACE` | Stream mode | Destination | Expected | Proves |
|---|---|---|---|---|---|
| **T0** | all in scope (comma list of DBs) | shared | `/dev/null` | keeps up | dsynct itself can read and process this rate |
| **T1** | all in scope (comma list of DBs) | shared | scratch | **falls behind** within minutes | reproduces the production problem |
| **T2** | `hot.txt` (comma-joined) | `--per-namespace-streams` | scratch | **keeps up** | the fix works for the hot set |
| **T1-cold** | `cold.txt` (comma-joined) | shared | scratch | **keeps up** | the cold tail is safe on one stream |

`run-cdc-test.sh` provides T0, T1 and T2. For **T1-cold**, run `T1` with
`NAMESPACE` set to the cold list.

```bash
./run-cdc-test.sh T0         # starts (tears down any previous test first)
./run-cdc-test.sh status     # dsync-progress every 60 s: gap, lag, catch-up ratio
./run-cdc-test.sh down       # stop + delete the test's Temporal state
# then edit .env (NAMESPACE etc.) and run the next test
```

**Slots for T2:** set `CONCURRENT_ACTIVITIES × WORKER_COUNT ≥ 2 × (hot
collections)`. If some hot collections never show a `dsync_StreamChanges`
activity in `status`, they are waiting for slots.

**Join a comma list:** `paste -sd, hot.txt`

### Pass criteria

- **Keeps up:** `LastEventTime` within ~1 minute of now, catch-up ratio ≈ 1.0,
  `EventsWritten` rate ≥ the profiled source rate for that namespace set,
  with a stable lag over ≥ 30 minutes.
- **Falls behind:** lag grows, ratio < 1.0, and a constant gap means
  writer-bound.

While each test runs, also record: source **replica** CPU (CloudWatch
`DBInstanceIdentifier` of the instance the URI reads from), worker CPU and
memory (`docker stats`), and scratch Atlas CPU / opcounters. OTel
`dsynct.write_updates` gives ms per write and batch behaviour.

### Decision

| T0 | T1 | T2 | T1-cold | Decision |
|---|---|---|---|---|
| ✓ | ✗ | ✓ | ✓ | **Go:** re-baseline with the hot/cold topology (§4–§5) |
| ✓ | ✗ | ✓ | ✗ | Move more namespaces into hot (lower `--max-cold-rate`), re-run T1-cold |
| ✓ | ✗ | ✗ | — | Not just the shared stream. Check replica CPU (many cursors), scratch write latency / RTT, slots. Talk to Adiom before re-baselining |
| ✗ | — | — | — | A dsynct-internal or source-read limit. Escalate to Adiom with the T0 numbers |
| ✓ | ✓ | — | — | The shared stream keeps up at this rate, so the original problem was something else. **Do not re-baseline**; re-diagnose |

Share the results (numbers, not screenshots with URIs) with Adiom before §4.

---

## 4. Retire the old run (only after a "Go")

```bash
cd <prefix>/compose

# 1. record the final state for the post-mortem
docker compose exec -T temporal temporal workflow describe --workflow-id "$OLD_WF" > old-wf-final.txt
dsync-progress.sh --workflow-id "$OLD_WF" > old-wf-progress.txt

# 2. stop it writing: terminate the workflow, then stop its workers/runner
docker compose exec temporal temporal workflow terminate --workflow-id "$OLD_WF" \
  --reason "re-baseline with per-namespace streams"
docker compose stop runner worker
```

**3. Empty the destination** (dsync requires empty destination databases for
initial sync). This is destructive, so read it twice:
- Run it against **the production destination URI from `.env`**, re-read from
  the file, not a shell variable that may be stale (runbook §T7 item 4).
- Drop only the databases or collections that are in scope (`hot.txt` +
  `cold.txt`). Never `admin`, `config` or `local`, and never databases from
  other migrations.
- Also drop `adiom-internal` on the destination if it exists (runbook §T6).
- Re-confirm with `show dbs` that only the expected databases are gone.

```bash
mongosh "<MDB_DEST from .env>" --quiet --eval '
  const dbs = ["<db1>","<db2>"];            // in-scope databases ONLY
  dbs.forEach(d => printjson({drop: d, res: db.getSiblingDB(d).dropDatabase()}));
  printjson(db.adminCommand({listDatabases:1, nameOnly:true}).databases.map(x=>x.name));'
```

---

## 5. Start the new run (two flows)

### 5.1 Pre-flight (repeat, even though it passed last time)

Runbook §2.1–§2.5: change streams enabled, retention `604800` (7 days covers a
~1.5-day initial sync plus catch-up), a live change-stream proof,
`fixIdTypes --mode detect` (new documents since the last check can introduce
mixed `_id` types) and `checkChangeStreams`. Re-generate `all.txt` and check
that no new collection has appeared since §2.

### 5.2 Configure flow A (hot) — main compose project

`<prefix>/compose/.env` (runbook §3, with these differences):

```bash
DOCDB_SRC=mongodb://<user>:<pw>@<docdb-cluster-endpoint>:27017/?tls=true&tlsCAFile=/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false
MDB_DEST=mongodb+srv://<user>:<pw>@<atlas-cluster>.mongodb.net/?retryWrites=true
QUEUE=dsync-<name>-hot
WORKFLOW_ID=<name>-hot
NAMESPACE=<paste -sd, hot.txt>
PER_NAMESPACE_STREAMS=true
ATTEMPTS_BEFORE_PAUSE=3
CONCURRENT_ACTIVITIES=<≥ (2 × hot streams + initial-sync tasks) ÷ workers>
PER_STREAM_WORKERS=4
DASHBOARD_PORT=8081
# DSYNCT_OTEL=true
# OTEL_EXPORTER_OTLP_ENDPOINT=http://<collector>:4317
# OTEL_SERVICE_NAME=dsynct-hot
```

Add `--max-partitions=500` to the runner's `run` leg (runbook §3 "Large source
databases" and §T9). The generated compose does not set it:

```bash
sed -i '/--namespace=\${NAMESPACE/a\      - --max-partitions=500' docker-compose.yml
grep -A8 '^  runner:' docker-compose.yml        # confirm placement and indent
```

### 5.3 Configure flow B (cold) — second compose project, same Temporal

```bash
cd <prefix>/compose
cp .env .env.cold
# edit .env.cold:
#   QUEUE=dsync-<name>-cold
#   WORKFLOW_ID=<name>-cold
#   NAMESPACE=<paste -sd, cold.txt>
#   PER_NAMESPACE_STREAMS=false
#   TEMPORAL_ADDR=host.docker.internal:7233   # same host only: relies on temporal
#                                             # publishing 7233 + the compose
#                                             # extra_hosts host-gateway entry.
#                                             # Flow B on another host: <temporal-host-ip>:7233
#   DASHBOARD_PORT=8083
#   OTEL_SERVICE_NAME=dsynct-cold
```

Check there is no overlap, and that hot ∪ cold = everything in scope:

```bash
comm -12 <(sort hot.txt) <(sort cold.txt)                   # must print nothing
sort -u hot.txt cold.txt | diff - <(sort -u all.txt)        # only deliberate exclusions
```

### 5.4 Sizing

- Slots: `CONCURRENT_ACTIVITIES × workers ≥ 2 × streams + initial-sync
  concurrency`, per flow. For example, 40 hot streams = 80 slots, plus 12 for
  initial sync gives 4 workers × 23.
- **Preferred (Adiom): separate worker pools,** so streams never compete with
  initial-sync tasks for slots:
  - **Stream pool:** workers with `--queue-name=dsync-<name>-hot-streams`, sized
    at `CONCURRENT_ACTIVITIES × workers ≥ 2 × streams`.
  - **Initial-sync pool:** workers with `--queue-name=dsync-<name>-hot-sync`,
    sized for copy throughput as before.
  - **Runner:** `run --queue-name=… --stream-queue-name=dsync-<name>-hot-streams
    --initial-sync-queue-name=dsync-<name>-hot-sync`.
  - Every worker in both pools carries the **same** source/destination/connector
    options (including `--per-namespace-streams`).
  - The generated compose has one worker service, so run the second pool as
    its own compose project, like flow B in §5.3 (`.env` with only `QUEUE`
    changed, `--no-deps worker`). Add the two flags to the runner's `run` leg.
  - Both are `run` options, verified on `markadiom/dsynct:latest`
    (2026-10). `--stream-queue-name`: "use this queue name for streaming
    activities instead of queue-name". `--initial-sync-queue-name`: "use
    this queue name for initial sync tasks instead of queue-name". Each
    falls back to `--queue-name` when unset. Workers only take `--queue-name`,
    so each pool's workers set it to that pool's queue. Still ask Adiom
    whether StreamLSN also runs on the stream pool and holds its slots during
    the whole initial sync.
  - Once the initial sync finishes, scale the sync pool to 0. The stream pool
    keeps running.
- Initial sync behaves as before (scale workers live, runbook §6). CDC needs
  slots, not CPU: per-namespace streams are light, so watch memory, not CPU.
- Watch the source **replica** CPU. N streams + N LSN cursors all read the
  same change log. If the replica saturates, fall back to reading from the
  primary (`readPreference=primary`) and discuss with Adiom.

### 5.5 Bring up (runbook §4 order — Temporal first, wait for it to be serving)

```bash
cd <prefix>/compose
docker compose up -d temporal                       # wait for :7233 (runbook §T9-A)
docker compose up -d --scale worker=<N> worker      # flow A workers
docker compose up -d runner                         # flow A runner -> <name>-hot

docker compose -p flow-cold --env-file .env.cold up -d --no-deps --scale worker=<M> worker
docker compose -p flow-cold --env-file .env.cold up -d --no-deps runner
```

### 5.6 Confirm it started correctly (first 15 minutes)

```bash
docker compose exec temporal temporal workflow list            # both <name>-hot and <name>-cold Running
dsync-progress.sh --workflow-id <name>-hot
dsync-progress.sh --workflow-id <name>-cold
dsync-find-pauses.sh --workflow-id <name>-hot
dsync-find-pauses.sh --workflow-id <name>-cold
```

- Both workflows are planning or copying (`bulk-N` children appear).
- Flow A shows **one stream partition per hot namespace** (a
  StreamChanges/StreamLSN pair each, with `Namespaces` naming the collection
  instead of `null`). Flow B shows exactly one partition.
- No slot starvation: pending activities aren't stuck in "scheduled" while
  workers are idle.

---

## 6. What to take from PRODUCTION-RUNBOOK.md for the new run

| Runbook section | Use for the new run? | Notes |
|---|---|---|
| §1 Host setup | Re-install the new bundle | `docker load` replaces `dsynct:enterprise`. Recreate containers with `up -d --force-recreate` |
| §2 Pre-flight (2.1–2.5) | **Yes, all** | Re-run `fixIdTypes` detect and the change-stream proof |
| §3 Configure | **Yes, modified** | One `.env` per flow; explicit comma `NAMESPACE` lists (not the "all databases" variant); `--max-partitions=500`; ports per flow |
| §4 Bring up | **Yes** | Plus the second project (§5.5) |
| §5 Monitor | **Yes** | Add `dsync-progress.sh` / `dsync-find-pauses.sh` per flow, plus OTel |
| §6 Scale workers | Yes | Per flow (`-p flow-cold` for B) |
| §7 Verification | **Yes, per flow** | §T8 memory caveats apply; `sample-ids`/`verify-ids` for large collections |
| §7.5 Empty collections | **Yes** | Run after both flows are in CDC |
| §8 Indexes | **Yes** | Once **both** flows' CDC lag is ~0, before cutover |
| §9 Cutover | **Yes, both flows** | Drain **both** to `EventsRead == EventsWritten` and lag 0 before stopping either |
| §10 Reverse CDC | **Yes, but split it the same way** | See §8 below: a single shared reverse stream would hit the same ~2k/s ceiling |
| §T1 CDC tuning | Superseded by §0 here | Writer knobs don't fix a shared multi-namespace stream |
| §T2 Pauses | Yes | `ATTEMPTS_BEFORE_PAUSE=3`; use `dsync-find-pauses.sh`; `--reset-attempts` |
| §T5/§T6 Restart | Yes | Restarts are safe (resume from cursor); a restart counts toward attempts-before-pause |
| §T9 Planning stall | Yes | `--max-partitions` per flow |

---

## 7. Monitoring the new run

Every few hours, per flow:

```bash
dsync-progress.sh --workflow-id <name>-hot  --interval 300
dsync-progress.sh --workflow-id <name>-cold --interval 300
dsync-find-pauses.sh --workflow-id <name>-hot
dsync-find-pauses.sh --workflow-id <name>-cold
docker stats --no-stream
```

| Phase | Healthy signature | Act if |
|---|---|---|
| Initial sync | `bulk-N` completing; Pending Child Workflows falling; worker CPU busy | progress stalls with 0 ops/s, so run `dsync-find-pauses.sh` **first** |
| CDC catch-up | catch-up ratio > 1 per stream; lag shrinking; gap not pinned at the buffer size | ratio < 1 on a cold-flow stream (cold set too busy) or on a single hot stream (one namespace too hot): escalate |
| Steady CDC | lag ~0, `LastEventTime` ≈ now | lag creeping up at peak hours |
| Ready for cutover | both flows: `EventsRead == EventsWritten`, lag 0 after source writes stop | — |

Note the rough timeline: the initial sync (~1.5 days for ~36 TB in the
production case), then catching up on the changes accumulated during it.
With the hot/cold split the catch-up should run several times faster than the
source rate. Track it with the catch-up ratio.

---

## 8. Cutover and reverse CDC with two flows

- **Before cutover:** re-check for collections created on the source since §5.3
  (neither flow carries them, and DDL is not replicated). Run runbook §7.5,
  then §8 indexes once both flows are at lag 0.
- **Cutover (runbook §9):** stop source writes → wait until **both** flows show
  `EventsRead == EventsWritten` and lag 0 → verify → terminate **both**
  workflows.
- **Reverse CDC (runbook §10):** after cutover the application writes to Atlas
  at the same rate, so a single shared reverse stream would hit the same
  ceiling. Mirror the forward topology: a reverse-hot flow with
  `PER_NAMESPACE_STREAMS=true` and a reverse-cold flow, both with
  `SKIP_INITIAL_SYNC=true`, new `QUEUE`/`WORKFLOW_ID`s, and swapped
  `DOCDB_SRC`/`MDB_DEST` (runbook §10 step 2). Start them **before** the
  application writes to Atlas. Adiom confirmed `--per-namespace-streams`
  also works with MongoDB/Atlas as the source (same connector, same writer
  flush behaviour). Validate the reverse topology in a test first
  if possible.

---

## 9. Questions to settle with Adiom before or during the run

Answered by Adiom: there is no resume-from-token / start-at-timestamp for a
new flow, no per-collection buffering on a shared stream, and no hand-over of a
running cursor (scaling is by individual streams). 2 slots per stream is
expected (use separate worker pools). No known DocumentDB cursor limits.
Collections created during a run are not supported. Never use
`--reset-heartbeats`. Both counters count from the same point.
`--per-namespace-streams` works with a MongoDB source. Namespace mapping /
destination configuration can redirect writes. No known DocumentDB-destination
limits.

Answered in the follow-up:
- Stream pools: streams (StreamChanges and StreamLSN) go to the stream queue
  and hold their slots permanently. `--stream-queue-name` /
  `--initial-sync-queue-name` exist on `run` (verified).
- Collections created mid-run: skipped with per-namespace streams.
- Pausing: use `DSYNCT_MODE=temporaltools` `pause` / `unpause`.
- The dashboard's "behind" figure = LSN − main-stream read.
- No read batch-size flag. The main stream uses `updateLookup`; the LSN
  stream does not.
- Namespace mapping: `--namespace-mapping` (exact pairs) or a transformer
  `namespacemapper` (CEL).

Still open:
- On a single shared database-level stream, are a mid-run collection's
  events applied or skipped? Is a separate non-overlapping flow the right
  remedy?
- Versioned image tags and a changelog (which version has the pause fix).
- The AWS CA bundle in the public image.
- Doc gaps: `--documentdb-sampling-fanout` is missing from help;
  `--otel-metric-interval` shows a `0s` default; `--max-partitions` has no
  default.
