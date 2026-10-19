# SVOC go-live commands (svoc-hot + svoc-cold)

INTERNAL ONLY. Run on the migration host as **root**, one block at a time, in
order. Each block runs in a `( ... )` subshell: on a problem it prints
`STOP: ...` and returns you to the prompt without closing your SSH session.
Do not continue past a `STOP`.

Already done, so not repeated here: profiling, the T0/T2 tests, the hot/cold
split, and the destination database drop. These commands use your existing
`~/cdc-plan/hot.txt` and `~/cdc-plan/cold.txt` exactly as they are.

| Item | Value |
|---|---|
| Hot flow | `svoc-hot`: per-namespace streams. Stream pool 4 workers × 20 slots, sync pool 3 × 4 |
| Cold flow | `svoc-cold`: one shared stream, pool 2 × 5 |
| Dashboards | hot `:8081`; cold `127.0.0.1:8083` (`ssh -L 8083:localhost:8083 <host>`) |
| Pristine prod config | `compose/.env.prod-svoc.bak` (every env file is rebuilt from it) |

---

## 1. Checks (read-only)

```bash
(
set -euo pipefail
C=/opt/docdb-migration/compose; P=~/cdc-plan
cd "$C"
# docker compose must parse the whole file; the runner needs STREAM_QUEUE,
# which .env only gets in block 2. Supply it for this shell only.
export STREAM_QUEUE=dsync-svoc-hot-streams
B=.env.prod-svoc.bak
[[ -f "$B" ]] || { echo "STOP: $C/$B missing"; exit 1; }

# 1a. hot/cold lists: present, clean, no overlap, no duplicates
for f in hot.txt cold.txt; do
  [[ -s "$P/$f" ]] || { echo "STOP: $P/$f missing or empty"; exit 1; }
  sed -i 's/\r$//; /^[[:space:]]*$/d' "$P/$f"
  if grep -nE '[[:space:],]|^[^.]*$' "$P/$f"; then echo "STOP: bad namespace line(s) in $f (above)"; exit 1; fi
  if sort "$P/$f" | uniq -d | grep .; then echo "STOP: duplicate lines in $f (above)"; exit 1; fi
done
if comm -12 <(sort -u "$P/hot.txt") <(sort -u "$P/cold.txt") | grep .; then
  echo "STOP: namespaces above are in BOTH hot and cold"; exit 1
fi
echo "hot=$(wc -l < "$P/hot.txt")  cold=$(wc -l < "$P/cold.txt")  customer*-in-hot=$(grep -c '\.customer' "$P/hot.txt" || true)"

# 1b. every source collection is in hot, cold or a signed-off exclude.txt
SRC="$(tr -d '\r' < "$B" | grep '^DOCDB_SRC=' | tail -n1 | cut -d= -f2- | sed 's|tlsCAFile=/certs/|tlsCAFile=/opt/docdb-migration/certs/|')"
mongosh "$SRC" --quiet --eval 'db.adminCommand({listDatabases:1}).databases
  .filter(d=>!["admin","local","config"].includes(d.name))
  .forEach(d=>db.getSiblingDB(d.name).getCollectionNames()
  .forEach(c=>print(d.name+"."+c)))' </dev/null | tr -d '\r' | sort -u > "$P/all-now.txt"
[[ -s "$P/all-now.txt" ]] || { echo "STOP: source listing is empty (connectivity?)"; exit 1; }
EXCL=(); [[ -s "$P/exclude.txt" ]] && EXCL=("$P/exclude.txt")
if ! diff <(sort -u "$P/all-now.txt") <(sort -u "$P/hot.txt" "$P/cold.txt" "${EXCL[@]}"); then
  echo "STOP: '<' = on the source but in no list (it would be silently skipped); '>' = in a list but not on the source"; exit 1
fi
echo "coverage OK: hot + cold$([[ ${#EXCL[@]} -gt 0 ]] && echo ' + exclude') = all $(wc -l < "$P/all-now.txt") source collections"

# 1c. Temporal healthy, prod-svoc not Running, new IDs unused
if ! HL="$(docker compose exec -T temporal temporal operator cluster health </dev/null 2>&1)"; then
  echo "$HL"; echo "STOP: cannot reach Temporal (error above). Check: docker compose ps temporal"; exit 1
fi
OUT="$(docker compose exec -T temporal temporal workflow list --query "WorkflowId='prod-svoc' AND ExecutionStatus='Running'" </dev/null)"
if grep -q prod-svoc <<<"$OUT"; then echo "STOP: prod-svoc is still Running. Run block 1R, then this block again"; exit 1; fi
for wf in svoc-hot svoc-cold; do
  OUT="$(docker compose exec -T temporal temporal workflow list --query "WorkflowId='$wf'" </dev/null)"
  if grep -q "$wf" <<<"$OUT"; then echo "STOP: workflow $wf already exists; inspect it, do not start a second one"; exit 1; fi
done
echo "Temporal OK; prod-svoc not Running; svoc-hot/svoc-cold unused"

# 1d. destination still empty (the drop is already done; this only checks)
DEST="$(tr -d '\r' < "$B" | grep '^MDB_DEST=' | tail -n1 | cut -d= -f2-)"
DBS="$(cut -d. -f1 "$P/hot.txt" "$P/cold.txt" | sort -u | sed 's/.*/"&"/' | paste -sd, -)"
mongosh "$DEST" --quiet --eval "
  const want=[${DBS},'adiom-internal'];
  const have=new Set(db.adminCommand({listDatabases:1,nameOnly:true}).databases.map(x=>x.name));
  const left=want.filter(d=>have.has(d));
  print('in-scope databases still on destination: '+JSON.stringify(left));
  if (left.length) quit(2);" </dev/null \
  || { echo "STOP: destination not empty, or not reachable"; exit 1; }

# 1e. runner flags present exactly once (already patched on this host)
R="$(awk '/^  runner:/{p=1;next} /^  [[:alnum:]_-]+:/{p=0} p' docker-compose.yml)"
SQ="$(grep -c -- '--stream-queue-name' <<<"$R" || true)"; MP="$(grep -c -- '--max-partitions=500' <<<"$R" || true)"
[[ "$SQ" -eq 1 && "$MP" -eq 1 ]] || { echo "STOP: runner has --stream-queue-name x$SQ, --max-partitions=500 x$MP (need 1 each). Run block 1F"; exit 1; }

# 1f. leftover test stack
if docker ps --format '{{.Names}}' | grep -q '^cdctest'; then
  echo "WARN: cdctest containers are still running. Stop them: (cd /opt/docdb-migration/cdctest && ./run-cdc-test.sh down)"
fi
echo "ALL CHECKS PASSED. Next: block 2"
)
```

**1R. Only if 1c says prod-svoc is still Running:**

```bash
(
set -euo pipefail
cd /opt/docdb-migration/compose
export STREAM_QUEUE=dsync-svoc-hot-streams   # see block 1
docker compose exec -T temporal temporal workflow describe --workflow-id prod-svoc </dev/null > ~/cdc-plan/prod-svoc-final.txt || true
docker compose exec -T temporal temporal workflow terminate --workflow-id prod-svoc \
  --reason "re-baseline: hot/cold per-namespace streams" </dev/null
docker compose stop runner worker
)
```

**1F. Only if 1e says a runner flag is missing.** The safest fix is to edit
`docker-compose.yml` by hand. Add these two lines under the runner's
`- "--namespace=..."` line, with the same indentation:

```text
      - --max-partitions=500
      - "--stream-queue-name=${STREAM_QUEUE:?set STREAM_QUEUE in .env}"
```

---

## 2. Env files (rebuilt from the pristine prod backup; safe to re-run)

```bash
(
set -euo pipefail
cd /opt/docdb-migration/compose; P=~/cdc-plan
umask 077
WF="$(tr -d '\r' < .env.prod-svoc.bak | grep '^WORKFLOW_ID=' | tail -n1)"
[[ "$WF" == "WORKFLOW_ID=prod-svoc" ]] \
  || { echo "STOP: .env.prod-svoc.bak has '$WF', expected WORKFLOW_ID=prod-svoc (backup was overwritten?)"; exit 1; }
command cp -f .env.prod-svoc.bak .env
command rm -f .env.tmp .env.new .env.hot-streams.new .env.cold.new
sed -i 's/\r$//' .env

HOT="$(paste -sd, "$P/hot.txt")"; COLD="$(paste -sd, "$P/cold.txt")"
HN="$(wc -l < "$P/hot.txt")"; CN="$(wc -l < "$P/cold.txt")"

setenv() {   # setenv FILE KEY=VALUE ...  (replace or append each key)
  local f="$1" kv k; shift
  for kv in "$@"; do
    k="${kv%%=*}"
    { grep -v "^${k}=" "$f" || true; printf '%s\n' "$kv"; } > "$f.new"
    command mv -f "$f.new" "$f"
  done
  chmod 600 "$f"
}

# hot flow: runner + initial-sync pool (main project "compose")
setenv .env WORKFLOW_ID=svoc-hot QUEUE=dsync-svoc-hot-sync STREAM_QUEUE=dsync-svoc-hot-streams \
  "NAMESPACE=$HOT" PER_NAMESPACE_STREAMS=true CONCURRENT_ACTIVITIES=4 PER_STREAM_WORKERS=4 \
  ATTEMPTS_BEFORE_PAUSE=3 SKIP_INITIAL_SYNC=false DASHBOARD_PORT=8081 TEMPORAL_ADDR=temporal:7233

# hot stream pool: workers only (project "hot-streams")
command cp -f .env .env.hot-streams
setenv .env.hot-streams QUEUE=dsync-svoc-hot-streams CONCURRENT_ACTIVITIES=20 TEMPORAL_ADDR=host.docker.internal:7233

# cold flow: runner + its pool (project "flow-cold")
command cp -f .env .env.cold
setenv .env.cold WORKFLOW_ID=svoc-cold QUEUE=dsync-svoc-cold STREAM_QUEUE=dsync-svoc-cold \
  "NAMESPACE=$COLD" PER_NAMESPACE_STREAMS=false CONCURRENT_ACTIVITIES=5 \
  TEMPORAL_ADDR=host.docker.internal:7233 DASHBOARD_PORT=127.0.0.1:8083

chmod 600 .env .env.hot-streams .env.cold .env.prod-svoc.bak
n() { grep '^NAMESPACE=' "$1" | tail -n1 | cut -d= -f2- | tr ',' '\n' | grep -c .; }
[[ "$(n .env)" -eq "$HN" && "$(n .env.hot-streams)" -eq "$HN" && "$(n .env.cold)" -eq "$CN" ]] \
  || { echo "STOP: NAMESPACE counts do not match hot=$HN / cold=$CN"; exit 1; }
for f in .env .env.hot-streams .env.cold; do
  echo "== $f  (NAMESPACE entries: $(n "$f"), mode $(stat -c %a "$f"))"
  grep -E '^(WORKFLOW_ID|QUEUE|STREAM_QUEUE|PER_NAMESPACE_STREAMS|CONCURRENT_ACTIVITIES|TEMPORAL_ADDR|DASHBOARD_PORT|SKIP_INITIAL_SYNC)=' "$f"
done
docker compose --env-file .env config >/dev/null
docker compose -p hot-streams --env-file .env.hot-streams config >/dev/null
docker compose -p flow-cold --env-file .env.cold config >/dev/null
echo "ENV OK. Next: block 3"
)
```

Expected: `.env` and `.env.hot-streams` have the hot count (30), `.env.cold` has
the cold count, and all three files are mode `600`. Never `cat` these files:
they contain passwords.

---

## 3. Start both flows

Run this straight after blocks 1 and 2. It re-checks the two conditions that
could have changed since then before starting anything.

```bash
(
set -euo pipefail
cd /opt/docdb-migration/compose; P=~/cdc-plan
OUT="$(docker compose exec -T temporal temporal workflow list --query "WorkflowId='prod-svoc' AND ExecutionStatus='Running'" </dev/null)"
if grep -q prod-svoc <<<"$OUT"; then echo "STOP: prod-svoc is Running"; exit 1; fi
DEST="$(grep '^MDB_DEST=' .env | tail -n1 | cut -d= -f2-)"
DBS="$(cut -d. -f1 "$P/hot.txt" "$P/cold.txt" | sort -u | sed 's/.*/"&"/' | paste -sd, -)"
mongosh "$DEST" --quiet --eval "const w=[${DBS},'adiom-internal'];
  const h=new Set(db.adminCommand({listDatabases:1,nameOnly:true}).databases.map(x=>x.name));
  if (w.some(d=>h.has(d))) quit(2);" </dev/null || { echo "STOP: destination not empty or unreachable"; exit 1; }

docker compose -p hot-streams --env-file .env.hot-streams up -d --no-deps --scale worker=4 worker   # hot stream pool
docker compose up -d --no-deps --force-recreate --scale worker=3 worker                              # hot sync pool
docker compose -p flow-cold --env-file .env.cold up -d --no-deps --scale worker=2 worker            # cold pool
sleep 10
docker compose up -d --no-deps --force-recreate runner                                              # starts svoc-hot
docker compose -p flow-cold --env-file .env.cold up -d --no-deps runner                             # starts svoc-cold

for i in $(seq 30); do
  H="$(docker compose exec -T temporal temporal workflow list --query "WorkflowId='svoc-hot' AND ExecutionStatus='Running'" </dev/null)"
  K="$(docker compose exec -T temporal temporal workflow list --query "WorkflowId='svoc-cold' AND ExecutionStatus='Running'" </dev/null)"
  if grep -q svoc-hot <<<"$H" && grep -q svoc-cold <<<"$K"; then echo "STARTED: svoc-hot and svoc-cold are Running"; exit 0; fi
  sleep 2
done
echo "STOP: partial start. Check 'docker compose logs runner' and 'docker compose -p flow-cold --env-file .env.cold logs runner'. Do not rerun this block blindly"
exit 1
)
```

---

## 4. First 15 minutes

```bash
cd /opt/docdb-migration/compose
/opt/docdb-migration/helpers/dsync-progress.sh    --compose-dir "$PWD" --workflow-id svoc-hot
/opt/docdb-migration/helpers/dsync-progress.sh    --compose-dir "$PWD" --workflow-id svoc-cold
/opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir "$PWD" --workflow-id svoc-hot
/opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir "$PWD" --workflow-id svoc-cold
docker compose logs runner --tail=30
docker compose -p flow-cold --env-file .env.cold logs runner --tail=30
docker stats --no-stream
```

What to look for:
- `bulk-N` child workflows appear for both flows (the initial sync is running).
- `svoc-hot` has one StreamLSN per hot collection (30). `svoc-cold` has exactly 1.
- The logs show no `namespace ... too long`, `unsupported connector` or `strconv.Atoi` errors.
- Replica CPU (CloudWatch, per instance) and Atlas CPU/disk look normal.

---

## 5. During the initial sync (~1.5 days) and the CDC catch-up

Every few hours, rerun the commands in block 4.

- **Paused activity:** fix the cause, then run
  `/opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir /opt/docdb-migration/compose --workflow-id svoc-hot --unpause`
  (and the same for `svoc-cold`). This uses `--reset-attempts` only. **Never add `--reset-heartbeats`.**
- **Restarts and recreates are safe.** Activities resume from their heartbeat.
  Use `up -d --force-recreate`, never `restart`.
- **Scale a sync pool:** for hot, run `docker compose up -d --no-deps --scale worker=N worker`.
  For cold, run `docker compose -p flow-cold --env-file .env.cold up -d --no-deps --scale worker=N worker`.
  Keep the hot stream pool at 4 or more (it needs at least 2 slots per hot collection).
- **Healthy CDC:** lag stays bounded across a peak window. Judge the trend over hours, not a single sample.
- **No new collections on the source until cutover.** A new collection gets no stream, so its writes are skipped.

---

## 6. Cutover readiness (only after application writes are stopped)

```bash
cd /opt/docdb-migration/compose
/opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir "$PWD" --workflow-id svoc-hot
/opt/docdb-migration/helpers/dsync-find-pauses.sh --compose-dir "$PWD" --workflow-id svoc-cold
/opt/docdb-migration/helpers/dsync-progress.sh --compose-dir "$PWD" --workflow-id svoc-hot  --interval 30
/opt/docdb-migration/helpers/dsync-progress.sh --compose-dir "$PWD" --workflow-id svoc-cold --interval 30
```

Ready only when **every** row of both flows shows all of the following:
- `gap` = 0
- `lsn-read` ≤ 0 (a negative value is fine; a positive value is still unread backlog)
- `w/s` = 0, and the counters stop changing
- no paused activities
- 30 hot rows and 1 cold row

After writes stop, the `lag_s` column keeps growing even on a fully drained
stream, so do not use it as the readiness signal.

Then run runbook §7 (verify), §7.5 (empty collections) and §8 (indexes), and
terminate both flows:

```bash
docker compose exec -T temporal temporal workflow terminate --workflow-id svoc-hot  --reason "cutover complete" </dev/null
docker compose exec -T temporal temporal workflow terminate --workflow-id svoc-cold --reason "cutover complete" </dev/null
```

Reverse CDC (runbook §10) mirrors this hot/cold split with
`SKIP_INITIAL_SYNC=true` and new queues and workflow IDs. Start it **before**
the applications write to Atlas.
