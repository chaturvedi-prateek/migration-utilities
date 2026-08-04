package main

// All scripts are self-contained: they cd to their own directory and source env.sh.
// They act on all instances, or on a single instance id passed as $1.

const startProcessesSh = `#!/usr/bin/env bash
# Launch mongosync for the target instance(s) and wait until each reaches IDLE.
# Review configs/*.mongosync.json before running.
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
mkdir -p pids logs
for id in $(targets "${1:-}"); do
  mkdir -p "logs/$id"
  echo "launching $id (port ${PORT[$id]})"
  nohup "$MONGOSYNC" --config "configs/$id.mongosync.json" > "logs/$id.out" 2>&1 &
  echo $! > "pids/$id.pid"
done
echo "waiting for IDLE..."
for id in $(targets "${1:-}"); do
  until curl -s "http://localhost:${PORT[$id]}/api/v1/progress" | jq -e '.progress.state=="IDLE"' >/dev/null 2>&1; do sleep 2; done
  echo "  $id IDLE (pid $(cat "pids/$id.pid"))"
done
echo "Ready. Review start-bodies/*.start.json, then run ./start-api.sh"
`

const startAPISh = `#!/usr/bin/env bash
# POST /start for the target instance(s) using the reviewed start body files.
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
for id in $(targets "${1:-}"); do
  echo "== start $id =="
  curl -s -X POST "http://localhost:${PORT[$id]}/api/v1/start" \
    -H 'Content-Type: application/json' --data @"start-bodies/$id.start.json" | jq .
done
`

const progressSh = `#!/usr/bin/env bash
# Continuously print progress for the target instance(s) every 10s until Ctrl-C.
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
trap 'echo; echo "stopped."; exit 0' INT
while true; do
  printf '\n%-18s %-12s %-26s %8s %7s %10s\n' ID STATE INFO COPIED LAG CANCOMMIT
  printf -- '-%.0s' $(seq 1 84); echo
  for id in $(targets "${1:-}"); do
    out=$(curl -s "http://localhost:${PORT[$id]}/api/v1/progress" 2>/dev/null \
      | jq -r --arg id "$id" '.progress as $p | [$id, ($p.state//"?"), (($p.info//"")|.[0:25]),
          (if (($p.collectionCopy.estimatedTotalBytes)//0) > 0
             then ((($p.collectionCopy.estimatedCopiedBytes)//0)*100/($p.collectionCopy.estimatedTotalBytes)|floor|tostring)+"%"
             else "-" end),
          (($p.lagTimeSeconds)//0|tostring), (($p.canCommit)//false|tostring)] | @tsv' 2>/dev/null)
    if [ -n "$out" ]; then
      echo "$out" | awk -F'\t' '{printf "%-18s %-12s %-26s %8s %7s %10s\n",$1,$2,$3,$4,$5,$6}'
    else
      printf '%-18s %-12s\n' "$id" "(unreachable)"
    fi
  done
  echo "updated $(date +%H:%M:%S)  —  Ctrl-C to stop"
  sleep 10
done
`

const commitSh = `#!/usr/bin/env bash
# POST /commit for the target instance(s). Confirm progress shows canCommit=true and
# low lag for all first (./progress.sh).
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
for id in $(targets "${1:-}"); do
  echo "== commit $id =="
  curl -s -X POST "http://localhost:${PORT[$id]}/api/v1/commit" -d '{}' | jq .
done
echo "Commit requested. Watch ./progress.sh until state=COMMITTED / canWrite=true,"
echo "repoint applications, then run ./stop.sh"
`

const stopSh = `#!/usr/bin/env bash
# Stop the target mongosync process(es). By default waits for terminal COMMITTED
# (index builds complete) before killing; set WAIT_COMMITTED=0 to stop immediately.
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
for id in $(targets "${1:-}"); do
  if [ "${WAIT_COMMITTED:-1}" = "1" ]; then
    echo "waiting for $id to reach COMMITTED..."
    until curl -s "http://localhost:${PORT[$id]}/api/v1/progress" | jq -e '.progress.state=="COMMITTED"' >/dev/null 2>&1; do sleep 5; done
  fi
  if [ -f "pids/$id.pid" ]; then
    kill "$(cat "pids/$id.pid")" 2>/dev/null && echo "stopped $id" || echo "$id already stopped"
  else
    echo "no pid file for $id"
  fi
done
`

const verifySh = `#!/usr/bin/env bash
# Count-compare each database source vs destination for the target instance(s).
# Exits non-zero on any mismatch. Whole-cluster jobs (no namespaces) verify all user DBs.
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
list_dbs='print(db.adminCommand({listDatabases:1,nameOnly:true}).databases.map(function(d){return d.name;}).filter(function(n){return ["admin","local","config","mongosync_reserved_for_internal_use","__mdb_internal_mongosync"].indexOf(n)<0;}).join(" "))'
count_of() { echo 'var t=0;db.getSiblingDB("'"$1"'").getCollectionNames().forEach(function(c){t+=db.getSiblingDB("'"$1"'")[c].countDocuments({});});print(t);'; }
rc=0
for id in $(targets "${1:-}"); do
  dbs=$(namespaces "$id" | tr '\n' ' ')
  if [ -z "${dbs// /}" ]; then dbs=$("$MONGOSH" "$(src "$id")" --quiet --eval "$list_dbs"); fi
  for db in $dbs; do
    s=$("$MONGOSH" "$(src "$id")" --quiet --eval "$(count_of "$db")")
    d=$("$MONGOSH" "$(dst "$id")" --quiet --eval "$(count_of "$db")")
    if [ "$s" = "$d" ]; then st=OK; else st=MISMATCH; rc=1; fi
    printf '%-18s %-24s source=%-12s dest=%-12s %s\n' "$id" "$db" "$s" "$d" "$st"
  done
done
[ $rc -eq 0 ] && echo "ALL OK" || echo "MISMATCHES FOUND"
exit $rc
`

const cleanupSh = `#!/usr/bin/env bash
# Drop the mongosync metadata databases on BOTH the source and destination of the
# target instance(s). Required before a consolidation (fan-in) merge.
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
DROP='["mongosync_reserved_for_internal_use","__mdb_internal_mongosync"].forEach(function(d){db.getSiblingDB(d).dropDatabase();});print("cleaned")'
for id in $(targets "${1:-}"); do
  echo "cleaning $id destination..."; "$MONGOSH" "$(dst "$id")" --quiet --eval "$DROP"
  echo "cleaning $id source...";      "$MONGOSH" "$(src "$id")" --quiet --eval "$DROP"
done
`

const pauseSh = `#!/usr/bin/env bash
# Pause the target mongosync sync(s).
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
for id in $(targets "${1:-}"); do
  curl -s -X POST "http://localhost:${PORT[$id]}/api/v1/pause" -d '{}' | jq -c .; echo "  <- $id paused"
done
`

const resumeSh = `#!/usr/bin/env bash
# Resume the target mongosync sync(s).
set -uo pipefail
cd "$(dirname "$0")"; source ./env.sh
for id in $(targets "${1:-}"); do
  curl -s -X POST "http://localhost:${PORT[$id]}/api/v1/resume" -d '{}' | jq -c .; echo "  <- $id resumed"
done
`
