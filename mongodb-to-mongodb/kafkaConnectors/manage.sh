#!/usr/bin/env bash
# Register / inspect / delete migration connectors via the Kafka Connect REST API.
#
# Usage:
#   CONNECT_URL=http://localhost:8083 ./manage.sh <command> [arg]
#
# Commands:
#   register-sources        POST every source/generated/*.json
#   register FILE           POST a single connector JSON
#   list                    list connector names
#   status [NAME]           status of one (or all) connectors + their tasks
#   lag                     consumer-group lag hint (prints how to check)
#   pause NAME | resume NAME
#   restart-tasks NAME      restart all FAILED tasks of a connector
#   delete NAME
set -euo pipefail
cd "$(dirname "$0")"
URL="${CONNECT_URL:-http://localhost:8083}"
command -v jq >/dev/null || { echo "ERROR: jq is required" >&2; exit 1; }

post() { curl -sS -X POST -H "Content-Type: application/json" --data @"$1" "$URL/connectors" | jq; }

cmd="${1:-help}"
case "$cmd" in
  register-sources)
    shopt -s nullglob
    files=(source/generated/mongo-source-*.json)
    [ ${#files[@]} -gt 0 ] || { echo "No generated sources. Run ./generate-sources.sh first." >&2; exit 1; }
    for f in "${files[@]}"; do echo ">> $f"; post "$f"; done
    ;;
  register)  post "${2:?usage: register FILE}" ;;
  list)      curl -sS "$URL/connectors" | jq ;;
  status)
    if [ -n "${2:-}" ]; then curl -sS "$URL/connectors/$2/status" | jq
    else for c in $(curl -sS "$URL/connectors" | jq -r '.[]'); do
           echo "== $c =="; curl -sS "$URL/connectors/$c/status" | jq '{state:.connector.state, tasks:[.tasks[]|{id,state}]}'
         done; fi ;;
  lag)
    echo "Check sink consumer lag with the broker tools, e.g.:"
    echo "  bin/kafka-consumer-groups.sh --bootstrap-server <broker>:9092 --describe --group connect-mongo-sink-cdc" ;;
  pause)   curl -sS -X PUT "$URL/connectors/${2:?}/pause"; echo "paused ${2}" ;;
  resume)  curl -sS -X PUT "$URL/connectors/${2:?}/resume"; echo "resumed ${2}" ;;
  restart-tasks)
    name="${2:?usage: restart-tasks NAME}"
    for t in $(curl -sS "$URL/connectors/$name/status" | jq -r '.tasks[]|select(.state=="FAILED")|.id'); do
      echo "restarting task $t"; curl -sS -X POST "$URL/connectors/$name/tasks/$t/restart"
    done ;;
  delete)  curl -sS -X DELETE "$URL/connectors/${2:?}"; echo "deleted ${2}" ;;
  *) grep '^#' "$0" | sed 's/^# \{0,1\}//' ;;
esac
