#!/usr/bin/env bash
#
# dsync-find-pauses.sh — find (and optionally unpause) paused dsynct activities
# across a flow's workflow tree.
#
#   dsync-find-pauses.sh [--workflow-id ID] [--compose-dir DIR] [--unpause [--keep-heartbeats]]
#
# A paused activity only shows in `temporal workflow describe` of the CHILD
# workflow that owns it (e.g. <wf>/bulk-5), not on the parent, and
# `temporal activity list --query 'TemporalPauseInfo IS NOT NULL'` does not
# find it. This walks every running workflow with RootWorkflowId=<ID> (plus the
# parent's pendingChildren), describes each, and reports activities in
# PauseRequested/Paused. --unpause runs, for each one:
#   temporal activity unpause --workflow-id <wf> --activity-id <id> \
#     --reset-attempts --reset-heartbeats
# --keep-heartbeats drops --reset-heartbeats (keeps the stored heartbeat).
#
# Defaults as dsync-progress.sh: --compose-dir $COMPOSE_DIR or
# <toolkit>/compose, workflow id from $WORKFLOW_ID or <compose-dir>/.env.
#
set -euo pipefail

WORKFLOW_ID="${WORKFLOW_ID:-}"
SELF_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
COMPOSE_DIR="${COMPOSE_DIR:-${SELF_DIR}/../compose}"
UNPAUSE="false"
RESET_HB=(--reset-heartbeats)

usage() { sed -n '3,20p' "$0" | sed 's/^# \{0,1\}//'; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }
log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workflow-id) WORKFLOW_ID="$2"; shift 2 ;;
    --compose-dir) COMPOSE_DIR="$2"; shift 2 ;;
    --unpause)     UNPAUSE="true"; shift ;;
    --keep-heartbeats) RESET_HB=(); shift ;;
    -h|--help)     usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done
command -v jq >/dev/null 2>&1 || die "jq is required (installed by the toolkit)"
[[ -f "${COMPOSE_DIR}/docker-compose.yml" ]] || die "no docker-compose.yml in ${COMPOSE_DIR}; pass --compose-dir"
if [[ -z "$WORKFLOW_ID" && -f "${COMPOSE_DIR}/.env" ]]; then
  WORKFLOW_ID="$(grep -E '^WORKFLOW_ID=' "${COMPOSE_DIR}/.env" | tail -n1 | cut -d= -f2-)"
fi
[[ -n "$WORKFLOW_ID" ]] || die "no workflow id: pass --workflow-id or set WORKFLOW_ID in ${COMPOSE_DIR}/.env"

# </dev/null: exec would otherwise drain the here-string the loop below reads.
tcli() { docker compose -f "${COMPOSE_DIR}/docker-compose.yml" exec -T temporal temporal "$@" </dev/null; }

tcli workflow describe --workflow-id "$WORKFLOW_ID" -o json >/dev/null \
  || die "cannot describe workflow ${WORKFLOW_ID} — is the temporal service up in ${COMPOSE_DIR}?"
WFS="$( {
  echo "$WORKFLOW_ID"
  tcli workflow list -o json \
    --query "RootWorkflowId='${WORKFLOW_ID}' AND ExecutionStatus='Running'" \
    | jq -r '.[]?.execution.workflowId' || true
  tcli workflow describe --workflow-id "$WORKFLOW_ID" -o json \
    | jq -r '.pendingChildren[]?.workflowId' || true
} | sort -u )"

log "checking $(grep -c . <<<"$WFS") running workflow(s) under ${WORKFLOW_ID}"
FOUND=0
while read -r wf; do
  [[ -n "$wf" ]] || continue
  PAUSED="$(tcli workflow describe --workflow-id "$wf" -o json 2>/dev/null \
    | jq -r '.pendingActivities[]? | select((.paused // false) or ((.state // "") | test("PAUSE")))
             | [.activityId, .activityType.name, .state, (.attempt // 0 | tostring),
                (.lastFailure.message // "-")] | @tsv')" || continue
  [[ -n "$PAUSED" ]] || continue
  while IFS=$'\t' read -r id type state attempt failure; do
    FOUND=$((FOUND + 1))
    printf '%s  activity-id=%s  %s  %s  attempt=%s\n    last failure: %s\n' \
      "$wf" "$id" "$type" "$state" "$attempt" "$failure"
    if [[ "$UNPAUSE" == "true" ]]; then
      tcli activity unpause --workflow-id "$wf" --activity-id "$id" \
        --reset-attempts "${RESET_HB[@]}" \
        && log "unpaused ${wf} / ${id}"
    fi
  done <<<"$PAUSED"
done <<<"$WFS"

if [[ "$FOUND" -eq 0 ]]; then
  log "no paused activities"
elif [[ "$UNPAUSE" != "true" ]]; then
  echo
  echo "${FOUND} paused activit$([[ $FOUND -eq 1 ]] && echo y || echo ies). Fix the cause above, then re-run with --unpause."
fi
