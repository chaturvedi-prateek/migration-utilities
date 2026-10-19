#!/usr/bin/env bash
#
# run-cdc-test.sh — Adiom's stream-only CDC throughput tests.
#
#   run-cdc-test.sh T0|T1|T2     start one test (tears down a previous test first)
#   run-cdc-test.sh status       progress of the running test
#   run-cdc-test.sh down         stop the test and delete its Temporal state
#
#   T0  shared stream            -> /dev/null         isolates dsynct-internal limits
#   T1  shared stream            -> SCRATCH_MDB_DEST  expected to fall behind
#   T2  --per-namespace-streams  -> SCRATCH_MDB_DEST  expected to keep up
#
# Reads ./.env (see .env.sample). Refuses to run when SCRATCH_MDB_DEST points
# at the same host as MDB_DEST in the main compose .env ($MAIN_ENV, default
# ../compose/.env).
#
set -euo pipefail

HERE="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
ENV_FILE="${HERE}/.env"
MAIN_ENV="${MAIN_ENV:-${HERE}/../compose/.env}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }
usage() { sed -n '3,16p' "$0" | sed 's/^# \{0,1\}//'; }

# .env values hold '&' and '?', so they are read, never sourced.
envget() { grep -E "^$1=" "$2" 2>/dev/null | tail -n1 | cut -d= -f2- || true; }
# mongodb[+srv]://user:pw@host1,host2/db?opts -> host1,host2 (lower-case)
hosts() { local u="${1#*://}"; u="${u##*@}"; u="${u%%/*}"; u="${u%%\?*}"; tr '[:upper:]' '[:lower:]' <<<"$u"; }
# An explicit -f disables compose's auto-loading of docker-compose.override.yml,
# so add it back when present (networks / port remaps for the test project).
compose() {
  local files=(-f "${HERE}/docker-compose.yml")
  [[ -f "${HERE}/docker-compose.override.yml" ]] && files+=(-f "${HERE}/docker-compose.override.yml")
  docker compose "${files[@]}" "$@"
}

TEST="${1:-}"
case "$TEST" in
  T0|T1|T2) ;;
  down)   compose down -v --remove-orphans; log "cdctest stopped and its Temporal state deleted"
          echo "Drop the scratch database on SCRATCH_MDB_DEST yourself if T1/T2 wrote to it."; exit 0 ;;
  status) ACTIVE="$(envget CDC_TEST "${HERE}/.active")"
          [[ -n "$ACTIVE" ]] || die "no cdctest run has been started"
          exec "${HERE}/../helpers/dsync-progress.sh" --compose-dir "$HERE" \
            --workflow-id "cdctest-${ACTIVE}" --interval 60 ;;
  -h|--help|"") usage; exit 0 ;;
  *) usage; die "unknown test '${TEST}'" ;;
esac

[[ -f "$ENV_FILE" ]] || die "${ENV_FILE} not found — cp .env.sample .env and fill it in"
[[ -n "$(envget DOCDB_SRC "$ENV_FILE")" ]] || die "DOCDB_SRC is not set in ${ENV_FILE}"
[[ -n "$(envget NAMESPACE "$ENV_FILE")" ]] || die "NAMESPACE is not set in ${ENV_FILE}"

SCRATCH="$(envget SCRATCH_MDB_DEST "$ENV_FILE")"
if [[ "$TEST" == "T0" ]]; then
  CDC_DEST="/dev/null"; PER_NAMESPACE_STREAMS="false"
else
  [[ -n "$SCRATCH" && "$SCRATCH" != *PASSWORD@scratch.xxxxx* ]] \
    || die "SCRATCH_MDB_DEST is not set in ${ENV_FILE} (needed for ${TEST})"
  CDC_DEST="$SCRATCH"
  PER_NAMESPACE_STREAMS="false"; [[ "$TEST" == "T2" ]] && PER_NAMESPACE_STREAMS="true"
  # Production protection: never stream a test into the real destination.
  if [[ -f "$MAIN_ENV" ]]; then
    PROD="$(envget MDB_DEST "$MAIN_ENV")"
    if [[ -n "$PROD" ]] && { [[ "$PROD" == "$SCRATCH" ]] || [[ "$(hosts "$PROD")" == "$(hosts "$SCRATCH")" ]]; }; then
      die "SCRATCH_MDB_DEST points at the PRODUCTION destination (MDB_DEST in ${MAIN_ENV}). Refusing."
    fi
  else
    warn "${MAIN_ENV} not found — cannot cross-check SCRATCH_MDB_DEST against MDB_DEST."
    warn "Make sure SCRATCH_MDB_DEST is NOT the production cluster. Set MAIN_ENV to check."
  fi
fi

export CDC_DEST PER_NAMESPACE_STREAMS
export QUEUE="cdctest-${TEST}" WORKFLOW_ID="cdctest-${TEST}"
WORKER_COUNT="$(envget WORKER_COUNT "$ENV_FILE")"; WORKER_COUNT="${WORKER_COUNT:-3}"

if [[ -n "$(compose ps -q 2>/dev/null)" ]]; then
  log "tearing down the previous cdctest run"
  compose down -v --remove-orphans
fi
printf 'CDC_TEST=%s\n' "$TEST" > "${HERE}/.active"

log "starting ${TEST}: dest=$([[ "$TEST" == T0 ]] && echo /dev/null || echo "scratch ($(hosts "$SCRATCH"))") per-namespace-streams=${PER_NAMESPACE_STREAMS}"
compose up -d temporal
for _ in $(seq 1 30); do
  compose exec -T temporal temporal operator cluster health </dev/null >/dev/null 2>&1 && break
  sleep 2
done
compose up -d --scale worker="$WORKER_COUNT" worker
compose up -d runner

cat <<EOF

${TEST} running (stream-only, starts at the live head). Let it run 30+ minutes
under peak write traffic, then judge:

  progress   ${HERE}/../helpers/dsync-progress.sh --compose-dir ${HERE} \\
               --workflow-id ${WORKFLOW_ID} --interval 60
  dashboard  http://<this-host>:8082      Temporal UI  http://<this-host>:18233

  KEEPS UP      every partition's LastEventTime stays within ~1 min of now for
                30+ min (lag_s flat, catchup ~1.0)
  FALLS BEHIND  lag_s grows steadily / catchup < 1.0. A constant read-written
                gap means the writer is the bottleneck.
  T0 falling behind => dsynct-internal limit (stream topology), not Atlas.

Teardown:  ${HERE}/run-cdc-test.sh down
EOF
