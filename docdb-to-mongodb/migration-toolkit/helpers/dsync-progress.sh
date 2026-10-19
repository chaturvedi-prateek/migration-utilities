#!/usr/bin/env bash
#
# dsync-progress.sh — CDC progress per stream partition, from Temporal heartbeats.
#
#   dsync-progress.sh [--workflow-id ID] [--interval S] [--compose-dir DIR]
#                     [--from-file sample.json [--from-file sample2.json]]
#
# Reads `temporal workflow describe -o json` (run inside the compose project's
# temporal container) and prints, per dsync_StreamChanges partition:
#   read / written    EventsRead / EventsWritten from the heartbeat
#   gap               read - written. A constant non-zero gap means the
#                     WRITER is the bottleneck, not the source read
#   last_event / lag  LastEventTime and how far it is behind now
#   lsn / lsn-read    LSN from the matching dsync_StreamLSN partition and how
#                     many events the reader still has to catch up on
# With --interval S it samples twice and adds the write rate and the catch-up
# ratio = d(LastEventTime) / d(wall clock); below 1.0 the stream is FALLING
# BEHIND. Any pending activity in PauseRequested/Paused is flagged (paused
# activities of child workflows are only visible via dsync-find-pauses.sh).
#
# Defaults: --compose-dir $COMPOSE_DIR or <toolkit>/compose, --workflow-id
# $WORKFLOW_ID or WORKFLOW_ID from <compose-dir>/.env.
# --from-file parses saved describe output instead (offline / dry run).
#
set -euo pipefail

WORKFLOW_ID="${WORKFLOW_ID:-}"
INTERVAL=""
SELF_DIR="$(cd "$(dirname "$(readlink -f "$0")")" && pwd)"
COMPOSE_DIR="${COMPOSE_DIR:-${SELF_DIR}/../compose}"
FILES=()

usage() { sed -n '3,25p' "$0" | sed 's/^# \{0,1\}//'; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workflow-id) WORKFLOW_ID="$2"; shift 2 ;;
    --interval)    INTERVAL="$2"; shift 2 ;;
    --compose-dir) COMPOSE_DIR="$2"; shift 2 ;;
    --from-file)   FILES+=("$2"); shift 2 ;;
    -h|--help)     usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done
command -v jq >/dev/null 2>&1 || die "jq is required (installed by the toolkit)"

if [[ "${#FILES[@]}" -eq 0 ]]; then
  [[ -f "${COMPOSE_DIR}/docker-compose.yml" ]] || die "no docker-compose.yml in ${COMPOSE_DIR}; pass --compose-dir"
  if [[ -z "$WORKFLOW_ID" && -f "${COMPOSE_DIR}/.env" ]]; then
    WORKFLOW_ID="$(grep -E '^WORKFLOW_ID=' "${COMPOSE_DIR}/.env" | tail -n1 | cut -d= -f2-)"
  fi
  [[ -n "$WORKFLOW_ID" ]] || die "no workflow id: pass --workflow-id or set WORKFLOW_ID in ${COMPOSE_DIR}/.env"
fi

sample() {  # sample <n> -> describe JSON on stdout
  if [[ "${#FILES[@]}" -gt 0 ]]; then
    cat "${FILES[$(( $1 < ${#FILES[@]} ? $1 : ${#FILES[@]} - 1 ))]}"
  else
    docker compose -f "${COMPOSE_DIR}/docker-compose.yml" exec -T temporal \
      temporal workflow describe --workflow-id "$WORKFLOW_ID" -o json
  fi
}

# Normalises one describe document into
#   {activities: [...], streams: [{idx, ns, read, written, last, lsn}]}
# Heartbeat details arrive decoded (JSON shorthand, the CLI default) or as raw
# base64 payloads (--no-json-shorthand-payloads); both are handled.
# shellcheck disable=SC2016
NORMALISE='
def hb: (.heartbeatDetails // [])
  | if type == "object" then (.payloads // []) | map(.data | @base64d | fromjson) else . end
  | .[0] // {};
def epoch: if . == null or startswith("0001-") then null
           else sub("\\.[0-9]+"; "") | fromdateiso8601 end;
(.pendingActivities // []) as $pa
| ($pa | map(select(.activityType.name == "dsync_StreamLSN") | hb | {key: (.Index | tostring), value: .LSN})
       | from_entries) as $lsn
| {activities: [$pa[] | {id: .activityId, type: .activityType.name, state: (.state // ""),
                         paused: (.paused // false), attempt, failure: (.lastFailure.message // null)}],
   pending: [$pa[] | select(.activityType.name == "dsync_StreamChanges") | hb
             | select(.Index == null)] | length,
   streams: [$pa[] | select(.activityType.name == "dsync_StreamChanges") | hb
             | select(.Index != null)
             | {idx: .Index, ns: (.Namespaces // []), read: (.EventsRead // 0),
                written: (.EventsWritten // 0), last: (.LastEventTime | epoch),
                lsn: $lsn[(.Index | tostring)]}]
            | sort_by(.idx)}'

S1="$(sample 0 | jq -c "$NORMALISE")" || die "could not read workflow ${WORKFLOW_ID:-from file}"
T1="$(date -u +%s)"
if [[ -n "$INTERVAL" ]]; then
  [[ "${#FILES[@]}" -gt 0 ]] || sleep "$INTERVAL"
  S2="$(sample 1 | jq -c "$NORMALISE")"
  T2="$(( T1 + INTERVAL ))"
  [[ "${#FILES[@]}" -gt 0 ]] || T2="$(date -u +%s)"
else
  S2="$S1"; T2="$T1"
fi

echo "workflow ${WORKFLOW_ID:-<file>}   $(date -u +%Y-%m-%dT%H:%M:%SZ)"
# shellcheck disable=SC2016
jq -rn --argjson a "$S1" --argjson b "$S2" --argjson t1 "$T1" --argjson t2 "$T2" '
def nsdesc: if length == 0 then "(all)" elif length == 1 then .[0]
            else "\(.[0]) +\(length - 1)" end;
def iso: if . == null then "-" else todate end;
($t2 - $t1) as $dt
| ($a.streams | map({key: (.idx | tostring), value: .}) | from_entries) as $prev
| (["idx", "namespaces", "read", "written", "gap", "last_event", "lag_s", "lsn", "lsn-read"]
   + (if $dt > 0 then ["w/s", "catchup"] else [] end)) as $hdr
| ($hdr | @tsv),
  ($b.streams[] | . as $s | $prev[($s.idx | tostring)] as $p
   | [ $s.idx, ($s.ns | nsdesc), $s.read, $s.written, ($s.read - $s.written),
       ($s.last | iso), (if $s.last then ($t2 - $s.last) else "-" end),
       ($s.lsn // "-"), (if $s.lsn then ($s.lsn - $s.read) else "-" end) ]
     + (if $dt > 0 then
          [ ((($s.written - ($p.written // $s.written)) / $dt * 10 | round) / 10),
            (if $p == null then "-"
             elif $s.read == $p.read and ($s.lsn // $s.read) <= $s.read then "idle"
             elif $s.last and $p.last then ((($s.last - $p.last) / $dt * 100 | round) / 100)
             else "-" end) ]
        else [] end)
   | @tsv),
  (if $b.pending > 0 then "(\($b.pending) StreamChanges partition(s) without a heartbeat yet)" else empty end),
  (if ($b.streams | length) == 0 and $b.pending == 0 then "(no dsync_StreamChanges activity pending — initial sync still running, or flow not streaming)" else empty end)
' | column -t -s $'\t'

PAUSED="$(jq -r '.activities[] | select(.paused or (.state | test("PAUSE")))
  | "  \(.type) activity-id=\(.id) state=\(.state) attempt=\(.attempt)\(if .failure then " failure=\(.failure)" else "" end)"' <<<"$S2")"
if [[ -n "$PAUSED" ]]; then
  echo
  warn "paused activities on ${WORKFLOW_ID:-<file>}:"
  echo "$PAUSED"
  echo "  unpause: dsync-find-pauses.sh --unpause   (uses --reset-attempts; resumes from the last heartbeat)"
fi
if [[ -n "$INTERVAL" ]]; then
  echo
  echo "catchup < 1.0 => stream falling behind; constant gap > 0 => writer-bound."
fi
