#!/usr/bin/env bash
#
# split-hot-cold.sh — split namespaces into a "hot" flow (per-namespace
# streams) and a "cold" flow (one shared stream) from a cs-rate-profile.js
# profile.
#
#   split-hot-cold.sh cs-profile.json [--max-cold-rate 500] [--all-namespaces all.txt]
#                                     [--outdir .]
#
# Greedy: takes namespaces into hot, highest rate first, until the remaining
# cold total is <= --max-cold-rate ev/s. Writes hot.txt / cold.txt (one
# namespace per line) and prints ready-to-paste --namespace lists.
#
# Namespaces with no writes during the profile window are NOT in the profile.
# Pass --all-namespaces (one db.coll per line) so they land in cold.txt;
# generate it with:
#   mongosh "$DOCDB_SRC" --quiet --eval 'db.adminCommand({listDatabases:1}).databases
#     .filter(d=>!["admin","local","config"].includes(d.name))
#     .forEach(d=>db.getSiblingDB(d.name).getCollectionNames()
#     .forEach(c=>print(d.name+"."+c)))' > all.txt
#
set -euo pipefail

MAX_COLD="500"
ALL_NS=""
OUTDIR="."
PROFILE=""

usage() { sed -n '3,20p' "$0" | sed 's/^# \{0,1\}//'; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-cold-rate)  MAX_COLD="$2"; shift 2 ;;
    --all-namespaces) ALL_NS="$2"; shift 2 ;;
    --outdir)         OUTDIR="$2"; shift 2 ;;
    -h|--help)        usage; exit 0 ;;
    -*) die "unknown option: $1" ;;
    *)  PROFILE="$1"; shift ;;
  esac
done
[[ -n "$PROFILE" && -f "$PROFILE" ]] || { usage; die "profile JSON not found: ${PROFILE:-<none>}"; }
command -v jq >/dev/null 2>&1 || die "jq is required (installed by the toolkit)"
[[ -z "$ALL_NS" || -f "$ALL_NS" ]] || die "--all-namespaces file not found: ${ALL_NS}"
mkdir -p "$OUTDIR"

SPLIT="$(jq -c --argjson max "$MAX_COLD" '
  (.byNamespace | sort_by(-.rate)) as $all
  | reduce $all[] as $n ({hot: [], cold: [], rest: ($all | map(.rate) | add // 0)};
      if .rest > $max then .hot += [$n] | .rest -= $n.rate else .cold += [$n] end)
  | {hot, cold, hotRate: (.hot | map(.rate) | add // 0), coldRate: .rest}' "$PROFILE")"

jq -r '.hot[].ns'  <<<"$SPLIT" > "${OUTDIR}/hot.txt"
jq -r '.cold[].ns' <<<"$SPLIT" > "${OUTDIR}/cold.txt"
if [[ -n "$ALL_NS" ]]; then
  # Unprofiled namespaces (no writes in the window) belong to the cold flow.
  UNSEEN="$(grep -vxF -f "${OUTDIR}/hot.txt" -f "${OUTDIR}/cold.txt" "$ALL_NS" \
    | grep -v '^[[:space:]]*$' || true)"
  [[ -z "$UNSEEN" ]] || printf '%s\n' "$UNSEEN" >> "${OUTDIR}/cold.txt"
fi

HOT_N="$(grep -c . "${OUTDIR}/hot.txt" || true)"
COLD_N="$(grep -c . "${OUTDIR}/cold.txt" || true)"
printf 'hot : %4s namespaces, %s ev/s  -> %s/hot.txt\n'  "$HOT_N"  "$(jq -r .hotRate <<<"$SPLIT")"  "$OUTDIR"
printf 'cold: %4s namespaces, %s ev/s  -> %s/cold.txt  (threshold %s ev/s)\n' \
  "$COLD_N" "$(jq -r .coldRate <<<"$SPLIT")" "$OUTDIR" "$MAX_COLD"

for side in hot cold; do
  f="${OUTDIR}/${side}.txt"
  [[ -s "$f" ]] || continue
  echo
  echo "# ${side} flow — .env (comma-separated --namespace is accepted by dsynct run):"
  echo "NAMESPACE=$(paste -sd, "$f")"
  echo "# ${side} flow — as run arguments:"
  sed 's/^/--namespace=/' "$f" | paste -sd' ' -
done

echo
warn "Collections created AFTER this split are in NEITHER list and will not be migrated"
warn "by either flow. Re-run the split (and add them to a flow) before initial sync."
[[ -n "$ALL_NS" ]] || warn "No --all-namespaces given: namespaces with no writes during the profile are in neither list."
warn "Each stream partition holds 2 activity slots (StreamChanges + StreamLSN) for its whole"
warn "lifetime: the hot flow needs CONCURRENT_ACTIVITIES x WORKER_COUNT >= 2 x ${HOT_N} plus headroom."
