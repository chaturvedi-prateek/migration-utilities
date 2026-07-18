#!/usr/bin/env bash
# Generate one source-connector JSON per cluster from sources.json + the template.
#
# Requires: jq
# Usage:    ./generate-sources.sh [sources.json]
# Output:   source/generated/mongo-source-<label>.json  (one per cluster)
#
# The namespace regex and change-stream pipeline are derived from each cluster's
# "databases" list. Same-name collections across clusters share topic cdc.<db>.<coll>
# (same topic.prefix in the template) so they MERGE into one target collection.
set -euo pipefail

cd "$(dirname "$0")"
SRC_FILE="${1:-sources.json}"
TEMPLATE="source/mongo-source.template.json"
OUT_DIR="source/generated"

command -v jq >/dev/null || { echo "ERROR: jq is required" >&2; exit 1; }
[ -f "$SRC_FILE" ] || { echo "ERROR: $SRC_FILE not found (copy sources.sample.json)" >&2; exit 1; }

mkdir -p "$OUT_DIR"
count=$(jq '.sources | length' "$SRC_FILE")
echo "Generating $count source connector configs from $SRC_FILE ..."

for i in $(seq 0 $((count - 1))); do
  label=$(jq -r ".sources[$i].label" "$SRC_FILE")
  uri=$(jq -r ".sources[$i].uri" "$SRC_FILE")

  # dbs = ["juno","orders"]  ->  regex "(juno|orders)\\..*"  and  $match on ns.db $in [...]
  db_alt=$(jq -r ".sources[$i].databases | join(\"|\")" "$SRC_FILE")
  # JSON string needs a literal backslash-dot; in bash double quotes "\\\\" => "\\".
  ns_regex="($db_alt)\\\\..*"
  pipeline=$(jq -c ".sources[$i].databases | [{\"\$match\": {\"ns.db\": {\"\$in\": .}}}]" "$SRC_FILE")
  # escape inner double-quotes for embedding as a JSON string value: " => \"
  pipeline_escaped=${pipeline//\"/\\\"}

  # Render via bash literal substitution (${var//find/replace}) — no backslash
  # reinterpretation, unlike sed, so JSON escapes survive intact.
  out_content="$TEMPLATE_BODY"
  out_content=${out_content//__LABEL__/$label}
  out_content=${out_content//__SOURCE_URI__/$uri}
  out_content=${out_content//__NS_REGEX__/$ns_regex}
  out_content=${out_content//__PIPELINE__/$pipeline_escaped}

  out="$OUT_DIR/mongo-source-$label.json"
  printf '%s\n' "$out_content" > "$out"

  # sanity: valid JSON?
  jq empty "$out" || { echo "ERROR: generated invalid JSON for $label" >&2; exit 1; }
  echo "  wrote $out"
done

echo "Done. Register with:  ./manage.sh register-sources"
