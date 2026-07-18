#!/usr/bin/env bash
# Generate one source-connector JSON per cluster from sources.json + the template.
#
# Requires: python3 (literal templating + JSON validation; no jq/sed escaping games)
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

command -v python3 >/dev/null || { echo "ERROR: python3 is required" >&2; exit 1; }
[ -f "$SRC_FILE" ] || { echo "ERROR: $SRC_FILE not found (copy sources.sample.json)" >&2; exit 1; }
[ -f "$TEMPLATE" ] || { echo "ERROR: $TEMPLATE not found" >&2; exit 1; }

SRC_FILE="$SRC_FILE" TEMPLATE="$TEMPLATE" OUT_DIR="$OUT_DIR" python3 - <<'PY'
import json, os, sys, re

src_file = os.environ["SRC_FILE"]
template = os.environ["TEMPLATE"]
out_dir  = os.environ["OUT_DIR"]

with open(template) as f:
    tpl = f.read()
with open(src_file) as f:
    sources = json.load(f)["sources"]

os.makedirs(out_dir, exist_ok=True)
print(f"Generating {len(sources)} source connector configs from {src_file} ...")

for s in sources:
    label = s["label"]
    uri   = s["uri"]
    dbs   = s["databases"]

    # regex: (db1|db2)\..*   — JSON-escaped so the file literally contains \\.
    ns_regex = "(" + "|".join(re.escape(d) for d in dbs) + ")\\..*"
    # change-stream pipeline as a JSON string value inside the connector config
    pipeline = json.dumps([{"$match": {"ns.db": {"$in": dbs}}}])

    # json.dumps(...)[1:-1] gives the properly-escaped INNER contents of a JSON
    # string (backslashes/quotes handled), which we drop into the "..." slots.
    def js(v): return json.dumps(v)[1:-1]

    body = (tpl.replace("__LABEL__", js(label))
               .replace("__SOURCE_URI__", js(uri))
               .replace("__NS_REGEX__", js(ns_regex))
               .replace("__PIPELINE__", js(pipeline)))

    out = os.path.join(out_dir, f"mongo-source-{label}.json")
    try:
        json.loads(body)  # validate
    except json.JSONDecodeError as e:
        print(f"ERROR: generated invalid JSON for {label}: {e}", file=sys.stderr)
        sys.exit(1)
    with open(out, "w") as f:
        f.write(body + "\n")
    print(f"  wrote {out}")

print("Done. Register with:  ./manage.sh register-sources")
PY
