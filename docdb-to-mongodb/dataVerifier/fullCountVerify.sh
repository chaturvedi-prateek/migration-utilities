#!/bin/bash
#
# Compare per-collection document counts, source vs destination.
# Uses estimatedDocumentCount (collection metadata, no scan). While CDC is live,
# busy collections can differ by a few documents; re-check them after source
# writes stop.
#
# Needs DOCDB_SRC and MDB_DEST (host-side URIs) in the environment.
# Exit status: 0 only if every collection PASSes.

: "${DOCDB_SRC:?set DOCDB_SRC}" "${MDB_DEST:?set MDB_DEST}"

PASS=0
FAIL=0
ERR=0

# Counts above 2^31 come back from mongosh as Long('…'); String() prints the
# plain number, so bash arithmetic can use it.
count() {
  mongosh "$1" --quiet --eval \
    "print(String(db.getSiblingDB('$2')['$3'].estimatedDocumentCount()))" 2>/dev/null | tr -d '\r'
}

DATABASES=$(mongosh "$DOCDB_SRC" --quiet --eval \
  'db.adminCommand({listDatabases:1}).databases.map(d=>d.name).filter(d=>d!="admin"&&d!="local"&&d!="config").join("\n")' 2>/dev/null | tr -d '\r')
if [ -z "$DATABASES" ]; then
  echo "ERROR: could not list databases on the source (check DOCDB_SRC / connectivity)" >&2
  exit 2
fi

while IFS= read -r DB; do
  [ -z "$DB" ] && continue
  echo "=== $DB ==="

  COLLECTIONS=$(mongosh "$DOCDB_SRC" --quiet --eval \
    "db.getSiblingDB('$DB').getCollectionNames().join('\n')" 2>/dev/null | tr -d '\r')

  while IFS= read -r COL; do
    [ -z "$COL" ] && continue

    SRC=$(count "$DOCDB_SRC" "$DB" "$COL")
    DST=$(count "$MDB_DEST" "$DB" "$COL")

    if ! [[ "$SRC" =~ ^[0-9]+$ && "$DST" =~ ^[0-9]+$ ]]; then
      echo "ERROR | $DB.$COL | could not count (src='$SRC' dst='$DST')"
      ERR=$((ERR + 1))
      continue
    fi

    DIFF=$((SRC - DST))
    if [ "$DIFF" -eq 0 ]; then
      echo "PASS | $DB.$COL | src=$SRC dst=$DST diff=0"
      PASS=$((PASS + 1))
    else
      echo "FAIL | $DB.$COL | src=$SRC dst=$DST diff=$DIFF"
      FAIL=$((FAIL + 1))
    fi
  done <<< "$COLLECTIONS"
done <<< "$DATABASES"

echo ""
echo "============================="
echo "PASSED: $PASS  FAILED: $FAIL  ERRORS: $ERR"
echo "============================="
[ "$FAIL" -eq 0 ] && [ "$ERR" -eq 0 ]
