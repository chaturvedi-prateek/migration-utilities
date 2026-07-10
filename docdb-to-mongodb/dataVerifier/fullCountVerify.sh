#!/bin/bash

PASS=0
FAIL=0

DATABASES=$(mongosh "$DOCDB_SRC" --quiet --eval \
  'db.adminCommand({listDatabases:1}).databases.map(d=>d.name).filter(d=>d!="admin"&&d!="local"&&d!="config").join("\n")' 2>/dev/null)

while IFS= read -r DB; do
  [ -z "$DB" ] && continue
  echo "=== $DB ==="

  COLLECTIONS=$(mongosh "$DOCDB_SRC" --quiet --eval \
    "db.getSiblingDB('$DB').getCollectionNames().join('\n')" 2>/dev/null)

  while IFS= read -r COL; do
    [ -z "$COL" ] && continue

    SRC=$(mongosh "$DOCDB_SRC" --quiet --eval \
      "db.getSiblingDB('$DB')['$COL'].estimatedDocumentCount()" 2>/dev/null)
    DST=$(mongosh "$MDB_DEST" --quiet --eval \
      "db.getSiblingDB('$DB')['$COL'].estimatedDocumentCount()" 2>/dev/null)
    DIFF=$((SRC - DST))

    if [ "$SRC" = "$DST" ]; then
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
echo "PASSED: $PASS  FAILED: $FAIL"
echo "============================="
