/**
 * finalize_unique_indexes.js
 *
 * Purpose:
 *   mongosync's commit process runs `collMod ... prepareUnique: true` on every
 *   destination index that is unique on the source, BEFORE canWrite becomes true.
 *   The final step - converting prepareUnique -> unique - happens AFTER
 *   canWrite:true, in the "WaitingForIndexConstraintEnablement" phase.
 *
 *   If mongosync crashed after canWrite:true but before reaching COMMITTED,
 *   that final step never ran. This script finds every index still sitting
 *   in prepareUnique state and finishes the conversion manually, the same way
 *   mongosync itself would have.
 *
 * Usage:
 *   Connect mongosh to the DESTINATION cluster (primary / mongos), then:
 *     load('finalize_unique_indexes.js')
 *   or paste the whole file into the shell.
 *
 *   Review the DRY RUN section output first. Only indexes that pass the dry
 *   run get finalized. Anything that fails is left as prepareUnique and
 *   printed so you can decide how to handle the duplicates (this is a lower
 *   environment, so leaving a few non-unique for now may be fine -
 *   just don't skip reading the report).
 */

function findPrepareUniqueIndexes() {
  const candidates = [];
  const skipDbs = new Set(["admin", "local", "config", "__mdb_internal_mongosync"]);

  const dbNames = db.getMongo().getDBNames();

  for (const dbName of dbNames) {
    if (skipDbs.has(dbName)) continue;

    const currentDb = db.getSiblingDB(dbName);
    const collNames = currentDb.getCollectionNames();

    for (const collName of collNames) {
      if (collName.startsWith("system.")) continue;

      let indexes;
      try {
        indexes = currentDb.getCollection(collName).getIndexes();
      } catch (e) {
        print(`  [skip] ${dbName}.${collName}: could not list indexes (${e.message})`);
        continue;
      }

      for (const idx of indexes) {
        if (idx.prepareUnique === true && idx.unique !== true) {
          candidates.push({
            dbName: dbName,
            collName: collName,
            indexName: idx.name,
            keyPattern: idx.key
          });
        }
      }
    }
  }

  return candidates;
}

function finalizeUniqueIndexes() {
  print("=== Scanning for indexes left in prepareUnique state ===");
  const candidates = findPrepareUniqueIndexes();

  if (candidates.length === 0) {
    print("No prepareUnique indexes found. Nothing to do - conversion may already be complete.");
    return { converted: [], failed: [] };
  }

  print(`Found ${candidates.length} index(es) still in prepareUnique state:\n`);
  candidates.forEach(c => print(`  - ${c.dbName}.${c.collName}  index="${c.indexName}"  key=${JSON.stringify(c.keyPattern)}`));

  const converted = [];
  const failed = [];

  print("\n=== Step 1: Dry run (checking for duplicate key violations) ===");
  for (const c of candidates) {
    const currentDb = db.getSiblingDB(c.dbName);
    const dryRunResult = currentDb.runCommand({
      collMod: c.collName,
      index: {
        keyPattern: c.keyPattern,
        unique: true
      },
      dryRun: true
    });

    if (dryRunResult.ok === 1) {
      print(`  [OK]   ${c.dbName}.${c.collName} (${c.indexName}) - no violations, safe to convert`);
      c.dryRunOk = true;
    } else {
      print(`  [FAIL] ${c.dbName}.${c.collName} (${c.indexName}) - duplicate key violations found:`);
      print(JSON.stringify(dryRunResult, null, 2));
      c.dryRunOk = false;
      c.dryRunError = dryRunResult;
    }
  }

  print("\n=== Step 2: Finalizing indexes that passed the dry run ===");
  for (const c of candidates) {
    if (!c.dryRunOk) {
      failed.push(c);
      continue;
    }

    const currentDb = db.getSiblingDB(c.dbName);
    const result = currentDb.runCommand({
      collMod: c.collName,
      index: {
        keyPattern: c.keyPattern,
        unique: true
      }
    });

    if (result.ok === 1) {
      print(`  [DONE] ${c.dbName}.${c.collName} (${c.indexName}) is now a true unique index.`);
      converted.push(c);
    } else {
      print(`  [ERROR] ${c.dbName}.${c.collName} (${c.indexName}) failed to finalize:`);
      print(JSON.stringify(result, null, 2));
      failed.push(c);
    }
  }

  print("\n=== Summary ===");
  print(`Converted: ${converted.length}`);
  converted.forEach(c => print(`  - ${c.dbName}.${c.collName} (${c.indexName})`));
  print(`Left as prepareUnique (needs manual attention): ${failed.length}`);
  failed.forEach(c => print(`  - ${c.dbName}.${c.collName} (${c.indexName})`));

  if (failed.length > 0) {
    print("\nFor collections with violations, either:");
    print("  1. Delete/merge the duplicate documents shown above, then re-run this script, or");
    print("  2. Leave the index as prepareUnique for now if the mismatch is acceptable in this environment.");
    print("     (prepareUnique still rejects *new* duplicate inserts going forward - it just doesn't");
    print("     retroactively enforce uniqueness on documents that already violate it.)");
  }

  return { converted, failed };
}

// Run it
finalizeUniqueIndexes();
