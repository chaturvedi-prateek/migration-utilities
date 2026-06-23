// ── Configuration ────────────────────────────────────────────────────────────
// Databases to scan. Use [] to scan every user database on the server.
const DATABASES = [
  "common-warranty-db",
  "svoi-db",
  // "another-db",
];
// ─────────────────────────────────────────────────────────────────────────────

const SEPARATOR  = "═".repeat(60);
const SEPARATOR2 = "─".repeat(60);

function ts()     { return new Date().toISOString(); }
function log(level, msg) { print(`[${ts()}] [${level}] ${msg}`); }
function logInfo (msg) { log("INFO ", msg); }
function logMixed(msg) { log("MIXED", msg); }
function logClean(msg) { log("CLEAN", msg); }
function logWarn (msg) { log("WARN ", msg); }

// Two index probes per collection — no collection scan.
//
// How it works:
//   MongoDB sorts mixed BSON types by BSON comparison order:
//     numbers < string < object < array < binData < objectId < bool < date ...
//   Sorting _id ASC  → first doc has the LOWEST  type in that order.
//   Sorting _id DESC → first doc has the HIGHEST type in that order.
//   If the two types differ, the collection contains more than one _id type.
//
// Cost: O(log N) per direction — the _id index is used; documents are not read.
function probeIdType(coll, sortDir) {
  const res = coll.aggregate([
    { $sort:    { _id: sortDir } },
    { $limit:   1 },
    { $project: { _idType: { $type: "$_id" } } }
  ]).toArray();
  return res.length ? res[0]._idType : null;
}

(async () => {
  const runStart = new Date();
  print(SEPARATOR);
  logInfo("detect_mixed_ids — fast _id type scan via ASC / DESC index probes");
  logInfo("Cost    : 2 index seeks per collection (no document reads)");
  logInfo("Method  : if type(min _id) != type(max _id) => MIXED");
  print(SEPARATOR2);

  let dbNames;
  if (DATABASES.length > 0) {
    dbNames = DATABASES;
  } else {
    dbNames = db.adminCommand({ listDatabases: 1 }).databases
      .map(d => d.name)
      .filter(n => !["admin", "local", "config"].includes(n));
  }

  logInfo(`Databases to scan : ${dbNames.length}`);
  print(SEPARATOR2);

  const mixedList   = [];
  let totalScanned  = 0;

  for (const dbName of dbNames) {
    const currentDB = db.getSiblingDB(dbName);
    let collNames;
    try {
      collNames = currentDB.getCollectionNames()
        .filter(c => !c.startsWith("system."));
    } catch (e) {
      logWarn(`${dbName} : cannot list collections — ${e.message}`);
      continue;
    }

    logInfo(`[${dbName}] ${collNames.length} collection(s)`);

    for (const collName of collNames) {
      totalScanned++;
      const coll = currentDB[collName];
      const ns   = `${dbName}.${collName}`;

      try {
        const minType = probeIdType(coll, 1);
        if (minType === null) {
          logClean(`  ${collName} — (empty)`);
          continue;
        }
        const maxType = probeIdType(coll, -1);

        if (minType !== maxType) {
          logMixed(`  ${collName} — ASC type: ${minType}  |  DESC type: ${maxType}`);
          mixedList.push({ ns, minType, maxType });
        } else {
          logClean(`  ${collName} — ${minType}`);
        }
      } catch (e) {
        logWarn(`  ${collName} : ${e.message}`);
      }
    }
  }

  const elapsed = ((new Date() - runStart) / 1000).toFixed(1);
  print(SEPARATOR);
  logInfo("SUMMARY");
  logInfo(`  Collections scanned         : ${totalScanned}`);
  logInfo(`  Collections with mixed _ids : ${mixedList.length}`);

  if (mixedList.length > 0) {
    print(SEPARATOR2);
    logInfo("Collections requiring attention:");
    mixedList.forEach(({ ns, minType, maxType }) =>
      logInfo(`  ${ns}  (min type: ${minType}, max type: ${maxType})`)
    );
    print(SEPARATOR2);
    logInfo("Next steps:");
    logInfo("  1. Run count_id_types.js on the above namespaces for exact counts per type");
    logInfo("  2. Add the namespaces to fix_id_types.js COLLECTIONS array and run to fix");
  } else {
    logInfo("  No mixed _id types found — safe to proceed with migration");
  }

  logInfo(`  Elapsed : ${elapsed}s`);
  print(SEPARATOR);
})();
