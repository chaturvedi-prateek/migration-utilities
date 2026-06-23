// ── Configuration ────────────────────────────────────────────────────────────
// Namespaces to count — "database.collection" format.
const NAMESPACES = [
  "common-warranty-db.vm_warranty_approval_online",
  "svoi-db.consumer_did_mapping_log",
  // "another-db.another-collection",
];
// ─────────────────────────────────────────────────────────────────────────────

const SEPARATOR  = "═".repeat(60);
const SEPARATOR2 = "─".repeat(60);

function ts()     { return new Date().toISOString(); }
function log(level, msg) { print(`[${ts()}] [${level}] ${msg}`); }
function logInfo (msg) { log("INFO ", msg); }
function logWarn (msg) { log("WARN ", msg); }

(async () => {
  const runStart = new Date();
  print(SEPARATOR);
  logInfo(`count_id_types — _id type distribution for ${NAMESPACES.length} namespace(s)`);
  logInfo("Method  : covered aggregation on _id index — reads index entries, not documents");
  print(SEPARATOR2);

  for (const ns of NAMESPACES) {
    const dot = ns.indexOf(".");
    if (dot < 1 || dot === ns.length - 1) {
      logWarn(`Invalid namespace "${ns}" — must be "database.collection"`);
      continue;
    }

    const dbName   = ns.slice(0, dot);
    const collName = ns.slice(dot + 1);
    const coll     = db.getSiblingDB(dbName)[collName];

    logInfo(`${ns}`);

    try {
      const rows = coll.aggregate([
        { $group: { _id: { $type: "$_id" }, count: { $sum: 1 } } },
        { $sort:  { count: -1 } }
      ], { hint: { _id: 1 } }).toArray();

      if (rows.length === 0) {
        logInfo("  (empty collection)");
        print(SEPARATOR2);
        continue;
      }

      const total = rows.reduce((s, r) => s + Number(r.count), 0);

      rows.forEach(r => {
        const count = Number(r.count);
        const pct   = ((count / total) * 100).toFixed(2);
        logInfo(`  ${String(r._id).padEnd(14)} ${String(count).padStart(12)} docs   ${pct.padStart(7)}%`);
      });

      logInfo(`  ${"total".padEnd(14)} ${String(total).padStart(12)} docs   100.00%`);

      if (rows.length > 1) {
        logInfo("  STATUS : MIXED — configure fix_id_types.js and run to fix before migration");
      } else {
        logInfo("  STATUS : OK — single _id type");
      }
    } catch (e) {
      logWarn(`  ${e.message}`);
    }

    print(SEPARATOR2);
  }

  const elapsed = ((new Date() - runStart) / 1000).toFixed(1);
  logInfo(`Elapsed : ${elapsed}s`);
  print(SEPARATOR);
})();
