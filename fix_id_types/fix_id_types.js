// ── Configuration ────────────────────────────────────────────────────────────
const COLLECTIONS = [
  {
    dbName:    "common-warranty-db",
    collName:  "vm_warranty_approval_online",
    wrongType: "string",
    // string "6374c4de..." → ObjectId("6374c4de...")
    fixFn: (doc) => ObjectId(doc._id)
  },
  {
    dbName:    "svoi-db",
    collName:  "consumer_did_mapping_log",
    wrongType: "object",
    // { _id: ObjectId("...") } → extract inner ObjectId
    fixFn: (doc) => doc._id._id
  },
  // Add more entries as needed:
  // {
  //   dbName:    "your-db",
  //   collName:  "your-collection",
  //   wrongType: "object",
  //   fixFn: (doc) => new ObjectId()   // compound object _id → new ObjectId
  // }
];

const DRY_RUN    = true;    // ← set to false when ready to make changes
const BATCH_SIZE = 500;     // docs per round-trip; lower if documents are very large (>32 KB each)
const LOG_EVERY  = 10_000;  // print a progress line after every N documents processed
// ─────────────────────────────────────────────────────────────────────────────

const SEPARATOR  = "═".repeat(60);
const SEPARATOR2 = "─".repeat(60);

function ts() { return new Date().toISOString(); }
function log(level, msg) { print(`[${ts()}] [${level}] ${msg}`); }

function logInfo  (msg) { log("INFO ", msg); }
function logOk    (msg) { log("OK   ", msg); }
function logSkip  (msg) { log("SKIP ", msg); }
function logWarn  (msg) { log("WARN ", msg); }
function logError (msg) { log("ERROR", msg); }
function logDry   (msg) { log("DRY  ", msg); }
function logStep  (msg) { log("STEP ", msg); }

// Returns a cursor with timeout disabled and server-side prefetch aligned to BATCH_SIZE.
// noCursorTimeout prevents expiry during long-running batch operations (critical for
// collections where scanning 1 M+ docs takes more than the default 10-minute idle timeout).
function openCursor(coll, query) {
  return coll.find(query).batchSize(BATCH_SIZE).noCursorTimeout();
}

// ─────────────────────────────────────────────────────────────────────────────

// Phase 1 — stream all wrong-type docs into the backup collection in batches.
// Progress is printed every LOG_EVERY docs, not every batch, to keep logs readable.
async function backupCollection({ dbName, collName, wrongType }, colIdx) {
  const ns         = `${dbName}.${collName}`;
  const backupNs   = `${ns}_id_fix_backup`;
  const currentDB  = db.getSiblingDB(dbName);
  const coll       = currentDB[collName];
  const backupColl = currentDB[`${collName}_id_fix_backup`];

  logInfo(`[BACKUP ${colIdx + 1}/${COLLECTIONS.length}] ${ns} → ${backupNs}`);

  let backed = 0, dupes = 0, errors = 0, lastLogAt = 0;
  const cursor = openCursor(coll, { _id: { $type: wrongType } });

  while (cursor.hasNext()) {
    const batch = [];
    while (batch.length < BATCH_SIZE && cursor.hasNext()) {
      batch.push(cursor.next());
    }

    if (DRY_RUN) {
      backed += batch.length;
      if (backed - lastLogAt >= LOG_EVERY) {
        logDry(`[${ns}] Would backup ${backed} docs so far...`);
        lastLogAt = backed;
      }
      continue;
    }

    try {
      backupColl.insertMany(batch, { ordered: false });
      backed += batch.length;
    } catch (e) {
      if (e.writeErrors) {
        const batchDupes  = e.writeErrors.filter(we => we.code === 11000).length;
        const batchErrors = e.writeErrors.filter(we => we.code !== 11000);
        dupes  += batchDupes;
        errors += batchErrors.length;
        backed += batch.length - batchErrors.length;
        batchErrors.forEach(we =>
          logError(`[${ns}] Backup FAILED at batch index ${we.index}: ${we.errmsg}`)
        );
        if (batchDupes > 0) logWarn(`[${ns}] ${batchDupes} docs in this batch already backed up — skipped`);
      } else {
        logError(`[${ns}] Backup batch FAILED entirely: ${e.message}`);
        errors += batch.length;
      }
    }

    if (backed - lastLogAt >= LOG_EVERY) {
      logInfo(`[${ns}] Backed up ${backed} docs so far...`);
      lastLogAt = backed;
    }
  }

  cursor.close();

  if (backed === 0 && dupes === 0 && errors === 0) {
    logOk(`[${ns}] No documents with wrong _id type found — nothing to backup`);
  } else {
    logOk(`[${ns}] Backup done: ${backed} backed up, ${dupes} already existed, ${errors} errors`);
  }

  return { ns, backed, dupes, errors };
}

// ─────────────────────────────────────────────────────────────────────────────

// Phase 2 — read from backup collection, insert corrected docs, delete originals.
// Using backup as source guarantees we only touch docs that were safely backed up.
async function fixCollection({ dbName, collName, fixFn }, colIdx) {
  const ns         = `${dbName}.${collName}`;
  const currentDB  = db.getSiblingDB(dbName);
  const coll       = currentDB[collName];
  const backupColl = currentDB[`${collName}_id_fix_backup`];

  logInfo(`[FIX ${colIdx + 1}/${COLLECTIONS.length}] ${ns}`);

  let fixed = 0, skipped = 0, errors = 0, processed = 0, lastLogAt = 0;
  const cursor = openCursor(backupColl, {});

  while (cursor.hasNext()) {
    const batch = [];
    while (batch.length < BATCH_SIZE && cursor.hasNext()) {
      batch.push(cursor.next());
    }
    processed += batch.length;

    if (DRY_RUN) {
      batch.forEach(doc => {
        try {
          const newId = fixFn(doc);
          if (newId === null || newId === undefined) throw new Error("fixFn returned null/undefined");
          fixed++;
        } catch (e) {
          logSkip(`[${ns}] _id ${JSON.stringify(doc._id)}: ${e.message}`);
          skipped++;
        }
      });
      if (processed - lastLogAt >= LOG_EVERY) {
        logDry(`[${ns}] Would fix ${fixed} docs so far (${skipped} skipped)...`);
        lastLogAt = processed;
      }
      continue;
    }

    // ── Derive new IDs (skip docs where fixFn fails) ──────────────
    const valid = [];
    batch.forEach(doc => {
      try {
        const newId = fixFn(doc);
        if (newId === null || newId === undefined) throw new Error("fixFn returned null/undefined");
        valid.push({ doc, newId });
      } catch (e) {
        logSkip(`[${ns}] _id ${JSON.stringify(doc._id)}: ${e.message}`);
        skipped++;
      }
    });

    if (valid.length === 0) continue;

    // ── Step 1: Insert corrected docs ─────────────────────────────
    const failedInsert = new Set();
    try {
      coll.insertMany(valid.map(v => Object.assign({}, v.doc, { _id: v.newId })), { ordered: false });
    } catch (e) {
      if (e.writeErrors) {
        e.writeErrors.forEach(we => {
          if (we.code === 11000) {
            // already inserted on a previous run — still proceed to delete the original
          } else {
            logError(`[${ns}] Insert FAILED at batch index ${we.index}: ${we.errmsg}`);
            failedInsert.add(we.index);
            errors++;
          }
        });
      } else {
        logError(`[${ns}] Insert batch FAILED entirely: ${e.message}`);
        errors += valid.length;
        continue;
      }
    }

    // ── Step 2: Batch delete originals ────────────────────────────
    // runCommand avoids "retryable writes not supported" error on DocumentDB
    const toDelete = valid.filter((_, i) => !failedInsert.has(i));
    try {
      const deleteResult = currentDB.runCommand({
        delete: collName,
        deletes: toDelete.map(v => ({ q: { _id: v.doc._id }, limit: 1 }))
      });
      if (deleteResult.ok) {
        (deleteResult.writeErrors || []).forEach(we =>
          logError(`[${ns}] Delete failed at batch index ${we.index}: ${we.errmsg}`)
        );
        errors += (deleteResult.writeErrors || []).length;
        fixed  += (deleteResult.n || 0);
      } else {
        throw new Error(JSON.stringify(deleteResult));
      }
    } catch (e) {
      logError(`[${ns}] Delete batch FAILED: ${e.message}`);
      errors += toDelete.length;
    }

    if (processed - lastLogAt >= LOG_EVERY) {
      logInfo(`[${ns}] Fixed ${fixed} docs so far (${skipped} skipped, ${errors} errors)...`);
      lastLogAt = processed;
    }
  }

  cursor.close();

  logOk(`[${ns}] Fix done: ${fixed} fixed, ${skipped} skipped, ${errors} errors`);
  return { ns, fixed, skipped, errors };
}

// ── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  const runStart = new Date();
  print(SEPARATOR);
  logInfo(`Script start`);
  logInfo(`Mode       : ${DRY_RUN ? "DRY RUN (no changes will be made)" : "LIVE (changes WILL be applied)"}`);
  logInfo(`Batch size : ${BATCH_SIZE} docs/round-trip`);
  logInfo(`Log every  : ${LOG_EVERY} docs`);
  logInfo(`Collections: ${COLLECTIONS.length}`);
  COLLECTIONS.forEach((c, i) =>
    logInfo(`  [${i + 1}] ${c.dbName}.${c.collName}  (wrongType: ${c.wrongType})`)
  );

  // ── Phase 1: Backup all collections in parallel ───────────────
  print(SEPARATOR);
  logInfo(`PHASE 1 — BACKUP ALL COLLECTIONS`);
  print(SEPARATOR2);
  const backupResults = await Promise.all(COLLECTIONS.map((c, i) => backupCollection(c, i)));

  // ── Phase 2: Insert + delete all collections in parallel ──────
  print(SEPARATOR);
  logInfo(`PHASE 2 — INSERT CORRECTED + DELETE ORIGINALS`);
  print(SEPARATOR2);
  const fixResults = await Promise.all(COLLECTIONS.map((c, i) => fixCollection(c, i)));

  // ── Global summary ─────────────────────────────────────────────
  const totalElapsed = ((new Date() - runStart) / 1000).toFixed(1);
  print(SEPARATOR);
  logInfo(`GLOBAL SUMMARY`);
  logInfo(SEPARATOR2);
  COLLECTIONS.forEach((c, i) => {
    const ns = `${c.dbName}.${c.collName}`;
    const b  = backupResults[i];
    const f  = fixResults[i];
    logInfo(`${ns}`);
    logInfo(`  Backup : ${b.backed} backed up, ${b.dupes} already existed, ${b.errors} errors`);
    logInfo(`  Fix    : ${f.fixed} fixed, ${f.skipped} skipped, ${f.errors} errors`);
  });
  print(SEPARATOR2);
  logInfo(`Total elapsed : ${totalElapsed}s`);
  logInfo(`Mode          : ${DRY_RUN ? "DRY RUN — no changes were made" : "LIVE — changes applied"}`);
  logInfo(`Script end`);
  print(SEPARATOR);
})();
