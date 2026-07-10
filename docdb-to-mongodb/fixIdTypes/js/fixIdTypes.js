// ── Configuration ────────────────────────────────────────────────────────────
const COLLECTIONS = [
  {
    namespace:      "common-warranty-db.vm_warranty_approval_online",
    wrongType:      "string",
    fixFn:          (doc) => ObjectId(doc._id),   // string "6374c4de..." → ObjectId("6374c4de...")
    newIdOnFailure: false,   // false = skip doc if fixFn fails
  },
  {
    namespace:      "svoi-db.consumer_did_mapping_log",
    wrongType:      "object",
    fixFn:          (doc) => doc._id._id,          // { _id: ObjectId("...") } → inner ObjectId
    newIdOnFailure: true,    // true = assign new ObjectId() if fixFn fails (e.g. compound object)
  },
  // ── Add more collections as needed: ─────────────────────────────────────────
  // {
  //   namespace:      "your-db.your-collection",
  //   wrongType:      "string",
  //   fixFn:          (doc) => ObjectId(doc._id),
  //   newIdOnFailure: false,
  // },
];

const DRY_RUN                = true;        // ← set to false when ready to apply changes
const BATCH_SIZE             = 1000;        // docs per insertMany / runCommand batch
const LOG_EVERY              = 1_000_000;   // progress line every N docs — keeps output sane on billion-scale collections
const MAX_INLINE_WARN        = 20;          // max individual WARN lines per phase; excess counted silently
const MAX_INLINE_SKIP        = 20;          // max individual SKIP lines per phase; excess counted silently
const DROP_BACKUP_ON_SUCCESS = false;       // set true to drop backup collection after a clean fix
// ─────────────────────────────────────────────────────────────────────────────

const SEPARATOR  = "═".repeat(60);
const SEPARATOR2 = "─".repeat(60);

function ts()  { return new Date().toISOString(); }
function log(level, msg) { print(`[${ts()}] [${level}] ${msg}`); }
function logInfo (msg) { log("INFO ", msg); }
function logOk   (msg) { log("OK   ", msg); }
function logWarn (msg) { log("WARN ", msg); }
function logError(msg) { log("ERROR", msg); }
function logDry  (msg) { log("DRY  ", msg); }
function logStep (msg) { log("STEP ", msg); }

// Comma-separated number: 1234567 → "1,234,567"
function fmt(n) {
  return String(Math.round(n)).replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Docs per second, rounded and formatted
function rate(count, startMs) {
  const secs = (Date.now() - startMs) / 1000;
  return secs > 0 ? fmt(Math.round(count / secs)) : "—";
}

function parseNamespace(ns) {
  const dot = ns.indexOf(".");
  if (dot < 1 || dot === ns.length - 1)
    throw new Error(`Invalid namespace "${ns}" — expected "database.collection"`);
  return { dbName: ns.slice(0, dot), collName: ns.slice(dot + 1) };
}

// noCursorTimeout prevents expiry during long batches on large collections.
function openCursor(coll, filter) {
  return coll.find(filter).batchSize(BATCH_SIZE).noCursorTimeout();
}

// runCommand avoids "retryable writes not supported" on DocumentDB.
function runBatchDelete(currentDB, collName, ids) {
  if (ids.length === 0) return { deleted: 0, errors: 0 };
  const result = currentDB.runCommand({
    delete: collName,
    deletes: ids.map(id => ({ q: { _id: id }, limit: 1 }))
  });
  if (!result.ok) throw new Error(`delete command failed: ${JSON.stringify(result)}`);
  const writeErrors = result.writeErrors || [];
  writeErrors.forEach(we => logError(`  Delete index ${we.index} failed: ${we.errmsg}`));
  return { deleted: result.n || 0, errors: writeErrors.length };
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 1 — Move wrong-type docs from source into the backup collection.
//
// Per batch:
//   a. insertMany(batch) into <collection>_id_fix_backup  (ordered:false — max throughput)
//   b. runCommand delete from source for IDs confirmed in backup
//
// Source is clean of wrong-type docs after this phase completes.
// The backup holds all originals; Phase 2 reads from it.
// ─────────────────────────────────────────────────────────────────────────────
function phase1Move({ namespace, wrongType }, colIdx) {
  const { dbName, collName } = parseNamespace(namespace);
  const currentDB  = db.getSiblingDB(dbName);
  const coll       = currentDB[collName];
  const backupColl = currentDB[`${collName}_id_fix_backup`];
  const backupNs   = `${dbName}.${collName}_id_fix_backup`;

  logStep(`Phase 1 — move _id type "${wrongType}" docs → ${backupNs}`);

  const cursor    = openCursor(coll, { _id: { $type: wrongType } });
  const phaseStart = Date.now();
  let moved = 0, dupes = 0, errors = 0, lastLogAt = 0;

  while (cursor.hasNext()) {
    const batch = [];
    while (batch.length < BATCH_SIZE && cursor.hasNext()) {
      batch.push(cursor.next());
    }

    if (DRY_RUN) {
      moved += batch.length;
      if (moved - lastLogAt >= LOG_EVERY || !cursor.hasNext()) {
        logDry(`  Would move ${fmt(moved)} docs (${rate(moved, phaseStart)}/sec) ...`);
        lastLogAt = moved;
      }
      continue;
    }

    // ── a. Insert batch into backup ──────────────────────────────────
    let toDelete = batch.map(d => d._id);

    try {
      backupColl.insertMany(batch, { ordered: false });
    } catch (e) {
      if (e.writeErrors) {
        const hardErrors    = e.writeErrors.filter(we => we.code !== 11000);
        const hardErrorIdxs = new Set(hardErrors.map(we => we.index));
        dupes  += e.writeErrors.filter(we => we.code === 11000).length;
        errors += hardErrors.length;
        hardErrors.forEach(we =>
          logError(`  Backup insert index ${we.index} failed: ${we.errmsg}`)
        );
        // Only delete IDs confirmed safe in backup (successes + dupes are both present there)
        toDelete = batch
          .filter((_, i) => !hardErrorIdxs.has(i))
          .map(d => d._id);
      } else {
        logError(`  Backup insertMany failed for entire batch: ${e.message}`);
        errors += batch.length;
        continue; // do not delete from source if backup failed
      }
    }

    // ── b. Delete confirmed-backup docs from source ──────────────────
    try {
      const { deleted, errors: delErrors } = runBatchDelete(currentDB, collName, toDelete);
      moved  += deleted;
      errors += delErrors;
    } catch (e) {
      logError(`  Source delete failed for batch: ${e.message}`);
      errors += toDelete.length;
    }

    if (moved - lastLogAt >= LOG_EVERY) {
      logInfo(`  Moved ${fmt(moved)} docs (${rate(moved, phaseStart)}/sec) ...`);
      lastLogAt = moved;
    }
  }

  cursor.close();
  const elapsed = ((Date.now() - phaseStart) / 1000).toFixed(1);

  if (DRY_RUN) {
    logDry(`  Would move ${fmt(moved)} docs total`);
  } else if (moved === 0 && dupes === 0 && errors === 0) {
    logOk(`  No documents with _id type "${wrongType}" found — nothing to move`);
  } else {
    logOk(`  Moved ${fmt(moved)} | already in backup ${fmt(dupes)} | errors ${fmt(errors)} | ${elapsed}s`);
  }

  return { moved, dupes, errors };
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2 — Read backup, transform _id, insert corrected docs into source.
//
// Live  : reads from backup (Phase 1 populated it).
// Dry run: reads from source with wrongType filter (backup does not exist yet).
//
// newIdOnFailure:
//   true  — assign new ObjectId() when fixFn cannot derive one; doc is migrated.
//   false — skip the doc; it stays in the backup for investigation.
//
// Inline SKIP / WARN lines are capped at MAX_INLINE_SKIP / MAX_INLINE_WARN per
// phase; the remaining count is reported once in the phase summary to keep logs
// readable on collections with millions of bad docs.
// ─────────────────────────────────────────────────────────────────────────────
function phase2Transform({ namespace, wrongType, fixFn, newIdOnFailure }, colIdx) {
  const { dbName, collName } = parseNamespace(namespace);
  const currentDB  = db.getSiblingDB(dbName);
  const coll       = currentDB[collName];
  const backupColl = currentDB[`${collName}_id_fix_backup`];

  logStep(`Phase 2 — transform and insert corrected docs into ${namespace}`);

  const readColl = DRY_RUN ? coll       : backupColl;
  const filter   = DRY_RUN ? { _id: { $type: wrongType } } : {};

  const cursor     = openCursor(readColl, filter);
  const phaseStart = Date.now();
  let inserted = 0, skipped = 0, errors = 0, processed = 0, lastLogAt = 0;
  let warnShown = 0, skipShown = 0, warnSuppressed = 0, skipSuppressed = 0;

  while (cursor.hasNext()) {
    const batch = [];
    while (batch.length < BATCH_SIZE && cursor.hasNext()) {
      batch.push(cursor.next());
    }
    processed += batch.length;

    // ── Derive corrected _id for each doc ────────────────────────────
    const corrected = [];
    batch.forEach(doc => {
      let newId;
      try {
        newId = fixFn(doc);
        if (newId === null || newId === undefined)
          throw new Error("fixFn returned null/undefined");
      } catch (e) {
        if (newIdOnFailure) {
          newId = new ObjectId();
          if (warnShown < MAX_INLINE_WARN) {
            logWarn(`  _id ${doc._id}: fixFn failed (${e.message}) — assigned new ObjectId ${newId}`);
            warnShown++;
          } else {
            warnSuppressed++;
          }
        } else {
          if (skipShown < MAX_INLINE_SKIP) {
            log("SKIP ", `  _id ${doc._id}: ${e.message}`);
            skipShown++;
          } else {
            skipSuppressed++;
          }
          skipped++;
          return;
        }
      }
      corrected.push(Object.assign({}, doc, { _id: newId }));
    });

    if (corrected.length === 0) continue;

    if (DRY_RUN) {
      inserted += corrected.length;
      if (processed - lastLogAt >= LOG_EVERY || !cursor.hasNext()) {
        logDry(`  Would insert ${fmt(inserted)} corrected docs (${rate(inserted, phaseStart)}/sec, ${fmt(skipped)} skipped) ...`);
        lastLogAt = processed;
      }
      continue;
    }

    // ── Insert corrected docs into source ────────────────────────────
    try {
      coll.insertMany(corrected, { ordered: false });
      inserted += corrected.length;
    } catch (e) {
      if (e.writeErrors) {
        const dupeCount  = e.writeErrors.filter(we => we.code === 11000).length;
        const hardErrors = e.writeErrors.filter(we => we.code !== 11000);
        inserted += corrected.length - hardErrors.length;
        errors   += hardErrors.length;
        if (dupeCount > 0)
          logWarn(`  ${fmt(dupeCount)} corrected doc(s) already in source (previous run) — skipped`);
        hardErrors.forEach(we =>
          logError(`  Insert index ${we.index} failed: ${we.errmsg}`)
        );
      } else {
        logError(`  insertMany failed for entire batch: ${e.message}`);
        errors += corrected.length;
      }
    }

    if (processed - lastLogAt >= LOG_EVERY) {
      logInfo(`  Inserted ${fmt(inserted)} docs (${rate(inserted, phaseStart)}/sec, ${fmt(skipped)} skipped, ${fmt(errors)} errors) ...`);
      lastLogAt = processed;
    }
  }

  cursor.close();
  const elapsed = ((Date.now() - phaseStart) / 1000).toFixed(1);

  // Report suppressed lines so nothing is silently lost
  if (warnSuppressed > 0)
    logWarn(`  ... ${fmt(warnSuppressed)} more WARN suppressed (showed first ${MAX_INLINE_WARN})`);
  if (skipSuppressed > 0)
    log("SKIP ", `  ... ${fmt(skipSuppressed)} more SKIP suppressed (showed first ${MAX_INLINE_SKIP})`);

  if (DRY_RUN) {
    logDry(`  Would insert ${fmt(inserted)} corrected docs (${fmt(skipped)} would be skipped)`);
  } else {
    logOk(`  Inserted ${fmt(inserted)} | skipped ${fmt(skipped)} | errors ${fmt(errors)} | ${elapsed}s`);
  }

  return { inserted, skipped, errors };
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3 — Drop the backup collection after a clean transform.
//
// Only drops if DROP_BACKUP_ON_SUCCESS = true AND Phase 2 reported zero errors.
// Otherwise the backup is retained with the manual drop command printed to log.
// ─────────────────────────────────────────────────────────────────────────────
function phase3Cleanup({ namespace }, p2Errors) {
  const { dbName, collName } = parseNamespace(namespace);
  const currentDB  = db.getSiblingDB(dbName);
  const backupColl = currentDB[`${collName}_id_fix_backup`];
  const backupNs   = `${dbName}.${collName}_id_fix_backup`;

  logStep(`Phase 3 — backup cleanup`);

  if (DRY_RUN) {
    logDry(`  Would ${DROP_BACKUP_ON_SUCCESS ? "drop" : "retain"} ${backupNs}`);
    return;
  }

  if (!DROP_BACKUP_ON_SUCCESS) {
    logInfo(`  Backup retained at ${backupNs}`);
    logInfo(`  Drop manually after verifying: db.getSiblingDB("${dbName}").${collName}_id_fix_backup.drop()`);
    return;
  }

  if (p2Errors > 0) {
    logWarn(`  Phase 2 had ${fmt(p2Errors)} error(s) — retaining backup for investigation`);
    logWarn(`  Backup at ${backupNs}`);
    return;
  }

  try {
    backupColl.drop();
    logOk(`  Backup collection dropped`);
  } catch (e) {
    logWarn(`  Could not drop backup: ${e.message}`);
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  const runStart = Date.now();
  print(SEPARATOR);
  logInfo("fixIdTypes — three-phase batch _id fix: Move → Transform → Cleanup");
  logInfo(`Mode                  : ${DRY_RUN ? "DRY RUN (no changes will be made)" : "LIVE (changes WILL be applied)"}`);
  logInfo(`Batch size            : ${fmt(BATCH_SIZE)} docs/round-trip`);
  logInfo(`Log every             : ${fmt(LOG_EVERY)} docs`);
  logInfo(`Max inline WARN/SKIP  : ${MAX_INLINE_WARN} / ${MAX_INLINE_SKIP} per phase`);
  logInfo(`Drop backup on success: ${DROP_BACKUP_ON_SUCCESS}`);
  logInfo(`Collections           : ${COLLECTIONS.length}`);
  COLLECTIONS.forEach((c, i) =>
    logInfo(`  [${i + 1}] ${c.namespace}  wrongType: ${c.wrongType}  newIdOnFailure: ${c.newIdOnFailure || false}`)
  );
  print(SEPARATOR);

  const summary = [];

  for (let i = 0; i < COLLECTIONS.length; i++) {
    const entry = COLLECTIONS[i];
    logInfo(`[${i + 1}/${COLLECTIONS.length}] ${entry.namespace}`);
    print(SEPARATOR2);

    const p1 = phase1Move(entry, i);
    print(SEPARATOR2);
    const p2 = phase2Transform(entry, i);
    print(SEPARATOR2);
    phase3Cleanup(entry, p2.errors);

    summary.push({ ns: entry.namespace, p1, p2 });
    print(SEPARATOR);
  }

  const elapsed = ((Date.now() - runStart) / 1000).toFixed(1);
  logInfo("GLOBAL SUMMARY");
  print(SEPARATOR2);
  summary.forEach(({ ns, p1, p2 }) => {
    logInfo(ns);
    logInfo(`  Phase 1 (move)      : moved ${fmt(p1.moved)}  dupes ${fmt(p1.dupes)}  errors ${fmt(p1.errors)}`);
    logInfo(`  Phase 2 (transform) : inserted ${fmt(p2.inserted)}  skipped ${fmt(p2.skipped)}  errors ${fmt(p2.errors)}`);
  });
  print(SEPARATOR2);
  logInfo(`Total elapsed : ${elapsed}s`);
  logInfo(`Mode          : ${DRY_RUN ? "DRY RUN — no changes were made" : "LIVE — changes applied"}`);
  print(SEPARATOR);
})();
