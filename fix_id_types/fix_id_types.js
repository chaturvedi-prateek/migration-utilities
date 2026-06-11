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
    dbName:    "svoi-db",             // ← update with actual db name
    collName:  "consumer_did_mapping_log",
    wrongType: "object",
    // { _id: ObjectId("...") } → extract inner ObjectId
    fixFn: (doc) => doc._id._id
  },
  {
    dbName:    "your-db",             // ← update with actual db name
    collName:  "your-collection",     // ← update with actual collection name
    wrongType: "object",
    // { mobilenumber: 343566, did: 324432 } → generate a brand new ObjectId
    fixFn: (doc) => new ObjectId()
  }
];

const DRY_RUN = true;   // ← set to false when ready to make changes
// ─────────────────────────────────────────────────────────────────────────────

const SEPARATOR  = "═".repeat(60);
const SEPARATOR2 = "─".repeat(60);

function ts() { return new Date().toISOString(); }

function log(level, msg) {
  print(`[${ts()}] [${level}] ${msg}`);
}

function logInfo  (msg) { log("INFO ", msg); }
function logOk    (msg) { log("OK   ", msg); }
function logSkip  (msg) { log("SKIP ", msg); }
function logWarn  (msg) { log("WARN ", msg); }
function logError (msg) { log("ERROR", msg); }
function logDry   (msg) { log("DRY  ", msg); }
function logStep  (msg) { log("STEP ", msg); }

// ─────────────────────────────────────────────────────────────────────────────

const runStart = new Date();

print(SEPARATOR);
logInfo(`Script start`);
logInfo(`Mode     : ${DRY_RUN ? "DRY RUN (no changes will be made)" : "LIVE (changes WILL be applied)"}`);
logInfo(`Collections to process : ${COLLECTIONS.length}`);
COLLECTIONS.forEach((c, i) =>
  logInfo(`  [${i + 1}] ${c.dbName}.${c.collName}  (wrongType: ${c.wrongType})`)
);
print(SEPARATOR);

const globalSummary = [];

COLLECTIONS.forEach(({ dbName, collName, wrongType, fixFn }, colIdx) => {
  const ns         = `${dbName}.${collName}`;
  const backupNs   = `${dbName}.${collName}_id_fix_backup`;
  const currentDB  = db.getSiblingDB(dbName);
  const coll       = currentDB[collName];
  const backupColl = currentDB[`${collName}_id_fix_backup`];

  const colStart = new Date();
  print(SEPARATOR2);
  logInfo(`[${colIdx + 1}/${COLLECTIONS.length}] Starting collection : ${ns}`);
  logInfo(`Backup collection    : ${backupNs}`);
  logInfo(`Wrong _id type       : ${wrongType}`);

  let totalFound = 0, fixed = 0, skipped = 0, errors = 0;

  // ── Fetch wrong documents ─────────────────────────────────────
  logStep(`Querying for documents with _id type "${wrongType}"...`);
  let wrongDocs;
  try {
    wrongDocs = coll.find({ _id: { $type: wrongType } }).toArray();
    totalFound = wrongDocs.length;
    logInfo(`Found ${totalFound} document(s) with wrong _id type`);
  } catch (e) {
    logError(`Failed to query collection ${ns}: ${e.message}`);
    globalSummary.push({ ns, totalFound: "QUERY FAILED", fixed: 0, skipped: 0, errors: 1 });
    return;
  }

  if (totalFound === 0) {
    logOk(`No documents to fix in ${ns}`);
    globalSummary.push({ ns, totalFound: 0, fixed: 0, skipped: 0, errors: 0 });
    return;
  }

  // ── Process each document ─────────────────────────────────────
  wrongDocs.forEach((doc, docIdx) => {
    const docLabel = `[doc ${docIdx + 1}/${totalFound}]`;
    logInfo(`${docLabel} Processing _id : ${JSON.stringify(doc._id)}`);

    // ── Derive new _id ──
    let newId;
    try {
      newId = fixFn(doc);
      if (newId === null || newId === undefined) throw new Error("fixFn returned null/undefined");
      logInfo(`${docLabel} Derived new _id : ${newId}`);
    } catch (e) {
      logSkip(`${docLabel} Cannot derive new _id — ${e.message}`);
      skipped++;
      return;
    }

    if (DRY_RUN) {
      logDry(`${docLabel} Would backup  → ${backupNs}`);
      logDry(`${docLabel} Would insert  → ${ns}  with _id: ${newId}`);
      logDry(`${docLabel} Would delete  → ${ns}  old _id: ${JSON.stringify(doc._id)}`);
      fixed++;
      return;
    }

    // ── Step 1: Backup ──
    logStep(`${docLabel} [1/3] Backing up original document to ${backupNs}`);
    try {
      backupColl.insertOne(doc);
      logOk(`${docLabel} [1/3] Backup successful`);
    } catch (e) {
      if (e.code === 11000) {
        logWarn(`${docLabel} [1/3] Backup already exists (duplicate key) — continuing`);
      } else {
        logError(`${docLabel} [1/3] Backup FAILED — aborting this document. Error: ${e.message}`);
        errors++;
        return;
      }
    }

    // ── Step 2: Insert corrected document ──
    logStep(`${docLabel} [2/3] Inserting corrected document with _id: ${newId}`);
    const newDoc = Object.assign({}, doc, { _id: newId });
    try {
      coll.insertOne(newDoc);
      logOk(`${docLabel} [2/3] Insert successful`);
    } catch (e) {
      if (e.code === 11000) {
        logWarn(`${docLabel} [2/3] Correct _id already exists — will still delete wrong doc`);
      } else {
        logError(`${docLabel} [2/3] Insert FAILED — aborting this document to avoid data loss. Error: ${e.message}`);
        errors++;
        return;
      }
    }

    // ── Step 3: Delete original wrong document ──
    logStep(`${docLabel} [3/3] Deleting original wrong document _id: ${JSON.stringify(doc._id)}`);
    try {
      const deleteResult = coll.deleteOne({ _id: doc._id });
      if (deleteResult.deletedCount === 1) {
        logOk(`${docLabel} [3/3] Delete successful`);
      } else {
        logWarn(`${docLabel} [3/3] Delete matched 0 documents — document may have already been removed`);
      }
    } catch (e) {
      logError(`${docLabel} [3/3] Delete FAILED. Error: ${e.message}`);
      errors++;
      return;
    }

    logOk(`${docLabel} Fix complete : ${JSON.stringify(doc._id)} → ${newId}`);
    fixed++;
  });

  // ── Collection summary ────────────────────────────────────────
  const colElapsed = ((new Date() - colStart) / 1000).toFixed(1);
  print(SEPARATOR2);
  logInfo(`Collection summary : ${ns}`);
  logInfo(`  Total found  : ${totalFound}`);
  logInfo(`  Fixed        : ${fixed}`);
  logInfo(`  Skipped      : ${skipped}`);
  logInfo(`  Errors       : ${errors}`);
  logInfo(`  Elapsed      : ${colElapsed}s`);
  if (!DRY_RUN && fixed > 0)
    logInfo(`  Backup at    : ${backupNs}`);

  globalSummary.push({ ns, totalFound, fixed, skipped, errors });
});

// ── Global summary ────────────────────────────────────────────────────────────
const totalElapsed = ((new Date() - runStart) / 1000).toFixed(1);
print(SEPARATOR);
logInfo(`GLOBAL SUMMARY`);
logInfo(SEPARATOR2);
globalSummary.forEach(s => {
  logInfo(`${s.ns}`);
  logInfo(`  Found: ${s.totalFound}  Fixed: ${s.fixed}  Skipped: ${s.skipped}  Errors: ${s.errors}`);
});
print(SEPARATOR2);
logInfo(`Total elapsed : ${totalElapsed}s`);
logInfo(`Mode          : ${DRY_RUN ? "DRY RUN — no changes were made" : "LIVE — changes applied"}`);
logInfo(`Script end`);
print(SEPARATOR);
