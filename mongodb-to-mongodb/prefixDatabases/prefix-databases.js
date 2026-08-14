// prefix-databases.js
//
// Copies every non-system database on a deployment to a new name with a prefix
// applied, then optionally drops the sources. MongoDB has no native "rename
// database", so this is a server-side copy via an aggregation $out stage plus
// explicit recreation of collection options, indexes, and views.
//
// Usage:
//   mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=true'  --file prefix-databases.js
//   mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=false' --file prefix-databases.js
//   mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=false, DROP_SOURCE=true' --file prefix-databases.js
//
// Resumable: completed collections are checkpointed in admin.db_rename_checkpoint
// and skipped on re-runs. Re-run the identical command to resume; a single
// failing collection never aborts the run.

// ---------------------------------------------------------------- parameters

const SYSTEM_DBS = ["admin", "local", "config"];

const prefix      = typeof PREFIX      !== "undefined" ? PREFIX      : "";
const dryRun      = typeof DRY_RUN     !== "undefined" ? DRY_RUN     : true;
const dropSource  = typeof DROP_SOURCE !== "undefined" ? DROP_SOURCE : false;
const resume      = typeof RESUME      !== "undefined" ? RESUME      : true;
// Collections with a TTL index drift during the copy because the TTL monitor
// expires documents on the source (and later the target). Warning, not failure.
const ttlTolerant = typeof TTL_TOLERANT !== "undefined" ? TTL_TOLERANT : true;
// Allowed |source - target| for collections WITHOUT a TTL index.
const tolerance   = typeof TOLERANCE   !== "undefined" ? TOLERANCE   : 0;
// Restrict the run to specific source databases: var ONLY_DBS=["a","b"]
const onlyDbs     = typeof ONLY_DBS    !== "undefined" ? ONLY_DBS    : null;

if (!prefix) {
  throw new Error('PREFIX is required, e.g. --eval \'var PREFIX="myprefix_"\'');
}

const CKPT_DB = "admin";
const CKPT_COLL = "db_rename_checkpoint";

// ----------------------------------------------------------------- utilities

const admin = db.getSiblingDB("admin");
const ck = db.getSiblingDB(CKPT_DB)[CKPT_COLL];

function ts() {
  return new Date().toISOString().replace("T", " ").slice(0, 19);
}

function log(msg) {
  print(ts() + "  " + msg);
}

function num(n) {
  return n.toLocaleString("en-US");
}

function duration(ms) {
  const s = Math.round(ms / 1000);
  if (s < 60) return s + "s";
  const m = Math.floor(s / 60);
  if (m < 60) return m + "m" + (s % 60) + "s";
  return Math.floor(m / 60) + "h" + (m % 60) + "m";
}

// The checkpoint key MUST include the target. Keying on {src, coll} alone means
// checkpoints written under one prefix cause silent skips under another.
function ckKey(src, target, coll) {
  return { src: src, target: target, coll: coll };
}

function isDone(src, target, coll) {
  return resume && !!ck.findOne(Object.assign(ckKey(src, target, coll), { done: true }));
}

function prepareCheckpointStore() {
  if (dryRun || !resume) return;
  // A legacy unique index on {src, coll} would block checkpointing the same
  // collection under a second prefix. Replace it with the prefix-aware key.
  for (const i of ck.getIndexes()) {
    if (Object.keys(i.key).join(",") === "src,coll") {
      log("dropping legacy checkpoint index " + i.name + " (not prefix-aware)");
      ck.dropIndex(i.name);
    }
  }
  ck.createIndex({ src: 1, target: 1, coll: 1 }, { unique: true });
}

function hasTTL(coll) {
  return coll.getIndexes().some(i => i.expireAfterSeconds !== undefined);
}

// ------------------------------------------------------------------ planning

function buildPlan() {
  let names = admin
    .runCommand({ listDatabases: 1, nameOnly: true })
    .databases.map(d => d.name)
    .filter(n => !SYSTEM_DBS.includes(n))
    .filter(n => !n.startsWith(prefix)); // never re-prefix an already-renamed db

  if (onlyDbs) names = names.filter(n => onlyDbs.includes(n));
  names.sort();

  const plan = [];
  for (const name of names) {
    const target = prefix + name;
    if (target.length > 63) {
      log("SKIP database " + name + ": target name exceeds the 63-char limit");
      continue;
    }
    const src = db.getSiblingDB(name);
    const entry = { name: name, target: target, todo: [], done: [], views: [] };

    for (const c of src.getCollectionInfos({ type: "collection" })) {
      if (c.name.startsWith("system.")) continue;
      const rec = { coll: c.name, options: c.options || {} };
      if (isDone(name, target, c.name)) entry.done.push(rec);
      else entry.todo.push(rec);
    }
    for (const v of src.getCollectionInfos({ type: "view" })) {
      entry.views.push({ coll: v.name, options: v.options || {} });
    }
    plan.push(entry);
  }
  return plan;
}

// --------------------------------------------------------------------- start

const startedAt = Date.now();

log("prefix=" + prefix + "  dryRun=" + dryRun + "  dropSource=" + dropSource +
    "  resume=" + resume + "  ttlTolerant=" + ttlTolerant + "  tolerance=" + tolerance);

prepareCheckpointStore();

const plan = buildPlan();

const totalColls = plan.reduce((a, e) => a + e.todo.length + e.done.length, 0);
const doneColls  = plan.reduce((a, e) => a + e.done.length, 0);
const todoColls  = plan.reduce((a, e) => a + e.todo.length, 0);
const totalViews = plan.reduce((a, e) => a + e.views.length, 0);
const dbsPending = plan.filter(e => e.todo.length > 0).length;

log("");
log("========================== PLAN ==========================");
log("databases          : " + plan.length + " in scope, " + dbsPending + " with work remaining");
log("collections        : " + num(totalColls) + " total");
log("  already complete : " + num(doneColls) + " (checkpointed, will be skipped)");
log("  remaining        : " + num(todoColls));
log("views              : " + num(totalViews));
log("----------------------------------------------------------");
for (const e of plan) {
  const all = e.done.length + e.todo.length;
  const status = e.todo.length === 0
    ? "COMPLETE (" + all + ")"
    : e.done.length + "/" + all + " done, " + e.todo.length + " left";
  log("  " + e.name + " -> " + e.target + "  [" + status + "]");
}
log("==========================================================");
log("");

if (dryRun) {
  log("DRY RUN - nothing was changed. Re-run with DRY_RUN=false to execute.");
  quit(0);
}

// ------------------------------------------------------------------ the copy

let copied = 0;
let docsCopied = 0;
let n = 0;
const drifted = [];    // TTL / within-tolerance drift, checkpointed as complete
const mismatches = []; // real count mismatches, NOT checkpointed
const failures = [];   // errors, NOT checkpointed

for (const entry of plan) {
  const name = entry.name, target = entry.target;
  const src = db.getSiblingDB(name), dst = db.getSiblingDB(target);

  if (entry.todo.length === 0) {
    log("[db] " + name + " -> " + target + " : all " + entry.done.length +
        " collection(s) already complete, skipping");
  } else {
    log("[db] " + name + " -> " + target + " : " + entry.todo.length +
        " to copy, " + entry.done.length + " already complete");
  }

  for (const item of entry.todo) {
    const coll = item.coll;
    n++;
    const pct = ((n / todoColls) * 100).toFixed(1);
    const est = src[coll].estimatedDocumentCount();
    const t0 = Date.now();

    log("  [" + n + "/" + todoColls + " " + pct + "%] " + name + "." + coll +
        " -> " + target + "." + coll + "  (~" + num(est) + " docs)");

    try {
      // 1. recreate the collection with its original options
      //    (capped, validator, collation, timeseries, ...)
      if (!dst.getCollectionInfos({ name: coll }).length) {
        dst.createCollection(coll, item.options);
      }

      // 2. copy the data server-side; $out atomically replaces the target, so a
      //    partially written collection from an earlier crash is discarded
      src[coll].aggregate(
        [{ $match: {} }, { $out: { db: target, coll: coll } }],
        { allowDiskUse: true }
      );

      // 3. recreate indexes ($out does not carry them over); _id_ is implicit
      const idx = src[coll].getIndexes().filter(i => i.name !== "_id_");
      if (idx.length) {
        idx.forEach(i => { delete i.v; delete i.ns; });
        dst.runCommand({ createIndexes: coll, indexes: idx });
      }

      // 4. verify. TTL collections drift on both sides during the copy, so a
      //    difference there is expected and must not abort the run.
      const s = src[coll].countDocuments({});
      const t = dst[coll].countDocuments({});
      const diff = t - s;
      const ttl = hasTTL(src[coll]);

      if (s !== t) {
        const detail = name + "." + coll + ": source=" + num(s) + " target=" + num(t) +
                       " diff=" + (diff > 0 ? "+" : "") + num(diff);
        if (ttl && ttlTolerant) {
          log("      WARN TTL drift - " + detail);
          drifted.push(detail + " [TTL]");
        } else if (Math.abs(diff) <= tolerance) {
          log("      WARN within tolerance - " + detail);
          drifted.push(detail + " [tolerance]");
        } else {
          log("      MISMATCH " + detail + "  (not checkpointed, retried next run)");
          mismatches.push(detail);
          continue;
        }
      }

      if (resume) {
        ck.updateOne(
          ckKey(name, target, coll),
          { $set: { done: true, docs: t, ttl: ttl, at: new Date() } },
          { upsert: true }
        );
      }

      copied++;
      docsCopied += t;
      log("      ok - " + num(t) + " docs, " + idx.length + " index(es), " +
          duration(Date.now() - t0));
    } catch (e) {
      const detail = name + "." + coll + ": " + e.message;
      log("      ERROR " + detail + "  (not checkpointed, retried next run)");
      failures.push(detail);
    }
  }

  // views are recreated last, once their source collections exist
  for (const v of entry.views) {
    try {
      if (!dst.getCollectionInfos({ name: v.coll }).length) {
        dst.createCollection(v.coll, {
          viewOn: v.options.viewOn,
          pipeline: v.options.pipeline,
          collation: v.options.collation
        });
        log("  view " + name + "." + v.coll + " -> " + target + "." + v.coll);
      }
    } catch (e) {
      const detail = "view " + name + "." + v.coll + ": " + e.message;
      log("  ERROR " + detail);
      failures.push(detail);
    }
  }

  if (dropSource) {
    const outstanding = mismatches.concat(failures).filter(m => m.indexOf(name + ".") === 0);
    if (outstanding.length) {
      log("  NOT dropping source " + name + ": " + outstanding.length +
          " collection(s) unresolved");
    } else {
      log("  dropping source database " + name);
      src.dropDatabase();
    }
  }
}

// ------------------------------------------------------------------- summary

const elapsed = Date.now() - startedAt;
const outstanding = mismatches.length + failures.length;
const completeNow = doneColls + copied;

log("");
log("========================= SUMMARY =========================");
log("elapsed              : " + duration(elapsed));
log("databases in scope   : " + plan.length);
log("collections total    : " + num(totalColls));
log("  complete before    : " + num(doneColls));
log("  copied this run    : " + num(copied) + "  (" + num(docsCopied) + " docs)");
log("  TTL/tolerated drift: " + num(drifted.length) + "  (counted as complete)");
log("  mismatched         : " + num(mismatches.length));
log("  errored            : " + num(failures.length));
log("-----------------------------------------------------------");
log("COMPLETE             : " + num(completeNow) + "/" + num(totalColls) +
    "  (" + ((completeNow / (totalColls || 1)) * 100).toFixed(1) + "%)");
log("REMAINING            : " + num(totalColls - completeNow));

if (drifted.length) {
  log("");
  log("TTL / tolerated drift (informational, checkpointed as complete):");
  drifted.forEach(d => log("  " + d));
}
if (mismatches.length) {
  log("");
  log("COUNT MISMATCHES (not checkpointed - re-run to retry):");
  mismatches.forEach(d => log("  " + d));
}
if (failures.length) {
  log("");
  log("ERRORS (not checkpointed - re-run to retry):");
  failures.forEach(d => log("  " + d));
}
log("===========================================================");

if (outstanding) {
  log("");
  log("Re-run the identical command to retry the " + outstanding +
      " outstanding collection(s); completed work is skipped automatically.");
}

quit(outstanding ? 1 : 0);

// Caveats:
//  - Not atomic and not online. Application writes arriving during the copy are
//    lost. Stop writers, or use a change-stream based tool for a live cutover.
//    TTL expiry is the one form of source-side change tolerated here.
//  - The resume checkpoint assumes the source is otherwise frozen. A collection
//    marked done is never re-copied.
//  - Resume granularity is one collection: a crash mid-$out redoes that whole
//    collection.
//  - Sharded collections are not re-sharded; $out writes an unsharded target.
//  - Users/roles scoped to the old database names must be recreated manually.
//  - Drop sources only after verifying counts and index parity.
