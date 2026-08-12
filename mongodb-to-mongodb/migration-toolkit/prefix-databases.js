// prefix-databases.js
//
// Copies every non-system database to a new name with a prefix applied, then
// (optionally) drops the sources. MongoDB has no native "rename database", so
// this is a server-side copy via $out plus explicit index/view recreation.
//
// Usage:
//   # 1. dry run
//   mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=true'  --file prefix-databases.js
//   # 2. real copy, sources kept
//   mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=false' --file prefix-databases.js
//   # 3. after verifying, drop sources
//   mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=false, DROP_SOURCE=true' --file prefix-databases.js
//
// Resume: completed collections are checkpointed in admin.db_rename_checkpoint,
// so an interrupted run skips what already finished. This is only valid if
// writes to the source are stopped -- see the caveats at the bottom of the file.
// Start a fresh migration with:
//   db.getSiblingDB("admin").db_rename_checkpoint.drop()

const SYSTEM_DBS = ["admin", "local", "config"];

const prefix     = typeof PREFIX      !== "undefined" ? PREFIX      : "";
const dryRun     = typeof DRY_RUN     !== "undefined" ? DRY_RUN     : true;
const dropSource = typeof DROP_SOURCE !== "undefined" ? DROP_SOURCE : false;
const resume     = typeof RESUME      !== "undefined" ? RESUME      : true;

if (!prefix) {
  throw new Error('PREFIX is required, e.g. --eval \'var PREFIX="myprefix_"\'');
}

const admin = db.getSiblingDB("admin");
const ck = admin.db_rename_checkpoint;
if (!dryRun && resume) {
  ck.createIndex({ src: 1, coll: 1 }, { unique: true });
}

const dbs = admin
  .runCommand({ listDatabases: 1, nameOnly: true })
  .databases.map(d => d.name)
  .filter(n => !SYSTEM_DBS.includes(n))
  .filter(n => !n.startsWith(prefix)); // idempotent: skip already-prefixed

print(`Databases to rename (${dbs.length}): ${dbs.join(", ") || "<none>"}`);
print(`prefix=${prefix} dryRun=${dryRun} dropSource=${dropSource} resume=${resume}\n`);

for (const name of dbs) {
  const target = prefix + name;
  if (target.length > 63) {
    print(`SKIP ${name}: target name "${target}" exceeds the 63-char limit`);
    continue;
  }

  const src = db.getSiblingDB(name);
  const dst = db.getSiblingDB(target);

  for (const c of src.getCollectionInfos({ type: "collection" })) {
    const coll = c.name;
    if (coll.startsWith("system.")) continue;

    if (!dryRun && resume && ck.findOne({ src: name, coll: coll, done: true })) {
      print(`  skip ${name}.${coll} (checkpointed)`);
      continue;
    }

    print(`${name}.${coll} -> ${target}.${coll}  (~${src[coll].estimatedDocumentCount()} docs)`);
    if (dryRun) continue;

    // 1. recreate the collection with its original options
    //    (capped, validator, collation, timeseries, ...)
    if (!dst.getCollectionInfos({ name: coll }).length) {
      dst.createCollection(coll, c.options || {});
    }

    // 2. copy the data server-side; $out atomically replaces the target, so a
    //    partially written collection from a previous crash is discarded
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

    // 4. verify before checkpointing
    const s = src[coll].countDocuments({});
    const t = dst[coll].countDocuments({});
    if (s !== t) {
      throw new Error(`COUNT MISMATCH ${name}.${coll}: source=${s} target=${t}`);
    }

    if (resume) {
      ck.updateOne(
        { src: name, coll: coll },
        { $set: { target: target, done: true, docs: t, at: new Date() } },
        { upsert: true }
      );
    }
  }

  // views are recreated last, once their source collections exist
  for (const v of src.getCollectionInfos({ type: "view" })) {
    print(`${name}.${v.name} (view) -> ${target}.${v.name}`);
    if (dryRun) continue;
    if (!dst.getCollectionInfos({ name: v.name }).length) {
      dst.createCollection(v.name, {
        viewOn: v.options.viewOn,
        pipeline: v.options.pipeline,
        collation: v.options.collation
      });
    }
  }

  if (!dryRun && dropSource) {
    print(`dropping source db ${name}`);
    src.dropDatabase();
  }
}

print("done.");

// Caveats:
//  - Not atomic and not online. Writes arriving during the copy are lost.
//    Stop writers, or use a change-stream based tool for a live cutover.
//  - The resume checkpoint assumes the source is frozen. If writers are still
//    running, a collection marked done can drift and will never be re-copied.
//  - Resume granularity is one collection: a crash mid-$out redoes that whole
//    collection.
//  - Sharded collections are not re-sharded; $out writes an unsharded target.
//  - Users/roles scoped to the old database names must be recreated.
//  - Drop sources only after verifying counts and index parity.
