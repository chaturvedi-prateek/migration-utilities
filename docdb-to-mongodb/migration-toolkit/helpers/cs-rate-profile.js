// cs-rate-profile.js — measure the source's change-stream write rate per namespace.
//
//   CS_SECONDS=300 CS_OUT=cs-profile.json \
//     mongosh "$DOCDB_SRC" --quiet --file cs-rate-profile.js
//
// Opens ONE cluster-wide change stream and counts events for CS_SECONDS
// (default 300), then prints events/s in total, by operationType and by
// namespace (all namespaces, highest rate first), and writes the same numbers
// as JSON to CS_OUT (default cs-profile.json) for split-hot-cold.sh.
//
// - Tails the live head only (no startAtOperationTime): run it during PEAK
//   write traffic, otherwise the profile under-reports what CDC must sustain.
// - fullDocument is NOT requested, so updates carry no post-image. Insert
//   events still carry the inserted document — expect network traffic about
//   equal to the insert volume.
// - On DocumentDB only collections/databases with change streams enabled
//   (modifyChangeStreams) emit events; anything else is invisible here.
// - Namespaces with no writes during the window are absent from the output.

const seconds = parseInt(process.env.CS_SECONDS || '300', 10);
const outFile = process.env.CS_OUT || 'cs-profile.json';
const fs = require('fs');

const startedAt = new Date();
const deadline = Date.now() + seconds * 1000;
const byOp = {};
const byNs = {};
let total = 0;
let nextReport = Date.now() + 60000;

const cs = db.getMongo().watch([], { maxAwaitTimeMS: 1000 });
print(`profiling cluster-wide change stream for ${seconds}s ...`);
try {
  while (Date.now() < deadline) {
    const ev = cs.tryNext();
    if (Date.now() >= nextReport) {
      print(`  ${Math.round((deadline - Date.now()) / 1000)}s left, ${total} events so far`);
      nextReport += 60000;
    }
    if (!ev) continue;
    total++;
    byOp[ev.operationType] = (byOp[ev.operationType] || 0) + 1;
    if (ev.ns && ev.ns.db) {
      const ns = ev.ns.coll ? `${ev.ns.db}.${ev.ns.coll}` : ev.ns.db;
      byNs[ns] = (byNs[ns] || 0) + 1;
    }
  }
} finally {
  cs.close();
}

const elapsed = (Date.now() - startedAt.getTime()) / 1000;
const rate = (n) => Math.round((n / elapsed) * 100) / 100;
const namespaces = Object.keys(byNs)
  .map((ns) => ({ ns, count: byNs[ns], rate: rate(byNs[ns]) }))
  .sort((a, b) => b.count - a.count);
const result = {
  startedAt: startedAt.toISOString(),
  seconds: Math.round(elapsed),
  total,
  ratePerSec: rate(total),
  byOperationType: Object.fromEntries(
    Object.keys(byOp).map((op) => [op, { count: byOp[op], rate: rate(byOp[op]) }])),
  byNamespace: namespaces,
};

print('');
print(`total: ${total} events in ${Math.round(elapsed)}s = ${result.ratePerSec} ev/s`);
print('by operationType:');
for (const [op, v] of Object.entries(result.byOperationType)) {
  print(`  ${op.padEnd(14)} ${String(v.count).padStart(10)}  ${v.rate} ev/s`);
}
print(`by namespace (${namespaces.length}):`);
for (const n of namespaces) {
  const share = total ? ((100 * n.count) / total).toFixed(1) : '0.0';
  print(`  ${String(n.rate).padStart(10)} ev/s  ${share.padStart(5)}%  ${n.ns}`);
}
fs.writeFileSync(outFile, JSON.stringify(result, null, 2));
print(`\nwrote ${outFile}`);
