package main

import (
	"fmt"
	"time"
)

// seedSource fills a source cluster's application database with ~mb megabytes of
// data spread over two collections, using ~1 KB documents inserted in bulk. Returns
// the total document count for later verification.
func seedSource(t *tools, nd *node, mb int) (int64, error) {
	uri := nd.sourceURI()
	// ~1 KB per doc; docsTotal ≈ mb*1024. Split across two collections.
	docsTotal := int64(mb) * 1024
	logf("seeding %s db=%s (~%d MB, %d docs)", nd.name, nd.db, mb, docsTotal)
	js := fmt.Sprintf(`
const db = db.getSiblingDB(%q);
const pad = "x".repeat(900);
const total = %d;
const batch = 1000;
for (const coll of ["orders","events"]) {
  let inserted = 0;
  const target = Math.floor(total/2);
  while (inserted < target) {
    const docs = [];
    const n = Math.min(batch, target - inserted);
    for (let i=0;i<n;i++) docs.push({_id: inserted+i, v: (inserted+i), pad: pad, ts: new Date()});
    db[coll].insertMany(docs, {ordered:false});
    inserted += n;
  }
}
print(db.orders.countDocuments({}) + db.events.countDocuments({}));
`, nd.db, docsTotal)
	start := time.Now()
	out, err := mongoshEval(t.mongosh, uri, js)
	if err != nil {
		return 0, fmt.Errorf("seed %s: %w (%s)", nd.name, err, out)
	}
	var count int64
	fmt.Sscanf(out, "%d", &count)
	logf("seeded %s: %d docs in %s", nd.name, count, time.Since(start).Round(time.Second))
	return count, nil
}
