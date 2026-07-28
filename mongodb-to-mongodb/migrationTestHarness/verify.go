package main

import (
	"fmt"
)

// verifyResult captures the pass/fail of a single check for the summary + logs.
type verifyResult struct {
	name string
	ok   bool
	msg  string
}

// verifyPhase1 confirms each destination holds the same doc count as its source.
func verifyPhase1(t *tools, cs *clusterSet, srcCounts []int64) []verifyResult {
	var out []verifyResult
	for i := range cs.sources {
		dstCount := dbDocCount(t, cs.dests[i].destURI(), cs.dests[i].db)
		ok := dstCount == srcCounts[i]
		out = append(out, verifyResult{
			name: "phase1/" + cs.sources[i].name,
			ok:   ok,
			msg:  fmt.Sprintf("db=%s source=%d dest=%d", cs.sources[i].db, srcCounts[i], dstCount),
		})
	}
	return out
}

// verifyHub confirms the hub ended up with EVERY cluster's database at the expected
// count, and that no mongosync metadata databases remain.
func verifyHub(t *tools, cs *clusterSet, srcCounts []int64) []verifyResult {
	var out []verifyResult
	hub := cs.dests[0].destURI()
	for i := range cs.sources {
		db := cs.sources[i].db
		hubCount := dbDocCount(t, hub, db)
		ok := hubCount == srcCounts[i]
		out = append(out, verifyResult{
			name: "hub/" + db,
			ok:   ok,
			msg:  fmt.Sprintf("expected=%d hub=%d", srcCounts[i], hubCount),
		})
	}
	// No leftover mongosync metadata on the hub.
	meta, _ := mongoshEval(t.mongosh, hub,
		`db.adminCommand({listDatabases:1}).databases.map(d=>d.name).filter(n=>/mongosync|mdb_internal/.test(n)).length`)
	out = append(out, verifyResult{
		name: "hub/no-metadata-dbs",
		ok:   meta == "0",
		msg:  "leftover mongosync metadata dbs=" + meta,
	})
	return out
}

func dbDocCount(t *tools, uri, db string) int64 {
	js := fmt.Sprintf(`var s=0;db.getSiblingDB(%q).getCollectionNames().forEach(c=>{s+=db.getSiblingDB(%q)[c].countDocuments({})});print(s)`, db, db)
	out, err := mongoshEval(t.mongosh, uri, js)
	if err != nil {
		return -1
	}
	var n int64
	fmt.Sscanf(out, "%d", &n)
	return n
}
