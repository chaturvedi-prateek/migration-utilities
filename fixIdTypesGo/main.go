package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ── Config ────────────────────────────────────────────────────────────────────

type detectCfg struct {
	// Databases to scan. Empty = all user databases on the server.
	Databases  []string `json:"databases"`
	OutputJSON string   `json:"output_json"` // path for JSON report; omit to skip
}

type countCfg struct {
	Namespaces []string `json:"namespaces"`
	OutputJSON string   `json:"output_json"`
}

type collectionCfg struct {
	Namespace      string `json:"namespace"`
	WrongType      string `json:"wrong_type"`
	FixStrategy    string `json:"fix_strategy"`
	NewIDOnFailure bool   `json:"new_id_on_failure"`
}

type fixCfg struct {
	Collections []collectionCfg `json:"collections"`
	BatchSize   int             `json:"batch_size"`
	Workers     int             `json:"workers"`
	// ThrottleMs sleeps this many milliseconds in every worker after each batch.
	// Increase to reduce load on the DocumentDB cluster at the cost of throughput.
	// 0 = no throttle (default).
	ThrottleMs          int    `json:"throttle_ms"`
	DropBackupOnSuccess bool   `json:"drop_backup_on_success"`
	OutputJSON          string `json:"output_json"`
}

type config struct {
	SourceURI string    `json:"source_uri"`
	LogFile   string    `json:"log_file"` // tee all output to this file (stdout always included)
	Detect    detectCfg `json:"detect"`
	Count     countCfg  `json:"count"`
	Fix       fixCfg    `json:"fix"`
}

// fix_strategy values
const (
	stratStringToObjectID = "string_to_objectid"
	stratNestedObjectID   = "nested_objectid"
	stratNewObjectID      = "new_objectid"
)

const (
	maxInlineWarn    = 20
	maxInlineSkip    = 20
	detectConcurrent = 20 // max parallel collection probes in detect mode
)

// ── Logger ────────────────────────────────────────────────────────────────────

var (
	logMu     sync.Mutex
	logWriter io.Writer = os.Stdout
)

func initLogWriter(logFile string) (io.Closer, error) {
	if logFile == "" {
		return nil, nil
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open log file %q: %w", logFile, err)
	}
	logWriter = io.MultiWriter(os.Stdout, f)
	return f, nil
}

func ts() string { return time.Now().UTC().Format("2006/01/02 15:04:05") }

func emit(level, format string, args ...interface{}) {
	logMu.Lock()
	fmt.Fprintf(logWriter, ts()+" "+level+" "+format+"\n", args...)
	logMu.Unlock()
}

func logInfo(f string, a ...interface{})  { emit("[INFO ]", f, a...) }
func logOk(f string, a ...interface{})    { emit("[OK   ]", f, a...) }
func logStep(f string, a ...interface{})  { emit("[STEP ]", f, a...) }
func logWarn(f string, a ...interface{})  { emit("[WARN ]", f, a...) }
func logError(f string, a ...interface{}) { emit("[ERROR]", f, a...) }
func logDry(f string, a ...interface{})   { emit("[DRY  ]", f, a...) }
func logSkip(f string, a ...interface{})  { emit("[SKIP ]", f, a...) }
func logMixed(f string, a ...interface{}) { emit("[MIXED]", f, a...) }
func logClean(f string, a ...interface{}) { emit("[CLEAN]", f, a...) }

func printSep() {
	logMu.Lock()
	fmt.Fprintln(logWriter, strings.Repeat("═", 60))
	logMu.Unlock()
}

func printSep2() {
	logMu.Lock()
	fmt.Fprintln(logWriter, strings.Repeat("─", 60))
	logMu.Unlock()
}

// ── Formatting ────────────────────────────────────────────────────────────────

func fmtInt(n int64) string {
	if n < 0 {
		return "-" + fmtInt(-n)
	}
	s := fmt.Sprintf("%d", n)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return string(out)
}

func fmtRate(n int64, start time.Time) string {
	secs := time.Since(start).Seconds()
	if secs < 0.1 {
		return "—/sec"
	}
	return fmtInt(int64(float64(n)/secs)) + "/sec"
}

// ── BSON helpers ──────────────────────────────────────────────────────────────

func getDocID(doc bson.D) interface{} {
	for _, e := range doc {
		if e.Key == "_id" {
			return e.Value
		}
	}
	return nil
}

func replaceID(doc bson.D, newID primitive.ObjectID) bson.D {
	out := make(bson.D, len(doc))
	copy(out, doc)
	for i, e := range out {
		if e.Key == "_id" {
			out[i].Value = newID
			return out
		}
	}
	return out
}

// probeIDType uses $sort + $limit + $project($type) to get the BSON type name
// of the _id at one end of the index.  Returns "" if the collection is empty.
// sortDir: 1 = ascending (min), -1 = descending (max).
func probeIDType(ctx context.Context, coll *mongo.Collection, sortDir int) (string, error) {
	pipeline := bson.A{
		bson.D{{Key: "$sort", Value: bson.D{{Key: "_id", Value: sortDir}}}},
		bson.D{{Key: "$limit", Value: 1}},
		bson.D{{Key: "$project", Value: bson.D{
			{Key: "_id", Value: 0},
			{Key: "t", Value: bson.D{{Key: "$type", Value: "$_id"}}},
		}}},
	}
	cur, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return "", err
	}
	defer cur.Close(ctx)
	if !cur.Next(ctx) {
		return "", nil // empty collection
	}
	var row struct {
		T string `bson:"t"`
	}
	if err := cur.Decode(&row); err != nil {
		return "", err
	}
	return row.T, nil
}

func deriveNewID(doc bson.D, strategy string, newIDOnFailure bool) (primitive.ObjectID, bool, string) {
	id := getDocID(doc)

	switch strategy {
	case stratStringToObjectID:
		str, ok := id.(string)
		if !ok {
			msg := fmt.Sprintf("_id is not string (got %T)", id)
			if newIDOnFailure {
				return primitive.NewObjectID(), false, msg + " — assigned new ObjectId"
			}
			return primitive.NilObjectID, true, msg
		}
		oid, err := primitive.ObjectIDFromHex(str)
		if err != nil {
			msg := fmt.Sprintf("_id %q is not valid 24-char hex: %v", str, err)
			if newIDOnFailure {
				return primitive.NewObjectID(), false, msg + " — assigned new ObjectId"
			}
			return primitive.NilObjectID, true, msg
		}
		return oid, false, ""

	case stratNestedObjectID:
		nested, ok := id.(bson.D)
		if !ok {
			msg := fmt.Sprintf("_id is not a document (got %T)", id)
			if newIDOnFailure {
				return primitive.NewObjectID(), false, msg + " — assigned new ObjectId"
			}
			return primitive.NilObjectID, true, msg
		}
		inner := getDocID(nested)
		oid, ok := inner.(primitive.ObjectID)
		if !ok {
			msg := fmt.Sprintf("nested _id field is not ObjectId (got %T)", inner)
			if newIDOnFailure {
				return primitive.NewObjectID(), false, msg + " — assigned new ObjectId"
			}
			return primitive.NilObjectID, true, msg
		}
		return oid, false, ""

	case stratNewObjectID:
		return primitive.NewObjectID(), false, ""

	default:
		msg := fmt.Sprintf("unknown fix_strategy %q", strategy)
		if newIDOnFailure {
			return primitive.NewObjectID(), false, msg + " — assigned new ObjectId"
		}
		return primitive.NilObjectID, true, msg
	}
}

func parseNS(ns string) (string, string, error) {
	dot := strings.Index(ns, ".")
	if dot < 1 || dot == len(ns)-1 {
		return "", "", fmt.Errorf("invalid namespace %q — must be database.collection", ns)
	}
	return ns[:dot], ns[dot+1:], nil
}

// ── Config loading ────────────────────────────────────────────────────────────

func loadConfig(path string) (*config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open %q: %w", path, err)
	}
	defer f.Close()
	var cfg config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("cannot parse %q: %w", path, err)
	}
	if cfg.SourceURI == "" {
		return nil, fmt.Errorf("source_uri is required")
	}
	// Apply defaults for fix section
	if cfg.Fix.BatchSize <= 0 {
		cfg.Fix.BatchSize = 1000
	}
	if cfg.Fix.Workers <= 0 {
		cfg.Fix.Workers = 4
	}
	return &cfg, nil
}

func validateFix(cfg *fixCfg) error {
	if len(cfg.Collections) == 0 {
		return fmt.Errorf("fix.collections is empty")
	}
	for i, c := range cfg.Collections {
		if _, _, err := parseNS(c.Namespace); err != nil {
			return fmt.Errorf("fix.collections[%d]: %w", i, err)
		}
		if c.WrongType == "" {
			return fmt.Errorf("fix.collections[%d]: wrong_type is required", i)
		}
		switch c.FixStrategy {
		case stratStringToObjectID, stratNestedObjectID, stratNewObjectID:
		default:
			return fmt.Errorf("fix.collections[%d]: unknown fix_strategy %q — valid: %s, %s, %s",
				i, c.FixStrategy, stratStringToObjectID, stratNestedObjectID, stratNewObjectID)
		}
	}
	return nil
}

// ── JSON export ───────────────────────────────────────────────────────────────

func writeJSON(path string, data interface{}) error {
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if err := os.WriteFile(path, b, 0644); err != nil {
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}

// ── DETECT mode ───────────────────────────────────────────────────────────────
//
// Two index probes per collection (sort _id ASC / DESC).
// BSON comparison order means different types land at different ends of the index.
// Runs up to detectConcurrent probes in parallel.

type detectEntry struct {
	Namespace   string `json:"namespace"`
	Status      string `json:"status"`                 // mixed | clean | empty | error
	MinType     string `json:"min_type,omitempty"`     // ASC probe type (only when mixed)
	MaxType     string `json:"max_type,omitempty"`     // DESC probe type (only when mixed)
	UniformType string `json:"uniform_type,omitempty"` // type when clean
	Error       string `json:"error,omitempty"`
}

type detectReport struct {
	Timestamp    string        `json:"timestamp"`
	TotalScanned int           `json:"total_scanned"`
	TotalMixed   int           `json:"total_mixed"`
	TotalClean   int           `json:"total_clean"`
	TotalEmpty   int           `json:"total_empty"`
	TotalError   int           `json:"total_error"`
	Results      []detectEntry `json:"results"`
}

func probeCollection(ctx context.Context, db *mongo.Database, collName string) detectEntry {
	ns := db.Name() + "." + collName
	coll := db.Collection(collName)

	minType, err := probeIDType(ctx, coll, 1)
	if err != nil {
		return detectEntry{Namespace: ns, Status: "error", Error: err.Error()}
	}
	if minType == "" {
		return detectEntry{Namespace: ns, Status: "empty"}
	}

	maxType, err := probeIDType(ctx, coll, -1)
	if err != nil {
		return detectEntry{Namespace: ns, Status: "error", Error: err.Error()}
	}

	if minType != maxType {
		return detectEntry{Namespace: ns, Status: "mixed", MinType: minType, MaxType: maxType}
	}
	return detectEntry{Namespace: ns, Status: "clean", UniformType: minType}
}

func runDetect(ctx context.Context, client *mongo.Client, cfg *detectCfg) {
	printSep()
	logInfo("MODE: detect — fast _id type scan (ASC / DESC index probes)")
	logInfo("Cost  : 2 index seeks per collection — no document reads")
	printSep2()

	// Resolve database list
	var dbNames []string
	if len(cfg.Databases) > 0 {
		dbNames = cfg.Databases
	} else {
		var err error
		dbNames, err = client.ListDatabaseNames(ctx,
			bson.D{{Key: "name", Value: bson.D{{Key: "$nin", Value: bson.A{"admin", "local", "config"}}}}},
		)
		if err != nil {
			logError("ListDatabaseNames: %v", err)
			return
		}
	}
	logInfo("Databases to scan : %d", len(dbNames))
	printSep2()

	// Collect all (dbName, collName) pairs first
	type pair struct{ db, coll string }
	var pairs []pair
	for _, dbName := range dbNames {
		db := client.Database(dbName)
		collNames, err := db.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			logError("[%s] ListCollectionNames: %v", dbName, err)
			continue
		}
		// Skip system collections
		for _, c := range collNames {
			if !strings.HasPrefix(c, "system.") {
				pairs = append(pairs, pair{dbName, c})
			}
		}
		logInfo("[%s] %d collection(s)", dbName, len(collNames))
	}

	printSep2()
	logInfo("Probing %d collections ...", len(pairs))

	// Parallel probes with bounded concurrency
	sem := make(chan struct{}, detectConcurrent)
	var wg sync.WaitGroup
	var mu sync.Mutex
	entries := make([]detectEntry, 0, len(pairs))

	for _, p := range pairs {
		wg.Add(1)
		sem <- struct{}{}
		go func(dbName, collName string) {
			defer func() { <-sem; wg.Done() }()
			entry := probeCollection(ctx, client.Database(dbName), collName)
			mu.Lock()
			entries = append(entries, entry)
			mu.Unlock()
			switch entry.Status {
			case "mixed":
				logMixed("%s — ASC: %s | DESC: %s", entry.Namespace, entry.MinType, entry.MaxType)
			case "error":
				logError("%s — %s", entry.Namespace, entry.Error)
			}
		}(p.db, p.coll)
	}
	wg.Wait()

	// Sort: mixed first, then clean, then empty/error
	sort.Slice(entries, func(i, j int) bool {
		order := map[string]int{"mixed": 0, "clean": 1, "empty": 2, "error": 3}
		oi, oj := order[entries[i].Status], order[entries[j].Status]
		if oi != oj {
			return oi < oj
		}
		return entries[i].Namespace < entries[j].Namespace
	})

	// Counts
	var nMixed, nClean, nEmpty, nError int
	for _, e := range entries {
		switch e.Status {
		case "mixed":
			nMixed++
		case "clean":
			nClean++
		case "empty":
			nEmpty++
		case "error":
			nError++
		}
	}

	printSep()
	logInfo("SUMMARY")
	logInfo("  Collections scanned : %s", fmtInt(int64(len(entries))))
	logInfo("  Mixed               : %s", fmtInt(int64(nMixed)))
	logInfo("  Clean               : %s", fmtInt(int64(nClean)))
	logInfo("  Empty               : %s", fmtInt(int64(nEmpty)))
	logInfo("  Errors              : %s", fmtInt(int64(nError)))

	if nMixed > 0 {
		printSep2()
		logInfo("Collections with mixed _id types:")
		for _, e := range entries {
			if e.Status == "mixed" {
				logMixed("  %s  (ASC: %s, DESC: %s)", e.Namespace, e.MinType, e.MaxType)
			}
		}
		printSep2()
		logInfo("Next steps:")
		logInfo("  1. Run --mode count on these namespaces for exact counts per type")
		logInfo("  2. Add them to fix.collections in config and run --mode fix --dry-run=true")
	} else {
		logInfo("  No mixed _id types found — safe to proceed with migration")
	}
	printSep()

	if cfg.OutputJSON == "" {
		return
	}
	report := detectReport{
		Timestamp:    time.Now().UTC().Format(time.RFC3339),
		TotalScanned: len(entries),
		TotalMixed:   nMixed,
		TotalClean:   nClean,
		TotalEmpty:   nEmpty,
		TotalError:   nError,
		Results:      entries,
	}
	if err := writeJSON(cfg.OutputJSON, report); err != nil {
		logError("Cannot write detect JSON: %v", err)
	} else {
		logInfo("Detect report written to %s", cfg.OutputJSON)
	}
}

// ── COUNT mode ────────────────────────────────────────────────────────────────
//
// Covered aggregation on the _id index to count documents per BSON type.
// Runs sequentially per namespace (aggregations can be heavy).

type typeEntry struct {
	BSONType string  `json:"bson_type"`
	Count    int64   `json:"count"`
	Percent  float64 `json:"percent"`
}

type countEntry struct {
	Namespace string      `json:"namespace"`
	Status    string      `json:"status"` // mixed | clean | empty | error
	Types     []typeEntry `json:"types,omitempty"`
	Total     int64       `json:"total,omitempty"`
	Error     string      `json:"error,omitempty"`
}

type countReport struct {
	Timestamp string       `json:"timestamp"`
	Results   []countEntry `json:"results"`
}

func countCollection(ctx context.Context, client *mongo.Client, ns string) countEntry {
	dbName, collName, err := parseNS(ns)
	if err != nil {
		return countEntry{Namespace: ns, Status: "error", Error: err.Error()}
	}
	coll := client.Database(dbName).Collection(collName)

	pipeline := bson.A{
		bson.D{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: bson.D{{Key: "$type", Value: "$_id"}}},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		bson.D{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
	}
	cur, err := coll.Aggregate(ctx, pipeline, options.Aggregate().SetHint(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return countEntry{Namespace: ns, Status: "error", Error: err.Error()}
	}
	defer cur.Close(ctx)

	type row struct {
		Type  string `bson:"_id"`
		Count int64  `bson:"count"`
	}
	var rows []row
	if err := cur.All(ctx, &rows); err != nil {
		return countEntry{Namespace: ns, Status: "error", Error: err.Error()}
	}
	if len(rows) == 0 {
		return countEntry{Namespace: ns, Status: "empty"}
	}

	var total int64
	for _, r := range rows {
		total += r.Count
	}

	types := make([]typeEntry, len(rows))
	for i, r := range rows {
		pct := 0.0
		if total > 0 {
			pct = float64(r.Count) / float64(total) * 100
		}
		types[i] = typeEntry{BSONType: r.Type, Count: r.Count, Percent: math.Round(pct*100) / 100}
	}

	status := "clean"
	if len(rows) > 1 {
		status = "mixed"
	}
	return countEntry{Namespace: ns, Status: status, Types: types, Total: total}
}

func runCount(ctx context.Context, client *mongo.Client, cfg *countCfg) {
	if len(cfg.Namespaces) == 0 {
		logError("count.namespaces is empty — add namespaces to the config")
		return
	}
	printSep()
	logInfo("MODE: count — _id type distribution (%d namespace(s))", len(cfg.Namespaces))
	logInfo("Method: covered aggregation on _id index")
	printSep2()

	entries := make([]countEntry, 0, len(cfg.Namespaces))
	for _, ns := range cfg.Namespaces {
		logInfo("%s", ns)
		entry := countCollection(ctx, client, ns)
		switch entry.Status {
		case "error":
			logError("  %s", entry.Error)
		case "empty":
			logInfo("  (empty collection)")
		default:
			for _, t := range entry.Types {
				logInfo("  %-14s %14s docs  %7.2f%%", t.BSONType, fmtInt(t.Count), t.Percent)
			}
			logInfo("  %-14s %14s docs  100.00%%", "total", fmtInt(entry.Total))
			if entry.Status == "mixed" {
				logWarn("  STATUS: MIXED — configure fix and run --mode fix")
			} else {
				logOk("  STATUS: OK — single _id type")
			}
		}
		entries = append(entries, entry)
		printSep2()
	}
	printSep()

	if cfg.OutputJSON == "" {
		return
	}
	report := countReport{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Results:   entries,
	}
	if err := writeJSON(cfg.OutputJSON, report); err != nil {
		logError("Cannot write count JSON: %v", err)
	} else {
		logInfo("Count report written to %s", cfg.OutputJSON)
	}
}

// ── FIX mode — Phase 1: Move ──────────────────────────────────────────────────

type p1Stats struct {
	Moved  int64 `json:"moved"`
	Dupes  int64 `json:"dupes"`
	Errors int64 `json:"errors"`
}

func phase1Move(ctx context.Context, client *mongo.Client, entry collectionCfg, batchSize, workers, throttleMs int, dryRun bool) p1Stats {
	dot := strings.Index(entry.Namespace, ".")
	dbName, collName := entry.Namespace[:dot], entry.Namespace[dot+1:]
	db := client.Database(dbName)
	sourceColl := db.Collection(collName)
	backupColl := db.Collection(collName + "_id_fix_backup")
	backupNs := dbName + "." + collName + "_id_fix_backup"

	logStep("Phase 1 — move _id type %q docs → %s", entry.WrongType, backupNs)
	if throttleMs > 0 {
		logInfo("  Throttle: %dms per batch per worker", throttleMs)
	}

	cursor, err := sourceColl.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: entry.WrongType}}}},
		options.Find().SetBatchSize(int32(batchSize)),
	)
	if err != nil {
		logError("Cannot open source cursor: %v", err)
		return p1Stats{Errors: 1}
	}
	defer cursor.Close(ctx)

	var moved, dupes, errors int64
	batchCh := make(chan []bson.D, workers*2)
	var wg sync.WaitGroup

	start := time.Now()
	ticker := time.NewTicker(30 * time.Second)
	tickDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				n := atomic.LoadInt64(&moved)
				if dryRun {
					logDry("  Would move %s docs (%s) ...", fmtInt(n), fmtRate(n, start))
				} else {
					logInfo("  Moved %s docs (%s) ...", fmtInt(n), fmtRate(n, start))
				}
			case <-tickDone:
				return
			}
		}
	}()

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				if dryRun {
					atomic.AddInt64(&moved, int64(len(batch)))
					if throttleMs > 0 {
						time.Sleep(time.Duration(throttleMs) * time.Millisecond)
					}
					continue
				}

				docs := make([]interface{}, len(batch))
				for i, d := range batch {
					docs[i] = d
				}

				// a. Backup insert (unordered)
				confirmedIDs := make([]interface{}, 0, len(batch))
				_, ierr := backupColl.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
				if ierr != nil {
					bwe, ok := ierr.(mongo.BulkWriteException)
					if !ok {
						logError("Backup insertMany failed: %v", ierr)
						atomic.AddInt64(&errors, int64(len(batch)))
						continue
					}
					hardFailed := make(map[int]bool, len(bwe.WriteErrors))
					for _, we := range bwe.WriteErrors {
						if we.Code == 11000 {
							atomic.AddInt64(&dupes, 1)
						} else {
							hardFailed[we.Index] = true
							atomic.AddInt64(&errors, 1)
							logError("Backup insert index %d: %v", we.Index, we.Message)
						}
					}
					for i, d := range batch {
						if !hardFailed[i] {
							confirmedIDs = append(confirmedIDs, getDocID(d))
						}
					}
				} else {
					for _, d := range batch {
						confirmedIDs = append(confirmedIDs, getDocID(d))
					}
				}

				if len(confirmedIDs) > 0 {
					// b. Delete confirmed IDs from source
					res, derr := sourceColl.DeleteMany(ctx,
						bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: confirmedIDs}}}},
					)
					if derr != nil {
						logError("Source deleteMany failed: %v", derr)
						atomic.AddInt64(&errors, int64(len(confirmedIDs)))
					} else {
						atomic.AddInt64(&moved, res.DeletedCount)
					}
				}

				if throttleMs > 0 {
					time.Sleep(time.Duration(throttleMs) * time.Millisecond)
				}
			}
		}()
	}

	// Reader
	batch := make([]bson.D, 0, batchSize)
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			logError("Cursor decode: %v", err)
			continue
		}
		batch = append(batch, doc)
		if len(batch) == batchSize {
			batchCh <- batch
			batch = make([]bson.D, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		batchCh <- batch
	}
	close(batchCh)

	if cerr := cursor.Err(); cerr != nil {
		logError("Cursor error: %v", cerr)
	}
	wg.Wait()
	ticker.Stop()
	close(tickDone)

	n, d, e := atomic.LoadInt64(&moved), atomic.LoadInt64(&dupes), atomic.LoadInt64(&errors)
	elapsed := time.Since(start).Seconds()
	switch {
	case dryRun:
		logDry("  Would move %s docs total", fmtInt(n))
	case n == 0 && d == 0 && e == 0:
		logOk("  No documents with _id type %q found — nothing to move", entry.WrongType)
	default:
		logOk("  Moved %s | dupes %s | errors %s | %.1fs", fmtInt(n), fmtInt(d), fmtInt(e), elapsed)
	}
	return p1Stats{Moved: n, Dupes: d, Errors: e}
}

// ── FIX mode — Phase 2: Transform ────────────────────────────────────────────

type p2Stats struct {
	Inserted    int64 `json:"inserted"`
	ResumeDupes int64 `json:"resume_dupes,omitempty"` // docs already in source from a prior interrupted run
	Skipped     int64 `json:"skipped"`
	Errors      int64 `json:"errors"`
}

func phase2Transform(ctx context.Context, client *mongo.Client, entry collectionCfg, batchSize, workers, throttleMs int, dryRun bool) p2Stats {
	dot := strings.Index(entry.Namespace, ".")
	dbName, collName := entry.Namespace[:dot], entry.Namespace[dot+1:]
	db := client.Database(dbName)
	sourceColl := db.Collection(collName)
	backupColl := db.Collection(collName + "_id_fix_backup")

	logStep("Phase 2 — transform and insert corrected docs into %s", entry.Namespace)

	var readColl *mongo.Collection
	var filter bson.D
	if dryRun {
		readColl = sourceColl
		filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: entry.WrongType}}}}
	} else {
		readColl = backupColl
		filter = bson.D{}
	}

	cursor, err := readColl.Find(ctx, filter,
		options.Find().SetBatchSize(int32(batchSize)),
	)
	if err != nil {
		logError("Cannot open cursor: %v", err)
		return p2Stats{Errors: 1}
	}
	defer cursor.Close(ctx)

	var inserted, skipped, errors, resumeDupes int64
	var warnCount, skipCount int64

	batchCh := make(chan []bson.D, workers*2)
	var wg sync.WaitGroup

	start := time.Now()
	ticker := time.NewTicker(30 * time.Second)
	tickDone := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				n := atomic.LoadInt64(&inserted)
				if dryRun {
					logDry("  Would insert %s docs (%s) ...", fmtInt(n), fmtRate(n, start))
				} else {
					logInfo("  Inserted %s docs (%s) ...", fmtInt(n), fmtRate(n, start))
				}
			case <-tickDone:
				return
			}
		}
	}()

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				corrected := make([]interface{}, 0, len(batch))
				for _, doc := range batch {
					newID, skip, msg := deriveNewID(doc, entry.FixStrategy, entry.NewIDOnFailure)
					if skip {
						atomic.AddInt64(&skipped, 1)
						if atomic.AddInt64(&skipCount, 1) <= maxInlineSkip {
							logSkip("_id %v: %s", getDocID(doc), msg)
						}
						continue
					}
					if msg != "" {
						if atomic.AddInt64(&warnCount, 1) <= maxInlineWarn {
							logWarn("_id %v: %s", getDocID(doc), msg)
						}
					}
					corrected = append(corrected, replaceID(doc, newID))
				}

				if len(corrected) == 0 {
					continue
				}
				if dryRun {
					atomic.AddInt64(&inserted, int64(len(corrected)))
					if throttleMs > 0 {
						time.Sleep(time.Duration(throttleMs) * time.Millisecond)
					}
					continue
				}

				_, ierr := sourceColl.InsertMany(ctx, corrected, options.InsertMany().SetOrdered(false))
				if ierr != nil {
					bwe, ok := ierr.(mongo.BulkWriteException)
					if !ok {
						logError("insertMany failed: %v", ierr)
						atomic.AddInt64(&errors, int64(len(corrected)))
					} else {
						hardErrCount, dupeCount := 0, 0
						for _, we := range bwe.WriteErrors {
							if we.Code == 11000 {
								dupeCount++
							} else {
								hardErrCount++
								logError("Insert index %d: %v", we.Index, we.Message)
							}
						}
						// Dupes = docs already in source from a previous interrupted run.
						// Accumulate silently; a single summary line is printed at phase end.
						atomic.AddInt64(&resumeDupes, int64(dupeCount))
						atomic.AddInt64(&inserted, int64(len(corrected)-hardErrCount-dupeCount))
						atomic.AddInt64(&errors, int64(hardErrCount))
					}
				} else {
					atomic.AddInt64(&inserted, int64(len(corrected)))
				}

				if throttleMs > 0 {
					time.Sleep(time.Duration(throttleMs) * time.Millisecond)
				}
			}
		}()
	}

	// Reader
	batch := make([]bson.D, 0, batchSize)
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			logError("Cursor decode: %v", err)
			continue
		}
		batch = append(batch, doc)
		if len(batch) == batchSize {
			batchCh <- batch
			batch = make([]bson.D, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		batchCh <- batch
	}
	close(batchCh)

	if cerr := cursor.Err(); cerr != nil {
		logError("Cursor error: %v", cerr)
	}
	wg.Wait()
	ticker.Stop()
	close(tickDone)

	ins := atomic.LoadInt64(&inserted)
	rdups := atomic.LoadInt64(&resumeDupes)
	skp := atomic.LoadInt64(&skipped)
	errs := atomic.LoadInt64(&errors)
	wc := atomic.LoadInt64(&warnCount)
	sc := atomic.LoadInt64(&skipCount)
	elapsed := time.Since(start).Seconds()

	if wc > maxInlineWarn {
		logWarn("... %s more WARN suppressed (showed first %d)", fmtInt(wc-maxInlineWarn), maxInlineWarn)
	}
	if sc > maxInlineSkip {
		logSkip("... %s more SKIP suppressed (showed first %d)", fmtInt(sc-maxInlineSkip), maxInlineSkip)
	}
	if rdups > 0 {
		logWarn("  %s doc(s) already in source from a previous run — resume duplicates, no data loss", fmtInt(rdups))
	}
	if dryRun {
		logDry("  Would insert %s corrected docs (%s would be skipped)", fmtInt(ins), fmtInt(skp))
	} else {
		logOk("  Inserted %s | resume-dupes %s | skipped %s | errors %s | %.1fs", fmtInt(ins), fmtInt(rdups), fmtInt(skp), fmtInt(errs), elapsed)
	}
	return p2Stats{Inserted: ins, ResumeDupes: rdups, Skipped: skp, Errors: errs}
}

// ── FIX mode — Phase 3: Cleanup ───────────────────────────────────────────────

func phase3Cleanup(ctx context.Context, client *mongo.Client, entry collectionCfg, p2Errors int64, dropOnSuccess, dryRun bool) {
	dot := strings.Index(entry.Namespace, ".")
	dbName, collName := entry.Namespace[:dot], entry.Namespace[dot+1:]
	backupNs := dbName + "." + collName + "_id_fix_backup"

	logStep("Phase 3 — backup cleanup")

	if dryRun {
		action := "retain"
		if dropOnSuccess {
			action = "drop"
		}
		logDry("  Would %s %s", action, backupNs)
		return
	}
	if !dropOnSuccess {
		logInfo("  Backup retained at %s", backupNs)
		logInfo("  Drop manually: db.getSiblingDB(%q).%s_id_fix_backup.drop()", dbName, collName)
		return
	}
	if p2Errors > 0 {
		logWarn("  Phase 2 had %s error(s) — retaining backup at %s", fmtInt(p2Errors), backupNs)
		return
	}
	if err := client.Database(dbName).Collection(collName + "_id_fix_backup").Drop(ctx); err != nil {
		logWarn("  Could not drop backup: %v", err)
	} else {
		logOk("  Backup dropped")
	}
}

// ── FIX mode — runner ─────────────────────────────────────────────────────────

type fixEntry struct {
	Namespace string  `json:"namespace"`
	Phase1    p1Stats `json:"phase1"`
	Phase2    p2Stats `json:"phase2"`
}

type fixReport struct {
	Timestamp      string     `json:"timestamp"`
	Mode           string     `json:"mode"`
	ElapsedSeconds float64    `json:"elapsed_seconds"`
	Results        []fixEntry `json:"results"`
}

func runFix(ctx context.Context, client *mongo.Client, cfg *fixCfg, dryRun bool) {
	if err := validateFix(cfg); err != nil {
		logError("%v", err)
		return
	}

	mode := "LIVE (changes WILL be applied)"
	if dryRun {
		mode = "DRY RUN (no changes will be made)"
	}

	printSep()
	logInfo("MODE: fix — three-phase batch _id fix: Move → Transform → Cleanup")
	logInfo("Run mode    : %s", mode)
	logInfo("Batch size  : %s", fmtInt(int64(cfg.BatchSize)))
	logInfo("Workers     : %d per phase", cfg.Workers)
	logInfo("Throttle    : %dms per batch per worker", cfg.ThrottleMs)
	logInfo("Drop backup : %v", cfg.DropBackupOnSuccess)
	logInfo("Collections : %d", len(cfg.Collections))
	for i, c := range cfg.Collections {
		logInfo("  [%d] %s  wrongType: %s  strategy: %s  newIdOnFailure: %v",
			i+1, c.Namespace, c.WrongType, c.FixStrategy, c.NewIDOnFailure)
	}
	printSep()

	runStart := time.Now()
	results := make([]fixEntry, 0, len(cfg.Collections))

	for i, entry := range cfg.Collections {
		logInfo("[%d/%d] %s", i+1, len(cfg.Collections), entry.Namespace)
		printSep2()
		p1 := phase1Move(ctx, client, entry, cfg.BatchSize, cfg.Workers, cfg.ThrottleMs, dryRun)
		printSep2()
		p2 := phase2Transform(ctx, client, entry, cfg.BatchSize, cfg.Workers, cfg.ThrottleMs, dryRun)
		printSep2()
		phase3Cleanup(ctx, client, entry, p2.Errors, cfg.DropBackupOnSuccess, dryRun)
		results = append(results, fixEntry{Namespace: entry.Namespace, Phase1: p1, Phase2: p2})
		printSep()
	}

	elapsed := time.Since(runStart).Seconds()
	logInfo("GLOBAL SUMMARY")
	printSep2()
	for _, r := range results {
		logInfo("%s", r.Namespace)
		logInfo("  Phase 1 (move)      : moved %s  dupes %s  errors %s",
			fmtInt(r.Phase1.Moved), fmtInt(r.Phase1.Dupes), fmtInt(r.Phase1.Errors))
		logInfo("  Phase 2 (transform) : inserted %s  resume-dupes %s  skipped %s  errors %s",
			fmtInt(r.Phase2.Inserted), fmtInt(r.Phase2.ResumeDupes), fmtInt(r.Phase2.Skipped), fmtInt(r.Phase2.Errors))
	}
	printSep2()
	logInfo("Total elapsed : %.1fs", elapsed)
	logInfo("Mode          : %s", mode)
	printSep()

	if cfg.OutputJSON == "" {
		return
	}
	report := fixReport{
		Timestamp:      time.Now().UTC().Format(time.RFC3339),
		Mode:           mode,
		ElapsedSeconds: elapsed,
		Results:        results,
	}
	if err := writeJSON(cfg.OutputJSON, report); err != nil {
		logError("Cannot write fix JSON: %v", err)
	} else {
		logInfo("Fix report written to %s", cfg.OutputJSON)
	}
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	modeFlag := flag.String("mode", "", "Operation: detect | count | fix")
	configFile := flag.String("config", "config.json", "Path to JSON config file")
	dryRun := flag.Bool("dry-run", true, "Dry run for fix mode (default: true)")
	flag.Parse()

	if *modeFlag == "" {
		fmt.Fprintf(os.Stderr,
			"Usage: fix-id-types --mode <detect|count|fix> [--config config.json] [--dry-run=true]\n\n"+
				"  detect   Fast scan — find collections with mixed _id types (2 index seeks/collection)\n"+
				"  count    Count documents per _id type for specified namespaces\n"+
				"  fix      Three-phase fix: Move wrong-type docs → Transform → Cleanup\n\n"+
				"Options:\n"+
				"  --config     Path to JSON config file (default: config.json)\n"+
				"  --dry-run    Preview changes without writing (fix mode only, default: true)\n",
		)
		os.Exit(1)
	}

	cfg, err := loadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}

	logCloser, err := initLogWriter(cfg.LogFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	if logCloser != nil {
		defer logCloser.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	client, err := mongo.Connect(ctx,
		options.Client().
			ApplyURI(cfg.SourceURI).
			SetRetryWrites(false), // DocumentDB does not support retryable writes
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: connect failed: %v\n", err)
		os.Exit(1)
	}
	defer client.Disconnect(ctx)

	if err := client.Ping(ctx, nil); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot reach server: %v\n", err)
		os.Exit(1)
	}

	switch *modeFlag {
	case "detect":
		runDetect(ctx, client, &cfg.Detect)
	case "count":
		runCount(ctx, client, &cfg.Count)
	case "fix":
		runFix(ctx, client, &cfg.Fix, *dryRun)
	default:
		fmt.Fprintf(os.Stderr, "ERROR: unknown mode %q — must be detect, count, or fix\n", *modeFlag)
		os.Exit(1)
	}
}
