package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
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

type collectionCfg struct {
	Namespace      string `json:"namespace"`
	WrongType      string `json:"wrong_type"`
	FixStrategy    string `json:"fix_strategy"`
	NewIDOnFailure bool   `json:"new_id_on_failure"`
}

type config struct {
	SourceURI           string          `json:"source_uri"`
	Collections         []collectionCfg `json:"collections"`
	BatchSize           int             `json:"batch_size"`
	Workers             int             `json:"workers"`
	DropBackupOnSuccess bool            `json:"drop_backup_on_success"`
}

// fix_strategy values
const (
	stratStringToObjectID = "string_to_objectid" // string "_id" that looks like hex → ObjectId
	stratNestedObjectID   = "nested_objectid"     // { _id: ObjectId("...") } → extract inner ObjectId
	stratNewObjectID      = "new_objectid"        // always generate a new ObjectId
)

const (
	maxInlineWarn = 20 // max individual WARN lines per phase; excess counted, reported at end
	maxInlineSkip = 20 // max individual SKIP lines per phase; excess counted, reported at end
)

// ── Logging ───────────────────────────────────────────────────────────────────

var logMu sync.Mutex

func ts() string { return time.Now().UTC().Format("2006/01/02 15:04:05") }

func emit(level, format string, args ...interface{}) {
	logMu.Lock()
	fmt.Printf(ts()+" "+level+" "+format+"\n", args...)
	logMu.Unlock()
}

func logInfo(f string, a ...interface{})  { emit("[INFO ]", f, a...) }
func logOk(f string, a ...interface{})    { emit("[OK   ]", f, a...) }
func logStep(f string, a ...interface{})  { emit("[STEP ]", f, a...) }
func logWarn(f string, a ...interface{})  { emit("[WARN ]", f, a...) }
func logError(f string, a ...interface{}) { emit("[ERROR]", f, a...) }
func logDry(f string, a ...interface{})   { emit("[DRY  ]", f, a...) }
func logSkip(f string, a ...interface{})  { emit("[SKIP ]", f, a...) }

func printSep() {
	logMu.Lock()
	fmt.Println(strings.Repeat("═", 60))
	logMu.Unlock()
}

func printSep2() {
	logMu.Lock()
	fmt.Println(strings.Repeat("─", 60))
	logMu.Unlock()
}

// ── Number formatting ─────────────────────────────────────────────────────────

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

// deriveNewID applies the configured fix strategy to a document and returns the
// corrected ObjectId.  Returns (zero, skip=true, reason) when the doc should be
// skipped.  Returns (newID, skip=false, warnMsg) when newIDOnFailure rescued a
// failed conversion.
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
	if len(cfg.Collections) == 0 {
		return nil, fmt.Errorf("collections list is empty")
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	for i, c := range cfg.Collections {
		if c.Namespace == "" {
			return nil, fmt.Errorf("collections[%d]: namespace is required", i)
		}
		dot := strings.Index(c.Namespace, ".")
		if dot < 1 || dot == len(c.Namespace)-1 {
			return nil, fmt.Errorf("collections[%d]: namespace %q must be database.collection", i, c.Namespace)
		}
		if c.WrongType == "" {
			return nil, fmt.Errorf("collections[%d]: wrong_type is required", i)
		}
		switch c.FixStrategy {
		case stratStringToObjectID, stratNestedObjectID, stratNewObjectID:
		default:
			return nil, fmt.Errorf("collections[%d]: unknown fix_strategy %q — valid values: %s, %s, %s",
				i, c.FixStrategy, stratStringToObjectID, stratNestedObjectID, stratNewObjectID)
		}
	}
	return &cfg, nil
}

// ── Phase 1 — Move wrong-type docs from source to backup ──────────────────────
//
// Pipeline:
//   reader goroutine → batchCh → N worker goroutines
//
// Each worker per batch:
//   a. InsertMany(batch) into <collection>_id_fix_backup  (ordered=false)
//   b. DeleteMany(_id $in confirmedIDs) from source
//
// Source is clean of wrong-type docs after this phase.
// ─────────────────────────────────────────────────────────────────────────────

type p1Result struct{ moved, dupes, errors int64 }

func phase1Move(ctx context.Context, client *mongo.Client, entry collectionCfg, batchSize, workers int, dryRun bool) p1Result {
	dot := strings.Index(entry.Namespace, ".")
	dbName, collName := entry.Namespace[:dot], entry.Namespace[dot+1:]

	db         := client.Database(dbName)
	sourceColl := db.Collection(collName)
	backupColl := db.Collection(collName + "_id_fix_backup")
	backupNs   := dbName + "." + collName + "_id_fix_backup"

	logStep("Phase 1 — move _id type %q docs → %s", entry.WrongType, backupNs)

	cursor, err := sourceColl.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$type", Value: entry.WrongType}}}},
		options.Find().
			SetBatchSize(int32(batchSize)).
			SetNoCursorTimeout(true),
	)
	if err != nil {
		logError("Cannot open source cursor: %v", err)
		return p1Result{errors: 1}
	}
	defer cursor.Close(ctx)

	var moved, dupes, errors int64

	batchCh := make(chan []bson.D, workers*2)
	var wg sync.WaitGroup

	// Progress ticker — time-based so output rate doesn't scale with throughput.
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

	// Workers
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				if dryRun {
					atomic.AddInt64(&moved, int64(len(batch)))
					continue
				}

				// Convert to []interface{} for InsertMany
				docs := make([]interface{}, len(batch))
				for i, d := range batch {
					docs[i] = d
				}

				// a. Backup insert (unordered — don't stop on first error)
				confirmedIDs := make([]interface{}, 0, len(batch))
				_, ierr := backupColl.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
				if ierr != nil {
					bwe, ok := ierr.(mongo.BulkWriteException)
					if !ok {
						logError("Backup insertMany failed for batch: %v", ierr)
						atomic.AddInt64(&errors, int64(len(batch)))
						continue
					}
					// Track which indices hard-failed (not duplicates)
					hardFailed := make(map[int]bool, len(bwe.WriteErrors))
					for _, we := range bwe.WriteErrors {
						if we.Code == 11000 {
							// Already in backup from a previous run — still delete from source.
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

				if len(confirmedIDs) == 0 {
					continue
				}

				// b. Delete confirmed IDs from source
				res, derr := sourceColl.DeleteMany(ctx,
					bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: confirmedIDs}}}},
				)
				if derr != nil {
					logError("Source deleteMany failed: %v", derr)
					atomic.AddInt64(&errors, int64(len(confirmedIDs)))
					continue
				}
				atomic.AddInt64(&moved, res.DeletedCount)
			}
		}()
	}

	// Reader — drain cursor into fixed-size batches and feed workers
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
	return p1Result{moved: n, dupes: d, errors: e}
}

// ── Phase 2 — Transform and insert corrected docs ─────────────────────────────
//
// Live mode  : reads from backup (Phase 1 populated it).
// Dry-run    : reads from source with wrongType filter (backup doesn't exist yet).
//
// WARN / SKIP lines are capped at maxInlineWarn / maxInlineSkip per phase.
// Excess count is printed once in the phase summary.
// ─────────────────────────────────────────────────────────────────────────────

type p2Result struct{ inserted, skipped, errors int64 }

func phase2Transform(ctx context.Context, client *mongo.Client, entry collectionCfg, batchSize, workers int, dryRun bool) p2Result {
	dot := strings.Index(entry.Namespace, ".")
	dbName, collName := entry.Namespace[:dot], entry.Namespace[dot+1:]

	db         := client.Database(dbName)
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
		options.Find().
			SetBatchSize(int32(batchSize)).
			SetNoCursorTimeout(true),
	)
	if err != nil {
		logError("Cannot open cursor for Phase 2: %v", err)
		return p2Result{errors: 1}
	}
	defer cursor.Close(ctx)

	var inserted, skipped, errors int64
	var warnCount, skipCount int64 // total attempts (shown + suppressed)

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
						// Only print the first maxInlineSkip individual lines
						if atomic.AddInt64(&skipCount, 1) <= maxInlineSkip {
							logSkip("_id %v: %s", getDocID(doc), msg)
						}
						continue
					}
					if msg != "" {
						// Non-fatal warning (newIDOnFailure rescued it)
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
					continue
				}

				_, ierr := sourceColl.InsertMany(ctx, corrected, options.InsertMany().SetOrdered(false))
				if ierr != nil {
					bwe, ok := ierr.(mongo.BulkWriteException)
					if !ok {
						logError("insertMany failed: %v", ierr)
						atomic.AddInt64(&errors, int64(len(corrected)))
						continue
					}
					hardErrCount, dupeCount := 0, 0
					for _, we := range bwe.WriteErrors {
						if we.Code == 11000 {
							dupeCount++
						} else {
							hardErrCount++
							logError("Insert index %d: %v", we.Index, we.Message)
						}
					}
					if dupeCount > 0 {
						logWarn("%s corrected doc(s) already in source (previous run) — skipped", fmtInt(int64(dupeCount)))
					}
					atomic.AddInt64(&inserted, int64(len(corrected)-hardErrCount))
					atomic.AddInt64(&errors, int64(hardErrCount))
				} else {
					atomic.AddInt64(&inserted, int64(len(corrected)))
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

	ins  := atomic.LoadInt64(&inserted)
	skp  := atomic.LoadInt64(&skipped)
	errs := atomic.LoadInt64(&errors)
	wc   := atomic.LoadInt64(&warnCount)
	sc   := atomic.LoadInt64(&skipCount)
	elapsed := time.Since(start).Seconds()

	// Summarise suppressed lines so nothing is silently lost
	if wc > maxInlineWarn {
		logWarn("... %s more WARN suppressed (showed first %d)", fmtInt(wc-maxInlineWarn), maxInlineWarn)
	}
	if sc > maxInlineSkip {
		logSkip("... %s more SKIP suppressed (showed first %d)", fmtInt(sc-maxInlineSkip), maxInlineSkip)
	}

	if dryRun {
		logDry("  Would insert %s corrected docs (%s would be skipped)", fmtInt(ins), fmtInt(skp))
	} else {
		logOk("  Inserted %s | skipped %s | errors %s | %.1fs", fmtInt(ins), fmtInt(skp), fmtInt(errs), elapsed)
	}
	return p2Result{inserted: ins, skipped: skp, errors: errs}
}

// ── Phase 3 — Drop backup ─────────────────────────────────────────────────────

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
		logInfo("  Drop manually after verifying: db.getSiblingDB(%q).%s_id_fix_backup.drop()", dbName, collName)
		return
	}

	if p2Errors > 0 {
		logWarn("  Phase 2 had %s error(s) — retaining backup at %s", fmtInt(p2Errors), backupNs)
		return
	}

	if err := client.Database(dbName).Collection(collName+"_id_fix_backup").Drop(ctx); err != nil {
		logWarn("  Could not drop backup: %v", err)
	} else {
		logOk("  Backup collection dropped")
	}
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	configFile := flag.String("config", "config.json", "Path to JSON config file")
	dryRun     := flag.Bool("dry-run", true, "Preview without making changes (default: true)")
	flag.Parse()

	cfg, err := loadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	client, err := mongo.Connect(ctx,
		options.Client().
			ApplyURI(cfg.SourceURI).
			SetRetryWrites(false), // required: DocumentDB does not support retryable writes
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

	runStart := time.Now()
	mode := "LIVE (changes WILL be applied)"
	if *dryRun {
		mode = "DRY RUN (no changes will be made)"
	}

	printSep()
	logInfo("fix-id-types — three-phase batch _id fix: Move → Transform → Cleanup")
	logInfo("Mode        : %s", mode)
	logInfo("Batch size  : %s docs/round-trip", fmtInt(int64(cfg.BatchSize)))
	logInfo("Workers     : %d per phase", cfg.Workers)
	logInfo("Drop backup : %v", cfg.DropBackupOnSuccess)
	logInfo("Collections : %d", len(cfg.Collections))
	for i, c := range cfg.Collections {
		logInfo("  [%d] %s  wrongType: %s  fixStrategy: %s  newIdOnFailure: %v",
			i+1, c.Namespace, c.WrongType, c.FixStrategy, c.NewIDOnFailure)
	}
	printSep()

	type result struct {
		ns string
		p1 p1Result
		p2 p2Result
	}
	results := make([]result, 0, len(cfg.Collections))

	for i, entry := range cfg.Collections {
		logInfo("[%d/%d] %s", i+1, len(cfg.Collections), entry.Namespace)
		printSep2()
		p1 := phase1Move(ctx, client, entry, cfg.BatchSize, cfg.Workers, *dryRun)
		printSep2()
		p2 := phase2Transform(ctx, client, entry, cfg.BatchSize, cfg.Workers, *dryRun)
		printSep2()
		phase3Cleanup(ctx, client, entry, p2.errors, cfg.DropBackupOnSuccess, *dryRun)
		results = append(results, result{ns: entry.Namespace, p1: p1, p2: p2})
		printSep()
	}

	logInfo("GLOBAL SUMMARY")
	printSep2()
	for _, r := range results {
		logInfo("%s", r.ns)
		logInfo("  Phase 1 (move)      : moved %s  dupes %s  errors %s",
			fmtInt(r.p1.moved), fmtInt(r.p1.dupes), fmtInt(r.p1.errors))
		logInfo("  Phase 2 (transform) : inserted %s  skipped %s  errors %s",
			fmtInt(r.p2.inserted), fmtInt(r.p2.skipped), fmtInt(r.p2.errors))
	}
	printSep2()
	logInfo("Total elapsed : %.1fs", time.Since(runStart).Seconds())
	logInfo("Mode          : %s", mode)
	printSep()
}
