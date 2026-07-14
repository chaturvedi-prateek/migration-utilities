// migrate-indexes copies indexes from a DocDB source to an Atlas destination.
//
// Two modes:
//
//	--mode=create  (pre-cutover)
//	  Reads every non-_id_ index from source, creates it on destination with:
//	    • TTL indexes   → expireAfterSeconds = MAX_INT  (deactivated during migration)
//	    • Unique indexes → unique: false                (no uniqueness violations during sync)
//	  All createIndex calls run in parallel across a worker pool.
//
//	--mode=rectify  (post-cutover)
//	  Phase 1 (parallel): prepareUnique on formerly-unique indexes + restore original TTL values
//	  Phase 2 (parallel): collMod unique:true on all formerly-unique indexes
//	  Phases are strictly sequential; operations within each phase are parallel.
//
// A background monitor goroutine polls currentOp on the destination every 5 s
// and prints any active index builds so you can watch progress.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const maxTTLSeconds = int32(2147483647)

var systemDBs = map[string]bool{"admin": true, "local": true, "config": true}

// ── Output (optional tee to log file) ──────────────────────────────────────────

var (
	logWriter io.Writer = os.Stdout
	errWriter io.Writer = os.Stderr
)

// initLogWriter tees all output to logFile (stdout/stderr always included).
// Returns an io.Closer for the opened file, or nil if no log file is configured.
func initLogWriter(logFile string) (io.Closer, error) {
	if logFile == "" {
		return nil, nil
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open log file %q: %w", logFile, err)
	}
	logWriter = io.MultiWriter(os.Stdout, f)
	errWriter = io.MultiWriter(os.Stderr, f)
	return f, nil
}

func outf(format string, args ...interface{}) { fmt.Fprintf(logWriter, format, args...) }
func outln(args ...interface{})               { fmt.Fprintln(logWriter, args...) }
func errf(format string, args ...interface{}) { fmt.Fprintf(errWriter, format, args...) }

// ── Config ────────────────────────────────────────────────────────────────────

type config struct {
	SourceURI      string   `json:"source_uri"`
	DestinationURI string   `json:"destination_uri"`
	Databases      []string `json:"databases"` // empty = all non-system databases
	LogFile        string   `json:"log_file"`  // optional: tee all output to this file (stdout always included)
}

func loadConfig(path string) (*config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open config file %q: %w", path, err)
	}
	defer f.Close()

	var cfg config
	if err := json.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("cannot parse config file: %w", err)
	}
	if cfg.SourceURI == "" {
		return nil, fmt.Errorf("config: source_uri is required")
	}
	if cfg.DestinationURI == "" {
		return nil, fmt.Errorf("config: destination_uri is required")
	}
	return &cfg, nil
}

// ── Index spec ────────────────────────────────────────────────────────────────

// indexSpec holds everything needed to recreate or rectify one index.
type indexSpec struct {
	DB          string
	Collection  string
	Name        string
	Keys        bson.D
	ExtraOpts   bson.D // sparse, partialFilterExpression, collation, weights, etc.
	IsUnique    bool
	HasTTL      bool
	OriginalTTL int32
}

// opResult is returned by each worker goroutine.
type opResult struct {
	spec indexSpec
	err  error
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	configFile := flag.String("config", "config.json", "Path to JSON config file")
	dryRun := flag.Bool("dry-run", true, "Print what would be done without applying changes (default: true)")
	mode := flag.String("mode", "create", "'create' = pre-cutover  |  'rectify' = post-cutover TTL+unique fix  |  'verify' = compare source vs destination")
	concurrency := flag.Int("concurrency", 8, "Max simultaneous index operations on the destination")
	flag.Parse()

	if *mode != "create" && *mode != "rectify" && *mode != "verify" {
		fatalf("ERROR: --mode must be 'create', 'rectify', or 'verify'\n")
	}

	cfg, err := loadConfig(*configFile)
	if err != nil {
		fatalf("ERROR: %v\n", err)
	}

	logCloser, err := initLogWriter(cfg.LogFile)
	if err != nil {
		fatalf("ERROR: %v\n", err)
	}
	if logCloser != nil {
		defer logCloser.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	srcClient := mustConnect(ctx, cfg.SourceURI, "source")
	defer srcClient.Disconnect(ctx)

	dstClient := mustConnect(ctx, cfg.DestinationURI, "destination")
	defer dstClient.Disconnect(ctx)

	modeLabel := "LIVE RUN — changes will be applied"
	if *dryRun {
		modeLabel = "DRY RUN — no changes will be written"
	}
	outf("\n%s\n  migrate-indexes  |  mode=%-8s  |  %s\n  Config: %s  |  Concurrency: %d\n%s\n",
		ruler(70), *mode, modeLabel, *configFile, *concurrency, ruler(70))

	specs, err := collectIndexSpecs(ctx, srcClient, cfg.Databases)
	if err != nil {
		fatalf("ERROR reading source indexes: %v\n", err)
	}
	outf("  Read %d indexes from source (excluding _id_)\n", len(specs))

	switch *mode {
	case "create":
		runCreate(ctx, dstClient, specs, *concurrency, *dryRun)
	case "rectify":
		runRectify(ctx, dstClient, specs, *concurrency, *dryRun)
	case "verify":
		runVerify(ctx, dstClient, specs, cfg.Databases)
	}
}

func mustConnect(ctx context.Context, uri, label string) *mongo.Client {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		fatalf("ERROR: cannot connect to %s: %v\n", label, err)
	}
	return client
}

// ── Collect indexes from source ───────────────────────────────────────────────

func collectIndexSpecs(ctx context.Context, src *mongo.Client, databases []string) ([]indexSpec, error) {
	if len(databases) == 0 {
		var res bson.M
		if err := src.Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}}).Decode(&res); err != nil {
			return nil, fmt.Errorf("listDatabases: %w", err)
		}
		for _, d := range res["databases"].(bson.A) {
			if dm, ok := d.(bson.M); ok {
				if name, _ := dm["name"].(string); name != "" && !systemDBs[name] {
					databases = append(databases, name)
				}
			}
		}
	}

	var specs []indexSpec
	for _, dbName := range databases {
		database := src.Database(dbName)
		// Note: do not filter listCollections by {type:"collection"} — Amazon
		// DocumentDB rejects that filter ("Field 'type' is not currently
		// supported"). List all collections and skip views/system client-side.
		collSpecs, err := database.ListCollectionSpecifications(ctx, bson.D{})
		if err != nil {
			return nil, fmt.Errorf("listCollections on %s: %w", dbName, err)
		}

		for _, collSpec := range collSpecs {
			collName := collSpec.Name
			if collSpec.Type == "view" || strings.HasPrefix(collName, "system.") {
				continue
			}
			cursor, err := database.Collection(collName).Indexes().List(ctx)
			if err != nil {
				return nil, fmt.Errorf("listIndexes on %s.%s: %w", dbName, collName, err)
			}
			var rawDocs []bson.D
			if err := cursor.All(ctx, &rawDocs); err != nil {
				return nil, fmt.Errorf("reading indexes for %s.%s: %w", dbName, collName, err)
			}
			for _, doc := range rawDocs {
				spec, skip := parseIndexDoc(dbName, collName, doc)
				if !skip {
					specs = append(specs, spec)
				}
			}
		}
	}
	return specs, nil
}

// parseIndexDoc builds an indexSpec from a raw listIndexes document.
// Returns skip=true for the _id_ index.
func parseIndexDoc(dbName, collName string, doc bson.D) (indexSpec, bool) {
	spec := indexSpec{DB: dbName, Collection: collName}
	spec.OriginalTTL = -1

	for _, elem := range doc {
		switch elem.Key {
		case "name":
			spec.Name, _ = elem.Value.(string)
		case "key":
			spec.Keys, _ = elem.Value.(bson.D)
		case "unique":
			spec.IsUnique, _ = elem.Value.(bool)
		case "expireAfterSeconds":
			spec.HasTTL = true
			spec.OriginalTTL = toInt32(elem.Value)
		case "v", "ns", "background":
			// strip: not needed on destination
		default:
			spec.ExtraOpts = append(spec.ExtraOpts, elem)
		}
	}

	return spec, spec.Name == "_id_"
}

func toInt32(v interface{}) int32 {
	switch n := v.(type) {
	case int32:
		return n
	case int64:
		return int32(n)
	case float64:
		return int32(n)
	}
	return 0
}

// ── Create mode ───────────────────────────────────────────────────────────────

func runCreate(ctx context.Context, dst *mongo.Client, specs []indexSpec, concurrency int, dryRun bool) {
	outf("\n  Creating %d indexes  (TTL indexes → MAX_INT, unique indexes → non-unique)\n\n", len(specs))

	var completed int64
	monDone := startMonitor(ctx, dst, &completed, int64(len(specs)))

	results := runPool(specs, concurrency, func(spec indexSpec) error {
		err := createIndex(ctx, dst, spec, dryRun)
		atomic.AddInt64(&completed, 1)
		return err
	})

	close(monDone)
	printSummary("create", results, int64(len(specs)))

	if *&dryRun {
		outln("\n  Re-run with --dry-run=false to apply changes.")
	} else {
		outln("\n  After cutover, re-run with --mode=rectify to restore TTL values and enforce unique constraints.")
	}
}

func createIndex(ctx context.Context, dst *mongo.Client, spec indexSpec, dryRun bool) error {
	notes := buildNotes(spec)
	if dryRun {
		outf("  [DRY RUN] createIndex  %s.%s.%s  keys=%s%s\n",
			spec.DB, spec.Collection, spec.Name, fmtKeys(spec.Keys), notes)
		return nil
	}

	// Build the index doc: key + name + extra options, then TTL/unique overrides.
	idxDoc := bson.D{
		{Key: "key", Value: spec.Keys},
		{Key: "name", Value: spec.Name},
	}
	idxDoc = append(idxDoc, spec.ExtraOpts...)
	if spec.HasTTL {
		idxDoc = append(idxDoc, bson.E{Key: "expireAfterSeconds", Value: maxTTLSeconds})
	}
	if spec.IsUnique {
		idxDoc = append(idxDoc, bson.E{Key: "unique", Value: false})
	}

	cmd := bson.D{
		{Key: "createIndexes", Value: spec.Collection},
		{Key: "indexes", Value: bson.A{idxDoc}},
	}
	if err := dst.Database(spec.DB).RunCommand(ctx, cmd).Err(); err != nil {
		return fmt.Errorf("createIndex %s.%s.%s: %w", spec.DB, spec.Collection, spec.Name, err)
	}
	outf("  OK  createIndex  %s.%s.%s%s\n", spec.DB, spec.Collection, spec.Name, notes)
	return nil
}

// ── Rectify mode ──────────────────────────────────────────────────────────────

func runRectify(ctx context.Context, dst *mongo.Client, specs []indexSpec, concurrency int, dryRun bool) {
	var phase1, phase2 []indexSpec
	for _, s := range specs {
		if s.IsUnique || s.HasTTL {
			phase1 = append(phase1, s)
		}
		if s.IsUnique {
			phase2 = append(phase2, s)
		}
	}

	if len(phase1) == 0 {
		outln("\n  Nothing to rectify — no TTL or unique indexes found on source.")
		return
	}

	// ── Phase 1: prepareUnique + TTL restores (all parallel) ──────────────
	outf("\n  Phase 1: %d operations  (prepareUnique + TTL restores, parallel)\n\n", len(phase1))

	var comp1 int64
	mon1 := startMonitor(ctx, dst, &comp1, int64(len(phase1)))

	res1 := runPool(phase1, concurrency, func(spec indexSpec) error {
		err := rectifyPhase1(ctx, dst, spec, dryRun)
		atomic.AddInt64(&comp1, 1)
		return err
	})
	close(mon1)
	printSummary("phase1", res1, int64(len(phase1)))

	if len(phase2) == 0 {
		return
	}

	// ── Phase 2: unique:true (parallel, strictly after phase 1) ───────────
	outf("\n  Phase 2: %d operations  (unique:true, parallel)\n\n", len(phase2))

	var comp2 int64
	mon2 := startMonitor(ctx, dst, &comp2, int64(len(phase2)))

	res2 := runPool(phase2, concurrency, func(spec indexSpec) error {
		err := rectifyPhase2(ctx, dst, spec, dryRun)
		atomic.AddInt64(&comp2, 1)
		return err
	})
	close(mon2)
	printSummary("phase2", res2, int64(len(phase2)))
}

func rectifyPhase1(ctx context.Context, dst *mongo.Client, spec indexSpec, dryRun bool) error {
	var errs []string

	if spec.IsUnique {
		if dryRun {
			outf("  [DRY RUN] prepareUnique  %s.%s.%s\n", spec.DB, spec.Collection, spec.Name)
		} else {
			cmd := bson.D{
				{Key: "collMod", Value: spec.Collection},
				{Key: "index", Value: bson.D{
					{Key: "name", Value: spec.Name},
					{Key: "prepareUnique", Value: true},
				}},
			}
			if err := dst.Database(spec.DB).RunCommand(ctx, cmd).Err(); err != nil {
				errs = append(errs, fmt.Sprintf("prepareUnique: %v", err))
			} else {
				outf("  OK  prepareUnique  %s.%s.%s\n", spec.DB, spec.Collection, spec.Name)
			}
		}
	}

	if spec.HasTTL {
		if dryRun {
			outf("  [DRY RUN] restoreTTL    %s.%s.%s  → %ds\n",
				spec.DB, spec.Collection, spec.Name, spec.OriginalTTL)
		} else {
			cmd := bson.D{
				{Key: "collMod", Value: spec.Collection},
				{Key: "index", Value: bson.D{
					{Key: "name", Value: spec.Name},
					{Key: "expireAfterSeconds", Value: spec.OriginalTTL},
				}},
			}
			if err := dst.Database(spec.DB).RunCommand(ctx, cmd).Err(); err != nil {
				errs = append(errs, fmt.Sprintf("restoreTTL: %v", err))
			} else {
				outf("  OK  restoreTTL    %s.%s.%s  → %ds\n",
					spec.DB, spec.Collection, spec.Name, spec.OriginalTTL)
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s.%s.%s: %s", spec.DB, spec.Collection, spec.Name, strings.Join(errs, "; "))
	}
	return nil
}

func rectifyPhase2(ctx context.Context, dst *mongo.Client, spec indexSpec, dryRun bool) error {
	if dryRun {
		outf("  [DRY RUN] unique:true   %s.%s.%s\n", spec.DB, spec.Collection, spec.Name)
		return nil
	}
	cmd := bson.D{
		{Key: "collMod", Value: spec.Collection},
		{Key: "index", Value: bson.D{
			{Key: "name", Value: spec.Name},
			{Key: "unique", Value: true},
		}},
	}
	if err := dst.Database(spec.DB).RunCommand(ctx, cmd).Err(); err != nil {
		return fmt.Errorf("%s.%s.%s unique:true: %w", spec.DB, spec.Collection, spec.Name, err)
	}
	outf("  OK  unique:true   %s.%s.%s\n", spec.DB, spec.Collection, spec.Name)
	return nil
}

// ── Worker pool ───────────────────────────────────────────────────────────────

func runPool(specs []indexSpec, concurrency int, fn func(indexSpec) error) []opResult {
	jobs := make(chan indexSpec, len(specs))
	out := make(chan opResult, len(specs))

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for spec := range jobs {
				out <- opResult{spec: spec, err: fn(spec)}
			}
		}()
	}

	for _, s := range specs {
		jobs <- s
	}
	close(jobs)

	go func() { wg.Wait(); close(out) }()

	var results []opResult
	for r := range out {
		results = append(results, r)
	}
	return results
}

// ── Progress monitor ──────────────────────────────────────────────────────────

// startMonitor starts a background goroutine that prints progress every 5 s.
// Close the returned channel to stop it.
func startMonitor(ctx context.Context, dst *mongo.Client, completed *int64, total int64) chan struct{} {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				c := atomic.LoadInt64(completed)
				outf("\n  [PROGRESS] %d / %d completed\n", c, total)
				printActiveIndexBuilds(ctx, dst)
			}
		}
	}()
	return done
}

// printActiveIndexBuilds polls currentOp on the destination and prints any
// in-progress index builds. Silently skips if the command is unavailable
// (e.g. Atlas shared tiers M0/M2/M5).
func printActiveIndexBuilds(ctx context.Context, dst *mongo.Client) {
	tctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	var res bson.M
	if err := dst.Database("admin").RunCommand(tctx, bson.D{
		{Key: "currentOp", Value: 1},
		{Key: "$all", Value: true},
	}).Decode(&res); err != nil {
		return
	}

	inprog, _ := res["inprog"].(bson.A)
	found := 0
	for _, raw := range inprog {
		m, ok := raw.(bson.M)
		if !ok {
			continue
		}
		msg, _ := m["msg"].(string)
		if !strings.Contains(msg, "Index Build") {
			continue
		}
		ns, _ := m["ns"].(string)
		outf("  [MONITOR]  %-45s  %s\n", ns, msg)
		found++
	}
	if found == 0 {
		outln("  [MONITOR]  No active index builds in currentOp")
	}
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func printSummary(phase string, results []opResult, total int64) {
	var failed []opResult
	for _, r := range results {
		if r.err != nil {
			failed = append(failed, r)
		}
	}
	outf("\n  ── %s summary  OK: %d  |  Errors: %d  |  Total: %d ──\n",
		phase, total-int64(len(failed)), len(failed), total)
	for _, r := range failed {
		errf("  ERROR  %s.%s.%s: %v\n",
			r.spec.DB, r.spec.Collection, r.spec.Name, r.err)
	}
}

func buildNotes(spec indexSpec) string {
	var parts []string
	if spec.HasTTL {
		parts = append(parts, fmt.Sprintf("TTL %d→MAX_INT", spec.OriginalTTL))
	}
	if spec.IsUnique {
		parts = append(parts, "unique→false")
	}
	if len(parts) == 0 {
		return ""
	}
	return "  [" + strings.Join(parts, ", ") + "]"
}

func fmtKeys(keys bson.D) string {
	parts := make([]string, 0, len(keys))
	for _, e := range keys {
		parts = append(parts, fmt.Sprintf("%s:%v", e.Key, e.Value))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func ruler(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '='
	}
	return string(b)
}

func fatalf(format string, args ...interface{}) {
	errf(format, args...)
	os.Exit(1)
}

// ── Verify mode ───────────────────────────────────────────────────────────────

type verifyStatus int

const (
	vOK verifyStatus = iota
	vPending
	vMismatch
	vMissing
	vExtra
)

func (s verifyStatus) label() string {
	switch s {
	case vOK:
		return "OK"
	case vPending:
		return "PENDING"
	case vMismatch:
		return "MISMATCH"
	case vMissing:
		return "MISSING"
	case vExtra:
		return "EXTRA"
	}
	return "?"
}

// runVerify compares the source index specs against the destination and reports
// per-index status. It is read-only. Exit codes:
//
//	0  every index matches (including unique + TTL)
//	4  all present with matching keys/options, but some are still in the
//	   migration-neutralized state (unique:false / TTL:MAX_INT) — run
//	   --mode=rectify, then re-verify
//	3  hard discrepancies found (missing, extra, or mismatched keys/options/etc.)
func runVerify(ctx context.Context, dst *mongo.Client, srcSpecs []indexSpec, databases []string) {
	dstSpecs, err := collectIndexSpecs(ctx, dst, databases)
	if err != nil {
		fatalf("ERROR reading destination indexes: %v\n", err)
	}

	dstByKey := make(map[string]indexSpec, len(dstSpecs))
	for _, s := range dstSpecs {
		dstByKey[specKey(s)] = s
	}
	srcByKey := make(map[string]indexSpec, len(srcSpecs))
	for _, s := range srcSpecs {
		srcByKey[specKey(s)] = s
	}

	outf("\n  Comparing %d source indexes against %d destination indexes\n\n",
		len(srcSpecs), len(dstSpecs))
	outf("  %-8s  %-55s  %s\n", "STATUS", "NAMESPACE.INDEX", "DETAIL")
	outf("  %s\n", ruler(100))

	var counts [5]int

	sort.Slice(srcSpecs, func(i, j int) bool { return specKey(srcSpecs[i]) < specKey(srcSpecs[j]) })
	for _, src := range srcSpecs {
		ns := fmt.Sprintf("%s.%s.%s", src.DB, src.Collection, src.Name)
		dsp, ok := dstByKey[specKey(src)]
		if !ok {
			counts[vMissing]++
			printVerifyRow(vMissing, ns, "not present on destination")
			continue
		}
		status, detail := compareSpec(src, dsp)
		counts[status]++
		printVerifyRow(status, ns, detail)
	}

	sort.Slice(dstSpecs, func(i, j int) bool { return specKey(dstSpecs[i]) < specKey(dstSpecs[j]) })
	for _, d := range dstSpecs {
		if _, ok := srcByKey[specKey(d)]; !ok {
			ns := fmt.Sprintf("%s.%s.%s", d.DB, d.Collection, d.Name)
			counts[vExtra]++
			printVerifyRow(vExtra, ns, "present on destination but not on source")
		}
	}

	outf("\n  ── verify summary ──\n")
	outf("  OK: %d  |  PENDING: %d  |  MISMATCH: %d  |  MISSING: %d  |  EXTRA: %d\n",
		counts[vOK], counts[vPending], counts[vMismatch], counts[vMissing], counts[vExtra])

	hard := counts[vMismatch] + counts[vMissing] + counts[vExtra]
	switch {
	case hard > 0:
		outln("\n  RESULT: FAIL — discrepancies found (see MISMATCH/MISSING/EXTRA above).")
		os.Exit(3)
	case counts[vPending] > 0:
		outln("\n  RESULT: INCOMPLETE — indexes created but not yet rectified " +
			"(unique:false / TTL:MAX_INT). Run --mode=rectify, then re-verify.")
		os.Exit(4)
	default:
		outln("\n  RESULT: PASS — destination indexes match source (keys, options, unique, TTL).")
	}
}

func printVerifyRow(status verifyStatus, ns, detail string) {
	if detail != "" {
		outf("  %-8s  %-55s  %s\n", status.label(), ns, detail)
	} else {
		outf("  %-8s  %s\n", status.label(), ns)
	}
}

// compareSpec compares a matched source/destination index pair. It treats the
// intentional migration-neutralized state (unique:false, TTL:MAX_INT) as
// PENDING rather than a hard mismatch, so verify is meaningful both before and
// after --mode=rectify.
func compareSpec(src, dst indexSpec) (verifyStatus, string) {
	var issues []string

	if canon(src.Keys) != canon(dst.Keys) {
		issues = append(issues, fmt.Sprintf("keys src=%s dst=%s", fmtKeys(src.Keys), fmtKeys(dst.Keys)))
	}
	if canon(src.ExtraOpts) != canon(dst.ExtraOpts) {
		issues = append(issues, "options differ")
	}
	if len(issues) > 0 {
		return vMismatch, strings.Join(issues, "; ")
	}

	uniquePending := src.IsUnique && !dst.IsUnique
	uniqueBad := (src.IsUnique != dst.IsUnique) && !uniquePending

	ttlPending := src.HasTTL && dst.HasTTL && dst.OriginalTTL == maxTTLSeconds && src.OriginalTTL != maxTTLSeconds
	ttlBad := false
	switch {
	case src.HasTTL != dst.HasTTL:
		ttlBad = true
	case src.HasTTL && dst.HasTTL && src.OriginalTTL != dst.OriginalTTL && !ttlPending:
		ttlBad = true
	}

	if uniqueBad || ttlBad {
		var d []string
		if uniqueBad {
			d = append(d, fmt.Sprintf("unique src=%t dst=%t", src.IsUnique, dst.IsUnique))
		}
		if ttlBad {
			d = append(d, fmt.Sprintf("TTL src=%s dst=%s", ttlStr(src), ttlStr(dst)))
		}
		return vMismatch, strings.Join(d, "; ")
	}

	if uniquePending || ttlPending {
		var d []string
		if uniquePending {
			d = append(d, "unique not yet enforced (dst=false)")
		}
		if ttlPending {
			d = append(d, fmt.Sprintf("TTL still MAX_INT (target %ds)", src.OriginalTTL))
		}
		return vPending, strings.Join(d, "; ")
	}

	return vOK, ""
}

func ttlStr(s indexSpec) string {
	if !s.HasTTL {
		return "none"
	}
	return fmt.Sprintf("%ds", s.OriginalTTL)
}

func specKey(s indexSpec) string {
	return s.DB + "\x00" + s.Collection + "\x00" + s.Name
}

// canon produces a stable, order-independent string for a BSON value so two
// logically-equal option documents compare equal regardless of field order.
func canon(v interface{}) string {
	switch t := v.(type) {
	case bson.D:
		sorted := make([]bson.E, len(t))
		copy(sorted, t)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i].Key < sorted[j].Key })
		var sb strings.Builder
		sb.WriteByte('{')
		for i, e := range sorted {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(e.Key)
			sb.WriteByte(':')
			sb.WriteString(canon(e.Value))
		}
		sb.WriteByte('}')
		return sb.String()
	case bson.M:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var sb strings.Builder
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(k)
			sb.WriteByte(':')
			sb.WriteString(canon(t[k]))
		}
		sb.WriteByte('}')
		return sb.String()
	case bson.A:
		var sb strings.Builder
		sb.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(canon(e))
		}
		sb.WriteByte(']')
		return sb.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}
