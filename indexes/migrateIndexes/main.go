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
	"os"
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

// ── Config ────────────────────────────────────────────────────────────────────

type config struct {
	SourceURI      string   `json:"source_uri"`
	DestinationURI string   `json:"destination_uri"`
	Databases      []string `json:"databases"` // empty = all non-system databases
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
	configFile  := flag.String("config", "config.json", "Path to JSON config file")
	dryRun      := flag.Bool("dry-run", true, "Print what would be done without applying changes (default: true)")
	mode        := flag.String("mode", "create", "'create' = pre-cutover  |  'rectify' = post-cutover TTL+unique fix")
	concurrency := flag.Int("concurrency", 8, "Max simultaneous index operations on the destination")
	flag.Parse()

	if *mode != "create" && *mode != "rectify" {
		fatalf("ERROR: --mode must be 'create' or 'rectify'\n")
	}

	cfg, err := loadConfig(*configFile)
	if err != nil {
		fatalf("ERROR: %v\n", err)
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
	fmt.Printf("\n%s\n  migrate-indexes  |  mode=%-8s  |  %s\n  Config: %s  |  Concurrency: %d\n%s\n",
		ruler(70), *mode, modeLabel, *configFile, *concurrency, ruler(70))

	specs, err := collectIndexSpecs(ctx, srcClient, cfg.Databases)
	if err != nil {
		fatalf("ERROR reading source indexes: %v\n", err)
	}
	fmt.Printf("  Read %d indexes from source (excluding _id_)\n", len(specs))

	switch *mode {
	case "create":
		runCreate(ctx, dstClient, specs, *concurrency, *dryRun)
	case "rectify":
		runRectify(ctx, dstClient, specs, *concurrency, *dryRun)
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
		collNames, err := database.ListCollectionNames(ctx, bson.D{{Key: "type", Value: "collection"}})
		if err != nil {
			return nil, fmt.Errorf("listCollections on %s: %w", dbName, err)
		}

		for _, collName := range collNames {
			if strings.HasPrefix(collName, "system.") {
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
	fmt.Printf("\n  Creating %d indexes  (TTL indexes → MAX_INT, unique indexes → non-unique)\n\n", len(specs))

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
		fmt.Println("\n  Re-run with --dry-run=false to apply changes.")
	} else {
		fmt.Println("\n  After cutover, re-run with --mode=rectify to restore TTL values and enforce unique constraints.")
	}
}

func createIndex(ctx context.Context, dst *mongo.Client, spec indexSpec, dryRun bool) error {
	notes := buildNotes(spec)
	if dryRun {
		fmt.Printf("  [DRY RUN] createIndex  %s.%s.%s  keys=%s%s\n",
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
	fmt.Printf("  OK  createIndex  %s.%s.%s%s\n", spec.DB, spec.Collection, spec.Name, notes)
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
		fmt.Println("\n  Nothing to rectify — no TTL or unique indexes found on source.")
		return
	}

	// ── Phase 1: prepareUnique + TTL restores (all parallel) ──────────────
	fmt.Printf("\n  Phase 1: %d operations  (prepareUnique + TTL restores, parallel)\n\n", len(phase1))

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
	fmt.Printf("\n  Phase 2: %d operations  (unique:true, parallel)\n\n", len(phase2))

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
			fmt.Printf("  [DRY RUN] prepareUnique  %s.%s.%s\n", spec.DB, spec.Collection, spec.Name)
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
				fmt.Printf("  OK  prepareUnique  %s.%s.%s\n", spec.DB, spec.Collection, spec.Name)
			}
		}
	}

	if spec.HasTTL {
		if dryRun {
			fmt.Printf("  [DRY RUN] restoreTTL    %s.%s.%s  → %ds\n",
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
				fmt.Printf("  OK  restoreTTL    %s.%s.%s  → %ds\n",
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
		fmt.Printf("  [DRY RUN] unique:true   %s.%s.%s\n", spec.DB, spec.Collection, spec.Name)
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
	fmt.Printf("  OK  unique:true   %s.%s.%s\n", spec.DB, spec.Collection, spec.Name)
	return nil
}

// ── Worker pool ───────────────────────────────────────────────────────────────

func runPool(specs []indexSpec, concurrency int, fn func(indexSpec) error) []opResult {
	jobs := make(chan indexSpec, len(specs))
	out  := make(chan opResult, len(specs))

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
				fmt.Printf("\n  [PROGRESS] %d / %d completed\n", c, total)
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
		fmt.Printf("  [MONITOR]  %-45s  %s\n", ns, msg)
		found++
	}
	if found == 0 {
		fmt.Println("  [MONITOR]  No active index builds in currentOp")
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
	fmt.Printf("\n  ── %s summary  OK: %d  |  Errors: %d  |  Total: %d ──\n",
		phase, total-int64(len(failed)), len(failed), total)
	for _, r := range failed {
		fmt.Fprintf(os.Stderr, "  ERROR  %s.%s.%s: %v\n",
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
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}
