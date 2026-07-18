// migrate-indexes (multi-source) copies indexes from MANY self-managed MongoDB
// sources to a single Atlas destination, merging same-namespace collections.
//
// It is the multi-source sibling of docdb-to-mongodb/indexes/migrateIndexes.
// The migration merges 10 on-prem replica sets into one Atlas cluster, so
// same-name collections combine and their indexes combine too. Two sources may
// define the same db.collection index name with different keys/options — a real
// conflict that MongoDB would reject or that would silently diverge. This tool
// reads every source, DEDUPES identical index specs, and DETECTS conflicts.
//
// Modes:
//
//	--mode=create  (pre-cutover)
//	  Reads every non-_id_ index from ALL sources, merges them, and creates each
//	  distinct index on the destination with:
//	    • TTL indexes   → expireAfterSeconds = MAX_INT  (deactivated during migration)
//	    • Unique indexes → unique: false                (no uniqueness violations during sync)
//	  Conflicting indexes are REPORTED and SKIPPED; the run exits non-zero (3) so
//	  you resolve each conflict before cutover. Non-conflicting indexes are still
//	  created. All createIndex calls run in parallel across a worker pool.
//
//	--mode=rectify  (post-cutover)
//	  Phase 1 (parallel): prepareUnique on formerly-unique indexes + restore original TTL values
//	  Phase 2 (parallel): collMod unique:true on all formerly-unique indexes
//	  Phases are strictly sequential; operations within each phase are parallel.
//
//	--mode=verify  (any time)
//	  Read-only. Compares the merged source index set against the destination.
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

type sourceEntry struct {
	Label string `json:"label"` // human-friendly source name, used in reports
	URI   string `json:"uri"`
}

type config struct {
	Sources        []sourceEntry `json:"sources"`         // one entry per self-managed source cluster
	DestinationURI string        `json:"destination_uri"` // the single Atlas target
	Databases      []string      `json:"databases"`       // empty = all non-system databases
	LogFile        string        `json:"log_file"`        // optional: tee all output to this file
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
	if len(cfg.Sources) == 0 {
		return nil, fmt.Errorf("config: at least one entry in 'sources' is required")
	}
	seen := map[string]bool{}
	for i := range cfg.Sources {
		if cfg.Sources[i].URI == "" {
			return nil, fmt.Errorf("config: sources[%d].uri is required", i)
		}
		if cfg.Sources[i].Label == "" {
			cfg.Sources[i].Label = fmt.Sprintf("source%d", i+1)
		}
		if seen[cfg.Sources[i].Label] {
			return nil, fmt.Errorf("config: duplicate source label %q", cfg.Sources[i].Label)
		}
		seen[cfg.Sources[i].Label] = true
	}
	if cfg.DestinationURI == "" {
		return nil, fmt.Errorf("config: destination_uri is required")
	}
	return &cfg, nil
}

// ── Index spec ────────────────────────────────────────────────────────────────

type indexSpec struct {
	Source      string // originating source label(s); comma-joined after merge
	DB          string
	Collection  string
	Name        string
	Keys        bson.D
	ExtraOpts   bson.D // sparse, partialFilterExpression, collation, weights, etc.
	IsUnique    bool
	HasTTL      bool
	OriginalTTL int32
}

type opResult struct {
	spec indexSpec
	err  error
}

// conflict records a namespace where sources disagree on an index definition.
type conflict struct {
	NS      string   // db.collection[.name] the conflict is about
	Kind    string   // "name" (same name, different def) or "keyspec" (same keys, different name)
	Details []string // one line per differing source definition
}

// ── Main ──────────────────────────────────────────────────────────────────────

func main() {
	configFile := flag.String("config", "config.json", "Path to JSON config file")
	dryRun := flag.Bool("dry-run", true, "Print what would be done without applying changes (default: true)")
	mode := flag.String("mode", "create", "'create' = pre-cutover  |  'rectify' = post-cutover TTL+unique fix  |  'verify' = compare merged sources vs destination")
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

	modeLabel := "LIVE RUN — changes will be applied"
	if *dryRun {
		modeLabel = "DRY RUN — no changes will be written"
	}
	outf("\n%s\n  migrate-indexes (multi-source)  |  mode=%-8s  |  %s\n  Config: %s  |  Sources: %d  |  Concurrency: %d\n%s\n",
		ruler(70), *mode, modeLabel, *configFile, len(cfg.Sources), *concurrency, ruler(70))

	// ── Read indexes from every source ────────────────────────────────────
	var allSpecs []indexSpec
	for _, src := range cfg.Sources {
		client := mustConnect(ctx, src.URI, "source:"+src.Label, *concurrency)
		specs, err := collectIndexSpecs(ctx, client, cfg.Databases, src.Label)
		client.Disconnect(ctx)
		if err != nil {
			fatalf("ERROR reading indexes from source %q: %v\n", src.Label, err)
		}
		outf("  Read %d indexes from source %q (excluding _id_)\n", len(specs), src.Label)
		allSpecs = append(allSpecs, specs...)
	}

	// ── Merge across sources; detect conflicts ────────────────────────────
	merged, conflicts := mergeSpecs(allSpecs)
	outf("\n  Merged to %d distinct indexes across %d sources  |  Conflicts: %d\n",
		len(merged), len(cfg.Sources), len(conflicts))
	if len(conflicts) > 0 {
		printConflicts(conflicts)
	}

	dstClient := mustConnect(ctx, cfg.DestinationURI, "destination", *concurrency)
	defer dstClient.Disconnect(ctx)

	switch *mode {
	case "create":
		runCreate(ctx, dstClient, merged, *concurrency, *dryRun)
		if len(conflicts) > 0 {
			outf("\n  RESULT: %d conflict(s) were SKIPPED — resolve them, then re-run create.\n", len(conflicts))
			os.Exit(3)
		}
	case "rectify":
		runRectify(ctx, dstClient, merged, *concurrency, *dryRun)
	case "verify":
		runVerify(ctx, dstClient, merged, cfg.Databases, len(conflicts))
	}
}

func mustConnect(ctx context.Context, uri, label string, maxPool int) *mongo.Client {
	if maxPool < 4 {
		maxPool = 4
	}
	opts := options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Minute).
		SetConnectTimeout(60 * time.Second).
		SetMaxPoolSize(uint64(maxPool) + 5).
		SetMaxConnecting(uint64(maxPool))

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		fatalf("ERROR: cannot connect to %s: %v\n", label, err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		fatalf("ERROR: cannot reach %s: %v\n", label, err)
	}
	return client
}

func isTransient(err error) bool {
	if err == nil {
		return false
	}
	if mongo.IsTimeout(err) || mongo.IsNetworkError(err) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "connection pool") ||
		strings.Contains(msg, "server selection") ||
		strings.Contains(msg, "context deadline exceeded")
}

func runCmdRetry(ctx context.Context, db *mongo.Database, cmd bson.D) error {
	const maxAttempts = 5
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = db.RunCommand(ctx, cmd).Err(); err == nil || !isTransient(err) {
			return err
		}
		if attempt == maxAttempts {
			break
		}
		backoff := time.Duration(attempt) * 3 * time.Second
		outf("  [RETRY %d/%d] transient error, retrying in %s: %v\n", attempt, maxAttempts, backoff, err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

// ── Collect indexes from one source ─────────────────────────────────────────

func collectIndexSpecs(ctx context.Context, src *mongo.Client, databases []string, sourceLabel string) ([]indexSpec, error) {
	dbs := databases
	if len(dbs) == 0 {
		var res bson.M
		if err := src.Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}}).Decode(&res); err != nil {
			return nil, fmt.Errorf("listDatabases: %w", err)
		}
		for _, d := range res["databases"].(bson.A) {
			if dm, ok := d.(bson.M); ok {
				if name, _ := dm["name"].(string); name != "" && !systemDBs[name] {
					dbs = append(dbs, name)
				}
			}
		}
	}

	var specs []indexSpec
	for _, dbName := range dbs {
		database := src.Database(dbName)
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
					spec.Source = sourceLabel
					specs = append(specs, spec)
				}
			}
		}
	}
	return specs, nil
}

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

// ── Merge across sources ────────────────────────────────────────────────────

// signature captures everything that must match for two index definitions to be
// considered identical (order-independent for options).
func signature(s indexSpec) string {
	return fmt.Sprintf("keys=%s;opts=%s;unique=%t;ttl=%d;hasttl=%t",
		canon(s.Keys), canon(s.ExtraOpts), s.IsUnique, s.OriginalTTL, s.HasTTL)
}

// mergeSpecs dedupes identical index definitions across sources and detects two
// kinds of conflict that MongoDB will not accept on the merged destination:
//
//	"name"    — same db.coll.name, but different keys/options/unique/TTL
//	"keyspec" — same db.coll with the same key pattern under different names
//
// Returns the mergeable (non-conflicting) specs and the list of conflicts.
func mergeSpecs(all []indexSpec) ([]indexSpec, []conflict) {
	// Group by db.coll.name.
	byName := map[string][]indexSpec{}
	order := []string{}
	for _, s := range all {
		k := specKey(s)
		if _, ok := byName[k]; !ok {
			order = append(order, k)
		}
		byName[k] = append(byName[k], s)
	}

	var merged []indexSpec
	var conflicts []conflict

	// name-level conflicts + dedup.
	nameConflicted := map[string]bool{}
	for _, k := range order {
		group := byName[k]
		sigSet := map[string]indexSpec{}
		var srcs []string
		for _, s := range group {
			sigSet[signature(s)] = s
			srcs = append(srcs, s.Source)
		}
		if len(sigSet) == 1 {
			// identical everywhere → keep one, record all contributing sources.
			one := group[0]
			one.Source = strings.Join(uniqueSorted(srcs), ",")
			merged = append(merged, one)
			continue
		}
		// differing definitions under the same name → conflict.
		nameConflicted[k] = true
		c := conflict{NS: fmt.Sprintf("%s.%s.%s", group[0].DB, group[0].Collection, group[0].Name), Kind: "name"}
		for sig, s := range sigSet {
			c.Details = append(c.Details, fmt.Sprintf("[%s] %s", s.Source, sig))
		}
		sort.Strings(c.Details)
		conflicts = append(conflicts, c)
	}

	// keyspec-level conflicts: within a collection, the same key pattern must not
	// appear under two different index names. Only consider indexes we would
	// otherwise create (skip ones already flagged as name conflicts).
	byKeyPattern := map[string][]string{} // "db\x00coll\x00canon(keys)" -> index names
	for i := range merged {
		s := merged[i]
		kp := s.DB + "\x00" + s.Collection + "\x00" + canon(s.Keys)
		byKeyPattern[kp] = append(byKeyPattern[kp], s.Name)
	}
	// Collect collections/keys that collide under multiple names.
	dropByKeyConflict := map[string]bool{} // specKey of every spec involved
	kpKeys := make([]string, 0, len(byKeyPattern))
	for kp := range byKeyPattern {
		kpKeys = append(kpKeys, kp)
	}
	sort.Strings(kpKeys)
	for _, kp := range kpKeys {
		names := uniqueSorted(byKeyPattern[kp])
		if len(names) > 1 {
			parts := strings.SplitN(kp, "\x00", 3)
			c := conflict{NS: parts[0] + "." + parts[1], Kind: "keyspec"}
			c.Details = append(c.Details, fmt.Sprintf("key pattern %s used by names: %s", parts[2], strings.Join(names, ", ")))
			conflicts = append(conflicts, c)
			for _, n := range names {
				dropByKeyConflict[parts[0]+"\x00"+parts[1]+"\x00"+n] = true
			}
		}
	}

	// Filter out keyspec-conflicting specs from the mergeable set.
	if len(dropByKeyConflict) > 0 {
		filtered := merged[:0]
		for _, s := range merged {
			if dropByKeyConflict[specKey(s)] {
				continue
			}
			filtered = append(filtered, s)
		}
		merged = filtered
	}

	return merged, conflicts
}

func uniqueSorted(in []string) []string {
	m := map[string]bool{}
	for _, s := range in {
		m[s] = true
	}
	out := make([]string, 0, len(m))
	for s := range m {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

func printConflicts(conflicts []conflict) {
	errf("\n  ── CONFLICTS (skipped; resolve before cutover) ──\n")
	for _, c := range conflicts {
		errf("  [%s] %s\n", strings.ToUpper(c.Kind), c.NS)
		for _, d := range c.Details {
			errf("       %s\n", d)
		}
	}
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

	if dryRun {
		outln("\n  Re-run with --dry-run=false to apply changes.")
	} else {
		outln("\n  After cutover, re-run with --mode=rectify to restore TTL values and enforce unique constraints.")
	}
}

func createIndex(ctx context.Context, dst *mongo.Client, spec indexSpec, dryRun bool) error {
	notes := buildNotes(spec)
	if dryRun {
		outf("  [DRY RUN] createIndex  %s.%s.%s  keys=%s%s  (from %s)\n",
			spec.DB, spec.Collection, spec.Name, fmtKeys(spec.Keys), notes, spec.Source)
		return nil
	}

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
	if err := runCmdRetry(ctx, dst.Database(spec.DB), cmd); err != nil {
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
		outln("\n  Nothing to rectify — no TTL or unique indexes found on sources.")
		return
	}

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
			if err := runCmdRetry(ctx, dst.Database(spec.DB), cmd); err != nil {
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
			if err := runCmdRetry(ctx, dst.Database(spec.DB), cmd); err != nil {
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
	if err := runCmdRetry(ctx, dst.Database(spec.DB), cmd); err != nil {
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

// runVerify compares the merged source index set against the destination.
// Exit codes:
//
//	0  every merged index matches (and no source conflicts)
//	4  all present with matching keys, but some still migration-neutralized
//	3  hard discrepancies (missing/extra/mismatch) OR unresolved source conflicts
func runVerify(ctx context.Context, dst *mongo.Client, srcSpecs []indexSpec, databases []string, conflictCount int) {
	dstSpecs, err := collectIndexSpecs(ctx, dst, databases, "destination")
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

	outf("\n  Comparing %d merged source indexes against %d destination indexes\n\n",
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
			printVerifyRow(vExtra, ns, "present on destination but not on merged sources")
		}
	}

	outf("\n  ── verify summary ──\n")
	outf("  OK: %d  |  PENDING: %d  |  MISMATCH: %d  |  MISSING: %d  |  EXTRA: %d  |  SOURCE CONFLICTS: %d\n",
		counts[vOK], counts[vPending], counts[vMismatch], counts[vMissing], counts[vExtra], conflictCount)

	hard := counts[vMismatch] + counts[vMissing] + counts[vExtra] + conflictCount
	switch {
	case hard > 0:
		outln("\n  RESULT: FAIL — discrepancies and/or unresolved source conflicts (see above).")
		os.Exit(3)
	case counts[vPending] > 0:
		outln("\n  RESULT: INCOMPLETE — indexes created but not yet rectified " +
			"(unique:false / TTL:MAX_INT). Run --mode=rectify, then re-verify.")
		os.Exit(4)
	default:
		outln("\n  RESULT: PASS — destination indexes match merged sources (keys, options, unique, TTL).")
	}
}

func printVerifyRow(status verifyStatus, ns, detail string) {
	if detail != "" {
		outf("  %-8s  %-55s  %s\n", status.label(), ns, detail)
	} else {
		outf("  %-8s  %s\n", status.label(), ns)
	}
}

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
