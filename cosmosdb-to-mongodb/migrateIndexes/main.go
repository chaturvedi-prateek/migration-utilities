// migrate-indexes copies indexes from an Azure Cosmos DB for MongoDB (RU)
// source to a MongoDB Atlas destination. Fork of docdb-to-mongodb/indexes/
// migrateIndexes with four Cosmos-specific changes:
//
//   - TTL indexes keyed on Cosmos's system field _ts have no Atlas equivalent.
//     They are SKIPPED unless config.ttl_field_map names a real date field for
//     that namespace, in which case the TTL is recreated on that field.
//   - ensure_collections: dsync does not create empty collections on the
//     destination; with this on, every source collection is created on Atlas
//     before its indexes (createIndexes on a missing collection would create
//     it implicitly anyway, but collections with no indexes would be lost).
//   - Source reads honour Cosmos RU throttling (error 16500 / RetryAfterMs).
//   - verify reports SKIPPED for unmapped _ts TTL indexes instead of MISSING.
//
// Three modes:
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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
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

	// EnsureCollections: in --mode=create, create every source collection on
	// the destination first (dsync skips empty collections).
	EnsureCollections bool `json:"ensure_collections"`

	// TTLFieldMap: "db.collection" -> date field to carry the TTL that Cosmos
	// keys on _ts. Namespaces not listed keep their _ts TTL index SKIPPED.
	TTLFieldMap map[string]string `json:"ttl_field_map"`
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

	// Cosmos: TTL index keyed on the system field _ts. If SkipReason is set the
	// index is not created and verify reports SKIPPED; if TTLRemappedFrom is
	// set, Keys already point at the mapped date field.
	TTLOnTS         bool
	TTLRemappedFrom string
	SkipReason      string
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

	srcClient := mustConnect(ctx, cfg.SourceURI, "source", *concurrency)
	defer srcClient.Disconnect(ctx)

	dstClient := mustConnect(ctx, cfg.DestinationURI, "destination", *concurrency)
	defer dstClient.Disconnect(ctx)

	modeLabel := "LIVE RUN — changes will be applied"
	if *dryRun {
		modeLabel = "DRY RUN — no changes will be written"
	}
	outf("\n%s\n  migrate-indexes  |  mode=%-8s  |  %s\n  Config: %s  |  Concurrency: %d\n%s\n",
		ruler(70), *mode, modeLabel, *configFile, *concurrency, ruler(70))

	if !cosmosRUHostRe.MatchString(hostOf(cfg.SourceURI)) {
		errf("  WARNING: source host %q is not *.mongo.cosmos.azure.com — this tool applies Cosmos DB rules (TTL on _ts, RU throttling). For DocumentDB use docdb-to-mongodb/indexes/migrateIndexes.\n", hostOf(cfg.SourceURI))
	}

	specs, colls, err := collectIndexSpecs(ctx, srcClient, cfg.Databases)
	if err != nil {
		fatalf("ERROR reading source indexes: %v\n", err)
	}
	specs = translateForCosmos(specs, cfg.TTLFieldMap)
	outf("  Read %d indexes from %d collections on source (excluding _id_)\n", len(specs), len(colls))

	switch *mode {
	case "create":
		if cfg.EnsureCollections {
			ensureCollections(ctx, dstClient, colls, *dryRun)
		}
		runCreate(ctx, dstClient, specs, *concurrency, *dryRun)
	case "rectify":
		runRectify(ctx, dstClient, specs, *concurrency, *dryRun)
	case "verify":
		runVerify(ctx, dstClient, specs, cfg.Databases)
	}
}

// ── Cosmos helpers ────────────────────────────────────────────────────────────

var (
	cosmosRUHostRe = regexp.MustCompile(`(?i)\.mongo\.cosmos\.azure\.com$`)
	hostFromURIRe  = regexp.MustCompile(`@([^/?,:]+)`)
	retryAfterRe   = regexp.MustCompile(`RetryAfterMs=(\d+)`)
)

func hostOf(uri string) string {
	if m := hostFromURIRe.FindStringSubmatch(uri); len(m) == 2 {
		return m[1]
	}
	return ""
}

// isThrottled recognises Cosmos RU throttling (HTTP 429 → Mongo error 16500).
func isThrottled(err error) (time.Duration, bool) {
	if err == nil {
		return 0, false
	}
	var ce mongo.CommandError
	code := int32(0)
	if errors.As(err, &ce) {
		code = ce.Code
	}
	msg := err.Error()
	if code != 16500 && !strings.Contains(msg, "16500") && !strings.Contains(msg, "RequestRateTooLarge") &&
		!strings.Contains(msg, "TooManyRequests") {
		return 0, false
	}
	d := 500 * time.Millisecond
	if m := retryAfterRe.FindStringSubmatch(msg); len(m) == 2 {
		if ms, e := strconv.Atoi(m[1]); e == nil && ms > 0 {
			d = time.Duration(ms) * time.Millisecond
		}
	}
	if d < 100*time.Millisecond {
		d = 100 * time.Millisecond
	}
	return d, true
}

// srcRetry re-runs a source-side read while Cosmos throttles it.
func srcRetry(ctx context.Context, label string, fn func() error) error {
	const maxAttempts = 12
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()
		wait, throttled := isThrottled(err)
		if !throttled {
			return err
		}
		if wait > 5*time.Second {
			wait = 5 * time.Second
		}
		outf("  [RETRY %d/%d] %s throttled (16500), retrying in %s\n", attempt, maxAttempts, label, wait)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("%s: still throttled after %d attempts: %w", label, maxAttempts, err)
}

// translateForCosmos applies the one rule the destination cannot infer: a TTL
// index on _ts is either remapped to a real date field or skipped.
func translateForCosmos(specs []indexSpec, ttlMap map[string]string) []indexSpec {
	out := make([]indexSpec, 0, len(specs))
	for _, s := range specs {
		if !s.HasTTL || len(s.Keys) != 1 || s.Keys[0].Key != "_ts" {
			out = append(out, s)
			continue
		}
		s.TTLOnTS = true
		ns := s.DB + "." + s.Collection
		field, ok := ttlMap[ns]
		if !ok || field == "" || field == "_ts" {
			s.SkipReason = "TTL on Cosmos system field _ts has no Atlas equivalent (add ttl_field_map[\"" + ns + "\"] to recreate it on a date field)"
			out = append(out, s)
			continue
		}
		s.TTLRemappedFrom = "_ts"
		s.Keys = bson.D{{Key: field, Value: s.Keys[0].Value}}
		s.Name = "ttl_" + strings.ReplaceAll(field, ".", "_")
		out = append(out, s)
	}
	return out
}

// ensureCollections creates every source collection on the destination.
// The Go driver's CreateCollection swallows NamespaceExists, so existence is
// checked explicitly to keep the summary truthful.
func ensureCollections(ctx context.Context, dst *mongo.Client, colls []nsRef, dryRun bool) {
	outf("\n  Ensuring %d collections exist on destination (dsync does not create empty ones)\n\n", len(colls))
	existing := map[string]bool{}
	for _, c := range colls {
		if _, seen := existing[c.DB+"\x00"]; seen {
			continue
		}
		existing[c.DB+"\x00"] = true
		names, err := dst.Database(c.DB).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			errf("  ERROR  listCollections %s on destination: %v\n", c.DB, err)
			continue
		}
		for _, n := range names {
			existing[c.DB+"\x00"+n] = true
		}
	}
	created, existed, failed := 0, 0, 0
	for _, c := range colls {
		if existing[c.DB+"\x00"+c.Collection] {
			existed++
			continue
		}
		if dryRun {
			outf("  [DRY RUN] createCollection  %s.%s\n", c.DB, c.Collection)
			continue
		}
		if err := dst.Database(c.DB).CreateCollection(ctx, c.Collection); err != nil {
			failed++
			errf("  ERROR  createCollection  %s.%s: %v\n", c.DB, c.Collection, err)
			continue
		}
		created++
		outf("  OK  createCollection  %s.%s\n", c.DB, c.Collection)
	}
	if dryRun {
		outf("\n  ── ensure summary  would create: %d  |  already exist: %d ──\n", len(colls)-existed, existed)
	} else {
		outf("\n  ── ensure summary  created: %d  |  already existed: %d  |  errors: %d ──\n", created, existed, failed)
	}
	if created > 0 || (dryRun && len(colls)-existed > 0) {
		outln("  Sharded Atlas targets: shard the collection now (sh.shardCollection) — before indexes and before dsync writes into it.")
	}
}

type nsRef struct {
	DB         string
	Collection string
}

func mustConnect(ctx context.Context, uri, label string, maxPool int) *mongo.Client {
	if maxPool < 4 {
		maxPool = 4
	}
	opts := options.Client().
		ApplyURI(uri).
		SetServerSelectionTimeout(5 * time.Minute). // wait out a busy primary during index builds
		SetConnectTimeout(60 * time.Second).
		SetMaxPoolSize(uint64(maxPool) + 5). // headroom above worker concurrency + monitor
		SetMaxConnecting(uint64(maxPool)).   // establish all workers' connections at once (default is 2)
		SetRetryWrites(false)                // Cosmos DB (RU) does not support retryable writes; harmless on Atlas

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

// isTransient reports whether an error is worth retrying — pool checkout, server
// selection, or network timeouts that can occur while a busy primary builds
// indexes.
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

// runCmdRetry runs a destination command, retrying transient failures with
// linear backoff. createIndexes / collMod are idempotent, so retrying is safe.
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

// ── Collect indexes from source ───────────────────────────────────────────────

func collectIndexSpecs(ctx context.Context, src *mongo.Client, databases []string) ([]indexSpec, []nsRef, error) {
	if len(databases) == 0 {
		var res bson.M
		if err := srcRetry(ctx, "listDatabases", func() error {
			return src.Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}}).Decode(&res)
		}); err != nil {
			return nil, nil, fmt.Errorf("listDatabases: %w", err)
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
	var colls []nsRef
	sort.Strings(databases)
	for _, dbName := range databases {
		database := src.Database(dbName)
		var collSpecs []*mongo.CollectionSpecification
		if err := srcRetry(ctx, "listCollections "+dbName, func() error {
			var e error
			collSpecs, e = database.ListCollectionSpecifications(ctx, bson.D{})
			return e
		}); err != nil {
			return nil, nil, fmt.Errorf("listCollections on %s: %w", dbName, err)
		}
		sort.Slice(collSpecs, func(i, j int) bool { return collSpecs[i].Name < collSpecs[j].Name })

		for _, collSpec := range collSpecs {
			collName := collSpec.Name
			if collSpec.Type == "view" || strings.HasPrefix(collName, "system.") {
				continue
			}
			colls = append(colls, nsRef{DB: dbName, Collection: collName})
			var rawDocs []bson.D
			if err := srcRetry(ctx, "listIndexes "+dbName+"."+collName, func() error {
				cursor, err := database.Collection(collName).Indexes().List(ctx)
				if err != nil {
					return err
				}
				rawDocs = nil
				return cursor.All(ctx, &rawDocs)
			}); err != nil {
				return nil, nil, fmt.Errorf("listIndexes on %s.%s: %w", dbName, collName, err)
			}
			for _, doc := range rawDocs {
				spec, skip := parseIndexDoc(dbName, collName, doc)
				if !skip {
					specs = append(specs, spec)
				}
			}
		}
	}
	return specs, colls, nil
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

	if dryRun {
		outln("\n  Re-run with --dry-run=false to apply changes.")
	} else {
		outln("\n  After cutover, re-run with --mode=rectify to restore TTL values and enforce unique constraints.")
	}
}

func createIndex(ctx context.Context, dst *mongo.Client, spec indexSpec, dryRun bool) error {
	notes := buildNotes(spec)
	if spec.SkipReason != "" {
		outf("  SKIP  %s.%s.%s  %s\n", spec.DB, spec.Collection, spec.Name, spec.SkipReason)
		return nil
	}
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
		if s.SkipReason != "" {
			continue
		}
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
	if spec.TTLRemappedFrom != "" {
		parts = append(parts, "TTL field _ts→"+spec.Keys[0].Key)
	}
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
	vSkipped
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
	case vSkipped:
		return "SKIPPED"
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
	dstSpecs, _, err := collectIndexSpecs(ctx, dst, databases)
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

	var counts [6]int

	sort.Slice(srcSpecs, func(i, j int) bool { return specKey(srcSpecs[i]) < specKey(srcSpecs[j]) })
	for _, src := range srcSpecs {
		ns := fmt.Sprintf("%s.%s.%s", src.DB, src.Collection, src.Name)
		if src.SkipReason != "" {
			counts[vSkipped]++
			printVerifyRow(vSkipped, ns, "TTL on _ts — not portable, intentionally absent on destination")
			continue
		}
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
	outf("  OK: %d  |  PENDING: %d  |  MISMATCH: %d  |  MISSING: %d  |  EXTRA: %d  |  SKIPPED: %d\n",
		counts[vOK], counts[vPending], counts[vMismatch], counts[vMissing], counts[vExtra], counts[vSkipped])

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
		if counts[vSkipped] > 0 {
			outf("\n  RESULT: PASS — destination indexes match source; %d _ts TTL index(es) intentionally skipped.\n", counts[vSkipped])
		} else {
			outln("\n  RESULT: PASS — destination indexes match source (keys, options, unique, TTL).")
		}
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
