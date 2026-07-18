// id-overlap-scan is a pre-flight tool for the many-clusters -> one-Atlas merge.
//
// When same-name collections from several source clusters merge into ONE target
// collection, two clusters may hold the same _id. With an upsert sink that
// silently overwrites; with an insert-only sink it lands in the DLQ. Either way
// you want to know the collision surface BEFORE starting. This tool reports, per
// merged namespace (db.collection present in >= 2 clusters), whether _ids overlap.
//
// Modes:
//
//	--mode=range  (default, fast)
//	  For each cluster: count + min + max _id (2 indexed queries). Reports where
//	  the [min,max] _id intervals of two clusters intersect. Cheap first pass;
//	  an intersecting range is a *candidate* for real overlap, not proof.
//
//	--mode=exact  (thorough, streaming)
//	  Streams _id from every cluster in sorted order and does a k-way merge to
//	  find the ACTUAL duplicate _ids shared across clusters. Memory stays O(k)
//	  (one _id per cluster cursor), so it scales to large collections.
//
// Read-only. Exit codes:
//
//	0  no overlap found
//	5  overlap found (range: intersecting intervals | exact: shared _ids)
//	1  connection / runtime / config error
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var systemDBs = map[string]bool{"admin": true, "local": true, "config": true}

// ── Output ──────────────────────────────────────────────────────────────────

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
func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(errWriter, format, args...)
	os.Exit(1)
}

// ── Config ──────────────────────────────────────────────────────────────────

type sourceEntry struct {
	Label     string   `json:"label"`
	URI       string   `json:"uri"`
	Databases []string `json:"databases"`
}

type config struct {
	Sources []sourceEntry `json:"sources"`
	LogFile string        `json:"log_file"`
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
	return &cfg, nil
}

// ── Main ────────────────────────────────────────────────────────────────────

func main() {
	configFile := flag.String("config", "config.json", "Path to JSON config file")
	mode := flag.String("mode", "range", "'range' = fast min/max interval check | 'exact' = streaming k-way merge for real duplicate _ids")
	sampleCap := flag.Int("sample", 20, "Max duplicate _ids to print per namespace (exact mode)")
	concurrency := flag.Int("concurrency", 4, "Namespaces scanned in parallel")
	flag.Parse()

	if *mode != "range" && *mode != "exact" {
		fatalf("ERROR: --mode must be 'range' or 'exact'\n")
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

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Hour)
	defer cancel()

	outf("\n%s\n  id-overlap-scan  |  mode=%s  |  sources=%d\n%s\n",
		ruler(70), *mode, len(cfg.Sources), ruler(70))

	// Connect all sources and discover namespaces.
	clients := map[string]*mongo.Client{}
	for _, s := range cfg.Sources {
		clients[s.Label] = mustConnect(ctx, s.URI, "source:"+s.Label)
	}
	defer func() {
		for _, c := range clients {
			c.Disconnect(ctx)
		}
	}()

	// nsToClusters: "db.coll" -> sorted cluster labels holding it.
	nsToClusters := map[string][]string{}
	for _, s := range cfg.Sources {
		nss, err := listNamespaces(ctx, clients[s.Label], s.Databases)
		if err != nil {
			fatalf("ERROR listing namespaces on %q: %v\n", s.Label, err)
		}
		for _, ns := range nss {
			nsToClusters[ns] = append(nsToClusters[ns], s.Label)
		}
	}

	// Only namespaces present in >= 2 clusters can collide.
	var merged []string
	for ns, labels := range nsToClusters {
		if len(labels) >= 2 {
			sort.Strings(labels)
			merged = append(merged, ns)
		}
	}
	sort.Strings(merged)

	outf("  Namespaces present in >=2 clusters (merge candidates): %d\n\n", len(merged))
	if len(merged) == 0 {
		outln("  No shared namespaces — nothing can collide. RESULT: PASS (no overlap).")
		return
	}

	overlaps := scanAll(ctx, clients, nsToClusters, merged, *mode, *sampleCap, *concurrency)

	outf("\n  ── summary  |  namespaces scanned: %d  |  with overlap: %d ──\n", len(merged), overlaps)
	if overlaps > 0 {
		if *mode == "range" {
			outln("\n  RESULT: OVERLAP CANDIDATES found — intervals intersect. Re-run --mode=exact to confirm real duplicate _ids.")
		} else {
			outln("\n  RESULT: OVERLAP found — real duplicate _ids exist across clusters. Decide handling before backfill.")
		}
		os.Exit(5)
	}
	outln("\n  RESULT: PASS — no _id overlap across merged namespaces.")
}

func mustConnect(ctx context.Context, uri, label string) *mongo.Client {
	opts := options.Client().ApplyURI(uri).
		SetServerSelectionTimeout(2 * time.Minute).
		SetConnectTimeout(60 * time.Second)
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		fatalf("ERROR: cannot connect to %s: %v\n", label, err)
	}
	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := client.Ping(pctx, nil); err != nil {
		fatalf("ERROR: cannot reach %s: %v\n", label, err)
	}
	return client
}

// ── Namespace discovery ─────────────────────────────────────────────────────

func listNamespaces(ctx context.Context, c *mongo.Client, databases []string) ([]string, error) {
	dbs := databases
	if len(dbs) == 0 {
		var res bson.M
		if err := c.Database("admin").RunCommand(ctx, bson.D{{Key: "listDatabases", Value: 1}}).Decode(&res); err != nil {
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
	var out []string
	for _, dbName := range dbs {
		if systemDBs[dbName] {
			continue
		}
		specs, err := c.Database(dbName).ListCollectionSpecifications(ctx, bson.D{})
		if err != nil {
			return nil, fmt.Errorf("listCollections on %s: %w", dbName, err)
		}
		for _, sp := range specs {
			if sp.Type == "view" || strings.HasPrefix(sp.Name, "system.") {
				continue
			}
			out = append(out, dbName+"."+sp.Name)
		}
	}
	return out, nil
}

// ── Scan driver (parallel over namespaces) ──────────────────────────────────

func scanAll(ctx context.Context, clients map[string]*mongo.Client, nsToClusters map[string][]string,
	merged []string, mode string, sampleCap, concurrency int) int {

	type job struct{ ns string }
	jobs := make(chan job, len(merged))
	var mu sync.Mutex
	overlaps := 0

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				labels := nsToClusters[j.ns]
				var found bool
				var report string
				if mode == "range" {
					found, report = scanRange(ctx, clients, j.ns, labels)
				} else {
					found, report = scanExact(ctx, clients, j.ns, labels, sampleCap)
				}
				mu.Lock()
				outf("%s", report)
				if found {
					overlaps++
				}
				mu.Unlock()
			}
		}()
	}
	for _, ns := range merged {
		jobs <- job{ns: ns}
	}
	close(jobs)
	wg.Wait()
	return overlaps
}

func splitNS(ns string) (string, string) {
	i := strings.Index(ns, ".")
	return ns[:i], ns[i+1:]
}

// ── Range mode ──────────────────────────────────────────────────────────────

type rangeInfo struct {
	label    string
	count    int64
	min, max interface{}
	empty    bool
}

func scanRange(ctx context.Context, clients map[string]*mongo.Client, ns string, labels []string) (bool, string) {
	db, coll := splitNS(ns)
	var infos []rangeInfo
	for _, label := range labels {
		c := clients[label].Database(db).Collection(coll)
		var sb strings.Builder
		cnt, err := c.EstimatedDocumentCount(ctx)
		if err != nil {
			return false, fmt.Sprintf("  [ERROR] %s (%s): count: %v\n", ns, label, err)
		}
		info := rangeInfo{label: label, count: cnt}
		if cnt == 0 {
			info.empty = true
			infos = append(infos, info)
			_ = sb
			continue
		}
		mn, err := edgeID(ctx, c, 1)
		if err != nil {
			return false, fmt.Sprintf("  [ERROR] %s (%s): min _id: %v\n", ns, label, err)
		}
		mx, err := edgeID(ctx, c, -1)
		if err != nil {
			return false, fmt.Sprintf("  [ERROR] %s (%s): max _id: %v\n", ns, label, err)
		}
		info.min, info.max = mn, mx
		infos = append(infos, info)
	}

	// Pairwise interval intersection: two intervals [aMin,aMax],[bMin,bMax]
	// intersect iff aMin <= bMax AND bMin <= aMax.
	var overlapPairs []string
	for i := 0; i < len(infos); i++ {
		for j := i + 1; j < len(infos); j++ {
			a, b := infos[i], infos[j]
			if a.empty || b.empty {
				continue
			}
			if cmpVal(a.min, b.max) <= 0 && cmpVal(b.min, a.max) <= 0 {
				overlapPairs = append(overlapPairs, fmt.Sprintf("%s∩%s", a.label, b.label))
			}
		}
	}

	var sb strings.Builder
	tag := "OK      "
	if len(overlapPairs) > 0 {
		tag = "OVERLAP?"
	}
	fmt.Fprintf(&sb, "  [%s] %s\n", tag, ns)
	for _, in := range infos {
		if in.empty {
			fmt.Fprintf(&sb, "       %-12s count=%d (empty)\n", in.label, in.count)
		} else {
			fmt.Fprintf(&sb, "       %-12s count=%d  min=%s  max=%s\n",
				in.label, in.count, fmtID(in.min), fmtID(in.max))
		}
	}
	if len(overlapPairs) > 0 {
		fmt.Fprintf(&sb, "       intersecting intervals: %s\n", strings.Join(overlapPairs, ", "))
	}
	return len(overlapPairs) > 0, sb.String()
}

// edgeID returns the min (dir=1) or max (dir=-1) _id via an indexed query.
func edgeID(ctx context.Context, c *mongo.Collection, dir int) (interface{}, error) {
	opts := options.FindOne().
		SetSort(bson.D{{Key: "_id", Value: dir}}).
		SetProjection(bson.D{{Key: "_id", Value: 1}})
	var doc struct {
		ID interface{} `bson:"_id"`
	}
	if err := c.FindOne(ctx, bson.D{}, opts).Decode(&doc); err != nil {
		return nil, err
	}
	return doc.ID, nil
}

// ── Exact mode (streaming k-way merge) ──────────────────────────────────────

type idHead struct {
	label string
	cur   *mongo.Cursor
	val   interface{}
	live  bool
}

func (h *idHead) advance(ctx context.Context) error {
	if h.cur.Next(ctx) {
		var doc struct {
			ID interface{} `bson:"_id"`
		}
		if err := h.cur.Decode(&doc); err != nil {
			return err
		}
		h.val, h.live = doc.ID, true
		return nil
	}
	h.live = false
	return h.cur.Err()
}

func scanExact(ctx context.Context, clients map[string]*mongo.Client, ns string, labels []string, sampleCap int) (bool, string) {
	db, coll := splitNS(ns)
	opts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetProjection(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(2000)

	var heads []*idHead
	for _, label := range labels {
		cur, err := clients[label].Database(db).Collection(coll).Find(ctx, bson.D{}, opts)
		if err != nil {
			return false, fmt.Sprintf("  [ERROR] %s (%s): find: %v\n", ns, label, err)
		}
		h := &idHead{label: label, cur: cur}
		if err := h.advance(ctx); err != nil {
			cur.Close(ctx)
			return false, fmt.Sprintf("  [ERROR] %s (%s): read: %v\n", ns, label, err)
		}
		heads = append(heads, h)
	}
	defer func() {
		for _, h := range heads {
			h.cur.Close(ctx)
		}
	}()

	var dupCount int64
	var samples []string
	// k-way merge: repeatedly take the minimum live head value; if >=2 heads
	// share it, that _id exists in multiple clusters => a real collision.
	for {
		var minVal interface{}
		haveMin := false
		for _, h := range heads {
			if !h.live {
				continue
			}
			if !haveMin || cmpVal(h.val, minVal) < 0 {
				minVal, haveMin = h.val, true
			}
		}
		if !haveMin {
			break // all cursors exhausted
		}
		var owners []string
		for _, h := range heads {
			if h.live && cmpVal(h.val, minVal) == 0 {
				owners = append(owners, h.label)
			}
		}
		if len(owners) >= 2 {
			dupCount++
			if len(samples) < sampleCap {
				samples = append(samples, fmt.Sprintf("%s in [%s]", fmtID(minVal), strings.Join(owners, ",")))
			}
		}
		// advance every head equal to the min.
		for _, h := range heads {
			if h.live && cmpVal(h.val, minVal) == 0 {
				if err := h.advance(ctx); err != nil {
					return dupCount > 0, fmt.Sprintf("  [ERROR] %s (%s): stream: %v\n", ns, h.label, err)
				}
			}
		}
	}

	var sb strings.Builder
	if dupCount == 0 {
		fmt.Fprintf(&sb, "  [OK      ] %s  (%s) — no shared _ids\n", ns, strings.Join(labels, ","))
		return false, sb.String()
	}
	fmt.Fprintf(&sb, "  [OVERLAP ] %s  (%s) — %d shared _id(s)\n", ns, strings.Join(labels, ","), dupCount)
	for _, s := range samples {
		fmt.Fprintf(&sb, "       %s\n", s)
	}
	if dupCount > int64(len(samples)) {
		fmt.Fprintf(&sb, "       ... and %d more (raise --sample to see)\n", dupCount-int64(len(samples)))
	}
	return true, sb.String()
}

// ── BSON value comparison ───────────────────────────────────────────────────

// typeRank orders _id values by BSON type first (subset covering common _id
// types), matching MongoDB's cross-type sort order closely enough for merge.
func typeRank(v interface{}) int {
	switch v.(type) {
	case nil:
		return 0
	case int32, int64, float64:
		return 1
	case string:
		return 2
	case primitive.Binary:
		return 4
	case primitive.ObjectID:
		return 5
	case bool:
		return 6
	case primitive.DateTime:
		return 7
	default:
		return 9
	}
}

func asFloat(v interface{}) float64 {
	switch n := v.(type) {
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	}
	return 0
}

// cmpVal returns -1/0/1. Same-type values compare by value; different types by
// BSON type rank; unknown types fall back to canonical string comparison.
func cmpVal(a, b interface{}) int {
	ra, rb := typeRank(a), typeRank(b)
	if ra != rb {
		if ra < rb {
			return -1
		}
		return 1
	}
	switch av := a.(type) {
	case nil:
		return 0
	case int32, int64, float64:
		return cmpFloat(asFloat(a), asFloat(b))
	case string:
		return strings.Compare(av, b.(string))
	case primitive.ObjectID:
		bv := b.(primitive.ObjectID)
		return bytes.Compare(av[:], bv[:])
	case primitive.Binary:
		bv := b.(primitive.Binary)
		if av.Subtype != bv.Subtype {
			if av.Subtype < bv.Subtype {
				return -1
			}
			return 1
		}
		return bytes.Compare(av.Data, bv.Data)
	case bool:
		bb := b.(bool)
		if av == bb {
			return 0
		}
		if !av {
			return -1
		}
		return 1
	case primitive.DateTime:
		bb := b.(primitive.DateTime)
		if av == bb {
			return 0
		}
		if av < bb {
			return -1
		}
		return 1
	default:
		return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
	}
}

func cmpFloat(a, b float64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}

func fmtID(v interface{}) string {
	switch t := v.(type) {
	case primitive.ObjectID:
		return "ObjectId(" + t.Hex() + ")"
	case string:
		return "\"" + t + "\""
	case primitive.Binary:
		return fmt.Sprintf("Binary(subtype=%d,len=%d)", t.Subtype, len(t.Data))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func ruler(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '='
	}
	return string(b)
}
