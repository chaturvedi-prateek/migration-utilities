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
// Output & resumability:
//
//	Detailed findings are written to a RESULTS LOG FILE (--out); the console
//	shows only progress + the final summary. Progress is checkpointed to a
//	JSON file (--checkpoint) after every namespace, and — in exact mode —
//	periodically WITHIN a namespace via an _id watermark. If the run is killed,
//	re-running with the same --config/--checkpoint resumes: finished namespaces
//	are skipped, and a half-scanned exact namespace continues from its watermark
//	instead of restarting. Use --restart to ignore an existing checkpoint.
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

// checkpoint every this many merged _ids, and at least this often, in exact mode.
const (
	exactCheckpointEveryN = 100_000
	checkpointMinInterval = 5 * time.Second
)

// ── Console (progress) ──────────────────────────────────────────────────────

func outf(format string, args ...interface{}) { fmt.Fprintf(os.Stdout, format, args...) }
func outln(args ...interface{})               { fmt.Fprintln(os.Stdout, args...) }
func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
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
	LogFile string        `json:"log_file"` // default results file if --out not given
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

// ── Result + checkpoint ─────────────────────────────────────────────────────

type nsResult struct {
	NS        string          `json:"ns"`
	Clusters  []string        `json:"clusters"`
	Overlap   bool            `json:"overlap"`
	DupCount  int64           `json:"dup_count"`
	Samples   []string        `json:"samples"`
	Done      bool            `json:"done"`
	Watermark json.RawMessage `json:"watermark,omitempty"` // exact-mode: extjson {"_id": <last processed>}
	Report    string          `json:"report"`              // human-readable block for the results file
}

type checkpoint struct {
	Mode      string               `json:"mode"`
	StartedAt string               `json:"started_at"`
	Results   map[string]*nsResult `json:"results"`
}

// checkpointer persists progress atomically and throttles disk writes.
type checkpointer struct {
	mu       sync.Mutex
	path     string
	cp       *checkpoint
	lastSave time.Time
}

func (c *checkpointer) get(ns string) *nsResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	if r, ok := c.cp.Results[ns]; ok {
		cp := *r // shallow copy is enough for the fields the caller reads
		return &cp
	}
	return nil
}

// put stores a result snapshot and saves to disk if forced or the throttle
// interval elapsed. Callers pass a fresh snapshot each time (no shared mutation).
func (c *checkpointer) put(r *nsResult, force bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cp.Results[r.NS] = r
	if force || time.Since(c.lastSave) >= checkpointMinInterval {
		c.saveLocked()
	}
}

func (c *checkpointer) saveLocked() {
	tmp := c.path + ".tmp"
	b, err := json.MarshalIndent(c.cp, "", "  ")
	if err != nil {
		return
	}
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return
	}
	_ = os.Rename(tmp, c.path) // atomic replace
	c.lastSave = time.Now()
}

func loadCheckpoint(path string) (*checkpoint, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cp checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		return nil, fmt.Errorf("checkpoint %q is corrupt: %w (use --restart to ignore)", path, err)
	}
	if cp.Results == nil {
		cp.Results = map[string]*nsResult{}
	}
	return &cp, nil
}

// watermark helpers — serialize a single _id value as extended JSON so its BSON
// type survives a restart.
func marshalWatermark(id interface{}) json.RawMessage {
	b, err := bson.MarshalExtJSON(bson.M{"_id": id}, true, false)
	if err != nil {
		return nil
	}
	return json.RawMessage(b)
}

func unmarshalWatermark(raw json.RawMessage) (interface{}, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var m bson.M
	if err := bson.UnmarshalExtJSON([]byte(raw), true, &m); err != nil {
		return nil, false
	}
	v, ok := m["_id"]
	return v, ok
}

// ── Main ────────────────────────────────────────────────────────────────────

func main() {
	configFile := flag.String("config", "config.json", "Path to JSON config file")
	mode := flag.String("mode", "range", "'range' = fast min/max interval check | 'exact' = streaming k-way merge for real duplicate _ids")
	sampleCap := flag.Int("sample", 20, "Max duplicate _ids to record per namespace (exact mode)")
	concurrency := flag.Int("concurrency", 4, "Namespaces scanned in parallel")
	out := flag.String("out", "", "Results log file (default: config.log_file, else overlap-report.<mode>.log)")
	checkpointPath := flag.String("checkpoint", "", "Checkpoint file for resume (default: <out>.checkpoint.json)")
	restart := flag.Bool("restart", false, "Ignore any existing checkpoint and start fresh")
	flag.Parse()

	if *mode != "range" && *mode != "exact" {
		fatalf("ERROR: --mode must be 'range' or 'exact'\n")
	}

	cfg, err := loadConfig(*configFile)
	if err != nil {
		fatalf("ERROR: %v\n", err)
	}

	// Resolve output + checkpoint paths.
	outPath := *out
	if outPath == "" {
		outPath = cfg.LogFile
	}
	if outPath == "" {
		outPath = fmt.Sprintf("overlap-report.%s.log", *mode)
	}
	ckptPath := *checkpointPath
	if ckptPath == "" {
		ckptPath = outPath + ".checkpoint.json"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	// Load or init checkpoint.
	var cp *checkpoint
	resuming := false
	if !*restart {
		cp, err = loadCheckpoint(ckptPath)
		if err != nil {
			fatalf("ERROR: %v\n", err)
		}
		if cp != nil && cp.Mode != *mode {
			fatalf("ERROR: checkpoint %q is for mode %q, but --mode=%q. Use --restart or a different --checkpoint.\n",
				ckptPath, cp.Mode, *mode)
		}
	}
	if cp == nil {
		cp = &checkpoint{Mode: *mode, StartedAt: time.Now().Format(time.RFC3339), Results: map[string]*nsResult{}}
	} else {
		resuming = true
	}
	ckpt := &checkpointer{path: ckptPath, cp: cp}

	// Open results file: append when resuming (keep prior run's blocks), else truncate.
	resFile, wroteHeader, err := openResults(outPath, resuming, *mode)
	if err != nil {
		fatalf("ERROR: %v\n", err)
	}
	defer resFile.Close()
	_ = wroteHeader

	outf("\n%s\n  id-overlap-scan  |  mode=%s  |  sources=%d\n  results: %s\n  checkpoint: %s%s\n%s\n",
		ruler(70), *mode, len(cfg.Sources), outPath, ckptPath,
		map[bool]string{true: "  (RESUMING)", false: ""}[resuming], ruler(70))

	// Connect + discover namespaces.
	clients := map[string]*mongo.Client{}
	for _, s := range cfg.Sources {
		clients[s.Label] = mustConnect(ctx, s.URI, "source:"+s.Label)
	}
	defer func() {
		for _, c := range clients {
			c.Disconnect(ctx)
		}
	}()

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
	var merged []string
	for ns, labels := range nsToClusters {
		if len(labels) >= 2 {
			sort.Strings(labels)
			merged = append(merged, ns)
		}
	}
	sort.Strings(merged)

	outf("  Merge-candidate namespaces (in >=2 clusters): %d\n", len(merged))
	if len(merged) == 0 {
		outln("  No shared namespaces — nothing can collide. RESULT: PASS (no overlap).")
		return
	}

	scanAll(ctx, clients, nsToClusters, merged, *mode, *sampleCap, *concurrency, ckpt, resFile)

	// Final tally from the checkpoint (covers this run + any resumed results).
	overlaps, scanned := 0, 0
	for _, ns := range merged {
		if r := ckpt.get(ns); r != nil && r.Done {
			scanned++
			if r.Overlap {
				overlaps++
			}
		}
	}
	ckpt.put(&nsResult{NS: "__final__", Done: true}, true) // force a final flush

	summary := fmt.Sprintf("\n  ── summary  |  namespaces scanned: %d/%d  |  with overlap: %d ──\n",
		scanned, len(merged), overlaps)
	outf("%s", summary)
	fmt.Fprint(resFile, summary)

	if overlaps > 0 {
		msg := "  RESULT: OVERLAP found — see results file. Decide handling before backfill.\n"
		if *mode == "range" {
			msg = "  RESULT: OVERLAP CANDIDATES found — intervals intersect. Re-run --mode=exact to confirm.\n"
		}
		outf("%s", msg)
		fmt.Fprint(resFile, msg)
		outf("\n  Full findings: %s\n", outPath)
		os.Exit(5)
	}
	pass := "  RESULT: PASS — no _id overlap across merged namespaces.\n"
	outf("%s", pass)
	fmt.Fprint(resFile, pass)
	outf("\n  Full findings: %s\n", outPath)
}

// openResults opens the results log file; truncates + writes a header on a fresh
// run, appends on resume. Returns whether a header was written.
func openResults(path string, resume bool, mode string) (*os.File, bool, error) {
	flags := os.O_CREATE | os.O_WRONLY
	if resume {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0644)
	if err != nil {
		return nil, false, fmt.Errorf("cannot open results file %q: %w", path, err)
	}
	if resume {
		fmt.Fprintf(f, "\n# ── resumed %s (mode=%s) ──\n", time.Now().Format(time.RFC3339), mode)
		return f, false, nil
	}
	fmt.Fprintf(f, "# id-overlap-scan results  |  mode=%s  |  %s\n\n", mode, time.Now().Format(time.RFC3339))
	return f, true, nil
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

// ── Scan driver (parallel over namespaces, resumable) ───────────────────────

func scanAll(ctx context.Context, clients map[string]*mongo.Client, nsToClusters map[string][]string,
	merged []string, mode string, sampleCap, concurrency int, ckpt *checkpointer, resFile *os.File) {

	var writeMu sync.Mutex // serialize console + results-file writes
	jobs := make(chan string, len(merged))
	total := len(merged)
	var doneCount int64

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ns := range jobs {
				labels := nsToClusters[ns]

				// Skip namespaces already finished in a previous run.
				if prev := ckpt.get(ns); prev != nil && prev.Done {
					writeMu.Lock()
					doneCount++
					outf("  [%d/%d] %s — already done (%s)\n", doneCount, total, ns,
						map[bool]string{true: "OVERLAP", false: "OK"}[prev.Overlap])
					writeMu.Unlock()
					continue
				}

				writeMu.Lock()
				outf("  [ .. ] scanning %s (%s) ...\n", ns, strings.Join(labels, ","))
				writeMu.Unlock()

				var res *nsResult
				if mode == "range" {
					res = scanRange(ctx, clients, ns, labels)
				} else {
					res = scanExact(ctx, clients, ns, labels, sampleCap, ckpt)
				}
				ckpt.put(res, true) // persist completed namespace immediately

				writeMu.Lock()
				doneCount++
				outf("  [%d/%d] %s — %s\n", doneCount, total, ns,
					map[bool]string{true: fmt.Sprintf("OVERLAP (%d)", res.DupCount), false: "OK"}[res.Overlap])
				fmt.Fprint(resFile, res.Report)
				writeMu.Unlock()
			}
		}()
	}
	for _, ns := range merged {
		jobs <- ns
	}
	close(jobs)
	wg.Wait()
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

func scanRange(ctx context.Context, clients map[string]*mongo.Client, ns string, labels []string) *nsResult {
	db, coll := splitNS(ns)
	res := &nsResult{NS: ns, Clusters: labels, Done: true}
	var infos []rangeInfo
	for _, label := range labels {
		c := clients[label].Database(db).Collection(coll)
		cnt, err := c.EstimatedDocumentCount(ctx)
		if err != nil {
			res.Report = fmt.Sprintf("  [ERROR] %s (%s): count: %v\n", ns, label, err)
			return res
		}
		info := rangeInfo{label: label, count: cnt}
		if cnt == 0 {
			info.empty = true
			infos = append(infos, info)
			continue
		}
		if info.min, err = edgeID(ctx, c, 1); err != nil {
			res.Report = fmt.Sprintf("  [ERROR] %s (%s): min _id: %v\n", ns, label, err)
			return res
		}
		if info.max, err = edgeID(ctx, c, -1); err != nil {
			res.Report = fmt.Sprintf("  [ERROR] %s (%s): max _id: %v\n", ns, label, err)
			return res
		}
		infos = append(infos, info)
	}

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
	res.Overlap = len(overlapPairs) > 0
	res.Report = sb.String()
	return res
}

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

// ── Exact mode (streaming k-way merge, resumable via _id watermark) ──────────

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

func scanExact(ctx context.Context, clients map[string]*mongo.Client, ns string, labels []string,
	sampleCap int, ckpt *checkpointer) *nsResult {

	db, coll := splitNS(ns)

	// Resume state: continue from the last checkpointed watermark if present.
	res := &nsResult{NS: ns, Clusters: labels}
	filter := bson.D{}
	if prev := ckpt.get(ns); prev != nil && !prev.Done {
		res.DupCount = prev.DupCount
		res.Samples = append([]string(nil), prev.Samples...)
		res.Watermark = prev.Watermark
		if wm, ok := unmarshalWatermark(prev.Watermark); ok {
			filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: wm}}}}
		}
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetProjection(bson.D{{Key: "_id", Value: 1}}).
		SetBatchSize(2000)

	var heads []*idHead
	for _, label := range labels {
		cur, err := clients[label].Database(db).Collection(coll).Find(ctx, filter, findOpts)
		if err != nil {
			res.Done = true
			res.Report = fmt.Sprintf("  [ERROR] %s (%s): find: %v\n", ns, label, err)
			return res
		}
		h := &idHead{label: label, cur: cur}
		if err := h.advance(ctx); err != nil {
			cur.Close(ctx)
			res.Done = true
			res.Report = fmt.Sprintf("  [ERROR] %s (%s): read: %v\n", ns, label, err)
			return res
		}
		heads = append(heads, h)
	}
	defer func() {
		for _, h := range heads {
			h.cur.Close(ctx)
		}
	}()

	var sinceCheckpoint int
	for {
		var minVal interface{}
		haveMin := false
		for _, h := range heads {
			if h.live && (!haveMin || cmpVal(h.val, minVal) < 0) {
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
			res.DupCount++
			if len(res.Samples) < sampleCap {
				res.Samples = append(res.Samples, fmt.Sprintf("%s in [%s]", fmtID(minVal), strings.Join(owners, ",")))
			}
		}
		// Advance every head sitting on the minimum, then record the watermark
		// (minVal is now fully processed). Persist periodically.
		for _, h := range heads {
			if h.live && cmpVal(h.val, minVal) == 0 {
				if err := h.advance(ctx); err != nil {
					res.Done = false
					res.Report = fmt.Sprintf("  [ERROR] %s: stream: %v\n", ns, err)
					return res
				}
			}
		}
		res.Watermark = marshalWatermark(minVal)
		sinceCheckpoint++
		if sinceCheckpoint >= exactCheckpointEveryN {
			sinceCheckpoint = 0
			ckpt.put(snapshotPartial(res), false) // throttled disk write
		}
	}

	res.Done = true
	res.Watermark = nil // completed; no resume point needed
	res.Overlap = res.DupCount > 0
	res.Report = renderExactReport(res)
	return res
}

// snapshotPartial returns a not-done copy for mid-namespace checkpointing.
func snapshotPartial(r *nsResult) *nsResult {
	cp := *r
	cp.Done = false
	cp.Samples = append([]string(nil), r.Samples...)
	return &cp
}

func renderExactReport(res *nsResult) string {
	var sb strings.Builder
	if res.DupCount == 0 {
		fmt.Fprintf(&sb, "  [OK      ] %s  (%s) — no shared _ids\n", res.NS, strings.Join(res.Clusters, ","))
		return sb.String()
	}
	fmt.Fprintf(&sb, "  [OVERLAP ] %s  (%s) — %d shared _id(s)\n", res.NS, strings.Join(res.Clusters, ","), res.DupCount)
	for _, s := range res.Samples {
		fmt.Fprintf(&sb, "       %s\n", s)
	}
	if res.DupCount > int64(len(res.Samples)) {
		fmt.Fprintf(&sb, "       ... and %d more (raise --sample to see)\n", res.DupCount-int64(len(res.Samples)))
	}
	return sb.String()
}

// ── BSON value comparison ───────────────────────────────────────────────────

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
