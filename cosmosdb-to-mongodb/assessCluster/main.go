// assessCluster reviews an Azure Cosmos DB for MongoDB (RU) account before a
// dsync / dsynct migration to MongoDB Atlas and reports everything that dsync
// either cannot handle or handles in a way that needs a decision.
//
// It is READ-ONLY: it never inserts, updates or deletes on the source.
//
// Modes (run one, or --mode all):
//
//	account        connection string + server version + dsync connector selection
//	inventory      per-collection counts, sizes, shard key, throughput, 2 MB / 16 MB doc cap
//	idtypes        _id BSON type per collection (ObjectId / single other type / MIXED)
//	duplicates     duplicate _id across shard-key values on sharded collections (silent data loss on Atlas)
//	indexes        index catalogue + Atlas portability (TTL on _ts, unique, wildcard, limits)
//	changestreams  functional change-stream probe + token-donor capability + physical partition count
//	residue        dsync dummy documents left behind on the source
//	waves          bin-pack namespaces into dsync flows (<= N per flow, >= 1 token donor per flow)
//
// Usage:
//
//	assessCluster --config config.json --mode all
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ── Config ────────────────────────────────────────────────────────────────────

type wavesCfg struct {
	MaxNamespaces  int    `json:"max_namespaces"`  // dsync --cosmos-reader-max-namespaces (default 8)
	NamespacesFile string `json:"namespaces_file"` // optional: one db.collection per line; empty = all
}

type config struct {
	SourceURI   string   `json:"source_uri"`
	LogFile     string   `json:"log_file"`
	Databases   []string `json:"databases"`   // empty = all non-system databases
	OutputDir   string   `json:"output_dir"`  // assessment.json, assessment.md, waves.csv land here
	Concurrency int      `json:"concurrency"` // parallel per-collection probes (RU burst!)
	MaxReport   int      `json:"max_report"`  // max _ids listed per finding
	Waves       wavesCfg `json:"waves"`
}

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
	if cfg.OutputDir == "" {
		cfg.OutputDir = "assessment"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.MaxReport <= 0 {
		cfg.MaxReport = 50
	}
	if cfg.Waves.MaxNamespaces <= 0 {
		cfg.Waves.MaxNamespaces = 8
	}
	return &cfg, nil
}

// ── Logger (same shape as the docdb-to-mongodb tools) ─────────────────────────

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
func logBlock(f string, a ...interface{}) { emit("[BLOCK]", f, a...) }

func printSep() {
	logMu.Lock()
	fmt.Fprintln(logWriter, strings.Repeat("═", 70))
	logMu.Unlock()
}

// ── Severity model ────────────────────────────────────────────────────────────
//
//	BLOCKER  dsync will fail, or data will be lost silently — fix before starting
//	ACTION   dsync will run, but the migration plan must handle it
//	INFO     recorded for the runbook

const (
	sevBlocker = "BLOCKER"
	sevAction  = "ACTION"
	sevInfo    = "INFO"
)

type finding struct {
	Severity  string `json:"severity"`
	Mode      string `json:"mode"`
	Namespace string `json:"namespace,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}

type findingSink struct {
	mu   sync.Mutex
	list []finding
}

func (s *findingSink) add(sev, mode, ns, code, msg string) {
	s.mu.Lock()
	s.list = append(s.list, finding{Severity: sev, Mode: mode, Namespace: ns, Code: code, Message: msg})
	s.mu.Unlock()
	switch sev {
	case sevBlocker:
		logBlock("%s %s %s — %s", mode, ns, code, msg)
	case sevAction:
		logWarn("%s %s %s — %s", mode, ns, code, msg)
	default:
		logInfo("%s %s %s — %s", mode, ns, code, msg)
	}
}

func (s *findingSink) count(sev string) int {
	n := 0
	for _, f := range s.list {
		if f.Severity == sev {
			n++
		}
	}
	return n
}

// ── Cosmos DB helpers ─────────────────────────────────────────────────────────
//
// dsync selects its CosmosDB connector purely by matching the URI hostname
// against this regex (connectors/mongo/util.go). A DNS alias or proxy in front
// of Cosmos makes dsync treat it as plain MongoDB.

var (
	cosmosRUHostRe    = regexp.MustCompile(`(?i)\.mongo\.cosmos\.azure\.com$`)
	cosmosVCoreHostRe = regexp.MustCompile(`(?i)\.mongocluster\.cosmos\.azure\.com$`)
	retryAfterRe      = regexp.MustCompile(`RetryAfterMs=(\d+)`)
	hostFromURIRe     = regexp.MustCompile(`@([^/?]+)`)
)

var systemDBs = map[string]bool{"admin": true, "config": true, "local": true}

type uriInfo struct {
	Hosts   []string
	Flavor  string // COSMOS_RU | COSMOS_VCORE | OTHER
	Options map[string]string
}

func parseURI(uri string) uriInfo {
	info := uriInfo{Flavor: "OTHER", Options: map[string]string{}}
	var hostPart, query string
	if u, err := url.Parse(uri); err == nil && u.Host != "" {
		hostPart = u.Host
		query = u.RawQuery
	} else {
		// Fall back for passwords the URL parser rejects.
		if m := hostFromURIRe.FindStringSubmatch(uri); len(m) == 2 {
			hostPart = m[1]
		}
		if i := strings.Index(uri, "?"); i >= 0 {
			query = uri[i+1:]
		}
	}
	for _, h := range strings.Split(hostPart, ",") {
		h = strings.TrimSpace(h)
		if i := strings.LastIndex(h, ":"); i > 0 {
			h = h[:i]
		}
		if h != "" {
			info.Hosts = append(info.Hosts, h)
		}
	}
	for _, kv := range strings.Split(query, "&") {
		if kv == "" {
			continue
		}
		k, v := kv, ""
		if i := strings.Index(kv, "="); i >= 0 {
			k, v = kv[:i], kv[i+1:]
		}
		info.Options[strings.ToLower(k)] = v
	}
	if len(info.Hosts) > 0 {
		switch {
		case cosmosRUHostRe.MatchString(info.Hosts[0]):
			info.Flavor = "COSMOS_RU"
		case cosmosVCoreHostRe.MatchString(info.Hosts[0]):
			info.Flavor = "COSMOS_VCORE"
		}
	}
	return info
}

// isThrottled recognises Cosmos RU throttling (HTTP 429 surfaced as Mongo
// error 16500) and returns the server-suggested back-off.
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
		!strings.Contains(msg, "TooManyRequests") && !strings.Contains(msg, "Request rate is large") {
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

// withRetry re-runs fn while Cosmos throttles it, up to maxAttempts.
func withRetry(ctx context.Context, label string, fn func() error) error {
	const maxAttempts = 12
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err = fn()
		wait, throttled := isThrottled(err)
		if !throttled {
			return err
		}
		wait = time.Duration(float64(wait) * (1 + 0.5*float64(attempt-1)))
		if wait > 5*time.Second {
			wait = 5 * time.Second
		}
		logWarn("%s throttled (16500) — retry %d/%d in %s", label, attempt, maxAttempts, wait)
		if serr := sleepCtx(ctx, wait); serr != nil {
			return serr
		}
	}
	return fmt.Errorf("%s: still throttled after %d attempts: %w", label, maxAttempts, err)
}

// docKeys returns the field names of a decoded sub-document regardless of
// whether the driver produced primitive.D or primitive.M / bson.M.
func docKeys(v interface{}) []string {
	switch d := v.(type) {
	case primitive.D:
		out := make([]string, 0, len(d))
		for _, e := range d {
			out = append(out, e.Key)
		}
		return out
	case primitive.M: // bson.M is an alias of primitive.M
		out := make([]string, 0, len(d))
		for k := range d {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	return nil
}

func asInt64(v interface{}) int64 {
	switch n := v.(type) {
	case int32:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	case int:
		return int64(n)
	}
	return 0
}

// collMeta is what Cosmos exposes through customAction: GetCollection.
type collMeta struct {
	ShardKey         []string // empty = unsharded (or sharded on _id)
	ShardKeyRaw      string
	Throughput       int64 // dedicated RU/s when present, 0 = shared (database-level)
	AutoscaleMax     int64
	MaxContentLength int64 // 2097152 = 2 MB default; 16 MB only when the account feature is on
}

func getCollectionMeta(ctx context.Context, db *mongo.Database, coll string) (collMeta, error) {
	var out collMeta
	var m bson.M
	err := withRetry(ctx, "GetCollection "+coll, func() error {
		return db.RunCommand(ctx, bson.D{{Key: "customAction", Value: "GetCollection"}, {Key: "collection", Value: coll}}).Decode(&m)
	})
	if err != nil {
		return out, err
	}
	if sk, ok := m["shardKeyDefinition"]; ok {
		out.ShardKey = docKeys(sk)
		if b, e := json.Marshal(sk); e == nil {
			out.ShardKeyRaw = string(b)
		}
	}
	// Sharded on _id behaves like unsharded for dsync's dummy-insert purposes.
	if len(out.ShardKey) == 1 && out.ShardKey[0] == "_id" {
		out.ShardKey = nil
	}
	if tp, ok := m["provisionedThroughput"]; ok {
		switch t := tp.(type) {
		case int32, int64, float64:
			out.Throughput = asInt64(t)
		case primitive.D:
			for _, e := range t {
				if e.Key == "throughput" || e.Key == "provisionedThroughput" {
					out.Throughput = asInt64(e.Value)
				}
			}
		case primitive.M:
			out.Throughput = asInt64(t["throughput"])
		}
	}
	if as, ok := m["autoScaleSettings"]; ok {
		switch t := as.(type) {
		case primitive.D:
			for _, e := range t {
				if e.Key == "maxThroughput" {
					out.AutoscaleMax = asInt64(e.Value)
				}
			}
		case primitive.M:
			out.AutoscaleMax = asInt64(t["maxThroughput"])
		}
	}
	out.MaxContentLength = asInt64(m["maxContentLength"])
	return out, nil
}

type collStatsOut struct {
	Count       int64
	Size        int64
	StorageSize int64
	AvgObjSize  int64
}

// collStats must be issued as a command: Cosmos DB for MongoDB 7.0 rejects the
// $collStats aggregation stage that mongosh's coll.stats() uses.
func getCollStats(ctx context.Context, db *mongo.Database, coll string) (collStatsOut, error) {
	var out collStatsOut
	var m bson.M
	err := withRetry(ctx, "collStats "+coll, func() error {
		return db.RunCommand(ctx, bson.D{{Key: "collStats", Value: coll}}).Decode(&m)
	})
	if err != nil {
		return out, err
	}
	out.Count = asInt64(m["count"])
	out.Size = asInt64(m["size"])
	out.StorageSize = asInt64(m["storageSize"])
	out.AvgObjSize = asInt64(m["avgObjSize"])
	return out, nil
}

// cosmosChangeStreamPipeline is the only shape Cosmos DB for MongoDB accepts:
// $match on operationType ∈ {insert,update,replace}, a $project, and
// fullDocument: updateLookup. Anything else fails with BadValue.
func cosmosChangeStreamPipeline() mongo.Pipeline {
	return mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "replace"}}}}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}, {Key: "fullDocument", Value: 1}, {Key: "ns", Value: 1}, {Key: "documentKey", Value: 1}}}},
	}
}

func listUserDatabases(ctx context.Context, client *mongo.Client, only []string) ([]string, error) {
	if len(only) > 0 {
		return only, nil
	}
	names, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range names {
		if !systemDBs[n] {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out, nil
}

func listCollections(ctx context.Context, db *mongo.Database) ([]string, error) {
	var names []string
	err := withRetry(ctx, "listCollections "+db.Name(), func() error {
		var e error
		names, e = db.ListCollectionNames(ctx, bson.D{})
		return e
	})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range names {
		if !strings.HasPrefix(n, "system.") {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out, nil
}

func fmtInt(n int64) string {
	if n < 0 {
		return "-" + fmtInt(-n)
	}
	s := strconv.FormatInt(n, 10)
	out := make([]byte, 0, len(s)+len(s)/3)
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	return string(out)
}

func fmtBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// ── Report model ──────────────────────────────────────────────────────────────

type accountReport struct {
	Hosts            []string          `json:"hosts"`
	Flavor           string            `json:"flavor"`
	ServerVersion    string            `json:"server_version"`
	MaxBSONObjectMiB int64             `json:"max_bson_object_mib"`
	URIOptions       map[string]string `json:"uri_options"`
}

type nsEntry struct {
	Namespace        string   `json:"namespace"`
	Database         string   `json:"database"`
	Collection       string   `json:"collection"`
	Count            int64    `json:"count"`
	SizeBytes        int64    `json:"size_bytes"`
	StorageBytes     int64    `json:"storage_bytes"`
	AvgObjBytes      int64    `json:"avg_obj_bytes"`
	ShardKey         []string `json:"shard_key,omitempty"`
	Sharded          bool     `json:"sharded"`
	ThroughputRU     int64    `json:"throughput_ru,omitempty"`
	AutoscaleMaxRU   int64    `json:"autoscale_max_ru,omitempty"`
	ThroughputScope  string   `json:"throughput_scope"` // dedicated | shared
	MaxContentLength int64    `json:"max_content_length"`
	Error            string   `json:"error,omitempty"`
}

type idTypeEntry struct {
	Namespace string `json:"namespace"`
	Status    string `json:"status"` // OK | SLOW_PATH | MIXED | EMPTY | ERROR
	MinType   string `json:"min_type,omitempty"`
	MaxType   string `json:"max_type,omitempty"`
	Error     string `json:"error,omitempty"`
}

type dupEntry struct {
	Namespace  string   `json:"namespace"`
	Scanned    int64    `json:"scanned"`
	Duplicates int64    `json:"duplicates"`
	Samples    []string `json:"samples,omitempty"` // extended JSON: {"_id":..., "<shardKey>":[...]}
	Error      string   `json:"error,omitempty"`
}

type indexEntry struct {
	Namespace   string   `json:"namespace"`
	Name        string   `json:"name"`
	Key         string   `json:"key"`
	Unique      bool     `json:"unique"`
	TTLSeconds  *int64   `json:"ttl_seconds,omitempty"`
	Kind        string   `json:"kind"` // regular | unique | ttl | wildcard | text | geo
	Portability string   `json:"portability"`
	Notes       []string `json:"notes,omitempty"`
}

type csEntry struct {
	Namespace          string `json:"namespace"`
	Status             string `json:"status"` // ENABLED | FAILED
	TokenDonor         bool   `json:"token_donor"`
	PhysicalPartitions int    `json:"physical_partitions"`
	Detail             string `json:"detail,omitempty"`
}

type residueEntry struct {
	Namespace string   `json:"namespace"`
	Found     int64    `json:"found"`
	Samples   []string `json:"samples,omitempty"`
	Error     string   `json:"error,omitempty"`
}

type waveEntry struct {
	Wave       string   `json:"wave"`
	Namespaces []string `json:"namespaces"`
	SizeBytes  int64    `json:"size_bytes"`
	Docs       int64    `json:"docs"`
	Donor      string   `json:"donor,omitempty"`
	Partitions int      `json:"physical_partitions"`
	Notes      []string `json:"notes,omitempty"`
}

type report struct {
	Timestamp     string         `json:"timestamp"`
	Modes         []string       `json:"modes"`
	Account       *accountReport `json:"account,omitempty"`
	Inventory     []nsEntry      `json:"inventory,omitempty"`
	IDTypes       []idTypeEntry  `json:"id_types,omitempty"`
	Duplicates    []dupEntry     `json:"duplicates,omitempty"`
	Indexes       []indexEntry   `json:"indexes,omitempty"`
	ChangeStreams []csEntry      `json:"change_streams,omitempty"`
	Residue       []residueEntry `json:"residue,omitempty"`
	Waves         []waveEntry    `json:"waves,omitempty"`
	Findings      []finding      `json:"findings"`
	Summary       map[string]int `json:"summary"`
}

// ── Collector ─────────────────────────────────────────────────────────────────

type collector struct {
	ctx      context.Context
	client   *mongo.Client
	cfg      *config
	sink     *findingSink
	inv      []nsEntry
	invOnce  sync.Once
	invErr   error
	metaMu   sync.Mutex
	metaByNS map[string]collMeta
}

func newCollector(ctx context.Context, client *mongo.Client, cfg *config, sink *findingSink) *collector {
	return &collector{ctx: ctx, client: client, cfg: cfg, sink: sink, metaByNS: map[string]collMeta{}}
}

// forEachCollection runs fn over every user collection with bounded parallelism.
func (c *collector) forEachCollection(fn func(dbName, collName string)) error {
	dbs, err := listUserDatabases(c.ctx, c.client, c.cfg.Databases)
	if err != nil {
		return fmt.Errorf("list databases: %w", err)
	}
	sem := make(chan struct{}, c.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, dbName := range dbs {
		colls, err := listCollections(c.ctx, c.client.Database(dbName))
		if err != nil {
			return fmt.Errorf("list collections %s: %w", dbName, err)
		}
		for _, coll := range colls {
			wg.Add(1)
			sem <- struct{}{}
			go func(d, cn string) {
				defer wg.Done()
				defer func() { <-sem }()
				fn(d, cn)
			}(dbName, coll)
		}
	}
	wg.Wait()
	return nil
}

func (c *collector) meta(dbName, collName string) (collMeta, error) {
	ns := dbName + "." + collName
	c.metaMu.Lock()
	m, ok := c.metaByNS[ns]
	c.metaMu.Unlock()
	if ok {
		return m, nil
	}
	m, err := getCollectionMeta(c.ctx, c.client.Database(dbName), collName)
	if err != nil {
		return m, err
	}
	c.metaMu.Lock()
	c.metaByNS[ns] = m
	c.metaMu.Unlock()
	return m, nil
}

// ── Mode: account ─────────────────────────────────────────────────────────────

func (c *collector) runAccount() *accountReport {
	logStep("account — connection string, flavor, server version")
	info := parseURI(c.cfg.SourceURI)
	rep := &accountReport{Hosts: info.Hosts, Flavor: info.Flavor, URIOptions: info.Options}

	switch info.Flavor {
	case "COSMOS_RU":
		logOk("hostname matches dsync's CosmosDB connector regex (\\.mongo\\.cosmos\\.azure\\.com$)")
	case "COSMOS_VCORE":
		c.sink.add(sevBlocker, "account", "", "VCORE_SOURCE", "host is Cosmos DB vCore (.mongocluster.cosmos.azure.com) — dsync supports vCore as a destination only (no change streams)")
	default:
		c.sink.add(sevBlocker, "account", "", "NOT_COSMOS_HOST", fmt.Sprintf("host %v does not match \\.mongo\\.cosmos\\.azure\\.com$ — dsync would select its generic MongoDB connector: no --cosmos-* flags, no deletes emulation, wrong partitioning. Connect to the real Cosmos hostname, not an alias/proxy", info.Hosts))
	}

	var bi bson.M
	if err := c.client.Database("admin").RunCommand(c.ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&bi); err == nil {
		rep.ServerVersion, _ = bi["version"].(string)
	}
	if rep.ServerVersion == "" {
		c.sink.add(sevAction, "account", "", "VERSION_UNKNOWN", "buildInfo did not return a version")
	} else {
		major, minor := parseVersion(rep.ServerVersion)
		if major < 3 || (major == 3 && minor < 6) {
			c.sink.add(sevBlocker, "account", "", "VERSION_TOO_OLD", fmt.Sprintf("server version %s < 3.6 — change streams do not exist; live CDC is impossible", rep.ServerVersion))
		} else {
			logOk("server version %s (change streams available)", rep.ServerVersion)
		}
	}

	var hello bson.M
	if err := c.client.Database("admin").RunCommand(c.ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err == nil {
		rep.MaxBSONObjectMiB = asInt64(hello["maxBsonObjectSize"]) / (1024 * 1024)
	}

	if v, ok := info.Options["retrywrites"]; !ok || strings.ToLower(v) != "false" {
		c.sink.add(sevAction, "account", "", "RETRYWRITES", "connection string should carry retrywrites=false — Cosmos DB for MongoDB (RU) does not support retryable writes")
	}
	if _, ok := info.Options["maxidletimems"]; !ok {
		c.sink.add(sevInfo, "account", "", "MAXIDLETIME", "connection string has no maxIdleTimeMS — Cosmos closes idle connections after 30 min; Microsoft's default string uses maxIdleTimeMS=120000")
	}
	return rep
}

func parseVersion(v string) (int, int) {
	parts := strings.Split(v, ".")
	major, minor := 0, 0
	if len(parts) > 0 {
		major, _ = strconv.Atoi(parts[0])
	}
	if len(parts) > 1 {
		minor, _ = strconv.Atoi(parts[1])
	}
	return major, minor
}

// ── Mode: inventory ───────────────────────────────────────────────────────────

const (
	atlasMaxDBNameBytes    = 64
	atlasMaxNamespaceBytes = 255
	atlasMaxIndexesPerColl = 64
	atlasMaxCompoundFields = 32
)

func (c *collector) inventory() ([]nsEntry, error) {
	c.invOnce.Do(func() {
		logStep("inventory — collStats + GetCollection per collection (concurrency %d)", c.cfg.Concurrency)
		var mu sync.Mutex
		c.invErr = c.forEachCollection(func(dbName, collName string) {
			ns := dbName + "." + collName
			e := nsEntry{Namespace: ns, Database: dbName, Collection: collName, ThroughputScope: "shared"}
			st, err := getCollStats(c.ctx, c.client.Database(dbName), collName)
			if err != nil {
				e.Error = "collStats: " + err.Error()
			} else {
				e.Count, e.SizeBytes, e.StorageBytes, e.AvgObjBytes = st.Count, st.Size, st.StorageSize, st.AvgObjSize
			}
			m, err := c.meta(dbName, collName)
			if err != nil {
				e.Error = strings.TrimSpace(e.Error + " GetCollection: " + err.Error())
			} else {
				e.ShardKey, e.Sharded = m.ShardKey, len(m.ShardKey) > 0
				e.MaxContentLength = m.MaxContentLength
				if m.Throughput > 0 || m.AutoscaleMax > 0 {
					e.ThroughputScope = "dedicated"
					e.ThroughputRU, e.AutoscaleMaxRU = m.Throughput, m.AutoscaleMax
				}
			}
			mu.Lock()
			c.inv = append(c.inv, e)
			mu.Unlock()
		})
		sort.Slice(c.inv, func(i, j int) bool { return c.inv[i].Namespace < c.inv[j].Namespace })
	})
	return c.inv, c.invErr
}

func (c *collector) runInventory() []nsEntry {
	inv, err := c.inventory()
	if err != nil {
		logError("inventory: %v", err)
		return inv
	}
	var totalDocs, totalBytes int64
	cap2MB := 0
	for _, e := range inv {
		totalDocs += e.Count
		totalBytes += e.SizeBytes
		if e.Error != "" {
			c.sink.add(sevAction, "inventory", e.Namespace, "STATS_ERROR", e.Error)
		}
		if len(e.Database) > atlasMaxDBNameBytes {
			c.sink.add(sevBlocker, "inventory", e.Namespace, "DB_NAME_TOO_LONG", fmt.Sprintf("database name is %d bytes; MongoDB limit is %d", len(e.Database), atlasMaxDBNameBytes))
		}
		if len(e.Namespace) > atlasMaxNamespaceBytes {
			c.sink.add(sevBlocker, "inventory", e.Namespace, "NAMESPACE_TOO_LONG", fmt.Sprintf("namespace is %d bytes; MongoDB limit is %d", len(e.Namespace), atlasMaxNamespaceBytes))
		}
		if e.Error == "" && e.Count == 0 {
			c.sink.add(sevAction, "inventory", e.Namespace, "EMPTY_COLLECTION", "dsync does not create empty collections on the destination — pre-create it (migrateIndexes with ensure_collections: true) or accept it will not exist on Atlas")
		}
		if e.MaxContentLength > 0 && e.MaxContentLength <= 2*1024*1024 {
			cap2MB++
		}
	}
	if cap2MB > 0 {
		c.sink.add(sevInfo, "inventory", "", "DOC_CAP_2MB", fmt.Sprintf("%d/%d collections report a 2 MB document cap (account feature EnableMongo16MBDocumentSupport is off) — irrelevant for Cosmos → Atlas, but the reverse/rollback flow cannot write back Atlas documents that grew past 2 MB", cap2MB, len(inv)))
	}
	logOk("inventory: %d namespaces, %s documents, %s data", len(inv), fmtInt(totalDocs), fmtBytes(totalBytes))
	return inv
}

// ── Mode: idtypes ─────────────────────────────────────────────────────────────
//
// Two index-only probes per collection (min and max _id by BSON sort order).
// Mixed types land at different ends of the _id index, so min-type != max-type
// is a definitive "mixed". Same technique as docdb-to-mongodb/fixIdTypes.

func probeIDType(ctx context.Context, coll *mongo.Collection, dir int) (string, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$sort", Value: bson.D{{Key: "_id", Value: dir}}}},
		{{Key: "$limit", Value: 1}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 0}, {Key: "t", Value: bson.D{{Key: "$type", Value: "$_id"}}}}}},
	}
	var t string
	err := withRetry(ctx, "idtype probe "+coll.Name(), func() error {
		cur, err := coll.Aggregate(ctx, pipeline)
		if err != nil {
			return err
		}
		defer cur.Close(ctx)
		t = ""
		if cur.Next(ctx) {
			var row struct {
				T string `bson:"t"`
			}
			if err := cur.Decode(&row); err != nil {
				return err
			}
			t = row.T
		}
		return cur.Err()
	})
	return t, err
}

func (c *collector) runIDTypes(report bool) []idTypeEntry {
	logStep("idtypes — ASC/DESC $type probes (2 index seeks per collection)")
	var mu sync.Mutex
	var out []idTypeEntry
	err := c.forEachCollection(func(dbName, collName string) {
		ns := dbName + "." + collName
		coll := c.client.Database(dbName).Collection(collName)
		e := idTypeEntry{Namespace: ns}
		minT, err := probeIDType(c.ctx, coll, 1)
		if err == nil && minT != "" {
			var maxT string
			maxT, err = probeIDType(c.ctx, coll, -1)
			e.MinType, e.MaxType = minT, maxT
		}
		switch {
		case err != nil:
			e.Status, e.Error = "ERROR", err.Error()
		case minT == "":
			e.Status = "EMPTY"
		case e.MinType != e.MaxType:
			e.Status = "MIXED"
		case minT == "objectId":
			e.Status = "OK"
		default:
			e.Status = "SLOW_PATH"
		}
		mu.Lock()
		out = append(out, e)
		mu.Unlock()
	})
	if err != nil {
		logError("idtypes: %v", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Namespace < out[j].Namespace })
	if !report {
		return out
	}
	for _, e := range out {
		switch e.Status {
		case "MIXED":
			c.sink.add(sevBlocker, "idtypes", e.Namespace, "MIXED_ID_TYPES", fmt.Sprintf("_id types range %s … %s — dsync's Cosmos partitioning does not support mixed _id types; the _id values must be made uniform at source before dsync starts (no Cosmos-safe automated fix exists yet — see the suite README)", e.MinType, e.MaxType))
		case "SLOW_PATH":
			c.sink.add(sevAction, "idtypes", e.Namespace, "NON_OBJECTID_ID", fmt.Sprintf("_id type is %s — dsync cannot range-partition it: single-task initial sync, no parallelism, full re-read on restart. Isolate in its own wave and start it first", e.MinType))
		case "ERROR":
			c.sink.add(sevAction, "idtypes", e.Namespace, "PROBE_ERROR", e.Error)
		}
	}
	return out
}

// ── Mode: duplicates ──────────────────────────────────────────────────────────
//
// On a sharded Cosmos collection _id is unique only per shard-key value, so
// two documents may share an _id. dsync addresses Atlas documents by _id and
// overwrites silently — one of the pair vanishes. We stream _id in server
// sorted order (Cosmos merges across partitions) and detect adjacent equals:
// O(1) memory, one index scan, resumable across 16500 throttling.

func numericValue(v bson.RawValue) (float64, bool) {
	switch v.Type {
	case bsontype.Int32:
		return float64(v.Int32()), true
	case bsontype.Int64:
		return float64(v.Int64()), true
	case bsontype.Double:
		return v.Double(), true
	}
	return 0, false
}

// idEqual mirrors the _id index semantics: numeric types compare by value.
func idEqual(a, b bson.RawValue) bool {
	if a.Type == b.Type {
		return bytes.Equal(a.Value, b.Value)
	}
	af, aok := numericValue(a)
	bf, bok := numericValue(b)
	return aok && bok && af == bf
}

func rawValueJSON(v bson.RawValue) string {
	if v.Type == 0 {
		return "null" // field absent from the document
	}
	doc := bson.D{{Key: "v", Value: v}}
	b, err := bson.MarshalExtJSON(doc, true, false)
	if err != nil {
		return strconv.Quote(v.String())
	}
	s := string(b)
	s = strings.TrimPrefix(s, `{"v":`)
	s = strings.TrimSuffix(s, `}`)
	return s
}

// sleepCtx waits for d unless the context is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

func lookupPath(doc bson.Raw, path string) (bson.RawValue, bool) {
	v, err := doc.LookupErr(strings.Split(path, ".")...)
	if err != nil {
		return bson.RawValue{}, false
	}
	return v, true
}

func (c *collector) scanDuplicates(dbName, collName string, shardKey []string) dupEntry {
	ns := dbName + "." + collName
	e := dupEntry{Namespace: ns}
	coll := c.client.Database(dbName).Collection(collName)

	proj := bson.D{{Key: "_id", Value: 1}}
	for _, k := range shardKey {
		proj = append(proj, bson.E{Key: k, Value: 1})
	}

	var prev *bson.RawValue
	var prevDoc bson.Raw
	seenOfPrev := 0 // consecutive docs seen with _id == prev (for exact resume)
	var lastID *bson.RawValue
	skipOfLast := 0

	for attempt := 0; attempt < 50; attempt++ {
		filter := bson.D{}
		if lastID != nil {
			filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gte", Value: *lastID}}}}
			skipOfLast = seenOfPrev
		}
		opts := options.Find().SetProjection(proj).SetSort(bson.D{{Key: "_id", Value: 1}}).SetBatchSize(1000)
		cur, err := coll.Find(c.ctx, filter, opts)
		if err != nil {
			if wait, ok := isThrottled(err); ok {
				if serr := sleepCtx(c.ctx, wait); serr != nil {
					e.Error = serr.Error()
					return e
				}
				continue
			}
			e.Error = err.Error()
			return e
		}
		iterErr := func() error {
			defer cur.Close(c.ctx)
			for cur.Next(c.ctx) {
				id := cur.Current.Lookup("_id")
				if lastID != nil && skipOfLast > 0 && idEqual(id, *lastID) {
					skipOfLast--
					continue
				}
				skipOfLast = 0
				e.Scanned++
				if prev != nil && idEqual(id, *prev) {
					seenOfPrev++
					e.Duplicates++
					if len(e.Samples) < c.cfg.MaxReport {
						e.Samples = append(e.Samples, describeDup(prevDoc, cur.Current, shardKey))
					}
				} else {
					seenOfPrev = 1
				}
				idCopy := bson.RawValue{Type: id.Type, Value: append([]byte(nil), id.Value...)}
				prev = &idCopy
				prevDoc = append(bson.Raw(nil), cur.Current...)
				lastID = &idCopy
			}
			return cur.Err()
		}()
		if iterErr == nil {
			return e
		}
		if wait, ok := isThrottled(iterErr); ok {
			logWarn("duplicates %s throttled mid-scan after %s docs — resuming from last _id in %s", ns, fmtInt(e.Scanned), wait)
			if serr := sleepCtx(c.ctx, wait); serr != nil {
				e.Error = serr.Error()
				return e
			}
			continue
		}
		e.Error = iterErr.Error()
		return e
	}
	e.Error = "gave up after repeated throttling"
	return e
}

func describeDup(a, b bson.Raw, shardKey []string) string {
	id := a.Lookup("_id")
	parts := []string{`"_id":` + rawValueJSON(id)}
	for _, k := range shardKey {
		va, _ := lookupPath(a, k)
		vb, _ := lookupPath(b, k)
		parts = append(parts, fmt.Sprintf(`%q:[%s,%s]`, k, rawValueJSON(va), rawValueJSON(vb)))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (c *collector) runDuplicates() []dupEntry {
	logStep("duplicates — sorted _id stream on sharded collections (1 index scan each)")
	inv, err := c.inventory()
	if err != nil {
		logError("duplicates: %v", err)
		return nil
	}
	var out []dupEntry
	var mu sync.Mutex
	sem := make(chan struct{}, c.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, ns := range inv {
		if !ns.Sharded || ns.Error != "" {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(ns nsEntry) {
			defer wg.Done()
			defer func() { <-sem }()
			e := c.scanDuplicates(ns.Database, ns.Collection, ns.ShardKey)
			mu.Lock()
			out = append(out, e)
			mu.Unlock()
		}(ns)
	}
	wg.Wait()
	sort.Slice(out, func(i, j int) bool { return out[i].Namespace < out[j].Namespace })
	for _, e := range out {
		switch {
		case e.Error != "":
			c.sink.add(sevAction, "duplicates", e.Namespace, "SCAN_ERROR", e.Error)
		case e.Duplicates > 0:
			c.sink.add(sevBlocker, "duplicates", e.Namespace, "DUPLICATE_IDS", fmt.Sprintf("%s duplicate _id value(s) across shard-key values (scanned %s) — dsync overwrites by _id on Atlas, one document of each pair is lost silently. Re-key or remove one side of every pair at source before starting (pairs are listed in assessment.json)", fmtInt(e.Duplicates), fmtInt(e.Scanned)))
		default:
			logOk("duplicates %s: none (scanned %s)", e.Namespace, fmtInt(e.Scanned))
		}
	}
	if len(out) == 0 {
		logOk("duplicates: no sharded collections — nothing to scan")
	}
	return out
}

// ── Mode: indexes ─────────────────────────────────────────────────────────────

func keyString(key bson.Raw) string {
	elems, err := key.Elements()
	if err != nil {
		return key.String()
	}
	parts := make([]string, 0, len(elems))
	for _, el := range elems {
		parts = append(parts, fmt.Sprintf("%s:%s", el.Key(), rawValueJSON(el.Value())))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func (c *collector) runIndexes() []indexEntry {
	logStep("indexes — listIndexes per collection, Atlas portability rules")
	invMap := map[string]nsEntry{}
	if inv, err := c.inventory(); err == nil {
		for _, e := range inv {
			invMap[e.Namespace] = e
		}
	}
	var mu sync.Mutex
	var out []indexEntry
	perColl := map[string]int{}
	err := c.forEachCollection(func(dbName, collName string) {
		ns := dbName + "." + collName
		coll := c.client.Database(dbName).Collection(collName)
		var docs []bson.Raw
		err := withRetry(c.ctx, "listIndexes "+ns, func() error {
			cur, err := coll.Indexes().List(c.ctx)
			if err != nil {
				return err
			}
			defer cur.Close(c.ctx)
			docs = nil
			for cur.Next(c.ctx) {
				docs = append(docs, append(bson.Raw(nil), cur.Current...))
			}
			return cur.Err()
		})
		if err != nil {
			c.sink.add(sevAction, "indexes", ns, "LIST_ERROR", err.Error())
			return
		}
		sharded := invMap[ns].Sharded
		var local []indexEntry
		for _, d := range docs {
			name, _ := d.Lookup("name").StringValueOK()
			if name == "_id_" {
				continue
			}
			keyDoc, ok := d.Lookup("key").DocumentOK()
			if !ok {
				continue
			}
			e := indexEntry{Namespace: ns, Name: name, Key: keyString(keyDoc), Kind: "regular", Portability: "OK"}
			if u, ok := d.Lookup("unique").BooleanOK(); ok && u {
				e.Unique = true
			}
			if ttlV, err := d.LookupErr("expireAfterSeconds"); err == nil {
				if secs, ok := numericValue(ttlV); ok {
					s := int64(secs)
					e.TTLSeconds = &s
				}
			}
			elems, _ := keyDoc.Elements()
			fieldCount := len(elems)
			isWild, isText, isGeo, ttlOnTS := false, false, false, false
			for _, el := range elems {
				k := el.Key()
				if strings.Contains(k, "$**") {
					isWild = true
				}
				if sv, ok := el.Value().StringValueOK(); ok {
					switch sv {
					case "text":
						isText = true
					case "2dsphere", "2d", "geoHaystack":
						isGeo = true
					}
				}
				if k == "_ts" && e.TTLSeconds != nil {
					ttlOnTS = true
				}
			}
			switch {
			case e.TTLSeconds != nil:
				e.Kind = "ttl"
			case e.Unique:
				e.Kind = "unique"
			case isWild:
				e.Kind = "wildcard"
			case isText:
				e.Kind = "text"
			case isGeo:
				e.Kind = "geo"
			}
			if ttlOnTS {
				e.Portability = "NOT_PORTABLE"
				e.Notes = append(e.Notes, "TTL keyed on Cosmos system field _ts — Atlas has no _ts; the application must nominate a real date field or TTL semantics are lost")
				c.sink.add(sevAction, "indexes", ns, "TTL_ON_TS", fmt.Sprintf("index %s: TTL on _ts (%ds) cannot be recreated on Atlas — choose a date field (migrateIndexes --ttl-field-map) or accept loss of expiry", name, *e.TTLSeconds))
			} else if e.TTLSeconds != nil {
				e.Notes = append(e.Notes, "create only at cutover — a TTL firing on Atlas during migration deletes documents that still exist on Cosmos and corrupts the deletes-emulation witness (playbook C-18)")
				c.sink.add(sevAction, "indexes", ns, "TTL_DEFER", fmt.Sprintf("index %s: TTL index — create at cutover, not during CDC (witness corruption)", name))
			}
			if e.Unique {
				note := "create as unique:false, convert with prepareUnique → unique at cutover (dsync replays full-document replaces out of order — duplicate-key errors during CDC otherwise)"
				if sharded {
					note += "; Cosmos forces unique indexes on sharded collections to be prefixed by the shard key, so this index is globally unique on Atlas too"
				}
				e.Notes = append(e.Notes, note)
				c.sink.add(sevAction, "indexes", ns, "UNIQUE_DEFER", fmt.Sprintf("index %s: unique — build non-unique first, rectify at cutover", name))
			}
			if isText {
				e.Notes = append(e.Notes, "text index — verify field types on Atlas after sync")
			}
			if fieldCount > atlasMaxCompoundFields {
				e.Portability = "NOT_PORTABLE"
				c.sink.add(sevBlocker, "indexes", ns, "TOO_MANY_KEY_FIELDS", fmt.Sprintf("index %s has %d fields; MongoDB compound index limit is %d", name, fieldCount, atlasMaxCompoundFields))
			}
			local = append(local, e)
		}
		mu.Lock()
		out = append(out, local...)
		perColl[ns] = len(local) + 1 // + _id_
		mu.Unlock()
	})
	if err != nil {
		logError("indexes: %v", err)
	}
	for ns, n := range perColl {
		if n > atlasMaxIndexesPerColl {
			c.sink.add(sevBlocker, "indexes", ns, "TOO_MANY_INDEXES", fmt.Sprintf("%d indexes; MongoDB limit is %d per collection", n, atlasMaxIndexesPerColl))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].Name < out[j].Name
	})
	logOk("indexes: %d non-_id indexes across %d collections", len(out), len(perColl))
	return out
}

// ── Mode: changestreams ───────────────────────────────────────────────────────
//
// Functional probe: open the one pipeline shape Cosmos accepts, then close.
// Token donor = unsharded. dsync obtains the T0 resume token by inserting a
// dummy document into each source collection; on a sharded collection that
// insert fails in dsync v0.28/v0.29 (shardKeyDefinition type-assertion), the
// namespace falls back to startAtOperationTime, and if NO namespace in the
// flow produced a token, CDC panics while initial sync looks healthy.

func (c *collector) runChangeStreams(report bool) []csEntry {
	logStep("changestreams — functional watch() probe + token-donor classification + physical partitions")
	var mu sync.Mutex
	var out []csEntry
	err := c.forEachCollection(func(dbName, collName string) {
		ns := dbName + "." + collName
		db := c.client.Database(dbName)
		coll := db.Collection(collName)
		e := csEntry{Namespace: ns}

		m, err := c.meta(dbName, collName)
		if err != nil {
			e.Detail = "GetCollection: " + err.Error()
		} else {
			e.TokenDonor = len(m.ShardKey) == 0
		}

		opts := options.ChangeStream().SetFullDocument(options.UpdateLookup).SetMaxAwaitTime(300 * time.Millisecond)
		err = withRetry(c.ctx, "watch "+ns, func() error {
			cs, err := coll.Watch(c.ctx, cosmosChangeStreamPipeline(), opts)
			if err != nil {
				return err
			}
			return cs.Close(c.ctx)
		})
		if err != nil {
			e.Status, e.Detail = "FAILED", strings.TrimSpace(e.Detail+" watch: "+err.Error())
		} else {
			e.Status = "ENABLED"
		}

		// Physical partitions = number of change-stream cursors any CDC client
		// needs for this collection. Cosmos exposes it via GetChangeStreamTokens.
		var tok bson.M
		tsNow := primitive.Timestamp{T: uint32(time.Now().Unix())}
		if err := withRetry(c.ctx, "GetChangeStreamTokens "+ns, func() error {
			return db.RunCommand(c.ctx, bson.D{
				{Key: "customAction", Value: "GetChangeStreamTokens"},
				{Key: "collection", Value: collName},
				{Key: "startAtOperationTime", Value: tsNow},
			}).Decode(&tok)
		}); err == nil {
			if arr, ok := tok["resumeAfterTokens"].(primitive.A); ok {
				e.PhysicalPartitions = len(arr)
			}
		}
		mu.Lock()
		out = append(out, e)
		mu.Unlock()
	})
	if err != nil {
		logError("changestreams: %v", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Namespace < out[j].Namespace })
	donors, partitions := 0, 0
	for _, e := range out {
		partitions += e.PhysicalPartitions
		if e.TokenDonor {
			donors++
		}
		if !report {
			continue
		}
		if e.Status == "FAILED" {
			c.sink.add(sevBlocker, "changestreams", e.Namespace, "WATCH_FAILED", e.Detail)
		} else if !e.TokenDonor {
			c.sink.add(sevInfo, "changestreams", e.Namespace, "SHARDED_NO_TOKEN", "sharded — dsync's dummy-insert token retrieval fails here; the flow needs at least one unsharded namespace (token donor)")
		}
	}
	logOk("changestreams: %d/%d collections opened; %d token donors; %d physical partitions total", len(out)-c.countFailed(out), len(out), donors, partitions)
	if !report {
		return out
	}
	if donors == 0 && len(out) > 0 {
		c.sink.add(sevBlocker, "changestreams", "", "NO_TOKEN_DONOR", "every collection is sharded — no dsync flow can obtain a T0 resume token; create a small unsharded collection (with ≥1 document) per flow")
	}
	if len(out) > 15 {
		c.sink.add(sevAction, "changestreams", "", "MANY_STREAMS", fmt.Sprintf("%d collections → %d parallel change-stream cursors in CDC at once; dsync caps a flow at 8 namespaces (\"keep under 15\") — plan multiple flows (waves) and confirm RU headroom with Adiom", len(out), partitions))
	}
	return out
}

func (c *collector) countFailed(in []csEntry) int {
	n := 0
	for _, e := range in {
		if e.Status == "FAILED" {
			n++
		}
	}
	return n
}

// ── Mode: residue ─────────────────────────────────────────────────────────────
//
// dsync's token retrieval inserts a dummy document and deletes it afterwards.
// If the delete is throttled or the process dies in between, the dummy stays:
//   unsharded → a document whose only field is _id
//   sharded   → _id plus every shard-key field set to "" and nothing else

func (c *collector) runResidue() []residueEntry {
	logStep("residue — dsync dummy documents left on the source")
	var mu sync.Mutex
	var out []residueEntry
	err := c.forEachCollection(func(dbName, collName string) {
		ns := dbName + "." + collName
		coll := c.client.Database(dbName).Collection(collName)
		e := residueEntry{Namespace: ns}
		m, err := c.meta(dbName, collName)
		if err != nil {
			e.Error = "GetCollection: " + err.Error()
		} else {
			filter := bson.D{}
			for _, k := range m.ShardKey {
				filter = append(filter, bson.E{Key: k, Value: ""})
			}
			filter = append(filter, bson.E{Key: "$expr", Value: bson.D{{Key: "$eq", Value: bson.A{
				bson.D{{Key: "$size", Value: bson.D{{Key: "$objectToArray", Value: "$$ROOT"}}}},
				1 + len(m.ShardKey),
			}}}})
			err = withRetry(c.ctx, "residue "+ns, func() error {
				cur, err := coll.Find(c.ctx, filter, options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
				if err != nil {
					return err
				}
				defer cur.Close(c.ctx)
				e.Found, e.Samples = 0, nil
				for cur.Next(c.ctx) {
					e.Found++
					if len(e.Samples) < c.cfg.MaxReport {
						e.Samples = append(e.Samples, rawValueJSON(cur.Current.Lookup("_id")))
					}
				}
				return cur.Err()
			})
			if err != nil {
				e.Error = err.Error()
			}
		}
		mu.Lock()
		out = append(out, e)
		mu.Unlock()
	})
	if err != nil {
		logError("residue: %v", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Namespace < out[j].Namespace })
	total := int64(0)
	for _, e := range out {
		total += e.Found
		switch {
		case e.Error != "":
			c.sink.add(sevAction, "residue", e.Namespace, "SCAN_ERROR", e.Error)
		case e.Found > 0:
			c.sink.add(sevAction, "residue", e.Namespace, "DSYNC_RESIDUE", fmt.Sprintf("%d document(s) shaped like dsync dummy records (%s) — confirm with the application owner and delete by _id", e.Found, strings.Join(e.Samples, " ")))
		}
	}
	if total == 0 {
		logOk("residue: none found")
	}
	return out
}

// ── Mode: waves ───────────────────────────────────────────────────────────────
//
// Constraints (playbook §2.6):
//   • ≤ max_namespaces per wave (dsync hard-fails GeneratePlan above the cap)
//   • ≥ 1 token donor (unsharded collection) per wave
//   • MIXED / SLOW_PATH collections isolated (own wave, largest first)
//   • otherwise balance by data size

type waveItem struct {
	NS         string
	Size       int64
	Docs       int64
	Donor      bool
	Special    string // "" | MIXED | SLOW_PATH
	Partitions int
}

func (c *collector) runWaves(idTypes []idTypeEntry, cs []csEntry) []waveEntry {
	logStep("waves — bin-pack into flows of ≤ %d namespaces with ≥ 1 token donor each", c.cfg.Waves.MaxNamespaces)
	inv, err := c.inventory()
	if err != nil {
		logError("waves: %v", err)
		return nil
	}
	allowed := map[string]bool{}
	if c.cfg.Waves.NamespacesFile != "" {
		b, err := os.ReadFile(c.cfg.Waves.NamespacesFile)
		if err != nil {
			logError("waves: read namespaces_file: %v", err)
			return nil
		}
		for _, line := range strings.Split(string(b), "\n") {
			if l := strings.TrimSpace(line); l != "" && !strings.HasPrefix(l, "#") {
				allowed[l] = true
			}
		}
	}
	idBy := map[string]string{}
	for _, e := range idTypes {
		idBy[e.Namespace] = e.Status
	}
	partBy := map[string]int{}
	for _, e := range cs {
		partBy[e.Namespace] = e.PhysicalPartitions
	}

	var items []waveItem
	for _, e := range inv {
		if e.Error != "" || (len(allowed) > 0 && !allowed[e.Namespace]) {
			continue
		}
		it := waveItem{NS: e.Namespace, Size: e.SizeBytes, Docs: e.Count, Donor: !e.Sharded, Partitions: partBy[e.Namespace]}
		if s := idBy[e.Namespace]; s == "MIXED" || s == "SLOW_PATH" {
			it.Special = s
		}
		items = append(items, it)
	}
	if len(items) == 0 {
		logWarn("waves: no namespaces to plan")
		return nil
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Size > items[j].Size })

	max := c.cfg.Waves.MaxNamespaces
	var special, donors, regular []waveItem
	for _, it := range items {
		switch {
		case it.Special != "":
			special = append(special, it)
		case it.Donor:
			donors = append(donors, it)
		default:
			regular = append(regular, it)
		}
	}
	// Donors that are themselves special still count as their own donor.
	regularCount := len(donors) + len(regular)
	numRegularWaves := int(math.Ceil(float64(regularCount) / float64(max)))

	type wave struct {
		items []waveItem
		size  int64
		donor string
		notes []string
	}
	var waves []*wave
	// 1. one dedicated wave per special collection
	for _, s := range special {
		w := &wave{items: []waveItem{s}, size: s.Size}
		w.notes = append(w.notes, fmt.Sprintf("isolated: %s _id (%s)", s.Special, s.NS))
		if s.Donor {
			w.donor = s.NS
		}
		waves = append(waves, w)
	}
	// 2. regular waves, seeded with one donor each (largest donors first)
	regularWaves := make([]*wave, 0, numRegularWaves)
	for i := 0; i < numRegularWaves; i++ {
		regularWaves = append(regularWaves, &wave{})
	}
	di := 0
	for _, w := range regularWaves {
		if di < len(donors) {
			w.items = append(w.items, donors[di])
			w.size += donors[di].Size
			w.donor = donors[di].NS
			di++
		}
	}
	// 3. special waves without a donor borrow the smallest remaining donor
	remainingDonors := donors[di:]
	sort.Slice(remainingDonors, func(i, j int) bool { return remainingDonors[i].Size < remainingDonors[j].Size })
	ri := 0
	for _, w := range waves {
		if w.donor == "" && ri < len(remainingDonors) && len(w.items) < max {
			w.items = append(w.items, remainingDonors[ri])
			w.size += remainingDonors[ri].Size
			w.donor = remainingDonors[ri].NS
			ri++
		}
	}
	// 4. everything else: largest-first into the lightest regular wave with room
	rest := append(append([]waveItem{}, remainingDonors[ri:]...), regular...)
	sort.Slice(rest, func(i, j int) bool { return rest[i].Size > rest[j].Size })
	for _, it := range rest {
		var target *wave
		for _, w := range regularWaves {
			if len(w.items) < max && (target == nil || w.size < target.size) {
				target = w
			}
		}
		if target == nil {
			target = &wave{}
			regularWaves = append(regularWaves, target)
		}
		target.items = append(target.items, it)
		target.size += it.Size
		if target.donor == "" && it.Donor {
			target.donor = it.NS
		}
	}
	waves = append(waves, regularWaves...)

	var out []waveEntry
	totalPartitions := 0
	for _, w := range waves {
		if len(w.items) == 0 {
			continue
		}
		name := fmt.Sprintf("wave%02d", len(out)+1)
		we := waveEntry{Wave: name, SizeBytes: w.size, Donor: w.donor, Notes: w.notes}
		for _, it := range w.items {
			we.Namespaces = append(we.Namespaces, it.NS)
			we.Docs += it.Docs
			we.Partitions += it.Partitions
		}
		sort.Strings(we.Namespaces)
		totalPartitions += we.Partitions
		if w.donor == "" {
			we.Notes = append(we.Notes, "NO_DONOR")
			c.sink.add(sevBlocker, "waves", name, "NO_DONOR", fmt.Sprintf("wave has no unsharded collection — dsync cannot obtain a T0 resume token; CDC will panic after initial sync. Add a small unsharded collection (≥1 document) to this wave or merge it into one that has a donor. Namespaces: %s", strings.Join(we.Namespaces, ", ")))
		}
		if len(we.Namespaces) > max {
			c.sink.add(sevBlocker, "waves", name, "OVER_CAP", fmt.Sprintf("%d namespaces > cap %d", len(we.Namespaces), max))
		}
		out = append(out, we)
	}
	logOk("waves: %d flows for %d namespaces; %d physical partitions (= concurrent change-stream cursors at full CDC)", len(out), len(items), totalPartitions)
	if len(out) > 1 {
		c.sink.add(sevInfo, "waves", "", "MULTI_FLOW", fmt.Sprintf("%d dsynct flows required — one --workflow-id / --queue-name / worker set each; all in CDC simultaneously until cutover unless a phased cutover is chosen", len(out)))
	}
	return out
}

// ── Outputs ───────────────────────────────────────────────────────────────────

func writeJSON(path string, v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func writeWavesCSV(path string, waves []waveEntry, inv []nsEntry, idTypes []idTypeEntry) error {
	invBy := map[string]nsEntry{}
	for _, e := range inv {
		invBy[e.Namespace] = e
	}
	idBy := map[string]string{}
	for _, e := range idTypes {
		idBy[e.Namespace] = e.Status
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"wave", "ns", "sizeBytes", "count", "idType", "cosmosSharded", "cosmosShardKey", "tokenDonor", "atlasSharded", "atlasShardKey", "fullDocumentKeyNeeded", "notes"})
	for _, wv := range waves {
		for _, ns := range wv.Namespaces {
			e := invBy[ns]
			sharded := "no"
			if e.Sharded {
				sharded = "yes"
			}
			donor := "no"
			if ns == wv.Donor {
				donor = "yes"
			}
			_ = w.Write([]string{wv.Wave, ns, strconv.FormatInt(e.SizeBytes, 10), strconv.FormatInt(e.Count, 10), idBy[ns], sharded, strings.Join(e.ShardKey, ","), donor, "", "", "", strings.Join(wv.Notes, "; ")})
		}
	}
	w.Flush()
	return w.Error()
}

func writeWaveEnvs(dir string, waves []waveEntry) error {
	for _, wv := range waves {
		var b strings.Builder
		fmt.Fprintf(&b, "WAVE=%s\n", wv.Wave)
		fmt.Fprintf(&b, "WORKFLOW_ID=${ENGAGEMENT}-%s\n", wv.Wave)
		fmt.Fprintf(&b, "QUEUE_NAME=%s\n", wv.Wave)
		parts := make([]string, 0, len(wv.Namespaces))
		for _, ns := range wv.Namespaces {
			parts = append(parts, "--namespace "+ns)
		}
		fmt.Fprintf(&b, "WAVE_NAMESPACES=\"%s\"\n", strings.Join(parts, " "))
		if wv.Donor != "" {
			fmt.Fprintf(&b, "# token donor: %s\n", wv.Donor)
		}
		for _, n := range wv.Notes {
			fmt.Fprintf(&b, "# %s\n", n)
		}
		if err := os.WriteFile(filepath.Join(dir, wv.Wave+".env"), []byte(b.String()), 0644); err != nil {
			return err
		}
	}
	return nil
}

func writeMarkdown(path string, r *report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Cosmos DB for MongoDB (RU) → Atlas: dsync readiness assessment\n\n")
	fmt.Fprintf(&b, "Generated %s · modes: %s\n\n", r.Timestamp, strings.Join(r.Modes, ", "))
	fmt.Fprintf(&b, "| Severity | Count |\n|---|---|\n| BLOCKER | %d |\n| ACTION | %d |\n| INFO | %d |\n\n", r.Summary[sevBlocker], r.Summary[sevAction], r.Summary[sevInfo])

	if r.Account != nil {
		fmt.Fprintf(&b, "## Account\n\n| | |\n|---|---|\n| Hosts | %s |\n| Flavor | %s |\n| Server version | %s |\n| maxBsonObjectSize | %d MiB |\n\n", strings.Join(r.Account.Hosts, ", "), r.Account.Flavor, r.Account.ServerVersion, r.Account.MaxBSONObjectMiB)
	}
	if len(r.Findings) > 0 {
		fmt.Fprintf(&b, "## Findings\n\n| Severity | Mode | Namespace | Code | Message |\n|---|---|---|---|---|\n")
		order := map[string]int{sevBlocker: 0, sevAction: 1, sevInfo: 2}
		fs := append([]finding(nil), r.Findings...)
		sort.SliceStable(fs, func(i, j int) bool {
			if order[fs[i].Severity] != order[fs[j].Severity] {
				return order[fs[i].Severity] < order[fs[j].Severity]
			}
			return fs[i].Namespace < fs[j].Namespace
		})
		for _, f := range fs {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s |\n", f.Severity, f.Mode, f.Namespace, f.Code, strings.ReplaceAll(f.Message, "|", "\\|"))
		}
		b.WriteString("\n")
	}
	if len(r.Inventory) > 0 {
		fmt.Fprintf(&b, "## Inventory\n\n| Namespace | Docs | Data | Avg doc | Shard key | Throughput | Doc cap |\n|---|---|---|---|---|---|---|\n")
		for _, e := range r.Inventory {
			tp := "shared"
			if e.ThroughputScope == "dedicated" {
				if e.AutoscaleMaxRU > 0 {
					tp = fmt.Sprintf("autoscale ≤ %d RU/s", e.AutoscaleMaxRU)
				} else {
					tp = fmt.Sprintf("%d RU/s", e.ThroughputRU)
				}
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s |\n", e.Namespace, fmtInt(e.Count), fmtBytes(e.SizeBytes), fmtBytes(e.AvgObjBytes), strings.Join(e.ShardKey, ","), tp, fmtBytes(e.MaxContentLength))
		}
		b.WriteString("\n")
	}
	if len(r.IDTypes) > 0 {
		fmt.Fprintf(&b, "## _id types\n\n| Namespace | Status | min type | max type |\n|---|---|---|---|\n")
		for _, e := range r.IDTypes {
			fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", e.Namespace, e.Status, e.MinType, e.MaxType)
		}
		b.WriteString("\n")
	}
	if len(r.Duplicates) > 0 {
		fmt.Fprintf(&b, "## Duplicate _id across shard-key values\n\n| Namespace | Scanned | Duplicates | Samples |\n|---|---|---|---|\n")
		for _, e := range r.Duplicates {
			fmt.Fprintf(&b, "| %s | %s | %s | `%s` |\n", e.Namespace, fmtInt(e.Scanned), fmtInt(e.Duplicates), strings.Join(e.Samples, " "))
		}
		b.WriteString("\n")
	}
	if len(r.Indexes) > 0 {
		fmt.Fprintf(&b, "## Indexes (excluding _id_)\n\n| Namespace | Name | Key | Kind | Portability | Notes |\n|---|---|---|---|---|---|\n")
		for _, e := range r.Indexes {
			fmt.Fprintf(&b, "| %s | %s | `%s` | %s | %s | %s |\n", e.Namespace, e.Name, e.Key, e.Kind, e.Portability, strings.Join(e.Notes, "; "))
		}
		b.WriteString("\n")
	}
	if len(r.ChangeStreams) > 0 {
		fmt.Fprintf(&b, "## Change streams\n\n| Namespace | Status | Token donor | Physical partitions | Detail |\n|---|---|---|---|---|\n")
		for _, e := range r.ChangeStreams {
			fmt.Fprintf(&b, "| %s | %s | %t | %d | %s |\n", e.Namespace, e.Status, e.TokenDonor, e.PhysicalPartitions, e.Detail)
		}
		b.WriteString("\n")
	}
	if len(r.Residue) > 0 {
		fmt.Fprintf(&b, "## dsync residue\n\n| Namespace | Found | Samples |\n|---|---|---|\n")
		for _, e := range r.Residue {
			fmt.Fprintf(&b, "| %s | %d | `%s` |\n", e.Namespace, e.Found, strings.Join(e.Samples, " "))
		}
		b.WriteString("\n")
	}
	if len(r.Waves) > 0 {
		fmt.Fprintf(&b, "## Proposed waves\n\n| Wave | Namespaces | Docs | Data | Donor | Partitions | Notes |\n|---|---|---|---|---|---|---|\n")
		for _, w := range r.Waves {
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %d | %s |\n", w.Wave, strings.Join(w.Namespaces, "<br>"), fmtInt(w.Docs), fmtBytes(w.SizeBytes), w.Donor, w.Partitions, strings.Join(w.Notes, "; "))
		}
		b.WriteString("\n")
	}
	return os.WriteFile(path, []byte(b.String()), 0644)
}

// ── main ──────────────────────────────────────────────────────────────────────

var allModes = []string{"account", "inventory", "idtypes", "duplicates", "indexes", "changestreams", "residue", "waves"}

func main() {
	cfgPath := flag.String("config", "config.json", "path to JSON config")
	mode := flag.String("mode", "all", "account | inventory | idtypes | duplicates | indexes | changestreams | residue | waves | all (comma-separate to combine)")
	timeout := flag.Duration("timeout", 6*time.Hour, "overall timeout")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	if closer, err := initLogWriter(cfg.LogFile); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	} else if closer != nil {
		defer closer.Close()
	}

	var modes []string
	if *mode == "all" {
		modes = allModes
	} else {
		known := map[string]bool{}
		for _, m := range allModes {
			known[m] = true
		}
		for _, m := range strings.Split(*mode, ",") {
			m = strings.TrimSpace(m)
			if !known[m] {
				fmt.Fprintf(os.Stderr, "error: unknown mode %q — valid: %s, all\n", m, strings.Join(allModes, ", "))
				os.Exit(2)
			}
			modes = append(modes, m)
		}
	}
	want := map[string]bool{}
	for _, m := range modes {
		want[m] = true
	}

	if err := os.MkdirAll(cfg.OutputDir, 0755); err != nil {
		fmt.Fprintln(os.Stderr, "error: output_dir:", err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	printSep()
	logInfo("assessCluster — Cosmos DB for MongoDB (RU) → dsync readiness  |  READ-ONLY")
	logInfo("modes: %s  |  output: %s", strings.Join(modes, ", "), cfg.OutputDir)
	printSep()

	clientOpts := options.Client().ApplyURI(cfg.SourceURI).
		SetAppName("assessCluster").
		SetServerSelectionTimeout(30 * time.Second)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		logError("connect: %v", err)
		os.Exit(1)
	}
	defer client.Disconnect(context.Background())
	if err := client.Ping(ctx, nil); err != nil {
		logError("ping: %v", err)
		os.Exit(1)
	}

	sink := &findingSink{}
	col := newCollector(ctx, client, cfg, sink)
	rep := &report{Timestamp: time.Now().UTC().Format(time.RFC3339), Modes: modes, Summary: map[string]int{}}

	if want["account"] {
		rep.Account = col.runAccount()
	}
	if want["inventory"] {
		rep.Inventory = col.runInventory()
	}
	if want["idtypes"] || want["waves"] {
		rep.IDTypes = col.runIDTypes(want["idtypes"])
	}
	if want["duplicates"] {
		rep.Duplicates = col.runDuplicates()
	}
	if want["indexes"] {
		rep.Indexes = col.runIndexes()
	}
	if want["changestreams"] || want["waves"] {
		rep.ChangeStreams = col.runChangeStreams(want["changestreams"])
	}
	if want["residue"] {
		rep.Residue = col.runResidue()
	}
	if want["waves"] {
		rep.Waves = col.runWaves(rep.IDTypes, rep.ChangeStreams)
		inv, _ := col.inventory()
		if err := writeWavesCSV(filepath.Join(cfg.OutputDir, "waves.csv"), rep.Waves, inv, rep.IDTypes); err != nil {
			logError("waves.csv: %v", err)
		}
		if err := writeWaveEnvs(cfg.OutputDir, rep.Waves); err != nil {
			logError("wave env files: %v", err)
		}
	}
	if !want["idtypes"] {
		rep.IDTypes = nil
	}
	if !want["changestreams"] {
		rep.ChangeStreams = nil
	}
	if !want["inventory"] {
		rep.Inventory = nil
	}

	rep.Findings = sink.list
	if rep.Findings == nil {
		rep.Findings = []finding{}
	}
	rep.Summary[sevBlocker] = sink.count(sevBlocker)
	rep.Summary[sevAction] = sink.count(sevAction)
	rep.Summary[sevInfo] = sink.count(sevInfo)

	if err := writeJSON(filepath.Join(cfg.OutputDir, "assessment.json"), rep); err != nil {
		logError("assessment.json: %v", err)
	}
	if err := writeMarkdown(filepath.Join(cfg.OutputDir, "assessment.md"), rep); err != nil {
		logError("assessment.md: %v", err)
	}

	printSep()
	logInfo("SUMMARY  BLOCKER: %d  |  ACTION: %d  |  INFO: %d", rep.Summary[sevBlocker], rep.Summary[sevAction], rep.Summary[sevInfo])
	logInfo("reports: %s/assessment.json  %s/assessment.md", cfg.OutputDir, cfg.OutputDir)
	printSep()

	switch {
	case rep.Summary[sevBlocker] > 0:
		os.Exit(3)
	case rep.Summary[sevAction] > 0:
		os.Exit(4)
	}
}
