// checkChangeStreams inspects an Amazon DocumentDB (or MongoDB) cluster and
// reports, for every collection, whether a change stream can be opened.
//
// DocumentDB does not expose a reliable "list which collections have change
// streams enabled" command, so the dependable signal is functional: try to
// open a change stream. If it opens, streams are enabled for that scope; if
// DocumentDB rejects it because change streams are not enabled, we report
// disabled. Any other error is surfaced as UNKNOWN so it isn't mistaken for a
// definitive "disabled".
//
// Usage:
//
//	export DOCDB_URI="mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred"
//	go run . -ca-file global-bundle.pem
//	go run . -ca-file global-bundle.pem -db mydb          # limit to one database
//	go run . -json                                        # machine-readable output
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type status string

const (
	statusEnabled  status = "ENABLED"
	statusDisabled status = "DISABLED"
	statusUnknown  status = "UNKNOWN"
)

type result struct {
	Database   string `json:"database"`
	Collection string `json:"collection"`
	Status     status `json:"status"`
	Detail     string `json:"detail,omitempty"`
}

// System databases we skip by default.
var systemDBs = map[string]bool{
	"admin":  true,
	"config": true,
	"local":  true,
}

func main() {
	uri := flag.String("uri", os.Getenv("DOCDB_URI"), "connection string (defaults to $DOCDB_URI)")
	caFile := flag.String("ca-file", "", "path to CA bundle (e.g. global-bundle.pem) for DocumentDB TLS")
	onlyDB := flag.String("db", "", "restrict the check to a single database")
	asJSON := flag.Bool("json", false, "emit results as JSON")
	timeout := flag.Duration("timeout", 30*time.Second, "overall operation timeout")
	flag.Parse()

	if *uri == "" {
		fmt.Fprintln(os.Stderr, "error: no connection string; set -uri or $DOCDB_URI")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	clientOpts := options.Client().ApplyURI(*uri)
	if *caFile != "" {
		tlsCfg, err := tlsConfigFromCA(*caFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: loading CA file: %v\n", err)
			os.Exit(1)
		}
		clientOpts.SetTLSConfig(tlsCfg)
	}

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: connect: %v\n", err)
		os.Exit(1)
	}
	defer client.Disconnect(context.Background())

	if err := client.Ping(ctx, nil); err != nil {
		fmt.Fprintf(os.Stderr, "error: ping: %v\n", err)
		os.Exit(1)
	}

	dbNames, err := listDatabases(ctx, client, *onlyDB)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: list databases: %v\n", err)
		os.Exit(1)
	}

	var results []result
	for _, dbName := range dbNames {
		db := client.Database(dbName)
		collNames, err := db.ListCollectionNames(ctx, map[string]any{})
		if err != nil {
			results = append(results, result{Database: dbName, Status: statusUnknown, Detail: "list collections: " + err.Error()})
			continue
		}
		for _, coll := range collNames {
			results = append(results, checkCollection(ctx, db.Collection(coll), dbName, coll))
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(results)
	} else {
		printTable(results)
	}

	// Non-zero exit if any collection is not confirmed enabled.
	for _, r := range results {
		if r.Status != statusEnabled {
			os.Exit(3)
		}
	}
}

// checkCollection tries to open a change stream on one collection.
func checkCollection(ctx context.Context, coll *mongo.Collection, dbName, collName string) result {
	// A short maxAwaitTime keeps the open cheap; we close immediately.
	opts := options.ChangeStream().SetMaxAwaitTime(500 * time.Millisecond)
	cs, err := coll.Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		if isChangeStreamNotEnabled(err) {
			return result{Database: dbName, Collection: collName, Status: statusDisabled}
		}
		return result{Database: dbName, Collection: collName, Status: statusUnknown, Detail: err.Error()}
	}
	_ = cs.Close(ctx)
	return result{Database: dbName, Collection: collName, Status: statusEnabled}
}

// isChangeStreamNotEnabled recognizes DocumentDB's "change streams not enabled"
// rejection. DocumentDB returns a message referencing change streams being
// disabled/not enabled rather than a stable dedicated error code, so we match
// on the message text defensively.
func isChangeStreamNotEnabled(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "change stream") &&
		(strings.Contains(msg, "not enabled") ||
			strings.Contains(msg, "disabled") ||
			strings.Contains(msg, "not supported"))
}

func listDatabases(ctx context.Context, client *mongo.Client, onlyDB string) ([]string, error) {
	if onlyDB != "" {
		return []string{onlyDB}, nil
	}
	names, err := client.ListDatabaseNames(ctx, map[string]any{})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range names {
		if !systemDBs[n] {
			out = append(out, n)
		}
	}
	return out, nil
}

func tlsConfigFromCA(caFile string) (*tls.Config, error) {
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates parsed from %s", caFile)
	}
	return &tls.Config{RootCAs: pool}, nil
}

func printTable(results []result) {
	fmt.Printf("%-25s %-30s %-10s %s\n", "DATABASE", "COLLECTION", "STATUS", "DETAIL")
	fmt.Println(strings.Repeat("-", 90))
	for _, r := range results {
		fmt.Printf("%-25s %-30s %-10s %s\n", r.Database, r.Collection, r.Status, r.Detail)
	}
}
