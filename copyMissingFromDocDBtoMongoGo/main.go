package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type namespace struct {
	db  string
	col string
}

var namespaces = []namespace{
	{"dms-trv-db", "st_tv_stock_log"},
	{"dms-trv-db", "sh_tv_stock"},
	{"ebook-delegate-db", "token"},
	{"ebook-usersession-db", "usersession"},
	{"svoc-db", "sync_job_log"},
}

const batchSize = 500

func main() {
	dryRun := flag.Bool("dry-run", true, "Print what would be done without applying changes (default: true)")
	flag.Parse()

	srcURI := os.Getenv("DOCDB_SRC")
	dstURI := os.Getenv("MDB_DEST")

	if srcURI == "" {
		fmt.Fprintln(os.Stderr, "ERROR: DOCDB_SRC environment variable not set")
		os.Exit(1)
	}
	if dstURI == "" {
		fmt.Fprintln(os.Stderr, "ERROR: MDB_DEST environment variable not set")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	srcClient, err := mongo.Connect(ctx, options.Client().ApplyURI(srcURI))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot connect to source: %v\n", err)
		os.Exit(1)
	}
	defer srcClient.Disconnect(ctx)

	dstClient, err := mongo.Connect(ctx, options.Client().ApplyURI(dstURI))
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: cannot connect to destination: %v\n", err)
		os.Exit(1)
	}
	defer dstClient.Disconnect(ctx)

	mode := "LIVE RUN — changes will be applied"
	if *dryRun {
		mode = "DRY RUN — no changes will be written"
	}
	fmt.Printf("\n%s\n  copy-missing-docs  |  %s\n%s\n", line(60), mode, line(60))

	totalInserted, totalDeleted, totalErrors := 0, 0, 0

	for _, ns := range namespaces {
		ins, del, errs := syncCollection(ctx, srcClient, dstClient, ns, *dryRun)
		totalInserted += ins
		totalDeleted += del
		totalErrors += errs
	}

	fmt.Printf("\n%s\n  TOTAL  INSERT: %d  |  DELETE: %d  |  Errors: %d\n%s\n",
		line(60), totalInserted, totalDeleted, totalErrors, line(60))

	if *dryRun {
		fmt.Println("\n  Re-run with --dry-run=false to apply changes.")
	}
}

func syncCollection(ctx context.Context, srcClient, dstClient *mongo.Client, ns namespace, dryRun bool) (inserted, deleted, errors int) {
	prefix := ""
	if dryRun {
		prefix = "[DRY RUN] "
	}
	fmt.Printf("\n%s── %s.%s ──\n", prefix, ns.db, ns.col)

	srcCol := srcClient.Database(ns.db).Collection(ns.col)
	dstCol := dstClient.Database(ns.db).Collection(ns.col)

	fmt.Print("  Fetching _ids from source...")
	srcIDs, err := fetchIDs(ctx, srcCol)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  ERROR fetching source _ids: %v\n", err)
		errors++
		return
	}
	fmt.Printf(" %d docs\n", len(srcIDs))

	fmt.Print("  Fetching _ids from destination...")
	dstIDs, err := fetchIDs(ctx, dstCol)
	if err != nil {
		fmt.Fprintf(os.Stderr, "\n  ERROR fetching destination _ids: %v\n", err)
		errors++
		return
	}
	fmt.Printf(" %d docs\n", len(dstIDs))

	// Compute missing and extra
	missingInDst := difference(srcIDs, dstIDs)
	extraInDst := difference(dstIDs, srcIDs)

	fmt.Printf("  To INSERT:   %8d docs\n", len(missingInDst))
	fmt.Printf("  To DELETE:   %8d docs\n", len(extraInDst))

	// INSERT missing documents
	if len(missingInDst) > 0 {
		ids := mapKeys(missingInDst)
		for i := 0; i < len(ids); i += batchSize {
			end := i + batchSize
			if end > len(ids) {
				end = len(ids)
			}
			batch := ids[i:end]

			filter := bson.M{"_id": bson.M{"$in": batch}}
			cursor, err := srcCol.Find(ctx, filter)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  ERROR reading source batch: %v\n", err)
				errors++
				continue
			}

			var docs []interface{}
			if err := cursor.All(ctx, &docs); err != nil {
				fmt.Fprintf(os.Stderr, "  ERROR decoding source batch: %v\n", err)
				errors++
				continue
			}
			cursor.Close(ctx)

			if dryRun {
				inserted += len(docs)
				for j, doc := range docs {
					if j >= 3 {
						fmt.Printf("  [DRY RUN] ... and %d more\n", len(docs)-3)
						break
					}
					if m, ok := doc.(bson.D); ok {
						for _, e := range m {
							if e.Key == "_id" {
								fmt.Printf("  [DRY RUN] Would INSERT _id=%v\n", e.Value)
								break
							}
						}
					}
				}
			} else {
				res, err := dstCol.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
				if err != nil {
					if bwe, ok := err.(mongo.BulkWriteException); ok {
						inserted += len(bwe.WriteErrors)
						errors += len(bwe.WriteErrors)
						fmt.Printf("  WARNING: %d insert errors in batch\n", len(bwe.WriteErrors))
					} else {
						fmt.Fprintf(os.Stderr, "  ERROR inserting batch: %v\n", err)
						errors++
					}
				} else {
					n := len(res.InsertedIDs)
					inserted += n
					fmt.Printf("  Inserted batch %d/%d: %d docs\n", i/batchSize+1, (len(ids)+batchSize-1)/batchSize, n)
				}
			}
		}
	}

	// DELETE extra documents (deleted from source)
	if len(extraInDst) > 0 {
		fmt.Printf("  WARNING: %d doc(s) exist in destination but NOT in source\n", len(extraInDst))
		shown := 0
		for id := range extraInDst {
			if shown >= 5 {
				fmt.Printf("    ... and %d more\n", len(extraInDst)-5)
				break
			}
			fmt.Printf("    Extra _id=%v\n", id)
			shown++
		}

		if dryRun {
			deleted = len(extraInDst)
			fmt.Printf("  [DRY RUN] Would DELETE %d doc(s) from destination\n", deleted)
		} else {
			for id := range extraInDst {
				res, err := dstCol.DeleteOne(ctx, bson.M{"_id": id})
				if err != nil {
					fmt.Fprintf(os.Stderr, "  ERROR deleting _id=%v: %v\n", id, err)
					errors++
				} else {
					deleted += int(res.DeletedCount)
				}
			}
			fmt.Printf("  Deleted %d doc(s) from destination\n", deleted)
		}
	}

	action := "Would"
	if !dryRun {
		action = "Done —"
	}
	fmt.Printf("  %s INSERT %d | DELETE %d | Errors %d\n", action, inserted, deleted, errors)
	return
}

func fetchIDs(ctx context.Context, col *mongo.Collection) (map[interface{}]struct{}, error) {
	cursor, err := col.Find(ctx, bson.D{}, options.Find().SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	ids := make(map[interface{}]struct{})
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}
		for _, e := range doc {
			if e.Key == "_id" {
				ids[e.Value] = struct{}{}
				break
			}
		}
	}
	return ids, cursor.Err()
}

func difference(a, b map[interface{}]struct{}) map[interface{}]struct{} {
	result := make(map[interface{}]struct{})
	for k := range a {
		if _, ok := b[k]; !ok {
			result[k] = struct{}{}
		}
	}
	return result
}

func mapKeys(m map[interface{}]struct{}) []interface{} {
	keys := make([]interface{}, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func line(n int) string {
	s := make([]byte, n)
	for i := range s {
		s[i] = '='
	}
	return string(s)
}
