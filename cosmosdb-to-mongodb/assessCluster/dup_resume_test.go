package main

import (
	"encoding/json"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

// The resume bookkeeping in scanDuplicates must not double-count or miss a
// duplicate when a scan is interrupted at any position and resumed with
// {_id: {$gte: last}} while skipping `seenOfPrev` copies of `last`.
// This test drives the same state machine against an in-memory sorted stream,
// interrupting after every possible prefix length.

func int32Val(n int32) bson.RawValue {
	_, v, _ := bson.MarshalValue(n)
	return bson.RawValue{Type: bsontype.Int32, Value: v}
}

func runWithInterrupts(stream []int32, breakAfter int) (dups int64, scanned int64) {
	var prev *bson.RawValue
	seenOfPrev := 0
	var lastID *bson.RawValue
	skipOfLast := 0
	interrupted := false

	for attempt := 0; attempt < 10; attempt++ {
		start := 0
		if lastID != nil {
			// emulate {_id: {$gte: last}} on a sorted stream
			for start < len(stream) && !idEqual(int32Val(stream[start]), *lastID) && stream[start] < lastID.Int32() {
				start++
			}
			skipOfLast = seenOfPrev
		}
		delivered := 0
		for i := start; i < len(stream); i++ {
			id := int32Val(stream[i])
			if lastID != nil && skipOfLast > 0 && idEqual(id, *lastID) {
				skipOfLast--
				continue
			}
			skipOfLast = 0
			scanned++
			if prev != nil && idEqual(id, *prev) {
				seenOfPrev++
				dups++
			} else {
				seenOfPrev = 1
			}
			idCopy := id
			prev = &idCopy
			lastID = &idCopy
			delivered++
			if !interrupted && delivered == breakAfter {
				interrupted = true
				break
			}
		}
		if interrupted && delivered == breakAfter {
			interrupted = true
			// simulate the throttle: loop to resume
			breakAfter = -1
			continue
		}
		return dups, scanned
	}
	return dups, scanned
}

func TestDuplicateResumeIsExact(t *testing.T) {
	// sorted stream with dupes of multiplicity 2 and 3, and singles
	stream := []int32{1, 2, 2, 3, 4, 4, 4, 5, 6, 6, 7}
	wantDups := int64(1 + 2 + 1) // 2→1, 4→2, 6→1
	wantScanned := int64(len(stream))
	for k := 1; k <= len(stream); k++ {
		d, s := runWithInterrupts(stream, k)
		if d != wantDups || s != wantScanned {
			t.Fatalf("interrupt after %d docs: dups=%d scanned=%d, want dups=%d scanned=%d", k, d, s, wantDups, wantScanned)
		}
	}
}

func TestIdEqualNumericAcrossTypes(t *testing.T) {
	_, i32, _ := bson.MarshalValue(int32(5))
	_, i64, _ := bson.MarshalValue(int64(5))
	_, dbl, _ := bson.MarshalValue(float64(5))
	a := bson.RawValue{Type: bsontype.Int32, Value: i32}
	b := bson.RawValue{Type: bsontype.Int64, Value: i64}
	c := bson.RawValue{Type: bsontype.Double, Value: dbl}
	if !idEqual(a, b) || !idEqual(b, c) || !idEqual(a, c) {
		t.Fatal("numeric _id equality across int32/int64/double must hold (matches _id index semantics)")
	}
	_, s, _ := bson.MarshalValue("5")
	if idEqual(a, bson.RawValue{Type: bsontype.String, Value: s}) {
		t.Fatal("string \"5\" must not equal numeric 5")
	}
}

func TestParseURI(t *testing.T) {
	cases := map[string]string{
		"mongodb://u:p%3D%3D@acct.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false":       "COSMOS_RU",
		"mongodb+srv://u:p@acct.mongocluster.cosmos.azure.com/?tls=true":                          "COSMOS_VCORE",
		"mongodb://u:p@localhost:27017/?replicaSet=rs0":                                           "OTHER",
		"mongodb://u:p@h1.mongo.cosmos.azure.com:10255,h2.mongo.cosmos.azure.com:10255/?ssl=true": "COSMOS_RU",
	}
	for uri, want := range cases {
		got := parseURI(uri)
		if got.Flavor != want {
			t.Errorf("%s: flavor %s, want %s", uri, got.Flavor, want)
		}
	}
	info := parseURI("mongodb://u:p@acct.mongo.cosmos.azure.com:10255/?ssl=true&RetryWrites=false&maxIdleTimeMS=120000")
	if info.Options["retrywrites"] != "false" || info.Options["maxidletimems"] != "120000" {
		t.Errorf("options not lower-cased/parsed: %v", info.Options)
	}
}

func TestIsThrottled(t *testing.T) {
	err := &fakeErr{"(RequestRateTooLarge) Error=16500, RetryAfterMs=357, Details='Response status code does not indicate success: TooManyRequests (429)'"}
	wait, ok := isThrottled(err)
	if !ok || wait.Milliseconds() != 357 {
		t.Fatalf("expected throttled with 357ms, got ok=%v wait=%s", ok, wait)
	}
	if _, ok := isThrottled(&fakeErr{"E11000 duplicate key"}); ok {
		t.Fatal("duplicate key must not be treated as throttling")
	}
}

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }

func TestRawValueJSONAbsentField(t *testing.T) {
	if got := rawValueJSON(bson.RawValue{}); got != "null" {
		t.Fatalf("absent field must render as null, got %q", got)
	}
	_, s, _ := bson.MarshalValue("acme")
	if got := rawValueJSON(bson.RawValue{Type: bsontype.String, Value: s}); got != `"acme"` {
		t.Fatalf("string must render quoted, got %s", got)
	}
	// describeDup must emit valid JSON even when one side lacks the shard key
	a, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(7)}, {Key: "tenant", Value: "acme"}})
	b, _ := bson.Marshal(bson.D{{Key: "_id", Value: int32(7)}})
	out := describeDup(a, b, []string{"tenant"})
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(out), &parsed); err != nil {
		t.Fatalf("describeDup produced invalid JSON %s: %v", out, err)
	}
}
