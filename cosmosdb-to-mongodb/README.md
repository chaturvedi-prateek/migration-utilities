# cosmosdb-to-mongodb

Utilities for migrating **Azure Cosmos DB for MongoDB (RU)** to **MongoDB Atlas**
with **dsync Enterprise (`dsynct`)**. Each tool closes a gap that dsync either
documents as a limitation or that only shows up on Cosmos.

Same conventions as [`docdb-to-mongodb`](../docdb-to-mongodb): one folder per
tool, single `main.go`, JSON config, `--dry-run=true` default on anything that
writes, prebuilt static binaries in `bin/`, exit codes usable as gates.

> **Nothing in this folder writes to the Cosmos source.** `assessCluster` is
> read-only; `migrateIndexes` reads index metadata from Cosmos and writes only to
> Atlas. dsync itself **does** write to the source (a dummy document per
> collection to obtain the T0 resume token) — see `assessCluster --mode residue`.

## Tools

| Tool | Phase | dsync gap it covers | Status |
|---|---|---|---|
| [`assessCluster`](assessCluster/) | pre-flight (read-only) | Connector selection by hostname · mixed / non-ObjectId `_id` · **duplicate `_id` across shard-key values** · no index replication (TTL on `_ts`, unique) · empty collections not created · token-donor rule for change streams · namespace cap → wave planning · dummy-record residue | **Built, verified on Cosmos 7.0** |
| `fixIdTypes` | pre-flight (writes source) | Mixed `_id` BSON types (dsync partitioning is ObjectId-only) | **Do not use `docdb-to-mongodb/fixIdTypes` on Cosmos** — see [Not Cosmos-safe](#not-cosmos-safe-docdb-tools-tested-and-rejected). A Cosmos-safe version is not yet built. |
| `fixDuplicateIds` | pre-flight (writes source) | Same `_id` under two shard-key values → silent overwrite on Atlas | planned — `assessCluster --mode duplicates` reports the pairs; the fix must be done manually or by the application team until this exists |
| [`migrateIndexes`](migrateIndexes/) | during CDC / cutover | dsync does not replicate indexes; TTL on `_ts` not portable; unique + TTL must be deferred; **`ensure_collections`** creates the empty collections dsync skips | **Built, verified Cosmos 7.0 → MongoDB 8.0** |
| `reconcileDocs` | post write-freeze | Deletes missed by emulation (inserts and deletes netting out inside an interval, TTL expiries); missing documents | planned — `docdb-to-mongodb/copyMissingDocs` **only reads** from the source (`find` on `_id`), so it is expected to be safe against Cosmos, but it has **not been tested** there and has no 16500 retry on its Atlas-side writes |
| `migration-toolkit` | host setup | Offline bundle + client-neutral production runbook | planned |

## Not Cosmos-safe: DocDB tools tested and rejected

The `docdb-to-mongodb` tools were written for Amazon DocumentDB, which never
throttles. Azure Cosmos DB for MongoDB (RU) throttles any request above the
provisioned RU/s (Mongo error `16500`, HTTP 429), and — critically — **does not
always surface throttling as an error**. Tested live on 26 Sep 2026 against
server version 7.0, shared 1000 RU/s database, 1,850-document collection
(1,500 ObjectId + 300 string + 50 int `_id`):

| Tool | Run | Result |
|---|---|---|
| `fixIdTypes --mode detect` / `count` | read-only | **Correct** (1,500 / 300 / 50) |
| `fixIdTypes --mode fix` (unmodified, `workers: 2`, `throttle_ms: 50`) | live | **Corrupted the collection.** 16500 on `InsertMany` / `DeleteMany` batches was logged and skipped: 17 originals left in both source and backup, 47 half-transformed documents, `errors` reported non-zero but the run continued. |
| `fixIdTypes --mode fix` with throttle-retry added to all three write points | live | Exposed a **second, DocDB-independent bug**: Phase 2 reads the shared backup collection **unfiltered**, so with two `wrong_type` entries for one namespace the second entry re-transforms the first entry's documents (300 duplicates with `new_objectid`). |
| …plus the backup read filtered by `wrong_type` | live | Exposed a **third**: a `find` cursor under throttling **ended early with `cursor.Err() == nil`**, and the phase reported `OK` on a truncated read (200 of 300 read; 100 left in backup). |
| …plus cursor resume on `_id` | live | Still wrong: `Moved 111` from batches of 100 with `errors 0`, 189 strings unconverted, 89 duplicated. Consistent with `deleteMany` returning a **partial count with no error** under throttling. Not yet confirmed. |

The patched source was **reverted**; `docdb-to-mongodb/fixIdTypes` is unchanged
and remains correct for DocumentDB. Conclusion: on Cosmos, *any* tool that
writes must (a) retry `16500` per failed index, (b) never trust cursor
exhaustion or a bulk-write count as proof of completion — it must re-query
until the set it is draining is genuinely empty — and (c) make every phase
idempotent by marking processed documents. `fixIdTypes` will need to be
rewritten on those principles for Cosmos, not patched.

`migrateIndexes` is unaffected: its source reads are metadata-only
(`listIndexes`) and its writes go to Atlas.

## Order of use

```
assessCluster --mode all           → fix every BLOCKER:
   MIXED_ID_TYPES   → application team / manual (no Cosmos-safe tool yet — see above)
   DUPLICATE_IDS    → application team / manual (assessCluster lists the pairs)
   NO_DONOR         → add an unsharded collection to the wave / re-plan
assessCluster --mode waves         → waves.csv + waveNN.env for dsynct
dsynct (initial sync → CDC)        → per wave
migrateIndexes --mode create       → once lag ≈ 0, before cutover (ensure_collections: true)
[write freeze]
reconcileDocs --mode report/apply  → after lag = 0 and the forced deletes cycle
migrateIndexes --mode rectify      → unique + TTL at cutover
```

## Cosmos DB facts that drive these tools

Verified against server version **7.0** on 26 Sep 2026 (details in each tool's README):

- `_id` is unique **per shard-key value** on sharded collections — duplicates across shard keys are legal and dsync overwrites them silently on Atlas.
- User unique indexes on sharded collections **must be prefixed by the shard key**, so they stay globally unique on Atlas.
- Change streams: **collection-level only**, no delete events, pipeline must be `$match operationType ∈ {insert,update,replace}` + `$project` + `fullDocument: updateLookup`.
- One TTL index per collection; TTL on the system field `_ts` has no Atlas equivalent.
- `coll.stats()` (`$collStats`) is rejected — use `runCommand({collStats})`.
- Throttling surfaces as Mongo error `16500` with `RetryAfterMs=` in the message — **but not always**: under load a `find` cursor can end early with no error, and bulk writes can report partial counts with no error (observed; see the table above).

## dsync facts that drive these tools

From `markadiom/dsynct@sha256:ea748b98…` (embeds dsync v0.28.0) and source at v0.29.0:

- CosmosDB connector chosen by hostname regex `(?i)\.mongo\.cosmos\.azure\.com$`.
- `--cosmos-reader-max-namespaces` default 8 ("keep under 15"); exceeding it fails `GeneratePlan`.
- T0 resume token is obtained by **inserting a dummy document** into each source collection. On sharded collections that insert fails (type-assertion bug on `shardKeyDefinition`); a flow with **no** unsharded namespace has an empty token map and **CDC panics after a healthy-looking initial sync**.
- `--cosmos-deletes-cdc <witness-uri>` (deletes emulation) uses the **destination** as witness; `--cosmos-stream-deletes-enabled` fails on Cosmos 7.0.
- Empty collections are not created on the destination.
