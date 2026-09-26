# assessCluster

Go binary that reviews an **Azure Cosmos DB for MongoDB (RU)** account before a
**dsync / dsynct** migration to MongoDB Atlas and reports everything dsync either
cannot handle or handles in a way that needs a decision.

**Read-only.** It never inserts, updates or deletes on the source. It is the
Cosmos counterpart of `docdb-to-mongodb/checkChangeStreams` + the `detect` mode
of `fixIdTypes`, extended with the checks that only Cosmos needs.

---

## What it finds

| Mode | Check | Why dsync needs it |
|---|---|---|
| `account` | Hostname matches `\.mongo\.cosmos\.azure\.com$`; not vCore; server ≥ 3.6; `retrywrites=false`; `maxIdleTimeMS` | dsync picks its **CosmosDB connector by hostname regex** — an alias/proxy silently gets the generic MongoDB connector (no `--cosmos-*` flags, no deletes emulation, wrong partitioning). vCore has no change streams. |
| `inventory` | Per collection: docs, bytes, avg doc, shard key, dedicated/shared RU, 2 MB vs 16 MB doc cap; empty collections; Atlas name limits | Sizing; **dsync does not create empty collections**; 2 MB cap breaks the reverse/rollback flow; 64-byte db name / 255-byte namespace limits. |
| `idtypes` | `_id` BSON type per collection: `OK` (ObjectId) / `SLOW_PATH` (one other type) / `MIXED` | dsync's Cosmos partitioning is **ObjectId-only** and **does not support mixed `_id` types** (Adiom limitation). Two index seeks per collection. |
| `duplicates` | Duplicate `_id` across shard-key values on **sharded** collections | On Cosmos `_id` is unique **per shard-key value**; dsync writes to Atlas by `_id` and **overwrites silently** — one document of each pair is lost. Streaming sorted-`_id` scan, O(1) memory. |
| `indexes` | Every non-`_id_` index classified: unique / TTL / wildcard / text / geo; TTL on `_ts`; Atlas limits | dsync **does not replicate indexes**. TTL on Cosmos's `_ts` cannot exist on Atlas. Unique indexes hit duplicate-key errors during CDC replay. TTL indexes must wait for cutover (witness corruption). |
| `changestreams` | Opens the one pipeline shape Cosmos accepts; classifies **token donor** (unsharded) vs sharded; counts physical partitions | dsync obtains each namespace's T0 resume token by **inserting a dummy document** into the source collection. That insert **fails on sharded collections** (dsync v0.28 / v0.29). If **no** namespace in a flow yields a token, initial sync completes, counts match, and **CDC panics** — every flow needs ≥ 1 unsharded collection. |
| `residue` | Documents shaped like dsync's dummy record (`{_id}` only, or `{_id, <shardKey>: ""}` only) | The dummy is deleted after the token is read; a throttled delete or a crash leaves it in production data. |
| `waves` | Bin-packs namespaces into dsync flows: ≤ N per flow (default 8), ≥ 1 token donor per flow, `MIXED`/`SLOW_PATH` isolated, balanced by size; writes `waves.csv` + one `.env` per wave | dsync hard-fails `GeneratePlan` above `--cosmos-reader-max-namespaces` (default 8, "keep under 15"), and Cosmos change streams are **per collection** — N collections in CDC = N cursors. |

Severities:

| Severity | Meaning | Exit code |
|---|---|---|
| `BLOCKER` | dsync will fail, or data will be lost silently — fix before starting | `3` |
| `ACTION` | dsync will run, but the migration plan must handle it | `4` |
| `INFO` | recorded for the runbook | `0` |

`1` = connection / runtime error, `2` = configuration error.

---

## Files

| File | Description |
|---|---|
| `bin/assessCluster-darwin-arm64` | macOS (Apple Silicon) static binary |
| `bin/assessCluster-darwin-amd64` | macOS (Intel) static binary |
| `bin/assessCluster-linux-amd64` | Linux x86-64 static binary |
| `bin/assessCluster-linux-arm64` | Linux ARM64 static binary |
| `bin/assessCluster-windows-amd64.exe` | Windows x86-64 static binary |
| `config.sample.json` | Config template |
| `main.go` | Source |
| `dup_resume_test.go` | Unit tests for the duplicate-scan resume logic, `_id` equality, URI parsing and throttle detection |

---

## Setup

```bash
cp config.sample.json config.json
```

```json
{
  "source_uri": "mongodb://<account>:<KEY>@<account>.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@<account>@",
  "log_file": "assessCluster.log",
  "databases": [],
  "output_dir": "assessment",
  "concurrency": 4,
  "max_report": 50,
  "waves": { "max_namespaces": 8, "namespaces_file": "" }
}
```

| Field | Required | Default | Description |
|---|---|---|---|
| `source_uri` | Yes | — | The **real** Cosmos hostname (`*.mongo.cosmos.azure.com`). Use the same key dsync will use. A read-only key is enough for this tool. |
| `log_file` | No | `""` | Tee all output to this file as well as stdout. |
| `databases` | No | all | Restrict to these databases. `[]` = every non-system database. |
| `output_dir` | No | `assessment` | Where `assessment.json`, `assessment.md`, `waves.csv`, `waveNN.env` are written. |
| `concurrency` | No | `4` | Parallel per-collection probes. **Every probe costs RU** — raise carefully on a shared-throughput database. |
| `max_report` | No | `50` | Max sample `_id`s listed per finding. |
| `waves.max_namespaces` | No | `8` | Must equal the `--cosmos-reader-max-namespaces` you will pass to dsynct. |
| `waves.namespaces_file` | No | `""` | One `db.collection` per line; restricts wave planning to those namespaces. |

---

## Usage

```bash
# Everything (recommended for the first run)
./bin/assessCluster-linux-amd64 --config config.json --mode all

# One or several modes
./bin/assessCluster-linux-amd64 --config config.json --mode account,inventory
./bin/assessCluster-linux-amd64 --config config.json --mode duplicates
./bin/assessCluster-linux-amd64 --config config.json --mode waves
```

**Windows (PowerShell):** `.\bin\assessCluster-windows-amd64.exe --config config.json --mode all`

| Flag | Default | Description |
|---|---|---|
| `--config` | `config.json` | Path to the JSON config. |
| `--mode` | `all` | `account` \| `inventory` \| `idtypes` \| `duplicates` \| `indexes` \| `changestreams` \| `residue` \| `waves` \| `all`, comma-separated to combine. `waves` collects `idtypes` and `changestreams` data internally without reporting their findings. |
| `--timeout` | `6h` | Overall timeout. |

Capture the exit code for a gate (piping through `tee` hides it):

```bash
./bin/assessCluster-linux-amd64 --config config.json --mode all | tee assess.out
code=${PIPESTATUS[0]}   # zsh: ${pipestatus[1]}
```

---

## Outputs

`assessment.md` — findings table sorted by severity, then one table per mode.
`assessment.json` — the same, machine-readable (`findings[]`, `inventory[]`, `id_types[]`, `duplicates[]`, `indexes[]`, `change_streams[]`, `residue[]`, `waves[]`, `summary`).
`waves.csv` — one row per namespace with `wave, ns, sizeBytes, count, idType, cosmosSharded, cosmosShardKey, tokenDonor, atlasSharded, atlasShardKey, fullDocumentKeyNeeded, notes`. The three `atlas*`/`fullDocumentKeyNeeded` columns are left for you to fill.
`waveNN.env` — ready-to-source fragments:

```bash
WAVE=wave02
WORKFLOW_ID=${ENGAGEMENT}-wave02
QUEUE_NAME=wave02
WAVE_NAMESPACES="--namespace shop.orders --namespace shop.events --namespace shop.customers"
# token donor: shop.orders
```

Sample console output (lab fixtures):

```
[STEP ] idtypes — ASC/DESC $type probes (2 index seeks per collection)
[BLOCK] idtypes lab.mixed_ids MIXED_ID_TYPES — _id types range int … string — dsync's Cosmos partitioning does not support mixed _id types; the _id values must be made uniform at source before dsync starts (no Cosmos-safe automated fix exists yet — see the suite README)
[STEP ] duplicates — sorted _id stream on sharded collections (1 index scan each)
[BLOCK] duplicates lab.sharded_dupes DUPLICATE_IDS — 5 duplicate _id value(s) across shard-key values (scanned 811) — dsync overwrites by _id on Atlas, one document of each pair is lost silently. Re-key or remove one side of every pair at source before starting (pairs are listed in assessment.json)
[WARN ] indexes lab.idx_variety TTL_ON_TS — index ttl_ts: TTL on _ts (86400s) cannot be recreated on Atlas — choose a date field (migrateIndexes --ttl-field-map) or accept loss of expiry
[INFO ] changestreams lab.sharded_dupes SHARDED_NO_TOKEN — sharded — dsync's dummy-insert token retrieval fails here; the flow needs at least one unsharded namespace (token donor)
[WARN ] residue lab.residue_unsharded DSYNC_RESIDUE — 1 document(s) shaped like dsync dummy records ({"$oid":"6ab7c5c8…"}) — confirm with the application owner and delete by _id
[OK   ] waves: 3 flows for 9 namespaces; 9 physical partitions (= concurrent change-stream cursors at full CDC)
[INFO ] SUMMARY  BLOCKER: 2  |  ACTION: 7  |  INFO: 5
```

---

## Finding codes

| Code | Severity | Mode | Meaning / next step |
|---|---|---|---|
| `NOT_COSMOS_HOST` | BLOCKER | account | Hostname will not select dsync's CosmosDB connector. Use the real `*.mongo.cosmos.azure.com` host. |
| `VCORE_SOURCE` | BLOCKER | account | vCore exposes no change streams; dsync supports it as destination only. |
| `VERSION_TOO_OLD` | BLOCKER | account | Server < 3.6 — no change streams. |
| `RETRYWRITES` | ACTION | account | Add `retrywrites=false`. |
| `MAXIDLETIME` | INFO | account | Add `maxIdleTimeMS=120000`. |
| `DB_NAME_TOO_LONG` / `NAMESPACE_TOO_LONG` | BLOCKER | inventory | Exceeds MongoDB's 64-byte / 255-byte limits — rename at source or map with `--namespace-mapping`. |
| `EMPTY_COLLECTION` | ACTION | inventory | Pre-create on Atlas (`migrateIndexes --mode=create` with `ensure_collections: true`). |
| `DOC_CAP_2MB` | INFO | inventory | Account-level; only matters for the reverse flow. |
| `MIXED_ID_TYPES` | BLOCKER | idtypes | Make `_id` types uniform at source. **Do not use `docdb-to-mongodb/fixIdTypes` on Cosmos** — it corrupts data under RU throttling (see suite README). |
| `NON_OBJECTID_ID` | ACTION | idtypes | Single-task initial sync; isolate in its own wave (the planner does this). |
| `DUPLICATE_IDS` | BLOCKER | duplicates | Re-key or remove one side of every pair at source; pairs are in `assessment.json` → `duplicates[].samples`. |
| `TTL_ON_TS` | ACTION | indexes | Nominate a date field for `migrateIndexes --ttl-field-map`, or accept loss of expiry. |
| `TTL_DEFER` | ACTION | indexes | Create at cutover only. |
| `UNIQUE_DEFER` | ACTION | indexes | `migrateIndexes` creates non-unique, then `rectify` at cutover. |
| `TOO_MANY_INDEXES` / `TOO_MANY_KEY_FIELDS` | BLOCKER | indexes | MongoDB limits 64 / 32. |
| `WATCH_FAILED` | BLOCKER | changestreams | Change stream could not be opened — see detail. |
| `SHARDED_NO_TOKEN` | INFO | changestreams | Needs a token donor in its wave (the planner enforces this). |
| `NO_TOKEN_DONOR` | BLOCKER | changestreams | Every collection is sharded — create a small unsharded collection (≥ 1 document) per flow. |
| `MANY_STREAMS` | ACTION | changestreams | > 15 collections — multiple flows; confirm RU headroom with Adiom. |
| `DSYNC_RESIDUE` | ACTION | residue | Confirm with the application owner, delete by `_id`. |
| `NO_DONOR` | BLOCKER | waves | A wave has no unsharded collection. |
| `OVER_CAP` | BLOCKER | waves | More namespaces than `max_namespaces`. |
| `MULTI_FLOW` | INFO | waves | Number of dsynct flows to run. |

---

## RU cost and throttling

Every probe is a real request. Costs, per collection: `account` ≈ 0 · `inventory` 2 commands · `idtypes` 2 index seeks · `indexes` 1 `listIndexes` · `changestreams` 1 stream open + 1 `GetChangeStreamTokens` · `residue` **one collection scan on unsharded collections** (indexed on the shard key for sharded ones) · `duplicates` **one `_id`-index scan per sharded collection**.

Cosmos throttling (HTTP 429, Mongo error `16500`) is handled: the tool honours `RetryAfterMs`, retries up to 12 times per operation, and the `duplicates` scan **resumes from the last `_id` seen** with exact bookkeeping (no double-count, no gap — see `dup_resume_test.go`). Lower `concurrency` if you see repeated `throttled` warnings; the source application is competing for the same RU.

---

## Cosmos DB behaviours this tool relies on (verified on server version 7.0, 26 Sep 2026)

| Behaviour | Consequence in the code |
|---|---|
| `coll.stats()` fails: `Unrecognized pipeline stage name: $collStats` | Uses `runCommand({collStats})`. |
| `customAction: GetCollection` returns `shardKeyDefinition`, `provisionedThroughput`/`autoScaleSettings`, `maxContentLength` | Source of shard key, RU scope and 2 MB/16 MB cap. `shardKeyDefinition` decodes as `primitive.D`, not a map — handled. |
| `watch()` requires `$match` on `operationType ∈ {insert,update,replace}`, a `$project`, and `fullDocument: "updateLookup"`; database-level `watch()` is rejected | The probe uses exactly that pipeline. |
| `customAction: GetChangeStreamTokens` returns one token per physical partition | Physical partition count. |
| Cross-partition `sort({_id:1})` returns a globally ordered stream on sharded collections | Adjacent-equality duplicate detection is sound. |
| Two documents with equal `_id` under different shard-key values **insert successfully** on a sharded collection | The `duplicates` check is necessary. |
| Unique indexes on sharded collections **must be prefixed by the shard key** (`cannot create unique index over {code:1} with shard key pattern {tenant:1}`); uniqueness is then enforced per shard-key value | User unique indexes remain globally unique on Atlas; only `_id` has the per-shard hazard. |
| One TTL index per collection; `text` indexes are rejected (`'text' is not supported`); unique indexes cannot be created on non-empty collections | Classification rules. |
| `$expr` / `$objectToArray` / `$size` supported; `$bsonSize` is not | Residue filter shape. |
| `deleteOne({_id})` without the shard key works on sharded collections | Relevant to any future source-side fix tool. |

## dsync behaviours this tool guards against (dsync v0.28.0 embedded in `markadiom/dsynct@sha256:ea748b98…`, source read at v0.29.0)

- Connector selection: `connectors/mongo/util.go` — `COSMOS_DB_RU_REGEX = (?i)\.mongo\.cosmos\.azure\.com$`.
- Namespace cap: `connectors/cosmos/conn.go` `GeneratePlan` → `too many namespaces to copy: N, max 8`.
- Token retrieval: `connectors/cosmos/util.go` `getLatestResumeToken` inserts a dummy document, reads the event's resume token, deletes the dummy. It reads `shardKeyDefinition` from `GetCollection` but asserts `map[string]interface{}` — the v2 Go driver decodes it as `bson.D`, so the assertion fails, the filter stays `{}`, and Cosmos rejects the insert with `document does not contain shard key`. Observed live. A flow whose token map is empty panics in `decodeResumeToken` (`conn.go:66`) when CDC starts. Report this to Adiom with those references.
- Deletes emulation uses the **destination** as witness — anything that removes documents on Atlas outside dsync (a TTL index) corrupts it.

---

## Build from source

Requires Go 1.22+.

```bash
go mod tidy
go test ./...
go build -o assessCluster .

# Cross-compile all common platforms into bin/
CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -ldflags="-s -w" -o bin/assessCluster-darwin-arm64 .
CGO_ENABLED=0 GOOS=darwin  GOARCH=amd64 go build -ldflags="-s -w" -o bin/assessCluster-darwin-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -ldflags="-s -w" -o bin/assessCluster-linux-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -ldflags="-s -w" -o bin/assessCluster-linux-arm64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o bin/assessCluster-windows-amd64.exe .
```

---

## Notes

- Keep `config.json` out of version control — it contains the account key.
- System databases (`admin`, `config`, `local`) and `system.*` collections are skipped.
- `duplicates` only scans **sharded** collections — an unsharded Cosmos collection cannot hold duplicate `_id`s.
- Run `waves` again after `_id` remediation has changed the estate; the plan depends on `_id` types.
