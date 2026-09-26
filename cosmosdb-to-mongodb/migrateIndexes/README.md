# migrateIndexes (Cosmos DB for MongoDB RU → Atlas)

Go tool that migrates indexes from an **Azure Cosmos DB for MongoDB (RU)** source
to a **MongoDB Atlas** destination and validates them for cutover. dsync does not
replicate indexes, and Cosmos has one index shape Atlas cannot express.

Fork of [`docdb-to-mongodb/indexes/migrateIndexes`](../../docdb-to-mongodb/indexes/migrateIndexes)
— same modes, flags, worker pool, monitor and verify semantics — with four
Cosmos-specific changes:

| Change | Why |
|---|---|
| **TTL on `_ts` is translated or skipped** | Cosmos keys TTL on its system field `_ts`, which does not exist on Atlas. With `ttl_field_map["db.coll"] = "<dateField>"` the TTL is recreated on that field (as `ttl_<dateField>`); without a mapping the index is `SKIP`ped in `create` and reported `SKIPPED` (not `MISSING`) in `verify`. |
| **`ensure_collections`** | dsync does not create empty collections on the destination (verified with `dsynct` against Cosmos 7.0). With this on, `--mode=create` first creates every source collection on Atlas so nothing is lost and every index has a collection to land on. |
| **RU-throttle retry on source reads** | Cosmos surfaces 429 as Mongo error `16500` with `RetryAfterMs=`; `listDatabases` / `listCollections` / `listIndexes` honour it. |
| **Source-host sanity warning** | Warns if the source is not `*.mongo.cosmos.azure.com`. |

Everything else is inherited: TTL → `MAX_INT` and unique → `unique:false` during
`create`; `prepareUnique` → `unique:true` and TTL restore during `rectify`;
parallel `createIndex`/`collMod` with a `currentOp` monitor every 5 s.

---

## Prebuilt binaries

| Platform | Binary |
| --- | --- |
| macOS (Apple Silicon) | `bin/migrateIndexes-darwin-arm64` |
| macOS (Intel) | `bin/migrateIndexes-darwin-amd64` |
| Linux (x86_64) | `bin/migrateIndexes-linux-amd64` |
| Linux (ARM64) | `bin/migrateIndexes-linux-arm64` |
| Windows (x86_64) | `bin/migrateIndexes-windows-amd64.exe` |

---

## Modes

| Mode | When | What it does |
| --- | --- | --- |
| `create` | during CDC, once lag ≈ 0, before cutover | (`ensure_collections`) create missing collections → read every non-`_id_` source index → translate `_ts` TTL → create on Atlas with TTL neutralised to `MAX_INT` and unique as `unique:false`. |
| `verify` | any time (read-only) | Compare source vs destination: `OK` / `PENDING` / `MISMATCH` / `MISSING` / `EXTRA` / `SKIPPED`. |
| `rectify` | at cutover, after the write freeze and `dsync verify` | Phase 1 (parallel): `prepareUnique` + restore TTL values. Phase 2 (parallel, strictly after): `unique:true`. Skipped `_ts` indexes are ignored. |

```
create  →  verify (expect INCOMPLETE, exit 4)  →  [cutover]  →  rectify  →  verify (expect PASS, exit 0)
```

**Why TTL and unique are deferred.** dsync replays Cosmos changes as full-document
replaces that can arrive out of order; a live unique index throws duplicate-key
errors while a value moves between documents. A live TTL on Atlas deletes documents
that still exist on Cosmos — and Atlas is dsync's **deletes-emulation witness**, so
those documents are then never re-copied (playbook risk C-18).

---

## Setup

```bash
cp config.sample.json config.json
```

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `source_uri` | Yes | — | Cosmos connection string (real `*.mongo.cosmos.azure.com` host, `retrywrites=false`). A read-only key is sufficient. |
| `destination_uri` | Yes | — | Atlas connection string with a user that can `createIndex` / `collMod` / `createCollection`. |
| `databases` | No | all | `[]` = every non-system database on the source. |
| `log_file` | No | `""` | Tee all output to this file. |
| `ensure_collections` | No | `false` | In `create`, create every source collection on the destination first. **Turn on** — dsync skips empty collections. If a collection must be **sharded on Atlas**, run `sh.shardCollection` right after `create --dry-run=false` and before dsync writes into it. |
| `ttl_field_map` | No | `{}` | `"db.collection": "dateField"`. Recreates the Cosmos `_ts` TTL on that field with the same `expireAfterSeconds`. The application owner must nominate the field — the tool will not guess. Unmapped `_ts` TTLs are skipped. |

---

## Usage

```bash
# 1. Pre-cutover — dry run, then apply
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=true
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=false

# 2. Expect INCOMPLETE (exit 4): unique/TTL still neutralised
./bin/migrateIndexes-linux-amd64 --config config.json --mode=verify

# 3. At cutover (after write freeze + data verification)
./bin/migrateIndexes-linux-amd64 --config config.json --mode=rectify --dry-run=true
./bin/migrateIndexes-linux-amd64 --config config.json --mode=rectify --dry-run=false

# 4. Expect PASS (exit 0)
./bin/migrateIndexes-linux-amd64 --config config.json --mode=verify
```

**Windows (PowerShell):** replace `./bin/migrateIndexes-linux-amd64` with `.\bin\migrateIndexes-windows-amd64.exe`.

| Flag | Default | Description |
| --- | --- | --- |
| `--config` | `config.json` | Path to the JSON config file. |
| `--mode` | `create` | `create` \| `verify` \| `rectify`. |
| `--dry-run` | `true` | Preview without writing. Ignored by `verify`. |
| `--concurrency` | `8` | Parallel index operations on the destination (also sizes the connection pool). Lower on small tiers. |

---

## Verify statuses and exit codes

| Status | Meaning |
| --- | --- |
| `OK` | Present with matching keys, options, `unique`, TTL. |
| `PENDING` | Present, still neutralised (`unique:false` / TTL `MAX_INT`) — run `rectify`. |
| `SKIPPED` | Source TTL on `_ts` with no `ttl_field_map` entry — intentionally absent on Atlas. Does **not** fail verify. |
| `MISMATCH` | Keys/options differ. |
| `MISSING` | On source, not on destination. |
| `EXTRA` | On destination, not on source (e.g. a remapped TTL index left over after removing its `ttl_field_map` entry). |

| Exit | Meaning |
| --- | --- |
| `0` | PASS (possibly with `SKIPPED`). |
| `4` | INCOMPLETE — run `rectify`, then re-verify. |
| `3` | FAIL — `MISMATCH` / `MISSING` / `EXTRA`. |
| `1` | Connection / runtime / config error. |

---

## Sample output (lab, Cosmos 7.0 → MongoDB 8.0)

```
  Read 6 indexes from 9 collections on source (excluding _id_)

  Ensuring 9 collections exist on destination (dsync does not create empty ones)
  OK  createCollection  lab.empty_coll
  ...
  ── ensure summary  created: 9  |  already existed: 0  |  errors: 0 ──

  Creating 6 indexes  (TTL indexes → MAX_INT, unique indexes → non-unique)
  OK  createIndex  lab.idx_variety.ttl_expiresAt  [TTL field _ts→expiresAt, TTL 86400→MAX_INT]
  OK  createIndex  lab.idx_variety.wild_all
  OK  createIndex  lab.uniq_coll.ux_email  [unique→false]
  ── create summary  OK: 6  |  Errors: 0  |  Total: 6 ──
```

Without a `ttl_field_map` entry:

```
  SKIP  lab.idx_variety.ttl_ts  TTL on Cosmos system field _ts has no Atlas equivalent (add ttl_field_map["lab.idx_variety"] to recreate it on a date field)
  ...
  SKIPPED   lab.idx_variety.ttl_ts   TTL on _ts — not portable, intentionally absent on destination
  RESULT: PASS — destination indexes match source; 1 _ts TTL index(es) intentionally skipped.
```

---

## Cosmos DB facts this tool relies on (verified on server version 7.0, 26 Sep 2026)

- `listIndexes` returns `v: 1` and an `ns` field — both stripped before `createIndexes` (MongoDB ≥ 4.4 rejects `ns`).
- Exactly one TTL index per collection; TTL may be keyed on `_ts` (system field, not in documents) or on a user date field.
- Unique indexes on sharded collections **must be prefixed by the shard key** and are then globally unique — so they stay globally unique on Atlas. Only `_id` has the per-shard uniqueness hazard (see `assessCluster --mode duplicates`).
- `text` indexes are rejected by Cosmos, so none will appear on the source.
- Wildcard (`$**`) and compound indexes port unchanged.
- **dsync does not create empty collections** on the destination — observed with `dsynct sync --skip-change-stream` over one empty and one populated collection: only the populated one appeared.

---

## Build from source

Requires Go 1.22+.

```bash
go mod tidy
go build -o migrateIndexes .

CGO_ENABLED=0 GOOS=darwin  GOARCH=arm64 go build -ldflags="-s -w" -o bin/migrateIndexes-darwin-arm64 .
CGO_ENABLED=0 GOOS=darwin  GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-darwin-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-linux-amd64 .
CGO_ENABLED=0 GOOS=linux   GOARCH=arm64 go build -ldflags="-s -w" -o bin/migrateIndexes-linux-arm64 .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o bin/migrateIndexes-windows-amd64.exe .
```

---

## Notes

- Re-running `create` after `rectify` reports `IndexKeySpecsConflict` / `IndexOptionsConflict` for the rectified indexes — expected; they already exist in their final form. Use `verify`.
- The `_id_` index is always skipped. System databases and `system.*` collections are skipped.
- Keep `config.json` out of version control — it contains credentials.
