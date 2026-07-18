# kafkaConnectors — 10 on-prem clusters → 1 Atlas cluster

Kafka Connect configs for the forward migration: **10 self-managed on-prem
replica sets → one 5-node Atlas cluster (GCP)** using the MongoDB Kafka
**source** and **sink** connectors, doing **full copy + CDC** in one lifecycle,
**merging** same-name collections, at high throughput.

The **reverse/rollback** pipeline (Atlas → self-managed) is a separate
playbook; this directory is the forward direction only.

```
on-prem RS x10 ──(source connectors)──▶ topics cdc.<db>.<coll> ──(sink)──▶ Atlas GCP
   read hidden nodes        same topic.prefix => same-name collections MERGE
```

---

## Files

| File | What it is |
| --- | --- |
| `connect-distributed.properties` | Distributed Connect **worker** config (durable Kafka-backed offsets). |
| `source/mongo-source.template.json` | Source-connector template (`__LABEL__`, `__SOURCE_URI__`, `__NS_REGEX__`, `__PIPELINE__`). |
| `sources.sample.json` | Inventory of the 10 clusters → copy to `sources.json` and edit. |
| `generate-sources.sh` | Renders `source/generated/mongo-source-<label>.json` for each cluster. |
| `sink/mongo-sink-cdc.json` | **Main** sink: CDC handler + namespace-merge + upsert (steady state). |
| `sink/mongo-sink-backfill.json` | **Collision-detection** sink: insert-only → duplicate `_id` to DLQ. |
| `manage.sh` | Register / status / pause / restart / delete via the Connect REST API. |

---

## Why these settings

- **Distributed mode** — offsets/config/status in replicated Kafka topics so a
  worker restart resumes each change stream from its **resume token** (no
  re-snapshot, no data loss). Standalone (as in the rollback playbook) is fine
  for one low-volume stream but not for 10 high-volume ones.
- **`copy.existing=true`** — snapshot **then** tail the change stream in one
  connector: satisfies "complete data + CDC as it comes."
- **Same `topic.prefix` (`cdc`) on all 10 sources** — same `db.coll` from every
  cluster lands on **one** topic `cdc.<db>.<coll>`, so the sink's
  `FieldPathNamespaceMapper` writes them into **one merged** target collection.
- **Read from hidden nodes** (`readPreferenceTags=nodeType:hidden`) — the full
  snapshot scan never touches production primaries.
- **`updateLookup`** — updates carry the full document so the sink can apply
  them without a round-trip.
- **Throughput** — `zstd` compression, batched producer, unordered bulk writes,
  `tasks.max` = topic partition count. Partition topics by `_id` hash for
  per-document ordering.

---

## The `_id`-collision requirement (read this)

You asked that a cross-source `_id` overlap **error** (so you decide how to copy
those) rather than silently overwrite. There's a genuine trade-off:

- The **CDC handler** (`mongo-sink-cdc.json`) applies changes with `ReplaceOne`
  **upsert** — correct, ordered, idempotent for live CDC, but a duplicate `_id`
  is an **overwrite**, not an error.
- **Insert-only** (`mongo-sink-backfill.json`) makes a duplicate `_id` throw
  `E11000` → **DLQ**, but insert mode writes the plain document and can't read
  `ns.db/ns.coll`, so you drive it **one sink per merged topic/collection** and
  set the source to `publish.full.document.only=true` during that phase.

**Recommended sequence per merged collection:**

1. **Backfill + detect (Phase A):** source `copy.existing=true`,
   `publish.full.document.only=true`; run `mongo-sink-backfill.json` (insert-only)
   for that topic. Real cross-source `_id` overlaps pile up in
   `dlq.collision.<db>.<coll>`. Inspect and decide (remap, keep newest, drop).
2. **Switch to steady-state (Phase B):** delete the backfill sink, set the
   source back to `publish.full.document.only=false`, and start
   `mongo-sink-cdc.json` (CDC handler + merge + upsert) for continuous CDC.

> Belt-and-suspenders: also run a **pre-flight `_id` overlap scan** (compare
> `_id` sets / min-max ranges across clusters for each merged namespace) before
> starting, so you know the collision surface up front rather than discovering it
> only via the DLQ.

If, for a given collection, you confirm `_id`s are globally unique, you can skip
Phase A and go straight to `mongo-sink-cdc.json`.

---

## Run it

```bash
# 0. Prereqs: distributed Connect workers running with connect-distributed.properties,
#    MongoDB Kafka Connector in plugin.path, topics' internal RF>=3.

# 1. Build the source configs   (requires python3)
cp sources.sample.json sources.json      # edit URIs (hidden nodes!) + databases
./generate-sources.sh                     # -> source/generated/mongo-source-*.json

# 2. (Optional but recommended) create target collections + indexes first
#    via ../migrateIndexes  (create -> verify)

# 3. Register sources (starts snapshot + CDC)
CONNECT_URL=http://connect-host:8083 ./manage.sh register-sources

# 4. Register the sink (Phase A backfill per collection, or Phase B CDC)
./manage.sh register sink/mongo-sink-cdc.json

# 5. Watch
./manage.sh status
./manage.sh lag
```

Stagger clusters (register 3–4 at a time) so 10 simultaneous snapshots don't
saturate Atlas writes or the on-prem→GCP link.

---

## Cutover & rollback

- **Zero-downtime:** keep apps writing on-prem until each collection's CDC lag is
  ~0, then flip that service's connection string to Atlas. Migrate service by
  service.
- **Rollback:** the reverse Atlas→self-managed pipeline stays running so the
  source remains current until the soak period ends.
- **Post-cutover indexes:** run `../migrateIndexes --mode=rectify` to restore
  real TTL values and enforce `unique:true`, then `--mode=verify` (expect PASS).
