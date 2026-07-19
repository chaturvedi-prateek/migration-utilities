# MongoDB → MongoDB Migration Playbook (many clusters → one Atlas cluster)

End-to-end runbook for consolidating **N self-managed source clusters** into a
**single MongoDB Atlas cluster**, using Kafka + the MongoDB Kafka connectors for
**full copy + continuous CDC**, **merging** same-name collections, at high
throughput, with **zero-downtime** cutover and a **rollback** path.

All tooling here is Go (single static binaries, nothing to download at the
customer site) plus JSON connector configs. Every tool shares one config shape:
`sources: [{ label, uri, databases }]`.

```
                 ┌──────────────┐   ┌───────────────┐   ┌──────────────┐   ┌──────────────┐
  PRE-FLIGHT     │ idOverlapScan│   │ migrateIndexes│   │ kafkaConnectors│  │ migrateIndexes│
  & MIGRATION →  │  (scan _ids) │ → │ create+verify │ → │ backfill+CDC   │→ │ rectify+verify│
                 └──────────────┘   └───────────────┘   └──────────────┘   └──────────────┘
                        │                                       │
                        └── resolve collisions                  └── cutover per service ; rollback pipeline stands by
```

---

## 0. Prerequisites

- **Network:** private connectivity source → GCP/Atlas (VPC peering / PrivateLink);
  Kafka Connect host can reach source `27017`, Atlas `27017`, and the Kafka brokers `9092`.
- **Source:** replica sets (change streams require a replica set). Add/confirm
  **hidden nodes** so the snapshot reads never touch production primaries.
- **Kafka:** brokers up; internal Connect topics with RF ≥ 3, `min.insync.replicas=2`.
  MongoDB Kafka Connector JAR in the Connect `plugin.path`.
- **Atlas:** target cluster sized for the **backfill peak** (much higher than steady CDC);
  DB user with write access; source networks allow-listed.
- **A single source inventory** reused by every tool:

  ```json
  { "sources": [
    { "label": "cluster01", "uri": "mongodb://user:pass@rs01:27017/?replicaSet=rs01&readPreference=secondary&readPreferenceTags=nodeType:hidden", "databases": ["db_a","db_b"] },
    { "label": "cluster02", "uri": "mongodb://user:pass@rs02:27017/?replicaSet=rs02&readPreference=secondary&readPreferenceTags=nodeType:hidden", "databases": ["db_a","db_c"] }
  ] }
  ```

> **Merge model:** all source connectors use the same `topic.prefix` (`cdc`), so
> same `db.coll` from every cluster lands on one topic `cdc.<db>.<coll>` and the
> sink writes it into **one** target collection.

---

## 1. Pre-flight — find `_id` collisions  (`idOverlapScan/`)

Because same-name collections merge, two clusters may share an `_id`. Find that
surface **before** starting, not via the DLQ later.

```bash
cd idOverlapScan
cp config.sample.json config.json      # paste the shared sources inventory

# Fast candidate pass
./bin/idOverlapScan-linux-amd64 --config config.json --mode=range

# Confirm real duplicate _ids (resumable; findings -> results file)
./bin/idOverlapScan-linux-amd64 --config config.json --mode=exact --out overlap.log
```

- Exit `0` = no overlap → merge as-is.
- Exit `5` = overlap → **resolve per namespace** before backfill: remap `_id`
  (compose with a source tag), keep-newest, drop, or split that namespace instead
  of merging. Re-run `--mode=exact` until clean (or accept known overlaps knowing
  the sink will DLQ them).

**Gate:** do not proceed to backfill with unresolved, unintended overlaps.

---

## 2. Pre-create indexes (neutralized)  (`migrateIndexes/`)

Create the merged target indexes **before** the sink starts, with unique
constraints and TTLs neutralized so the CDC sync can't trip them mid-flight.

```bash
cd ../migrateIndexes
cp config.sample.json config.json      # same sources + destination_uri (Atlas)

# Dry run, then apply: creates every distinct index (unique->false, TTL->MAX_INT)
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=true
./bin/migrateIndexes-linux-amd64 --config config.json --mode=create --dry-run=false

# Confirm (expect INCOMPLETE = created but neutralized)
./bin/migrateIndexes-linux-amd64 --config config.json --mode=verify
```

- **Conflicts** (same `db.coll` index name with different defs across clusters,
  or same key pattern under different names) are **reported and skipped**, exit
  `3`. Resolve, then re-run `create`.
- Collections are auto-created by the sink on first write; only indexes need this
  step. (Optionally build big secondary indexes *after* backfill for speed.)

**Gate:** `create` summary has 0 errors and no unresolved conflicts.

---

## 3. Stand up the rollback pipeline (before any cutover)

Bring up the **reverse** Atlas → self-managed pipeline using the ready-made
configs in `kafkaConnectors/rollback/` (separate Connect cluster, Atlas source,
self-managed sink). Keep it ready so a cutover service can fail back with the
target kept current. Do this **before** flipping any writes.

```bash
cd kafkaConnectors
# start the rollback Connect workers with rollback/connect-distributed.properties, then:
CONNECT_URL=http://rollback-host:8084 ./manage.sh register rollback/mongo-source-atlas.json
CONNECT_URL=http://rollback-host:8084 ./manage.sh register rollback/mongo-sink-selfmanaged.json
```

> **Merged namespaces don't un-merge:** rollback restores to a single
> consolidated self-managed target, not the original N clusters. To retain
> fail-back to the *original* topology, keep those source clusters intact until
> the soak period ends. See `kafkaConnectors/rollback/README.md`.

> `mongorestoreResume/` is a separate, optional helper for **restore-based** moves
> (resuming a `mongorestore --oplogReplay` that stopped midway) — not part of the
> Kafka path, but useful for a dump/restore seed or recovery.

---

## 4. Backfill + CDC  (`kafkaConnectors/`)

```bash
cd ../kafkaConnectors
cp sources.sample.json sources.json    # same inventory
./generate-sources.sh                  # -> source/generated/mongo-source-*.json  (requires python3)

# Start Connect workers (distributed mode) on the Connect host:
#   bin/connect-distributed.sh config/connect-distributed.properties

# Register the 10 source connectors (snapshot + CDC). Stagger 3-4 at a time.
CONNECT_URL=http://connect-host:8083 ./manage.sh register-sources
```

Then the sink, per the collision strategy:

- **Phase A — collision-detection backfill** (per merged collection): source with
  `publish.full.document.only=true`; register `sink/mongo-sink-backfill.json`
  (insert-only). Real cross-source `_id` overlaps land in
  `dlq.collision.<db>.<coll>` instead of silently overwriting. Reconcile.
- **Phase B — steady-state CDC:** delete the backfill sink, set source back to
  `publish.full.document.only=false`, register `sink/mongo-sink-cdc.json`
  (ChangeStreamHandler + namespace-merge + upsert). CDC now flows continuously.

```bash
./manage.sh register sink/mongo-sink-cdc.json
./manage.sh status        # connector + task states
./manage.sh lag           # consumer-group lag
```

> If step 1 confirmed `_id`s are globally unique for a collection, skip Phase A
> and go straight to the CDC sink.

**Gate:** all source connectors `RUNNING`, sink lag trending to ~0.

---

## 5. Validate

- **Counts** per merged namespace (source totals vs Atlas), accounting for merges.
- **Content spot-checks** (hash sampled docs both sides).
- **CDC correctness:** insert/update/delete on a source → appears on Atlas; check
  delete propagation and per-`_id` ordering.
- **DLQ empty** (or only expected/reconciled collisions).

**Gate:** validation clean and CDC lag ~0 before cutover.

---

## 6. Cutover (zero-downtime, service by service)

For each service: when its collections' CDC lag is ~0, flip its connection string
to Atlas. Apps keep writing to the source until flipped, so migrate incrementally
— no big-bang. The rollback pipeline (step 3) keeps the source current in case a
service must fail back.

---

## 7. Post-cutover — enforce indexes  (`migrateIndexes/`)

Once source writes for a namespace are frozen, restore real constraints:

```bash
cd ../migrateIndexes
./bin/migrateIndexes-linux-amd64 --config config.json --mode=rectify --dry-run=false
./bin/migrateIndexes-linux-amd64 --config config.json --mode=verify     # expect PASS (exit 0)
```

`rectify` restores original TTL values and enforces `unique:true` (via
`prepareUnique` → `collMod unique:true`).

**Gate:** `verify` returns PASS.

---

## 8. Decommission

After a soak period with the rollback pipeline running and no issues, tear down
the reverse pipeline and retire the source clusters.

---

## Tool reference

| Step | Tool | Doc |
| --- | --- | --- |
| 1 | `idOverlapScan/` — pre-flight `_id` collision scan (resumable, results file) | [README](idOverlapScan/README.md) |
| 2,7 | `migrateIndexes/` — multi-source index create / verify / rectify | [README](migrateIndexes/README.md) |
| 3,4 | `kafkaConnectors/` — distributed worker + source template/generator + sinks (forward **and**, reversed, the rollback pipeline) | [README](kafkaConnectors/README.md) |
| opt | `mongorestoreResume/` — resume a `mongorestore --oplogReplay` (restore-based moves / recovery) | [README](mongorestoreResume/README.md) |

---

## Cutover checklist

- [ ] Connectivity, hidden nodes, Kafka internal topics RF≥3, connector plugin installed
- [ ] Shared `sources` inventory prepared (hidden-node read preference)
- [ ] `idOverlapScan --mode=exact` → overlaps resolved (or knowingly accepted)
- [ ] `migrateIndexes --mode=create` applied; `verify` = INCOMPLETE (neutralized)
- [ ] Rollback (reverse) pipeline stood up
- [ ] Source connectors registered (staggered); backfill collisions reconciled from DLQ
- [ ] CDC sink registered; lag ~0
- [ ] Validation (counts, content, CDC insert/update/delete, DLQ) clean
- [ ] Services flipped to Atlas one by one
- [ ] `migrateIndexes --mode=rectify` → `verify` = PASS
- [ ] Soak period passed → reverse pipeline torn down, sources retired
