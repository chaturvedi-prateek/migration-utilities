# kafkaConnectors/rollback — reverse pipeline (Atlas → self-managed)

The **rollback / fail-back** direction of the migration: tail the **Atlas**
change stream and apply it to a **self-managed** target, so after cutover the
target stays current and a service can be flipped back if needed.

```
Atlas GCP ──(rollback-source-atlas)──▶ topics rb.<db>.<coll> ──(rollback-sink-selfmanaged)──▶ self-managed target
   (source of truth after cutover)                              preserve namespaces (no merge)
```

Stand this up **before** flipping any service's writes to Atlas (Playbook step 3),
and keep it running through the soak period.

---

## Files

| File | What it is |
| --- | --- |
| `connect-distributed.properties` | Worker config for a **separate** Connect cluster (own `group.id`, own internal topics, REST on `:8084`). |
| `mongo-source-atlas.json` | Source: tails the Atlas change stream (no `copy.existing` — target is already seeded). Topic prefix `rb`. |
| `mongo-sink-selfmanaged.json` | Sink: `ChangeStreamHandler` + `FieldPathNamespaceMapper` + upsert → writes to the self-managed target, preserving `db.coll`. |

---

## Why it's configured this way

- **Separate Connect cluster** (`group.id=mongo-rollback-connect`, `rollback`
  internal topics, port `8084`, topic prefix `rb`) so forward and rollback never
  share offsets or topics.
- **No `copy.existing`** — rollback only needs the **changes** applied to Atlas
  from the cutover point onward; the target already holds the pre-cutover data.
- **Upsert / last-writer-wins** — this direction is **one Atlas → one target**,
  no merge, so there's no cross-source `_id` collision concern; a plain
  `ChangeStreamHandler` upsert is correct.
- **`updateLookup`** so updates carry the full document.

---

## Run it

```bash
# On the rollback Connect host, start workers with the rollback worker config:
#   bin/connect-distributed.sh config/rollback/connect-distributed.properties

# Register the reverse pipeline (edit URIs + the ns.db $in list first):
CONNECT_URL=http://rollback-host:8084 ../manage.sh register rollback/mongo-source-atlas.json
CONNECT_URL=http://rollback-host:8084 ../manage.sh register rollback/mongo-sink-selfmanaged.json

CONNECT_URL=http://rollback-host:8084 ../manage.sh status
```

To **fail a service back**: confirm the rollback sink lag is ~0 for its
collections, then point that service's connection string back at the
self-managed target.

---

## Important: merged namespaces don't un-merge

If same-name collections from several source clusters were **merged** into one
Atlas collection during the forward migration, this rollback restores that data
to a **single consolidated self-managed target** — it does **not** split it back
into the original N clusters. Plan fail-back against the consolidated target, not
the original per-cluster topology.

If you need to preserve the ability to roll back to the *original* per-cluster
clusters, keep those clusters intact (don't decommission) and fail back by
repointing services there, using this pipeline only for namespaces that were
**not** merged.

---

## Teardown

After the soak period with no fail-back needed (Playbook step 8), delete the
rollback connectors and stop the rollback Connect cluster:

```bash
CONNECT_URL=http://rollback-host:8084 ../manage.sh delete rollback-sink-selfmanaged
CONNECT_URL=http://rollback-host:8084 ../manage.sh delete rollback-source-atlas
```
