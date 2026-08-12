# prefixDatabases

`prefix-databases.js` copies every non-system database on a MongoDB deployment
to a new name with a prefix applied (e.g. `sales` → `myprefix_sales`), and
optionally drops the originals afterward.

MongoDB has no native "rename database" operation, so this is a **server-side
copy** using an aggregation `$out` stage, followed by explicit recreation of
collection options, indexes, and views.

## Requirements

- `mongosh` with connectivity to the target deployment.
- A connection user with rights to read the source databases and create
  databases/collections/indexes (and drop databases if using `DROP_SOURCE`).

## Usage

The script is driven by variables passed with `--eval`:

```bash
# 1. dry run — lists what would be copied, changes nothing
mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=true'  --file prefix-databases.js

# 2. real copy, sources kept
mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=false' --file prefix-databases.js

# 3. after verifying, drop the sources
mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=false, DROP_SOURCE=true' --file prefix-databases.js
```

## Parameters

| Variable      | Default | Description                                                        |
| ------------- | ------- | ------------------------------------------------------------------ |
| `PREFIX`      | —       | **Required.** String prepended to each database name.              |
| `DRY_RUN`     | `true`  | When `true`, only prints the plan. Set `false` to perform the copy.|
| `DROP_SOURCE` | `false` | Drop each source database after a successful copy.                 |
| `RESUME`      | `true`  | Checkpoint completed collections and skip them on re-runs.         |

## How it works

For each non-system database (excluding `admin`, `local`, `config`, and any name
already starting with `PREFIX`):

1. Recreates each collection on the target with its original options (capped,
   validator, collation, timeseries, …).
2. Copies data server-side with `$out` (atomic replace of the target).
3. Recreates all non-`_id_` indexes.
4. Verifies source and target document counts match before checkpointing.
5. Recreates views last, once their source collections exist.
6. Drops the source database if `DROP_SOURCE=true`.

## Resume

Completed collections are recorded in `admin.db_rename_checkpoint`, so an
interrupted run skips work that already finished. This is only safe if writes to
the source are stopped. To start a fresh migration, drop the checkpoint:

```javascript
db.getSiblingDB("admin").db_rename_checkpoint.drop()
```

## Idempotency

Databases already starting with `PREFIX` are skipped, and target names longer
than the 63-character limit are skipped with a warning. Re-running with the same
`PREFIX` resumes rather than duplicating work.

## Caveats

- **Not atomic and not online.** Writes arriving during the copy are lost. Stop
  writers, or use a change-stream based tool for a live cutover.
- The resume checkpoint assumes the source is frozen. If writers are still
  running, a collection marked done can drift and will never be re-copied.
- Resume granularity is one collection: a crash mid-`$out` redoes that whole
  collection.
- Sharded collections are not re-sharded; `$out` writes an unsharded target.
- Users/roles scoped to the old database names must be recreated manually.
- Drop sources only after verifying counts and index parity.
