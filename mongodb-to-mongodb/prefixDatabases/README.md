# prefixDatabases

`prefix-databases.js` copies every non-system database on a MongoDB deployment
to a new name with a prefix applied (e.g. `sales` → `myprefix_sales`), and
optionally drops the originals afterward.

MongoDB has no native "rename database" operation, so this is a **server-side
copy** using an aggregation `$out` stage, followed by explicit recreation of
collection options, indexes, and views.

The script is **resumable** and **fault-tolerant**: completed collections are
checkpointed and skipped on re-runs, and a single failing collection never
aborts the run.

## Requirements

- `mongosh` with connectivity to the deployment.
- A user with rights to read the source databases, create databases /
  collections / indexes, and write to `admin.db_rename_checkpoint` (plus
  `dropDatabase` if using `DROP_SOURCE`).

## Usage

Driven by variables passed with `--eval`:

```bash
# 1. dry run — prints the full plan, changes nothing
mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=true'  --file prefix-databases.js

# 2. real copy, sources kept
mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=false' --file prefix-databases.js

# 3. after verifying, drop the sources
mongosh "$URI" --eval 'var PREFIX="myprefix_", DRY_RUN=false, DROP_SOURCE=true' --file prefix-databases.js
```

For a long run, detach it so a dropped session doesn't kill the copy:

```bash
nohup mongosh "$URI?socketTimeoutMS=0&connectTimeoutMS=60000&serverSelectionTimeoutMS=60000" \
  --eval 'var PREFIX="myprefix_", DRY_RUN=false' \
  --file prefix-databases.js > ./prefix-databases.log 2>&1 < /dev/null &
```

## Parameters

| Variable       | Default | Description                                                                          |
| -------------- | ------- | ------------------------------------------------------------------------------------ |
| `PREFIX`       | —       | **Required.** String prepended to each database name.                                |
| `DRY_RUN`      | `true`  | When `true`, prints the plan only. Set `false` to execute.                           |
| `DROP_SOURCE`  | `false` | Drop each source database after all its collections copy cleanly.                    |
| `RESUME`       | `true`  | Checkpoint completed collections and skip them on re-runs.                           |
| `TTL_TOLERANT` | `true`  | Treat count drift on TTL collections as a warning rather than a failure.             |
| `TOLERANCE`    | `0`     | Allowed `\|source - target\|` for collections **without** a TTL index.               |
| `ONLY_DBS`     | `null`  | Restrict the run to specific source databases, e.g. `var ONLY_DBS=["db1","db2"]`.    |

## How it works

For each non-system database (excluding `admin`, `local`, `config`, and any name
already starting with `PREFIX`):

1. Recreates each collection on the target with its original options (capped,
   validator, collation, timeseries, …).
2. Copies data server-side with `$out` (atomic replace of the target).
3. Recreates all non-`_id_` indexes — `$out` does not carry them over.
4. Verifies source and target document counts, then checkpoints.
5. Recreates views last, once their source collections exist.
6. Drops the source database if `DROP_SOURCE=true` **and** nothing in that
   database is unresolved.

## Logging

A dry run (or the start of any run) prints a plan showing per-database progress
and totals:

```
========================== PLAN ==========================
databases          : 52 in scope, 38 with work remaining
collections        : 431 total
  already complete : 274 (checkpointed, will be skipped)
  remaining        : 157
views              : 0
----------------------------------------------------------
  db_one   -> myprefix_db_one    [COMPLETE (14)]
  db_two   -> myprefix_db_two    [9/23 done, 14 left]
==========================================================
```

Each collection logs a timestamped progress line, and the run ends with a
summary of what completed, what drifted, and what still needs a re-run:

```
========================= SUMMARY =========================
elapsed              : 2h14m
collections total    : 431
  complete before    : 274
  copied this run    : 155  (48,201,377 docs)
  TTL/tolerated drift: 6  (counted as complete)
  mismatched         : 1
  errored            : 1
-----------------------------------------------------------
COMPLETE             : 429/431  (99.5%)
REMAINING            : 2
```

Exit code is `0` when nothing is outstanding, `1` otherwise — usable in a
wrapper script.

## Resume

Completed collections are recorded in `admin.db_rename_checkpoint`, keyed by
`{source database, target database, collection}`. Re-run the **identical**
command to resume; finished work is skipped and only outstanding collections
are retried.

Collections that mismatched or errored are deliberately **not** checkpointed, so
they are retried automatically on the next run. A crash mid-`$out` is also safe:
`$out` replaces the target collection wholesale, so a partial copy is discarded
rather than merged.

To start a completely fresh migration:

```javascript
db.getSiblingDB("admin").db_rename_checkpoint.drop()
```

> The checkpoint key includes the target database name. Checkpoints written
> under one prefix will therefore never cause skips under a different prefix.
> If an older checkpoint collection with a `{src, coll}` unique index is present,
> the script drops that index and replaces it with the prefix-aware one.

## TTL collections

Collections with a TTL index legitimately change during the copy: the TTL
monitor expires documents on the source after `$out` has read them, and on the
target once the TTL index is recreated. This produces small count differences in
either direction.

With `TTL_TOLERANT=true` (the default) these are logged as `WARN TTL drift`,
counted as complete, and listed in the summary. Set `TTL_TOLERANT=false` to
treat them as hard mismatches. Count differences on **non**-TTL collections are
always mismatches unless within `TOLERANCE`.

## Caveats

- **Not atomic and not online.** Application writes arriving during the copy are
  lost. Stop writers, or use a change-stream based tool for a live cutover. TTL
  expiry is the one form of source-side change tolerated here.
- The resume checkpoint assumes the source is otherwise frozen — a collection
  marked done is never re-copied.
- Resume granularity is one collection: a crash mid-`$out` redoes that whole
  collection.
- Sharded collections are not re-sharded; `$out` writes an unsharded target.
- Users and roles scoped to the old database names must be recreated manually.
- Drop sources only after verifying counts and index parity.
