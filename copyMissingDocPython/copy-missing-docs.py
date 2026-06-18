#!/usr/bin/env python3
"""
Copy missing documents from DocumentDB (source) to MongoDB Atlas (destination).

Set environment variables before running:
  export DOCDB_SRC="mongodb://user:pass@docdb-host:27017/?tls=true&tlsCAFile=..."
  export MDB_DEST="mongodb+srv://user:pass@cluster.mongodb.net/"

Run dry-run first (DRY_RUN = True), review output, then set DRY_RUN = False.
"""

import os
import sys
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

# ── Config ────────────────────────────────────────────────────────────────────
DRY_RUN    = True   # Change to False to actually apply changes
BATCH_SIZE = 500    # Documents per insert batch

NAMESPACES = [
    ("dms-trv-db",           "st_tv_stock_log"),   # src=52  dst=45  diff=7
    ("dms-trv-db",           "sh_tv_stock"),        # src=755555 dst=755554 diff=1
    ("ebook-delegate-db",    "token"),              # src=0  dst=1   diff=-1 (extra in dst)
    ("ebook-usersession-db", "usersession"),        # src=77687 dst=77685 diff=2
    ("svoc-db",              "sync_job_log"),       # src=8155 dst=8031 diff=124
]
# ──────────────────────────────────────────────────────────────────────────────


def sync_collection(src_db, dst_db, col_name, dry_run):
    ns = f"{src_db.name}.{col_name}"
    prefix = "[DRY RUN] " if dry_run else ""
    print(f"\n{prefix}── {ns} ──")

    src_col = src_db[col_name]
    dst_col = dst_db[col_name]

    # Fetch all _ids from both sides
    print("  Fetching _ids from source...")
    src_ids = {doc["_id"] for doc in src_col.find({}, {"_id": 1})}
    print("  Fetching _ids from destination...")
    dst_ids = {doc["_id"] for doc in dst_col.find({}, {"_id": 1})}

    missing_in_dst = src_ids - dst_ids   # in source but not destination → INSERT
    extra_in_dst   = dst_ids - src_ids   # in destination but not source  → DELETE

    print(f"  Source:      {len(src_ids):>8} docs")
    print(f"  Destination: {len(dst_ids):>8} docs")
    print(f"  To INSERT:   {len(missing_in_dst):>8} docs")
    print(f"  To DELETE:   {len(extra_in_dst):>8} docs")

    inserted = 0
    deleted  = 0
    errors   = 0

    # ── INSERT missing documents ──────────────────────────────────────────────
    if missing_in_dst:
        missing_list = list(missing_in_dst)
        for i in range(0, len(missing_list), BATCH_SIZE):
            batch_ids = missing_list[i : i + BATCH_SIZE]
            docs = list(src_col.find({"_id": {"$in": batch_ids}}))
            if not docs:
                continue
            if dry_run:
                inserted += len(docs)
                for doc in docs[:3]:
                    print(f"  [DRY RUN] Would INSERT _id={doc['_id']}")
                if len(docs) > 3:
                    print(f"  [DRY RUN] ... and {len(docs) - 3} more")
            else:
                try:
                    result = dst_col.insert_many(docs, ordered=False)
                    inserted += len(result.inserted_ids)
                    print(f"  Inserted batch {i // BATCH_SIZE + 1}: {len(result.inserted_ids)} docs")
                except BulkWriteError as e:
                    n = e.details.get("nInserted", 0)
                    inserted += n
                    errs = e.details.get("writeErrors", [])
                    errors += len(errs)
                    print(f"  WARNING: batch had {len(errs)} errors, {n} inserted")
                    for err in errs[:3]:
                        print(f"    Error on _id={err.get('keyValue', {})}: {err.get('errmsg', '')}")

    # ── DELETE documents removed from source ─────────────────────────────────
    if extra_in_dst:
        print(f"  WARNING: {len(extra_in_dst)} doc(s) exist in destination but NOT in source")
        for _id in list(extra_in_dst)[:5]:
            print(f"    Extra _id={_id}")
        if len(extra_in_dst) > 5:
            print(f"    ... and {len(extra_in_dst) - 5} more")

        if dry_run:
            deleted = len(extra_in_dst)
            print(f"  [DRY RUN] Would DELETE {deleted} doc(s) from destination")
        else:
            for _id in extra_in_dst:
                result = dst_col.delete_one({"_id": _id})
                deleted += result.deleted_count
            print(f"  Deleted {deleted} doc(s) from destination")

    # ── Summary ───────────────────────────────────────────────────────────────
    action = "Would" if dry_run else "Done —"
    print(f"  {action} INSERT {inserted} | DELETE {deleted} | Errors {errors}")
    return inserted, deleted, errors


def main():
    src_uri = os.environ.get("DOCDB_SRC")
    dst_uri = os.environ.get("MDB_DEST")

    if not src_uri:
        print("ERROR: DOCDB_SRC environment variable not set")
        sys.exit(1)
    if not dst_uri:
        print("ERROR: MDB_DEST environment variable not set")
        sys.exit(1)

    mode = "DRY RUN — no changes will be written" if DRY_RUN else "LIVE RUN — changes will be applied"
    print(f"\n{'='*60}")
    print(f"  copy-missing-docs.py  |  {mode}")
    print(f"{'='*60}")

    src_client = MongoClient(src_uri)
    dst_client = MongoClient(dst_uri)

    total_inserted = 0
    total_deleted  = 0
    total_errors   = 0

    for db_name, col_name in NAMESPACES:
        ins, dlt, err = sync_collection(
            src_client[db_name],
            dst_client[db_name],
            col_name,
            DRY_RUN,
        )
        total_inserted += ins
        total_deleted  += dlt
        total_errors   += err

    print(f"\n{'='*60}")
    print(f"  TOTAL  INSERT: {total_inserted}  |  DELETE: {total_deleted}  |  Errors: {total_errors}")
    print(f"{'='*60}")

    if DRY_RUN:
        print("\n  Set DRY_RUN = False in the script and re-run to apply changes.")

    src_client.close()
    dst_client.close()


if __name__ == "__main__":
    main()
