#!/usr/bin/env python3
"""
oplogResumeLocal.py — Runs on LOCAL machine (needs only: pip install pymongo)

Two modes:

  Mode 1 — Generate a mongosh script to find the resume point on the server:
    python3 oplogResumeLocal.py --dump oplog.bson --generate

    → Produces find_resume.js  (copy this to the server and run with mongosh)

  Mode 2 — Filter the dump from a known resume point:
    python3 oplogResumeLocal.py --dump oplog.bson --filter --t 1718500000 --i 1 --out ./resume_dir

    → Produces ./resume_dir/oplog.bson  (copy this to server and run mongorestore)
"""

import argparse
import os
import struct
import sys

try:
    import bson
    if not hasattr(bson, 'decode'):
        raise ImportError
except ImportError:
    print("ERROR: Wrong or missing bson package.")
    print("  Fix: pip uninstall bson && pip install pymongo")
    sys.exit(1)


def read_entries(dump_path):
    entries = []
    with open(dump_path, "rb") as f:
        while True:
            size_bytes = f.read(4)
            if len(size_bytes) < 4:
                break
            size = struct.unpack("<i", size_bytes)[0]
            raw = size_bytes + f.read(size - 4)
            doc = bson.decode(raw)
            ts = doc.get("ts")
            if ts:
                entries.append((ts.time, ts.inc))
    return entries


def generate_mongosh_script(entries, out_file="find_resume.js"):
    """Embed last 500 ts values into a mongosh script the server can run."""
    tail = entries[-500:]
    first_t, first_i = entries[0]
    last_t,  last_i  = entries[-1]

    ts_array = ",\n  ".join(f"Timestamp({t}, {i})" for t, i in tail)

    script = f"""// find_resume.js — run on the server with:
//   mongosh "mongodb+srv://user:pass@cluster" --file find_resume.js
//
// Dump range: Timestamp({first_t}, {first_i}) -> Timestamp({last_t}, {last_i})
// Total entries in dump: {len(entries)}

(async () => {{
  const tsValues = [
  {ts_array}
  ];

  const oplog = db.getSiblingDB("local")["oplog.rs"];

  // Check if already fully applied
  const lastApplied = await oplog.findOne(
    {{ ts: Timestamp({last_t}, {last_i}) }}
  );
  if (lastApplied) {{
    print("STATUS: COMPLETE — replay already fully applied.");
    print("RESUME_T: {last_t}");
    print("RESUME_I: {last_i}");
    return;
  }}

  // Find last matching ts from the tail of the dump
  const result = await oplog.find(
    {{ ts: {{ $in: tsValues }} }},
    {{ ts: 1, _id: 0 }}
  ).sort({{ $natural: -1 }}).limit(1).toArray();

  if (result.length === 0) {{
    print("STATUS: NOT_FOUND — no matching entries on target oplog (may have rolled over).");
    print("RESUME_T: UNKNOWN");
    print("RESUME_I: UNKNOWN");
    return;
  }}

  const ts = result[0].ts;
  print("STATUS: FOUND");
  print("RESUME_T: " + ts.getTime());
  print("RESUME_I: " + ts.getInc());
}})();
"""

    with open(out_file, "w") as f:
        f.write(script)

    print(f"Generated: {out_file}")
    print(f"  Embedded last {len(tail)} ts values from dump ({len(entries)} total)")
    print(f"\nNext steps:")
    print(f"  1. Copy {out_file} to the server")
    print(f'  2. Run: mongosh "mongodb+srv://..." --file {out_file} --quiet')
    print(f"  3. Note the RESUME_T and RESUME_I values from output")
    print(f"  4. Run this script in --filter mode with those values")


def filter_dump(dump_path, resume_t, resume_i, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "oplog.bson")

    count = skipped = 0
    with open(dump_path, "rb") as fin, open(out_path, "wb") as fout:
        while True:
            size_bytes = fin.read(4)
            if len(size_bytes) < 4:
                break
            size = struct.unpack("<i", size_bytes)[0]
            raw  = size_bytes + fin.read(size - 4)
            doc  = bson.decode(raw)
            ts   = doc.get("ts")
            if ts and (ts.time, ts.inc) > (resume_t, resume_i):
                fout.write(raw)
                count += 1
            else:
                skipped += 1

    print(f"Filtered dump: {skipped} entries skipped, {count} entries written")
    print(f"Output: {out_path}")

    if count == 0:
        print("WARNING: No entries written — resume point may be at or past end of dump.")
        return

    print(f"\nNext steps:")
    print(f"  1. Copy {out_path} to the server restore directory")
    print(f'  2. Run: mongorestore --uri="mongodb+srv://..." --oplogReplay --dir={out_dir}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump",     required=True, help="Path to oplog.bson")
    parser.add_argument("--generate", action="store_true", help="Generate mongosh find_resume.js")
    parser.add_argument("--filter",   action="store_true", help="Filter dump from resume point")
    parser.add_argument("--t",        type=int, help="Resume Timestamp t value (from find_resume.js output)")
    parser.add_argument("--i",        type=int, help="Resume Timestamp i value (from find_resume.js output)")
    parser.add_argument("--out",      default="./resume_dir", help="Output dir for filtered dump")
    parser.add_argument("--js",       default="find_resume.js", help="Output file for mongosh script")
    args = parser.parse_args()

    if not args.generate and not args.filter:
        parser.print_help()
        sys.exit(1)

    print(f"Reading dump: {args.dump}")
    entries = read_entries(args.dump)
    if not entries:
        print("ERROR: No entries found in dump.")
        sys.exit(1)
    print(f"  {len(entries):,} entries  |  "
          f"Timestamp({entries[0][0]}, {entries[0][1]}) → Timestamp({entries[-1][0]}, {entries[-1][1]})")

    if args.generate:
        generate_mongosh_script(entries, args.js)

    if args.filter:
        if args.t is None or args.i is None:
            print("ERROR: --filter requires --t and --i")
            sys.exit(1)
        print(f"\nFiltering from Timestamp({args.t}, {args.i}) …")
        filter_dump(args.dump, args.t, args.i, args.out)


if __name__ == "__main__":
    main()
