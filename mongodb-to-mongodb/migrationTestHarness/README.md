# migrationTestHarness

End-to-end test of the **10 → 10 → 1** migration playbook on a single Linux host.
It exercises the *real* `mongosyncOrchestrator` binary against real `mongosync`, so
issues in the playbook surface before production.

What it does, in order:

1. **Acquire tools** — finds `mongod`, `mongosh`, `mongosync` on `PATH` or downloads
   them into `<base-dir>/tools`; locates `mongosyncOrchestrator` (shipped alongside).
2. **Provision** — starts *N* source + *N* destination single-node replica sets as
   local `mongod` processes (destinations run with `--auth`/keyfile and a migration
   user, mirroring the Atlas requirement that the destination have a user).
3. **Seed** — writes ~`--data-size-mb` of disjoint data (`appdbNN`) into each source.
4. **Phase 1** — `mongosyncOrchestrator sync`: 10 parallel 1:1 syncs to steady state
   (left running).
5. **Cutover** — `mongosyncOrchestrator commit`: commit all 10 together, then verify
   each destination matches its source.
6. **Phase 2** — `mongosyncOrchestrator consolidate --verify`: fold dst02..dstN into
   dst01 (the hub) one at a time, then verify the hub holds every database and has no
   leftover mongosync metadata.
7. **Bundle** — writes `<base-dir>/migtest-logs.tgz` (all logs + the generated
   `orchestrator.json`) to share back.

## Requirements

- **Linux x86_64** (mongosync ships x86_64 only). An EC2 `m5.4xlarge`
  (16 vCPU / 64 GB) or larger is recommended for 10 clusters at multi-GB each.
- **Disk:** budget ≳ `2 × N × data-size` on a fast volume (source + destination copies,
  plus the hub ends up holding all N databases). For 10×2 GB, provision ≥ 150 GB.
- `curl`, `tar`, `openssl` on `PATH` (standard on Amazon Linux / Ubuntu).
- Outbound HTTPS to `fastdl.mongodb.org` / `downloads.mongodb.com` (or pre-stage the
  tarballs and point `HARNESS_SERVER_TGZ`, `HARNESS_MONGOSH_TGZ`,
  `HARNESS_MONGOSYNC_TGZ` at local files, then pass `--skip-download`).

## Run

```shell
tar -xzf migration-playbook-test.tgz && cd migration-playbook-test

# Full run: 10 clusters, ~2 GB each, working dir on a big fast volume.
./migrationTestHarness --clusters 10 --data-size-mb 2048 --base-dir /mnt/migtest

# Quick smoke test first (recommended):
./migrationTestHarness --clusters 3 --data-size-mb 128 --base-dir /mnt/migtest-smoke
```

Flags:

| Flag | Default | Meaning |
| :---- | :---- | :---- |
| `--clusters` | 10 | number of source (and destination) clusters |
| `--data-size-mb` | 2048 | approx data seeded per source cluster |
| `--base-dir` | `./migtest` | working dir for tools, data, logs |
| `--skip-download` | false | require tools present; do not download |
| `--keep` | false | leave clusters running after the test |
| `--teardown-only` | false | stop clusters from a prior `--keep` run and exit |
| `--emit-config <path>` | — | write the `orchestrator.json` it would use and exit |

## Result

- Console + `harness.log` show every step; each phase prints `PASS`/`FAIL` checks.
- Exit code `0` = all checks passed, `1` = at least one failed.
- **Share `<base-dir>/migtest-logs.tgz`** back for review — it contains `harness.log`,
  `phase1-sync.log`, `phase1-commit.log`, `phase2-consolidate.log`, each mongosync's
  own logs, and the generated `orchestrator.json`.

## Cleanup

Clusters are torn down automatically unless `--keep`. To stop a kept run:

```shell
./migrationTestHarness --teardown-only --clusters 10 --base-dir /mnt/migtest
# then remove the data dir if desired:
rm -rf /mnt/migtest/data
```
