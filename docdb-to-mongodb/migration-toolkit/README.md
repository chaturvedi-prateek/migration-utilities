# migration-toolkit (DocumentDB → MongoDB Atlas)

Two scripts that stage and install the complete toolchain a **DocumentDB →
Atlas** migration host needs, for in-VPC EC2 hosts with **no internet access**.

- `fetch-docdb-migration-toolkit.sh` runs on a bastion / jump host that *does*
  have internet access **and Docker**. It downloads every tool, builds the
  `dsynct` container image, saves the Temporal image, and packs everything into
  a single self-checksumming tarball.
- `install-docdb-migration-toolkit.sh` runs on the air-gapped migration host,
  from inside the unpacked tarball. It verifies checksums, installs the native
  binaries, and `docker load`s the images.

```
bastion (online + docker)                 EC2 migration host (in-VPC)
  fetch-docdb-migration-toolkit.sh  ─scp─▶  install-docdb-migration-toolkit.sh
        │                                        │
        ▼                                        ▼
  bundle.tar.gz + .sha256          /opt/docdb-migration + /usr/local/bin
```

This is the DocumentDB counterpart to
[`mongodb-to-mongodb/migration-toolkit`](../../mongodb-to-mongodb/migration-toolkit),
which stages `mongosync` for MongoDB-to-MongoDB migrations. The two are
independent — a DocumentDB source uses **dsync Enterprise (`dsynct`)**, not
mongosync.

## What gets installed

| Tool                    | Form            | Purpose                                                        |
| ----------------------- | --------------- | -------------------------------------------------------------- |
| `dsynct`                | binary + image  | dsync **Enterprise** — initial sync + CDC, Temporal-coordinated |
| Temporal                | Docker image    | Coordinator: holds the flow plan and distributes copy tasks     |
| `mongosh`               | binary          | MongoDB shell (connectivity checks, verification)               |
| `jq`                    | binary          | Parses the dsynct progress API                                  |
| `global-bundle.pem`     | file            | AWS RDS/DocumentDB TLS CA bundle                                |
| `fixIdTypes`            | binary          | Mandatory pre-flight: detect → count → fix mixed `_id` BSON types |
| `migrateIndexes`        | binary          | Post-sync index create, then `--mode=rectify` at cutover        |
| `checkChangeStreams`    | binary          | Confirms change streams are enabled with sufficient retention   |
| `copyMissingDocs`       | binary          | Reconciles per-collection count gaps found during verification  |
| `tmux` / `screen`       | OS package      | Persistent session (optional — containers cover this)           |
| `procps` (`watch`)      | OS package      | Progress-monitor loop (optional)                                |
| `curl`                  | OS package      | dsynct progress API calls                                       |

**Docker is a prerequisite, not a payload.** The bundle carries saved images,
but the Docker engine itself must already be installed on the migration host.

`tmux`/`screen`, `watch`, and `curl` are OS packages, only staged when the
bastion runs the **same OS** as the target host — dependency resolution is
otherwise incorrect. When skipped, the bundle contains a
`NEEDED-OS-PACKAGES.txt` listing what to install, and the installer prints
root-free substitutes.

## The Enterprise binary

`dsynct` is a **licensed MongoDB/Adiom Enterprise artifact**. This toolkit never
downloads it. Supply it with `--dsynct-bin`, pointing at the build matching the
**target** architecture (both `amd64` and `arm64` are shipped by Adiom):

```bash
./fetch-docdb-migration-toolkit.sh --dsynct-bin /path/to/amd/dsynct
```

With no flag the script also checks, in order:

```
$DSYNCT_BIN
~/Documents/Tools/dsync-enterprise/{amd,arm}/dsynct
~/dsync-enterprise/{amd,arm}/dsynct
./vendor/dsynct/dsynct-linux-{amd64,arm64}
```

Do not commit the binary to this repository or publish the produced bundle —
both contain licensed code.

Note the distinction the plan documents draw: the **OSS `dsync`** binary lacks
`--namespace-fanout` / `--documentdb-sampling-fanout` in v0.27.0 and earlier,
and cannot address clusters past ~2 billion records. Enterprise `dsynct`
supplies both, and adds the Temporal-coordinated multi-worker topology this
toolkit is built around. The installer fails if the staged binary lacks the
`worker` and `temporal` sub-commands, which is how an OSS binary staged by
mistake is caught.

## Requirements

- **Bastion:** internet access, Docker (running), `curl` or `wget`, `sha256sum`
  or `shasum`.
- **Migration host:** `x86_64` or `aarch64` — unlike mongosync, dsynct ships
  both. Docker engine installed.
- **Supported OS:** `rhel9`, `ubuntu2204`, `amazon2`, `amazon2023`.
- **Placement:** the migration hosts must be in the **same VPC and region** as
  the DocumentDB cluster. Cross-region is the single biggest performance risk in
  the plan.

## 1. Fetch the bundle (online bastion)

```bash
./fetch-docdb-migration-toolkit.sh --dsynct-bin ~/dsync-enterprise/amd/dsynct
```

Defaults target x86_64 with the bastion's own OS. Override as needed:

```bash
# arm64 Graviton migration host running Ubuntu 22.04
./fetch-docdb-migration-toolkit.sh \
  --arch aarch64 --os ubuntu2204 \
  --dsynct-bin ~/dsync-enterprise/arm/dsynct

# no Docker on the bastion — native binaries only
./fetch-docdb-migration-toolkit.sh --dsynct-bin ./dsynct --skip-images
```

### Options

| Option                     | Description                                       | Default                    |
| -------------------------- | ------------------------------------------------- | -------------------------- |
| `--arch <x86_64\|aarch64>` | Target CPU architecture                           | `x86_64`                   |
| `--os <id>`                | `rhel9` \| `ubuntu2204` \| `amazon2` \| `amazon2023` | detected                |
| `--dsynct-bin <path>`      | Enterprise `dsynct` for the target arch           | see above                  |
| `--dsynct-tag <ref>`       | Tag for the built dsynct image                    | `dsynct:enterprise`        |
| `--dsynct-base <image>`    | Base image for dsynct                             | `alpine:3.20`              |
| `--temporal-image <ref>`   | Temporal CLI image                                | `temporalio/temporal:1.8.2` |
| `--mongosh-version <v>`    | mongosh version                                   | `2.3.8`                    |
| `--jq-version <v>`         | jq version                                        | `1.7.1`                    |
| `--tools-dir <path>`       | migration-utilities checkout for the helper binaries | this repo               |
| `--tools-ref <ref>`        | Branch/tag to pull helpers from when no checkout is found | `master`           |
| `--skip-images`            | Do not build/pull/save Docker images              | off                        |
| `--skip-os-packages`       | Do not download tmux/screen/procps/curl           | off                        |
| `--outdir <dir>`           | Where to write the bundle                         | `$PWD`                     |

Before downloading, the script runs a **preflight** check that probes every
artifact URL and the Docker daemon, so a bad version or a stopped daemon fails
with a clear diagnosis instead of a partial bundle. It then produces:

```
docdb-migration-toolkit-<os>-<arch>-<stamp>.tar.gz
docdb-migration-toolkit-<os>-<arch>-<stamp>.tar.gz.sha256
```

The dsynct image is built `FROM alpine` with a single `COPY` — because dsynct is
a statically linked Go binary and no build instruction executes, a foreign-arch
build works on the bastion without qemu.

## 2. Transfer and install (migration host)

```bash
sha256sum -c docdb-migration-toolkit-<os>-<arch>-<stamp>.tar.gz.sha256
tar xzf     docdb-migration-toolkit-<os>-<arch>-<stamp>.tar.gz
cd          docdb-migration-toolkit-<os>-<arch>-<stamp>
sudo ./install-docdb-migration-toolkit.sh
```

### Install locations

|                    | as root                              | as normal user                     |
| ------------------ | ------------------------------------ | ---------------------------------- |
| Install root       | `/opt/docdb-migration`               | `~/docdb-migration`                |
| Symlinks           | `/usr/local/bin`                     | `~/docdb-migration/bin`            |
| Compose files      | `<prefix>/compose`                   | same                               |
| Sample configs     | `<prefix>/configs`                   | same                               |
| DocumentDB CA      | `<prefix>/certs/global-bundle.pem`   | same                               |
| PATH setup         | `/etc/profile.d/docdb-migration.sh`  | `~/docdb-migration/env.sh` (source it) |

### Options

| Option               | Description                               | Default |
| -------------------- | ----------------------------------------- | ------- |
| `--prefix <dir>`     | Install root                              | see table |
| `--bindir <dir>`     | Where symlinks go                         | see table |
| `--skip-images`      | Do not `docker load` the images           | off     |
| `--skip-os-packages` | Do not install staged RPMs/DEBs           | off     |
| `--verify-only`      | Check checksums + report, install nothing | off     |

The installer exports `$DSYNCT` and `$DOCDB_CA` in the profile script, so the
plan's command snippets work unchanged after sourcing it.

## 3. Run the multi-worker topology

The bundle ships a compose file implementing the coordinator + N workers +
runner topology from the dsync Enterprise multi-worker playbook:

```
   ┌──────────┐  holds flow plan + task queue
   │ temporal │  :7233 gRPC   :8233 UI
   └────┬─────┘
        │ poll for partition-copy tasks
  ┌─────┴──────────────┐
  │ worker × N         │  ← the unit of scale
  └─────┬──────────────┘
        │
   ┌────┴─────┐
   │  runner  │  submits the workflow once, serves the dashboard on :8080
   └──────────┘
```

```bash
cd /opt/docdb-migration/compose
cp .env.sample .env      # DOCDB_SRC, MDB_DEST, QUEUE, WORKFLOW_ID, NAMESPACE

docker compose up -d temporal
docker compose up -d --scale worker=3 worker
docker compose up -d runner
```

Add workers **while the migration is running** — re-run the `--scale` command
with a higher number and new workers immediately pull pending tasks. Stopping a
worker is equally safe: Temporal reassigns its in-flight tasks.

Three details the compose file encodes, each learned the hard way in the
validation runs:

- Workers run `app --no-progress`. Several dsynct processes on one host cannot
  all bind the progress port; the runner serves the dashboard.
- `--persist` is an **`app`** flag, never a `run` flag.
- All workers sharing a queue must have **identical** source/dest/transform
  config. Different configs need a different `QUEUE`, or tasks get mixed.

`DOCDB_SRC` in `.env` must set `tlsCAFile=/certs/global-bundle.pem` — the
in-container path where the image ships the AWS bundle.

### Verify the work is actually distributed

```bash
temporal workflow show --workflow-id "$WORKFLOW_ID" -o json \
  | grep -oE '"identity": *"[^"]*"' | sort | uniq -c | sort -rn
```

Uneven splits are normal — workers grab tasks by polling, so total throughput is
what matters, not an even split.

## 4. Pre-flight on the source

Two checks are mandatory before any sync starts, both installed by this toolkit:

```bash
checkChangeStreams          # change streams enabled, retention >= 604800s (7 days)
fixIdTypes --mode detect --config config.json
```

Every collection must report `[CLEAN]`. dsynct partitions by `_id` range and
**fails the whole flow plan** on a collection with mixed BSON `_id` types. Sample
configs for each tool are installed under `<prefix>/configs/`.

## Verify only

To validate a transferred bundle without installing anything:

```bash
./install-docdb-migration-toolkit.sh --verify-only
```
