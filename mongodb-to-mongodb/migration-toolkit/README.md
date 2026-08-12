# migration-toolkit

Two scripts that stage and install the complete toolchain a MongoDB-to-MongoDB
migration host needs, for environments where that host has **no internet
access**.

- `fetch-migration-toolkit.sh` runs on a bastion / jump host that *does* have
  internet access. It downloads every tool, verifies it, and packs everything
  into a single self-checksumming tarball.
- `install-migration-toolkit.sh` runs on the air-gapped migration host, from
  inside the unpacked tarball. It verifies checksums and installs the toolchain
  with no network access.

```
bastion (online)                         migration host (air-gapped)
  fetch-migration-toolkit.sh  ── scp ──▶  install-migration-toolkit.sh
        │                                        │
        ▼                                        ▼
  bundle.tar.gz + .sha256                 /opt/mongo-migration + /usr/local/bin
```

## What gets installed

| Tool                    | Purpose                                                      |
| ----------------------- | ----------------------------------------------------------- |
| `mongosync`             | The MongoDB cluster-to-cluster sync engine                  |
| `mongosh`               | MongoDB shell (control commands, verification)              |
| `jq`                    | JSON parsing for the mongosync control API                  |
| `mongosyncOrchestrator` | Drives the parallel mongosync jobs (Phase 1)                |
| `tmux` / `screen`       | Persistent session for long-running jobs (optional)         |
| `procps` (`watch`)      | Progress-monitor loop (optional)                            |
| `curl`                  | mongosync control API calls (`/start`, `/commit`, `/progress`) |

`tmux`/`screen`, `watch`, and `curl` are OS packages. They are only staged when
the bastion runs the **same OS** as the target host, because dependency
resolution is otherwise incorrect. When skipped, the bundle contains a
`NEEDED-OS-PACKAGES.txt` listing what to install on the host, and the installer
prints root-free substitutes (`nohup`, a shell `curl`/`jq` monitor loop).

## Requirements

- **Migration host must be x86_64.** `mongosync` publishes x86_64 builds only;
  no aarch64/arm64 artifact exists.
- **Supported OS:** `rhel9`, `ubuntu2204`, `amazon2023`. RHEL 8 is detected but
  has no `mongosync` build past 1.18.0, so the default 1.21.0 is unavailable
  there — build the host on RHEL 9, Ubuntu 22.04, or Amazon Linux 2023.
- The bastion needs `curl` or `wget`, and `sha256sum` (or `shasum`).

## 1. Fetch the bundle (online bastion)

```bash
./fetch-migration-toolkit.sh
```

Defaults target the documented migration-host spec (x86_64, auto-detected OS).
Override as needed:

```bash
# explicit OS / arch
./fetch-migration-toolkit.sh --os ubuntu2204 --arch x86_64

# choose an output directory
./fetch-migration-toolkit.sh --os amazon2023 --outdir /data/bundles
```

### Options

| Option                     | Description                                          | Default        |
| -------------------------- | ---------------------------------------------------- | -------------- |
| `--arch <x86_64\|aarch64>` | Target CPU architecture                              | `x86_64`       |
| `--os <id>`                | `rhel8` \| `rhel9` \| `ubuntu2204` \| `amazon2023`   | detected       |
| `--mongosync-version <v>`  | mongosync version                                    | `1.21.0`       |
| `--mongosh-version <v>`    | mongosh version                                      | `2.3.8`        |
| `--jq-version <v>`         | jq version                                           | `1.7.1`        |
| `--orchestrator <path>`    | Use a local `mongosyncOrchestrator` binary instead of downloading it | — |
| `--orchestrator-ref <ref>` | Branch/tag to pull the orchestrator from             | `master`       |
| `--skip-os-packages`       | Do not download tmux/screen/watch/curl packages      | off            |
| `--outdir <dir>`           | Where to write the bundle                            | `$PWD`         |
| `-h, --help`               | Show usage                                           |                |

Before downloading, the script runs a **preflight** check that probes every
artifact URL, so a bad version/OS combination fails with a clear diagnosis
instead of a partial download. It then downloads each tool, checksums the
payload (`SHA256SUMS`), writes a `manifest.env`, copies both scripts into the
bundle, and produces:

```
mongo-migration-toolkit-<os>-<arch>-<stamp>.tar.gz
mongo-migration-toolkit-<os>-<arch>-<stamp>.tar.gz.sha256
```

## 2. Transfer and install (air-gapped host)

```bash
sha256sum -c mongo-migration-toolkit-<os>-<arch>-<stamp>.tar.gz.sha256
tar xzf   mongo-migration-toolkit-<os>-<arch>-<stamp>.tar.gz
cd        mongo-migration-toolkit-<os>-<arch>-<stamp>
sudo ./install-migration-toolkit.sh
```

The installer verifies checksums, confirms the host arch/OS matches the bundle,
unpacks the tools, and symlinks the binaries onto `PATH`. Nothing here needs
root — run it as a normal user for a self-contained install under `$HOME`.

### Install locations

|              | as root                       | as normal user               |
| ------------ | ----------------------------- | ---------------------------- |
| Install root | `/opt/mongo-migration`        | `~/mongo-migration`          |
| Symlinks     | `/usr/local/bin`              | `~/mongo-migration/bin`      |
| PATH setup   | `/etc/profile.d/mongo-migration.sh` | `~/mongo-migration/env.sh` (source it) |

### Options

| Option                 | Description                                | Default |
| ---------------------- | ------------------------------------------ | ------- |
| `--prefix <dir>`       | Install root                               | see table above |
| `--bindir <dir>`       | Where symlinks go                          | see table above |
| `--skip-os-packages`   | Do not install staged RPMs/DEBs            | off     |
| `--verify-only`        | Check checksums + report, install nothing  | off     |
| `-h, --help`           | Show usage                                 |         |

After a successful install the script verifies the toolchain and prints each
tool's version. A user-local install prints how to persist `PATH`:

```bash
echo 'source ~/mongo-migration/env.sh' >> ~/.bashrc
source ~/mongo-migration/env.sh
```

## Verify only

To validate a transferred bundle without installing anything:

```bash
./install-migration-toolkit.sh --verify-only
```
