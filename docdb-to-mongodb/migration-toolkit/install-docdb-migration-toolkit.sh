#!/usr/bin/env bash
#
# install-docdb-migration-toolkit.sh
#
# Run this on the AIR-GAPPED EC2 MIGRATION HOST, from inside the unpacked bundle
# produced by fetch-docdb-migration-toolkit.sh. Needs no internet access.
#
#   tar xzf docdb-migration-toolkit-<os>-<arch>-<stamp>.tar.gz
#   cd docdb-migration-toolkit-<os>-<arch>-<stamp>
#   sudo ./install-docdb-migration-toolkit.sh
#
# Installs into /opt/docdb-migration and symlinks binaries into /usr/local/bin.
# Docker images (Temporal, dsynct) are loaded with `docker load`.
#
set -euo pipefail

# Default install location depends on privilege: root gets the system-wide
# paths, an unprivileged user gets a self-contained tree under $HOME. Nothing
# in this toolchain needs root except installing OS packages and Docker images.
if [[ "$(id -u)" -eq 0 ]]; then
  PREFIX="/opt/docdb-migration"
  BINDIR="/usr/local/bin"
else
  PREFIX="${HOME}/docdb-migration"
  BINDIR="${HOME}/docdb-migration/bin"
fi
SKIP_IMAGES="false"
SKIP_OS_PACKAGES="false"
VERIFY_ONLY="false"

usage() {
  cat <<'USAGE'
Usage: install-docdb-migration-toolkit.sh [options]

Runs with or without root. As root it installs system-wide; as a normal user it
installs entirely under $HOME. Docker image loading needs docker group access.

  --prefix <dir>        Install root
                        (root: /opt/docdb-migration, user: ~/docdb-migration)
  --bindir <dir>        Where symlinks go
                        (root: /usr/local/bin, user: ~/docdb-migration/bin)
  --skip-images         Do not `docker load` the Temporal/dsynct images
  --skip-os-packages    Do not install staged RPMs/DEBs
  --verify-only         Check checksums + report, install nothing
  -h, --help            This message
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix)           PREFIX="$2"; shift 2 ;;
    --bindir)           BINDIR="$2"; shift 2 ;;
    --skip-images)      SKIP_IMAGES="true"; shift ;;
    --skip-os-packages) SKIP_OS_PACKAGES="true"; shift ;;
    --verify-only)      VERIFY_ONLY="true"; shift ;;
    -h|--help)          usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }
ok()   { printf '\033[1;32m  ok\033[0m %s\n' "$*"; }

BUNDLE_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$BUNDLE_DIR"

[[ -f manifest.env ]] || die "manifest.env not found; run this from inside the unpacked bundle"
# shellcheck disable=SC1091
. ./manifest.env

log "bundle ${BUNDLE_NAME}  (os=${TARGET_OS} arch=${ARCH})"

# ------------------------------------------------------- sanity: host match ---
HOST_ARCH="$(uname -m)"
[[ "$HOST_ARCH" == "$ARCH" ]] \
  || die "bundle is for ${ARCH} but this host is ${HOST_ARCH}. Re-run fetch with --arch ${HOST_ARCH}."
if [[ -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  log "host: ${PRETTY_NAME:-unknown} (${HOST_ARCH})"
fi

# ------------------------------------------------------------- checksums -----
if [[ -f SHA256SUMS ]]; then
  log "verifying checksums"
  sha256sum -c SHA256SUMS --quiet || die "checksum mismatch — bundle is corrupt or truncated"
  ok "all payload checksums match"
else
  warn "no SHA256SUMS in bundle; skipping integrity check"
fi

if [[ "$VERIFY_ONLY" == "true" ]]; then
  log "--verify-only: nothing installed"
  exit 0
fi

if [[ "$(id -u)" -eq 0 ]]; then IS_ROOT="true"; else
  IS_ROOT="false"
  log "no root privileges — installing user-locally into ${PREFIX}"
fi

# Standalone binaries live in libexec, never in ${PREFIX}/bin: with a user-local
# install ${BINDIR} can BE ${PREFIX}/bin, and symlinking a file onto itself
# yields a broken link that silently shadows to whatever is next on PATH.
LIBEXEC="${PREFIX}/libexec"

mkdir -p "$LIBEXEC" "$BINDIR" "${PREFIX}/compose" "${PREFIX}/configs" "${PREFIX}/certs" \
  || die "cannot create ${PREFIX} / ${BINDIR}; pass --prefix <writable dir>"
[[ -w "$PREFIX" && -w "$BINDIR" ]] \
  || die "${PREFIX} or ${BINDIR} is not writable; pass --prefix <writable dir>"

link() {  # link <source> <name>
  local src="$1" dest="${BINDIR}/$2"
  if [[ "$src" -ef "$dest" ]] 2>/dev/null || [[ "$src" == "$dest" ]]; then
    ok "${dest} (in place)"
    return
  fi
  ln -sfn "$src" "$dest"
  [[ -x "$dest" ]] || die "created a broken link: ${dest} -> ${src}"
  ok "${dest} -> ${src}"
}

# ------------------------------------------------------- dsynct (Enterprise) --
log "installing Enterprise dsynct"
install -m 0755 "payload/dsynct/dsynct-linux-${GOARCH}" "${LIBEXEC}/dsynct"
link "${LIBEXEC}/dsynct" dsynct

# --------------------------------------------------------------- mongosh -----
log "installing mongosh ${MONGOSH_VERSION}"
rm -rf "${PREFIX}/mongosh-${MONGOSH_VERSION}"
mkdir -p "${PREFIX}/mongosh-${MONGOSH_VERSION}"
tar -xzf "payload/${MONGOSH_TGZ}" -C "${PREFIX}/mongosh-${MONGOSH_VERSION}" --strip-components=1
MONGOSH_PATH="${PREFIX}/mongosh-${MONGOSH_VERSION}/bin/mongosh"
[[ -x "$MONGOSH_PATH" ]] || die "mongosh binary not found inside ${MONGOSH_TGZ}"
link "$MONGOSH_PATH" mongosh

# -------------------------------------------------------------------- jq -----
log "installing jq ${JQ_VERSION}"
install -m 0755 "payload/${JQ_BIN}" "${LIBEXEC}/jq"
link "${LIBEXEC}/jq" jq

# --------------------------------------------------------- helper binaries ---
log "installing migration helper binaries"
for tool in ${HELPER_TOOLS}; do
  if [[ -f "payload/tools/${tool}" ]]; then
    install -m 0755 "payload/tools/${tool}" "${LIBEXEC}/${tool}"
    link "${LIBEXEC}/${tool}" "$tool"
  else
    warn "${tool} is not in this bundle"
  fi
done

if compgen -G "payload/configs/*.json" > /dev/null; then
  cp payload/configs/*.json "${PREFIX}/configs/"
  ok "sample configs -> ${PREFIX}/configs/"
fi

# ---------------------------------------------------------- DocumentDB CA ----
install -m 0644 payload/global-bundle.pem "${PREFIX}/certs/global-bundle.pem"
ok "AWS DocumentDB CA -> ${PREFIX}/certs/global-bundle.pem"

# --------------------------------------------------------------- compose -----
cp compose/docker-compose.yml compose/.env.sample "${PREFIX}/compose/"
if [[ -f compose/Dockerfile.dsynct ]]; then
  cp compose/Dockerfile.dsynct "${PREFIX}/compose/"
fi
ok "compose files -> ${PREFIX}/compose/"

# ------------------------------------------------------------ OS packages ----
# Installs tmux/screen/procps/curl AND (unless already present) the Docker
# engine, since both live as RPMs/DEBs in the same os-packages/ directory.
# Runs BEFORE the docker-images section below, which needs `docker` on PATH.
if [[ "$SKIP_OS_PACKAGES" == "true" ]]; then
  log "skipping OS packages (--skip-os-packages)"
elif [[ "$IS_ROOT" != "true" ]]; then
  warn "not root: cannot install tmux/screen/procps/docker system-wide (optional — see below)."
elif compgen -G "os-packages/*.rpm" > /dev/null; then
  log "installing staged RPMs (tmux, screen, procps-ng, curl, docker engine)"
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y --disablerepo='*' os-packages/*.rpm || warn "dnf install reported errors"
  elif command -v yum >/dev/null 2>&1; then
    # yum, like dnf, checks the local rpmdb before failing on a requirement —
    # it will not touch already-installed packages (e.g. systemd, glibc) that
    # satisfy a dependency, unlike a blind `rpm -Uvh`.
    yum install -y os-packages/*.rpm || warn "yum install reported errors"
  else
    warn "neither dnf nor yum found; falling back to rpm (less safe: no dependency check against repos)"
    rpm -Uvh --replacepkgs os-packages/*.rpm || warn "rpm install reported errors"
  fi
elif compgen -G "os-packages/*.deb" > /dev/null; then
  log "installing staged DEBs (tmux, screen, procps, curl, docker engine)"
  dpkg -i os-packages/*.deb || warn "dpkg reported errors (possibly unmet deps)"
  command -v apt-get >/dev/null 2>&1 && apt-get install -y -f >/dev/null 2>&1 || true
else
  warn "no OS packages staged in this bundle."
  if [[ -f NEEDED-OS-PACKAGES.txt ]]; then
    sed 's/^/      /' NEEDED-OS-PACKAGES.txt >&2
  fi
fi

# ------------------------------------------------------------ docker engine --
# The target host is assumed to start with neither Docker nor Podman. If one
# is already present and working, it is left alone. Otherwise the engine
# staged above is started, enabled, and the docker-compose plugin installed.
ENGINE="none"
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  ENGINE="docker"
  ok "docker engine already present and running"
elif command -v podman >/dev/null 2>&1 && podman info >/dev/null 2>&1; then
  ENGINE="podman"
  ok "podman already present and running — leaving it as the container engine"
elif command -v docker >/dev/null 2>&1 && [[ "$IS_ROOT" == "true" ]]; then
  log "docker installed but not running — starting it"
  if command -v systemctl >/dev/null 2>&1; then
    systemctl enable --now docker || warn "systemctl could not start docker — check 'systemctl status docker'"
  else
    service docker start || warn "could not start the docker service"
  fi
  docker info >/dev/null 2>&1 && ENGINE="docker" || warn "docker installed but still not reachable after starting the service"
else
  warn "no working container engine (docker/podman) found on this host."
  if [[ "$IS_ROOT" != "true" ]]; then
    warn "re-run this installer as root to install and start the Docker engine."
  else
    warn "Docker was not staged in this bundle (was --skip-docker-engine set on fetch?)."
  fi
fi

if [[ "$ENGINE" == "docker" ]]; then
  # Docker Compose v2 ships as a CLI plugin binary, not a package.
  if [[ -f "payload/${DOCKER_COMPOSE_BIN:-}" ]]; then
    if [[ "$IS_ROOT" == "true" ]]; then
      COMPOSE_PLUGIN_DIR="/usr/local/lib/docker/cli-plugins"
    else
      COMPOSE_PLUGIN_DIR="${HOME}/.docker/cli-plugins"
    fi
    mkdir -p "$COMPOSE_PLUGIN_DIR"
    install -m 0755 "payload/${DOCKER_COMPOSE_BIN}" "${COMPOSE_PLUGIN_DIR}/docker-compose"
    ok "docker compose plugin -> ${COMPOSE_PLUGIN_DIR}/docker-compose"
  else
    warn "docker-compose plugin binary not in this bundle; 'docker compose' will not work"
  fi

  # A non-root operator needs docker-group membership to run docker without
  # sudo; group membership only takes effect on their NEXT login/shell.
  if [[ "$IS_ROOT" == "true" ]]; then
    TARGET_USER="${SUDO_USER:-$(logname 2>/dev/null || true)}"
    if [[ -n "$TARGET_USER" && "$TARGET_USER" != "root" ]]; then
      getent group docker >/dev/null 2>&1 || groupadd docker
      if usermod -aG docker "$TARGET_USER"; then
        ok "added ${TARGET_USER} to the docker group (log out/in, or run 'newgrp docker', to use docker without sudo)"
      fi
    fi
  fi
fi

# ---------------------------------------------------------- docker images ----
DOCKER_OK="false"
if [[ "$SKIP_IMAGES" == "true" ]]; then
  log "skipping Docker images (--skip-images)"
elif [[ "$ENGINE" != "docker" ]]; then
  if [[ "$ENGINE" == "podman" ]]; then
    warn "podman is present but this installer only 'docker load's images automatically."
    warn "Load them manually: podman load -i payload/images/<tar>"
  else
    warn "no working docker engine — Temporal and dsynct images were not loaded."
    warn "The dsynct binary is still installed natively at ${BINDIR}/dsynct."
  fi
else
  DOCKER_OK="true"
  for tar in "${TEMPORAL_IMAGE_TAR:-}" "${DSYNCT_IMAGE_TAR:-}"; do
    [[ -n "$tar" && -f "payload/images/${tar}" ]] || continue
    log "docker load < payload/images/${tar}"
    docker load -i "payload/images/${tar}" || warn "docker load failed for ${tar}"
  done
fi

# -------------------------------------------------------------- profile -----
if [[ "$IS_ROOT" == "true" ]]; then
  ENV_FILE="/etc/profile.d/docdb-migration.sh"
else
  ENV_FILE="${PREFIX}/env.sh"
fi
cat > "$ENV_FILE" <<EOF
# Added by install-docdb-migration-toolkit.sh
export PATH="${BINDIR}:\$PATH"
export DSYNCT="${BINDIR}/dsynct"
export DOCDB_CA="${PREFIX}/certs/global-bundle.pem"
EOF
chmod 0644 "$ENV_FILE"
ok "$ENV_FILE"

# Without root the new PATH is not picked up automatically, so make it
# available to this verification pass and tell the operator how to persist it.
export PATH="${BINDIR}:${PATH}"

# ------------------------------------------------------------ verification ---
echo
log "verifying installed toolchain"
FAIL=0
check() {  # check <name> <version-cmd...>
  local name="$1"; shift
  if command -v "$name" >/dev/null 2>&1; then
    ok "$(printf '%-20s' "$name") $("$@" 2>&1 | head -n1)"
  else
    warn "$(printf '%-20s' "$name") MISSING"
    FAIL=1
  fi
}

# dsynct has no --version; --help prints the sub-command set. Require the
# Temporal-mode sub-commands so an OSS dsync binary staged by mistake is caught.
if command -v dsynct >/dev/null 2>&1; then
  DSYNCT_HELP="$(dsynct --help 2>&1 || true)"
  if grep -q 'worker' <<<"$DSYNCT_HELP" && grep -q 'temporal' <<<"$DSYNCT_HELP"; then
    ok "$(printf '%-20s' dsynct) Enterprise build (worker/run/app/temporal present)"
  else
    warn "$(printf '%-20s' dsynct) present but 'worker'/'temporal' sub-commands are missing —"
    warn "                     this looks like the OSS dsync binary, not Enterprise dsynct."
    FAIL=1
  fi
else
  warn "$(printf '%-20s' dsynct) MISSING"
  FAIL=1
fi

check mongosh mongosh --version
check jq      jq --version
check curl    curl --version

for tool in ${HELPER_TOOLS}; do
  if command -v "$tool" >/dev/null 2>&1; then
    ok "$(printf '%-20s' "$tool") installed"
  else
    warn "$(printf '%-20s' "$tool") MISSING"
    FAIL=1
  fi
done

case "$ENGINE" in
  docker)
    if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet docker; then
      ok "$(printf '%-20s' "docker service") active (enabled at boot)"
    elif docker info >/dev/null 2>&1; then
      ok "$(printf '%-20s' "docker service") reachable"
    else
      warn "$(printf '%-20s' "docker service") not active"
      FAIL=1
    fi
    if docker compose version >/dev/null 2>&1; then
      ok "$(printf '%-20s' "docker compose") $(docker compose version 2>&1 | head -n1)"
    else
      warn "$(printf '%-20s' "docker compose") MISSING (compose plugin not installed)"
      FAIL=1
    fi
    ;;
  podman)
    ok "$(printf '%-20s' "container engine") podman (pre-existing — left as-is)"
    ;;
  *)
    warn "$(printf '%-20s' "container engine") MISSING — no docker or podman"
    FAIL=1
    ;;
esac

if [[ "$DOCKER_OK" == "true" ]]; then
  for img in "${TEMPORAL_IMAGE:-}" "${DSYNCT_TAG:-}"; do
    [[ -n "$img" ]] || continue
    if docker image inspect "$img" >/dev/null 2>&1; then
      ok "$(printf '%-20s' "image") ${img}"
    else
      warn "$(printf '%-20s' "image") ${img} NOT loaded"
      FAIL=1
    fi
  done
fi

# tmux/screen and watch are conveniences, not hard requirements: containers or
# systemd cover the persistent session and a shell loop covers monitoring.
SOFT_MISSING=()
command -v watch >/dev/null 2>&1 \
  && ok "$(printf '%-20s' watch) $(watch --version 2>&1 | head -n1)" \
  || SOFT_MISSING+=("watch")
# `command -v` only proves the binary is on PATH, not that it runs — a tmux
# built against a newer ncurses/libtinfo ABI than what's on an older AMI
# installs fine but crashes with a symbol lookup error at execution time.
# Actually invoke it and fall through to screen if it fails.
TMUX_OUT=""
if command -v tmux >/dev/null 2>&1; then
  if TMUX_OUT="$(tmux -V 2>&1)"; then
    :
  else
    warn "$(printf '%-20s' tmux) installed but not runnable (${TMUX_OUT}) — falling back to screen"
    TMUX_OUT=""
  fi
fi
if [[ -n "$TMUX_OUT" ]]; then
  ok "$(printf '%-20s' tmux) ${TMUX_OUT}"
elif command -v screen >/dev/null 2>&1; then
  ok "$(printf '%-20s' screen) $(screen --version 2>&1 | head -n1)"
else
  SOFT_MISSING+=("tmux/screen")
fi

echo
if [[ "$FAIL" -ne 0 ]]; then
  warn "toolchain INCOMPLETE — resolve the items above before starting the migration."
  exit 1
fi

log "core toolchain complete."
if [[ "$IS_ROOT" != "true" ]]; then
  cat <<EOF

  Installed without root. Add the toolkit to your PATH:

      echo 'source ${ENV_FILE}' >> ~/.bashrc
      source ${ENV_FILE}
EOF
fi

if [[ "${#SOFT_MISSING[@]}" -gt 0 ]]; then
  cat <<EOF

  Optional tools not present: ${SOFT_MISSING[*]}
  Not required — substitutes:

    persistent session (instead of tmux/screen)
        run the workers and runner as containers (compose, below) or as
        systemd units, both of which survive an SSH disconnect

    monitor loop (instead of watch)
        while :; do curl -s localhost:8080/progress | grep -m1 '^data: ' \\
          | sed 's/^data: //' \\
          | jq -c '.RunnerSyncProgress | {state:.SyncState, docs:.NumDocsSynced, lag:.Lag}'
          sleep 30; done
EOF
fi

if [[ "$ENGINE" == "docker" && "$IS_ROOT" == "true" && -n "${TARGET_USER:-}" && "$TARGET_USER" != "root" ]]; then
  cat <<EOF

  ${TARGET_USER} was added to the docker group just now — that only takes
  effect on their NEXT login/shell. Until then, run docker/compose commands
  as root, or have ${TARGET_USER} run: newgrp docker
EOF
fi

cat <<EOF

Next steps — multi-worker topology:

    cd ${PREFIX}/compose
    cp .env.sample .env        # fill in DOCDB_SRC, MDB_DEST, QUEUE, NAMESPACE

    docker compose up -d temporal                          # coordinator
    docker compose up -d --scale worker=3 worker           # copy workers
    docker compose up -d runner                            # submit + dashboard

    Temporal UI      http://<this-host>:8233
    dsynct dashboard http://<this-host>:8080

Pre-flight on the source, before starting any sync:

    checkChangeStreams        # change streams enabled + retention >= 7 days
    fixIdTypes --mode detect --config ${PREFIX}/configs/fixIdTypes.config.sample.json
    # every collection must report [CLEAN] before the initial sync starts
EOF
