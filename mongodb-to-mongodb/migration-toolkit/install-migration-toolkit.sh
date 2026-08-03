#!/usr/bin/env bash
#
# install-migration-toolkit.sh
#
# Run this on the AIR-GAPPED MIGRATION HOST, from inside the unpacked bundle
# produced by fetch-migration-toolkit.sh. Needs no internet access.
#
#   tar xzf mongo-migration-toolkit-<os>-<arch>-<stamp>.tar.gz
#   cd mongo-migration-toolkit-<os>-<arch>-<stamp>
#   sudo ./install-migration-toolkit.sh
#
# Installs into /opt/mongo-migration and symlinks binaries into /usr/local/bin.
#
set -euo pipefail

# Default install location depends on privilege: root gets the system-wide
# paths, an unprivileged user gets a self-contained tree under $HOME. Nothing
# in this toolchain needs root — the binaries are userspace and mongosync's
# control API port (27182) is unprivileged.
if [[ "$(id -u)" -eq 0 ]]; then
  PREFIX="/opt/mongo-migration"
  BINDIR="/usr/local/bin"
else
  PREFIX="${HOME}/mongo-migration"
  BINDIR="${HOME}/mongo-migration/bin"
fi
SKIP_OS_PACKAGES="false"
VERIFY_ONLY="false"

usage() {
  cat <<'USAGE'
Usage: install-migration-toolkit.sh [options]

Runs with or without root. As root it installs system-wide; as a normal user it
installs entirely under $HOME and needs no sudo.

  --prefix <dir>        Install root
                        (root: /opt/mongo-migration, user: ~/mongo-migration)
  --bindir <dir>        Where symlinks go
                        (root: /usr/local/bin, user: ~/mongo-migration/bin)
  --skip-os-packages    Do not install staged RPMs/DEBs
  --verify-only         Check checksums + report, install nothing
  -h, --help            This message
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix)           PREFIX="$2"; shift 2 ;;
    --bindir)           BINDIR="$2"; shift 2 ;;
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
if [[ "$HOST_ARCH" != "$ARCH" ]]; then
  if [[ "$HOST_ARCH" != "x86_64" ]]; then
    die "this host is ${HOST_ARCH}, but mongosync is published for x86_64 only.
     The migration host must be x86_64 — this host cannot run the migration."
  fi
  die "bundle is for ${ARCH} but this host is ${HOST_ARCH}. Re-run fetch with --arch ${HOST_ARCH}."
fi
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

if [[ "$(id -u)" -eq 0 ]]; then
  IS_ROOT="true"
else
  IS_ROOT="false"
  log "no root privileges — installing user-locally into ${PREFIX} (no sudo needed)"
fi

# Standalone binaries (jq, the orchestrator) live in libexec, never in
# ${PREFIX}/bin: with a user-local install ${BINDIR} can BE ${PREFIX}/bin, and
# symlinking a file onto itself yields a broken link that silently shadows to
# whatever is next on PATH.
LIBEXEC="${PREFIX}/libexec"

mkdir -p "$LIBEXEC" "${BINDIR}" \
  || die "cannot create ${PREFIX} / ${BINDIR}; pass --prefix <writable dir>"
[[ -w "$PREFIX" && -w "$BINDIR" ]] \
  || die "${PREFIX} or ${BINDIR} is not writable; pass --prefix <writable dir>"

link() {  # link <source> <name>
  local src="$1" name="$2" dest="${BINDIR}/$2"
  if [[ "$src" -ef "$dest" ]] 2>/dev/null || [[ "$src" == "$dest" ]]; then
    ok "${dest} (in place)"          # already at its final location
    return
  fi
  ln -sfn "$src" "$dest"
  [[ -x "$dest" ]] || die "created a broken link: ${dest} -> ${src}"
  ok "${dest} -> ${src}"
}

# ------------------------------------------------------------- mongosync -----
log "installing mongosync ${MONGOSYNC_VERSION}"
rm -rf "${PREFIX}/mongosync-${MONGOSYNC_VERSION}"
mkdir -p "${PREFIX}/mongosync-${MONGOSYNC_VERSION}"
tar -xzf "payload/${MONGOSYNC_TGZ}" -C "${PREFIX}/mongosync-${MONGOSYNC_VERSION}" --strip-components=1
MONGOSYNC_PATH="$(find "${PREFIX}/mongosync-${MONGOSYNC_VERSION}" -type f -name mongosync -perm -u+x | head -n1)"
[[ -n "$MONGOSYNC_PATH" ]] || die "mongosync binary not found inside ${MONGOSYNC_TGZ}"
link "$MONGOSYNC_PATH" mongosync

# --------------------------------------------------------------- mongosh -----
log "installing mongosh ${MONGOSH_VERSION}"
rm -rf "${PREFIX}/mongosh-${MONGOSH_VERSION}"
mkdir -p "${PREFIX}/mongosh-${MONGOSH_VERSION}"
tar -xzf "payload/${MONGOSH_TGZ}" -C "${PREFIX}/mongosh-${MONGOSH_VERSION}" --strip-components=1
MONGOSH_PATH="${PREFIX}/mongosh-${MONGOSH_VERSION}/bin/mongosh"
[[ -x "$MONGOSH_PATH" ]] || die "mongosh binary not found inside ${MONGOSH_TGZ}"
link "$MONGOSH_PATH" mongosh
# mongosh ships a crypt shared library used for some auth/FLE paths
if [[ -f "${PREFIX}/mongosh-${MONGOSH_VERSION}/bin/mongosh_crypt_v1.so" ]]; then
  ok "mongosh_crypt_v1.so present"
fi

# -------------------------------------------------------------------- jq -----
log "installing jq ${JQ_VERSION}"
install -m 0755 "payload/${JQ_BIN}" "${LIBEXEC}/jq"
link "${LIBEXEC}/jq" jq

# ------------------------------------------------- mongosyncOrchestrator -----
if [[ -f payload/mongosyncOrchestrator ]]; then
  log "installing mongosyncOrchestrator"
  install -m 0755 payload/mongosyncOrchestrator "${LIBEXEC}/mongosyncOrchestrator"
  link "${LIBEXEC}/mongosyncOrchestrator" mongosyncOrchestrator
else
  warn "mongosyncOrchestrator is NOT in this bundle — Phase 1 cannot run without it."
  warn "Copy the linux/${ARCH} build to ${BINDIR}/mongosyncOrchestrator and chmod +x it."
fi

# ------------------------------------------------------------ OS packages ----
if [[ "$SKIP_OS_PACKAGES" == "true" ]]; then
  log "skipping OS packages (--skip-os-packages)"
elif [[ "$IS_ROOT" != "true" ]]; then
  warn "not root: cannot install tmux/screen/procps system-wide."
  warn "These are optional — see the workarounds printed at the end."
elif compgen -G "os-packages/*.rpm" > /dev/null; then
  log "installing staged RPMs (tmux, screen, procps-ng, curl)"
  if command -v dnf >/dev/null 2>&1; then
    dnf install -y --disablerepo='*' os-packages/*.rpm \
      || warn "dnf install reported errors; check output above"
  else
    rpm -Uvh --replacepkgs os-packages/*.rpm || warn "rpm install reported errors"
  fi
elif compgen -G "os-packages/*.deb" > /dev/null; then
  log "installing staged DEBs (tmux, screen, procps, curl)"
  dpkg -i os-packages/*.deb || warn "dpkg reported errors (possibly unmet deps); check output above"
else
  warn "no OS packages staged in this bundle."
  if [[ -f NEEDED-OS-PACKAGES.txt ]]; then
    warn "install them per NEEDED-OS-PACKAGES.txt:"
    sed 's/^/      /' NEEDED-OS-PACKAGES.txt >&2
  else
    warn "tmux/screen and watch(procps) must come from the host's internal repo or OS media."
  fi
fi

# -------------------------------------------------------------- profile -----
if [[ "$IS_ROOT" == "true" ]]; then
  ENV_FILE="/etc/profile.d/mongo-migration.sh"
else
  ENV_FILE="${PREFIX}/env.sh"
fi
cat > "$ENV_FILE" <<EOF
# Added by install-migration-toolkit.sh
export PATH="${BINDIR}:\$PATH"
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
    ok "$(printf '%-22s' "$name") $("$@" 2>&1 | head -n1)"
  else
    warn "$(printf '%-22s' "$name") MISSING"
    FAIL=1
  fi
}

check mongosync              mongosync --version
check mongosh                mongosh --version
check jq                     jq --version
# The orchestrator has no --version flag: an unrecognised argument makes it fall
# through to loading orchestrator.json and print "error: ..." while still exiting
# 0. So probe --help and require its banner in the output.
if command -v mongosyncOrchestrator >/dev/null 2>&1; then
  ORCH_HELP="$(mongosyncOrchestrator --help 2>&1 | head -n1)"
  if [[ "$ORCH_HELP" == *mongosyncOrchestrator* ]]; then
    ok "$(printf '%-22s' mongosyncOrchestrator) ${ORCH_HELP}"
  else
    warn "$(printf '%-22s' mongosyncOrchestrator) present but not runnable: ${ORCH_HELP}"
    FAIL=1
  fi
else
  warn "$(printf '%-22s' mongosyncOrchestrator) MISSING (see note above)"
  FAIL=1
fi
check curl                   curl --version

# tmux/screen and watch are conveniences, not hard requirements: nohup covers
# the persistent session and a shell loop covers the monitoring. Report them
# separately so a missing tmux does not fail an otherwise-good install.
SOFT_MISSING=()
command -v watch >/dev/null 2>&1 \
  && ok "$(printf '%-22s' watch) $(watch --version 2>&1 | head -n1)" \
  || SOFT_MISSING+=("watch")
if   command -v tmux   >/dev/null 2>&1; then ok "$(printf '%-22s' tmux)   $(tmux -V)"
elif command -v screen >/dev/null 2>&1; then ok "$(printf '%-22s' screen) $(screen --version 2>&1 | head -n1)"
else SOFT_MISSING+=("tmux/screen")
fi

echo
if [[ "$FAIL" -ne 0 ]]; then
  warn "toolchain INCOMPLETE — resolve the items above before starting Phase 1."
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
  Not required — root-free substitutes:

    persistent session (instead of tmux/screen)
        nohup mongosyncOrchestrator sync --config orchestrator.json \\
              --poll 15 --lag 5 > orchestrator.out 2>&1 &
        # follow with: tail -f orchestrator.out

    monitor loop (instead of watch, Phase 2 step 6)
        while :; do curl -s localhost:27182/api/v1/progress \\
          | jq -c '{state:.progress.state, lag:.progress.lagTimeSeconds,
                    canCommit:.progress.canCommit}'; sleep 10; done
EOF
fi
