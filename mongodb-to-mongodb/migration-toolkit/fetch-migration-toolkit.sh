#!/usr/bin/env bash
#
# fetch-migration-toolkit.sh
#
# Run this on a BASTION / JUMP HOST THAT HAS INTERNET ACCESS.
#
# Downloads every tool the migration host needs and packs them into a single
# tarball that can be copied (scp / sneakernet) to the air-gapped migration
# host, where install-migration-toolkit.sh unpacks and installs them.
#
# Defaults target the documented migration-host spec: x86_64, RHEL 8/9.
# Override with --arch and --os when the target differs from this bastion.
#
#   ./fetch-migration-toolkit.sh
#   ./fetch-migration-toolkit.sh --os ubuntu2204 --arch x86_64
#   ./fetch-migration-toolkit.sh --os amazon2023 --outdir /data/bundles
#
set -euo pipefail

# ---------------------------------------------------------------- defaults ---
ARCH="x86_64"
TARGET_OS=""                    # auto-detected below if not supplied
MONGOSYNC_VERSION="1.21.0"
MONGOSH_VERSION="2.3.8"
JQ_VERSION="1.7.1"
OUTDIR="$(pwd)"
ORCHESTRATOR_BIN=""             # local prebuilt binary; overrides the download
ORCHESTRATOR_REF="master"       # branch/tag in the migration-utilities repo
SKIP_OS_PACKAGES="false"
ORCHESTRATOR_REPO="chaturvedi-prateek/migration-utilities"
ORCHESTRATOR_PATH="mongodb-to-mongodb/mongosyncOrchestrator/bin"

usage() {
  cat <<'USAGE'
Usage: fetch-migration-toolkit.sh [options]

  --arch <x86_64|aarch64>     Target CPU architecture        (default: x86_64)
  --os <id>                   Target OS: rhel8 | rhel9 | ubuntu2204 |
                              amazon2023                     (default: detected)
  --mongosync-version <v>     mongosync version              (default: 1.21.0)
  --mongosh-version <v>       mongosh version                (default: 2.3.8)
  --jq-version <v>            jq version                     (default: 1.7.1)
  --orchestrator <path>       Use a LOCAL mongosyncOrchestrator binary instead
                              of downloading it from the migration-utilities repo
  --orchestrator-ref <ref>    Branch/tag to pull the orchestrator from
                                                             (default: master)
  --skip-os-packages          Do not attempt to download tmux/watch RPMs/DEBs
  --outdir <dir>              Where to write the bundle      (default: $PWD)
  -h, --help                  This message
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --arch)               ARCH="$2"; shift 2 ;;
    --os)                 TARGET_OS="$2"; shift 2 ;;
    --mongosync-version)  MONGOSYNC_VERSION="$2"; shift 2 ;;
    --mongosh-version)    MONGOSH_VERSION="$2"; shift 2 ;;
    --jq-version)         JQ_VERSION="$2"; shift 2 ;;
    --orchestrator)       ORCHESTRATOR_BIN="$2"; shift 2 ;;
    --orchestrator-ref)   ORCHESTRATOR_REF="$2"; shift 2 ;;
    --skip-os-packages)   SKIP_OS_PACKAGES="true"; shift ;;
    --outdir)             OUTDIR="$2"; shift 2 ;;
    -h|--help)            usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

# ------------------------------------------------------------ OS detection ---
# Returns non-zero rather than calling die(): this runs inside $( ), where an
# exit would abort the subshell and skip any || fallback the caller supplied.
detect_os() {
  [[ -r /etc/os-release ]] || return 1
  # shellcheck disable=SC1091
  . /etc/os-release
  case "${ID}:${VERSION_ID%%.*}" in
    rhel:8|centos:8|rocky:8|almalinux:8) echo "rhel8"      ;;
    rhel:9|centos:9|rocky:9|almalinux:9) echo "rhel9"      ;;
    ubuntu:22)                           echo "ubuntu2204" ;;
    amzn:2023)                           echo "amazon2023" ;;
    *) return 1 ;;
  esac
}

if [[ -z "$TARGET_OS" ]]; then
  TARGET_OS="$(detect_os || true)"
  [[ -n "$TARGET_OS" ]] \
    || die "could not detect this bastion's OS; pass --os <rhel8|rhel9|ubuntu2204|amazon2023>"
  log "no --os given; detected this bastion as '${TARGET_OS}' and using it as the target"
fi

case "$TARGET_OS" in
  rhel8|rhel9|ubuntu2204|amazon2023) ;;
  *) die "unsupported --os '${TARGET_OS}' (rhel8|rhel9|ubuntu2204|amazon2023)" ;;
esac
case "$ARCH" in
  x86_64|aarch64) ;;
  *) die "unsupported --arch '${ARCH}' (x86_64|aarch64)" ;;
esac

# The migration plan states mongosync ships x86_64 only. Verified against
# fastdl.mongodb.org: no aarch64/arm64 mongosync build exists for any distro.
if [[ "$ARCH" != "x86_64" ]]; then
  die "arch '${ARCH}': mongosync publishes x86_64 builds only (no aarch64 artifact exists).
     The migration host must be x86_64. Re-run with --arch x86_64."
fi

# mongosync dropped RHEL 8 after 1.18.0 — rhel80 artifacts stop there, while
# 1.21.0 (the version the migration plan requires) exists for rhel90,
# ubuntu2204, ubuntu2404, amazon2 and amazon2023 only.
if [[ "$TARGET_OS" == "rhel8" ]]; then
  warn "mongosync has no RHEL 8 build past 1.18.0; ${MONGOSYNC_VERSION} is not published for rhel80."
  warn "Build the migration host on RHEL 9 (--os rhel9), Ubuntu 22.04, or Amazon Linux 2023."
fi

# --------------------------------------------------------------- URL setup ---
# mongosync distro slug per target OS.
case "$TARGET_OS" in
  rhel8)      MS_SLUG="rhel80"   ;;
  rhel9)      MS_SLUG="rhel90"   ;;
  ubuntu2204) MS_SLUG="ubuntu2204" ;;
  amazon2023) MS_SLUG="amazon2023" ;;
esac

# mongosh and jq use their own arch naming.
case "$ARCH" in
  x86_64)  MONGOSH_ARCH="x64";   JQ_ARCH="amd64" ;;
  aarch64) MONGOSH_ARCH="arm64"; JQ_ARCH="arm64" ;;
esac

MONGOSYNC_TGZ="mongosync-${MS_SLUG}-${ARCH}-${MONGOSYNC_VERSION}.tgz"
MONGOSYNC_URL="https://fastdl.mongodb.org/tools/mongosync/${MONGOSYNC_TGZ}"

MONGOSH_TGZ="mongosh-${MONGOSH_VERSION}-linux-${MONGOSH_ARCH}.tgz"
MONGOSH_URL="https://downloads.mongodb.com/compass/${MONGOSH_TGZ}"

JQ_BIN="jq-linux-${JQ_ARCH}"
JQ_URL="https://github.com/jqlang/jq/releases/download/jq-${JQ_VERSION}/${JQ_BIN}"

# mongosyncOrchestrator is published as prebuilt per-platform binaries in the
# migration-utilities repo. Go naming: amd64/arm64, not x86_64/aarch64.
case "$ARCH" in
  x86_64)  ORCH_GOARCH="amd64" ;;
  aarch64) ORCH_GOARCH="arm64" ;;
esac
ORCH_FILE="mongosyncOrchestrator-linux-${ORCH_GOARCH}"
ORCH_URL="https://raw.githubusercontent.com/${ORCHESTRATOR_REPO}/${ORCHESTRATOR_REF}/${ORCHESTRATOR_PATH}/${ORCH_FILE}"

# ----------------------------------------------------------- staging setup ---
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BUNDLE_NAME="mongo-migration-toolkit-${TARGET_OS}-${ARCH}-${STAMP}"
STAGE="$(mktemp -d)"
PKGDIR="${STAGE}/${BUNDLE_NAME}"
trap 'rm -rf "$STAGE"' EXIT

mkdir -p "${PKGDIR}/payload" "${PKGDIR}/os-packages"
mkdir -p "$OUTDIR"

fetch() {  # fetch <url> <dest>
  local url="$1" dest="$2"
  log "downloading $(basename "$dest")"
  if command -v curl >/dev/null 2>&1; then
    curl -fSL --retry 3 --retry-delay 2 -o "$dest" "$url"
  elif command -v wget >/dev/null 2>&1; then
    wget -q --tries=3 -O "$dest" "$url"
  else
    die "neither curl nor wget available on this bastion"
  fi
}

# ------------------------------------------------------------- preflight ----
# Probe every artifact before downloading anything, so a bad version/OS combo
# fails with a diagnosis instead of a bare curl 403 halfway through.
# Note: fastdl.mongodb.org returns 403 (not 404) for keys that do not exist,
# and rejects HEAD — hence the ranged GET.
probe() {  # probe <url> -> prints HTTP status
  curl -sL -o /dev/null -w '%{http_code}' -r 0-10 "$1" 2>/dev/null || echo 000
}

log "preflight: checking artifact availability"
PREFLIGHT_FAIL=0
PREFLIGHT_URLS=("mongosync ${MONGOSYNC_URL}" "mongosh ${MONGOSH_URL}" "jq ${JQ_URL}")
[[ -z "$ORCHESTRATOR_BIN" ]] && PREFLIGHT_URLS+=("orchestrator ${ORCH_URL}")

for spec in "${PREFLIGHT_URLS[@]}"; do
  name="${spec%% *}"; url="${spec#* }"
  code="$(probe "$url")"
  if [[ "$code" == "200" || "$code" == "206" ]]; then
    printf '    %-10s ok\n' "$name"
  else
    warn "$(printf '%-10s' "$name") HTTP ${code} — not published at ${url}"
    PREFLIGHT_FAIL=1
  fi
done

if [[ "$PREFLIGHT_FAIL" -ne 0 ]]; then
  echo >&2
  if [[ "$TARGET_OS" == "rhel8" ]]; then
    die "RHEL 8 has no mongosync build past 1.18.0.
     Either target RHEL 9 / Ubuntu 22.04 / Amazon Linux 2023 (--os rhel9),
     or pin --mongosync-version 1.18.0 (below the 1.21 the plan requires)."
  fi
  die "one or more artifacts are not published for os=${TARGET_OS} arch=${ARCH}.
     Check the version against https://www.mongodb.com/try/download/mongosync/releases/archive"
fi

# ---------------------------------------------------------------- payloads ---
fetch "$MONGOSYNC_URL" "${PKGDIR}/payload/${MONGOSYNC_TGZ}"
fetch "$MONGOSH_URL"   "${PKGDIR}/payload/${MONGOSH_TGZ}"
fetch "$JQ_URL"        "${PKGDIR}/payload/${JQ_BIN}"

# mongosyncOrchestrator: prebuilt binaries live in the migration-utilities repo.
# A local --orchestrator path takes precedence (e.g. an internally reviewed build).
if [[ -n "$ORCHESTRATOR_BIN" ]]; then
  [[ -f "$ORCHESTRATOR_BIN" ]] || die "--orchestrator path not found: ${ORCHESTRATOR_BIN}"
  log "including mongosyncOrchestrator from ${ORCHESTRATOR_BIN}"
  cp "$ORCHESTRATOR_BIN" "${PKGDIR}/payload/mongosyncOrchestrator"
else
  fetch "$ORCH_URL" "${PKGDIR}/payload/mongosyncOrchestrator"
  log "mongosyncOrchestrator from ${ORCHESTRATOR_REPO}@${ORCHESTRATOR_REF} (${ORCH_FILE})"
fi
chmod +x "${PKGDIR}/payload/mongosyncOrchestrator"

# Sanity-check it is actually a Linux ELF for the target arch — raw.githubusercontent
# serves an HTML error page with HTTP 200 in some failure modes.
if command -v file >/dev/null 2>&1; then
  ORCH_TYPE="$(file -b "${PKGDIR}/payload/mongosyncOrchestrator")"
  case "$ORCH_TYPE" in
    *ELF*64-bit*) ok_arch="yes" ;;
    *) die "downloaded orchestrator is not a 64-bit ELF binary: ${ORCH_TYPE}" ;;
  esac
  log "orchestrator: ${ORCH_TYPE}"
fi

# ------------------------------------------------------------- OS packages ---
# tmux/screen and watch(procps-ng) have shared-library dependencies, so unlike
# the tarballs above they need real packages. Dependency resolution is only
# correct when this bastion runs the SAME OS as the target.
BASTION_OS="$(detect_os 2>/dev/null || true)"
BASTION_OS="${BASTION_OS:-unknown}"

if [[ "$SKIP_OS_PACKAGES" == "true" ]]; then
  log "skipping OS packages (--skip-os-packages)"
elif [[ "$BASTION_OS" != "$TARGET_OS" ]]; then
  warn "bastion OS '${BASTION_OS}' != target OS '${TARGET_OS}'."
  warn "Skipping tmux/screen/watch package download: dependency resolution would be wrong."
  warn "See NEEDED-OS-PACKAGES.txt in the bundle for what to install on the host."
  case "$TARGET_OS" in
    ubuntu2204) PKG_CMD="apt-get install -y tmux screen procps curl" ;;
    *)          PKG_CMD="dnf install -y tmux screen procps-ng curl"  ;;
  esac
  cat > "${PKGDIR}/NEEDED-OS-PACKAGES.txt" <<EOF
These OS packages are NOT in this bundle and must be installed on the migration
host from its internal repo / Satellite / OS media. They were skipped because
the bastion that built this bundle ran '${BASTION_OS}', not '${TARGET_OS}',
so dependency resolution would have produced the wrong packages.

  tmux (or screen)  persistent session for the 10 parallel mongosync jobs
  procps-ng/procps  provides watch(1), used by the Phase 2 merge monitor loop
  curl              mongosync control API calls (/start, /commit, /progress)

On the migration host (if it has repo access):
  ${PKG_CMD}

Otherwise re-run fetch-migration-toolkit.sh on a ${TARGET_OS} bastion, which
will stage these packages with their dependencies into os-packages/.

Note: procps-ng and curl are usually present on a base RHEL 9 install; tmux
often is not. Verify with: command -v tmux watch curl
EOF
else
  log "downloading OS packages (tmux, screen, procps-ng/procps, curl) with dependencies"
  case "$TARGET_OS" in
    rhel8|rhel9|amazon2023)
      PKGS=(tmux screen procps-ng curl)
      if command -v dnf >/dev/null 2>&1; then
        dnf download --resolve --alldeps --downloaddir "${PKGDIR}/os-packages" "${PKGS[@]}" \
          || warn "dnf download failed (needs dnf-plugins-core); stage tmux/watch manually"
      else
        warn "dnf not found; cannot stage RPMs"
      fi
      ;;
    ubuntu2204)
      PKGS=(tmux screen procps curl)
      ( cd "${PKGDIR}/os-packages" && \
        apt-get install --reinstall --print-uris -qq -y "${PKGS[@]}" \
          | cut -d"'" -f2 | grep -E '^https?://' > .uris ) \
        || warn "apt-get --print-uris failed; stage tmux/watch manually"
      if [[ -s "${PKGDIR}/os-packages/.uris" ]]; then
        while read -r u; do fetch "$u" "${PKGDIR}/os-packages/$(basename "$u")"; done \
          < "${PKGDIR}/os-packages/.uris"
        rm -f "${PKGDIR}/os-packages/.uris"
      fi
      ;;
  esac
fi

# ------------------------------------------------------------- manifest ------
cat > "${PKGDIR}/manifest.env" <<EOF
# Generated by fetch-migration-toolkit.sh on $(hostname) at ${STAMP}
BUNDLE_NAME="${BUNDLE_NAME}"
TARGET_OS="${TARGET_OS}"
ARCH="${ARCH}"
MONGOSYNC_VERSION="${MONGOSYNC_VERSION}"
MONGOSYNC_TGZ="${MONGOSYNC_TGZ}"
MONGOSH_VERSION="${MONGOSH_VERSION}"
MONGOSH_TGZ="${MONGOSH_TGZ}"
JQ_VERSION="${JQ_VERSION}"
JQ_BIN="${JQ_BIN}"
ORCHESTRATOR_SOURCE="${ORCHESTRATOR_BIN:-${ORCHESTRATOR_REPO}@${ORCHESTRATOR_REF}/${ORCH_FILE}}"
EOF

log "generating SHA256 checksums"
# sha256sum on Linux, shasum -a 256 on macOS; both emit "<hash>  <path>".
if command -v sha256sum >/dev/null 2>&1; then SUM=(sha256sum)
elif command -v shasum  >/dev/null 2>&1; then SUM=(shasum -a 256)
else die "no sha256sum or shasum available to checksum the bundle"
fi
( cd "${PKGDIR}" && find payload os-packages -type f | LC_ALL=C sort \
    | tr '\n' '\0' | xargs -0 "${SUM[@]}" > SHA256SUMS )

cp "$0" "${PKGDIR}/fetch-migration-toolkit.sh" 2>/dev/null || true
INSTALLER="$(dirname "$0")/install-migration-toolkit.sh"
if [[ -f "$INSTALLER" ]]; then
  cp "$INSTALLER" "${PKGDIR}/install-migration-toolkit.sh"
  chmod +x "${PKGDIR}/install-migration-toolkit.sh"
else
  warn "install-migration-toolkit.sh not found next to this script; copy it to the host separately"
fi

BUNDLE="${OUTDIR}/${BUNDLE_NAME}.tar.gz"
log "packing ${BUNDLE}"
tar -C "$STAGE" -czf "$BUNDLE" "$BUNDLE_NAME"
( cd "$OUTDIR" && "${SUM[@]}" "$(basename "$BUNDLE")" > "${BUNDLE}.sha256" )

cat <<EOF

Bundle ready:
  ${BUNDLE}
  ${BUNDLE}.sha256   ($(cut -d' ' -f1 < "${BUNDLE}.sha256"))
  size: $(du -h "$BUNDLE" | cut -f1)

Transfer to the migration host, then:
  sha256sum -c $(basename "${BUNDLE}").sha256
  tar xzf $(basename "$BUNDLE")
  sudo ./${BUNDLE_NAME}/install-migration-toolkit.sh
EOF
