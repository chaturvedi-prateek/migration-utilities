#!/usr/bin/env bash
#
# fetch-docdb-migration-toolkit.sh
#
# Run this on a BASTION / JUMP HOST THAT HAS INTERNET ACCESS AND DOCKER.
#
# Stages every tool the DocumentDB -> MongoDB Atlas migration hosts need and
# packs them into a single tarball that can be copied (scp / sneakernet) to the
# in-VPC EC2 hosts, where install-docdb-migration-toolkit.sh unpacks them.
#
# dsynct comes from one of two sources:
#   --dsynct-bin    a licensed Enterprise binary you supply; the image is built
#                   from it (also used when a binary is found at the usual
#                   local paths).
#   --dsynct-image  otherwise: Adiom's public image (default
#                   markadiom/dsynct:latest) is pulled for the target platform,
#                   the binary is extracted from it, and the AWS CA bundle is
#                   layered on top. Docker Hub labels this image "trial" —
#                   confirm production licensing with Adiom before using it.
# Temporal and dsynct are staged as saved Docker images; everything else
# installs as native binaries.
#
#   ./fetch-docdb-migration-toolkit.sh --os amazon2023      # public image
#   ./fetch-docdb-migration-toolkit.sh --dsynct-bin ~/dsync-enterprise/amd/dsynct
#   ./fetch-docdb-migration-toolkit.sh --arch aarch64 --os ubuntu2204 \
#       --dsynct-bin ~/dsync-enterprise/arm/dsynct
#
set -euo pipefail

# ---------------------------------------------------------------- defaults ---
ARCH="x86_64"
TARGET_OS=""                    # auto-detected below if not supplied
DSYNCT_BIN="${DSYNCT_BIN:-}"    # licensed Enterprise binary — never downloaded
DSYNCT_IMAGE="${DSYNCT_IMAGE:-}" # public image used when no binary is supplied
DSYNCT_IMAGE_DEFAULT="markadiom/dsynct:latest"
DSYNCT_TAG="dsynct:enterprise"
DSYNCT_BASE="alpine:3.20"
TEMPORAL_IMAGE="temporalio/temporal:1.8.2"
MONGOSH_VERSION="2.3.8"
JQ_VERSION="1.7.1"
TOOLS_DIR=""                    # local migration-utilities checkout
TOOLS_REF="master"
TOOLS_REPO="chaturvedi-prateek/migration-utilities"
DOCDB_CA_URL="https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"
DOCKER_COMPOSE_VERSION="v5.5.0"
SKIP_IMAGES="false"
SKIP_OS_PACKAGES="false"
SKIP_DOCKER_ENGINE="false"      # target host is assumed to have NO docker/podman
OUTDIR="$(pwd)"

usage() {
  cat <<'USAGE'
Usage: fetch-docdb-migration-toolkit.sh [options]

  --arch <x86_64|aarch64>     Target CPU architecture        (default: x86_64)
  --os <id>                   Target OS: rhel9 | ubuntu2204 | amazon2 |
                              amazon2023                     (default: detected)
  --dsynct-bin <path>         Enterprise dsynct binary for the TARGET arch.
                              Falls back to $DSYNCT_BIN, then to the usual
                              local checkout paths (see the README). The image
                              is built from it.
  --dsynct-image <ref>        Public dsynct image to pull when no binary is
                              given/found; the binary is extracted from it.
                              Falls back to $DSYNCT_IMAGE.
                              (default: markadiom/dsynct:latest — labelled
                              "trial" on Docker Hub; confirm licensing with Adiom)
  --dsynct-tag <ref>          Tag for the staged dsynct image (default: dsynct:enterprise)
  --dsynct-base <image>       Base image for dsynct when built from --dsynct-bin
                                                              (default: alpine:3.20)
  --temporal-image <ref>      Temporal CLI image              (default: temporalio/temporal:1.8.2)
  --mongosh-version <v>       mongosh version                 (default: 2.3.8)
  --jq-version <v>            jq version                      (default: 1.7.1)
  --tools-dir <path>          migration-utilities checkout to take the helper
                              binaries from  (default: the repo this script is in)
  --tools-ref <ref>           Branch/tag to pull helpers from when no local
                              checkout is available            (default: master)
  --docker-compose-version <v> Docker Compose v2 plugin version (default: v5.5.0)
  --skip-images               Do not build/pull/save Docker images
  --skip-os-packages          Do not download tmux/screen/procps/curl packages
  --skip-docker-engine        Do not stage the Docker engine (docker/containerd/
                              runc) — use when the target host already has
                              Docker or Podman installed
  --outdir <dir>              Where to write the bundle        (default: $PWD)
  -h, --help                  This message
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --arch)             ARCH="$2"; shift 2 ;;
    --os)               TARGET_OS="$2"; shift 2 ;;
    --dsynct-bin)       DSYNCT_BIN="$2"; shift 2 ;;
    --dsynct-image)     DSYNCT_IMAGE="$2"; shift 2 ;;
    --dsynct-tag)       DSYNCT_TAG="$2"; shift 2 ;;
    --dsynct-base)      DSYNCT_BASE="$2"; shift 2 ;;
    --temporal-image)   TEMPORAL_IMAGE="$2"; shift 2 ;;
    --mongosh-version)  MONGOSH_VERSION="$2"; shift 2 ;;
    --jq-version)       JQ_VERSION="$2"; shift 2 ;;
    --tools-dir)        TOOLS_DIR="$2"; shift 2 ;;
    --tools-ref)        TOOLS_REF="$2"; shift 2 ;;
    --docker-compose-version) DOCKER_COMPOSE_VERSION="$2"; shift 2 ;;
    --skip-images)       SKIP_IMAGES="true"; shift ;;
    --skip-os-packages)  SKIP_OS_PACKAGES="true"; shift ;;
    --skip-docker-engine) SKIP_DOCKER_ENGINE="true"; shift ;;
    --outdir)           OUTDIR="$2"; shift 2 ;;
    -h|--help)          usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage; exit 2 ;;
  esac
done

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[error]\033[0m %s\n' "$*" >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ------------------------------------------------------------ OS detection ---
# Returns non-zero rather than calling die(): this runs inside $( ), where an
# exit would abort the subshell and skip any || fallback the caller supplied.
detect_os() {
  [[ -r /etc/os-release ]] || return 1
  # shellcheck disable=SC1091
  . /etc/os-release
  case "${ID}:${VERSION_ID%%.*}" in
    rhel:9|centos:9|rocky:9|almalinux:9) echo "rhel9"      ;;
    ubuntu:22)                           echo "ubuntu2204" ;;
    amzn:2)                              echo "amazon2"    ;;
    amzn:2023)                           echo "amazon2023" ;;
    *) return 1 ;;
  esac
}

if [[ -z "$TARGET_OS" ]]; then
  TARGET_OS="$(detect_os || true)"
  [[ -n "$TARGET_OS" ]] \
    || die "could not detect this bastion's OS; pass --os <rhel9|ubuntu2204|amazon2|amazon2023>"
  log "no --os given; detected this bastion as '${TARGET_OS}' and using it as the target"
fi

case "$TARGET_OS" in
  rhel9|ubuntu2204|amazon2|amazon2023) ;;
  *) die "unsupported --os '${TARGET_OS}' (rhel9|ubuntu2204|amazon2|amazon2023)" ;;
esac
case "$ARCH" in
  x86_64)  GOARCH="amd64"; MONGOSH_ARCH="x64";   DOCKER_PLATFORM="linux/amd64" ;;
  aarch64) GOARCH="arm64"; MONGOSH_ARCH="arm64"; DOCKER_PLATFORM="linux/arm64" ;;
  *) die "unsupported --arch '${ARCH}' (x86_64|aarch64)" ;;
esac

# ------------------------------------------------------ locate dsynct binary ---
# An explicit --dsynct-bin / --dsynct-image wins. With neither, a binary at the
# common local layout is used; failing that, the public image is pulled.
[[ -n "$DSYNCT_BIN" && -n "$DSYNCT_IMAGE" ]] \
  && die "--dsynct-bin and --dsynct-image are mutually exclusive"
if [[ -z "$DSYNCT_BIN" && -z "$DSYNCT_IMAGE" ]]; then
  case "$ARCH" in
    x86_64)  ARCH_DIR="amd" ;;
    aarch64) ARCH_DIR="arm" ;;
  esac
  for cand in \
      "${HOME}/Documents/Tools/dsync-enterprise/${ARCH_DIR}/dsynct" \
      "${HOME}/dsync-enterprise/${ARCH_DIR}/dsynct" \
      "${SCRIPT_DIR}/vendor/dsynct/dsynct-linux-${GOARCH}"; do
    if [[ -n "$cand" && -f "$cand" ]]; then DSYNCT_BIN="$cand"; break; fi
  done
  [[ -n "$DSYNCT_BIN" ]] || DSYNCT_IMAGE="$DSYNCT_IMAGE_DEFAULT"
fi

check_dsynct_elf() {  # check_dsynct_elf <path> <what>
  command -v file >/dev/null 2>&1 || return 0
  DSYNCT_TYPE="$(file -b "$1")"
  case "$ARCH:$DSYNCT_TYPE" in
    x86_64:*ELF*x86-64*|aarch64:*ELF*aarch64*) ;;
    *) die "$2 is not a Linux ${ARCH} ELF binary: ${DSYNCT_TYPE}" ;;
  esac
  log "dsynct: ${DSYNCT_TYPE}"
}

if [[ -n "$DSYNCT_BIN" ]]; then
  DSYNCT_MODE="binary"
  [[ -f "$DSYNCT_BIN" ]] || die "--dsynct-bin path not found: ${DSYNCT_BIN}"
  # raw.githubusercontent serves HTML error pages with HTTP 200 in some failure
  # modes, and an arch mismatch here would only surface on the migration host.
  check_dsynct_elf "$DSYNCT_BIN" "--dsynct-bin"
  DSYNCT_SOURCE="$DSYNCT_BIN"
else
  DSYNCT_MODE="image"
  DSYNCT_SOURCE="image:${DSYNCT_IMAGE}"
  log "dsynct: no --dsynct-bin; using public image ${DSYNCT_IMAGE}"
  warn "${DSYNCT_IMAGE} is labelled \"trial\" on Docker Hub and ships a trial LICENSE.md"
  warn "(evaluation only, time-limited). Confirm production licensing with Adiom before use."
fi

# --------------------------------------------------------------- URL setup ---
MONGOSH_TGZ="mongosh-${MONGOSH_VERSION}-linux-${MONGOSH_ARCH}.tgz"
MONGOSH_URL="https://downloads.mongodb.com/compass/${MONGOSH_TGZ}"

JQ_BIN="jq-linux-${GOARCH}"
JQ_URL="https://github.com/jqlang/jq/releases/download/jq-${JQ_VERSION}/${JQ_BIN}"

# Docker Compose v2 ships as a CLI-plugin binary, not a package, on AL2/AL2023 —
# neither distro's repos carry a docker-compose-plugin rpm. Asset naming matches
# our ARCH directly (x86_64/aarch64), no GOARCH translation needed.
DOCKER_COMPOSE_BIN="docker-compose-linux-${ARCH}"
DOCKER_COMPOSE_URL="https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/${DOCKER_COMPOSE_BIN}"

# Helper binaries from this repo, in the order the migration plan uses them.
#   <name>:<repo path to bin dir>
HELPERS=(
  "fixIdTypes:docdb-to-mongodb/fixIdTypes/go/bin"
  "migrateIndexes:docdb-to-mongodb/indexes/migrateIndexes/bin"
  "checkChangeStreams:docdb-to-mongodb/checkChangeStreams/bin"
  "copyMissingDocs:docdb-to-mongodb/copyMissingDocs/go/bin"
)
# Sample configs shipped alongside the binaries that need them.
HELPER_CONFIGS=(
  "fixIdTypes:docdb-to-mongodb/fixIdTypes/go/config.sample.json"
  "migrateIndexes:docdb-to-mongodb/indexes/migrateIndexes/config.sample.json"
  "copyMissingDocs:docdb-to-mongodb/copyMissingDocs/go/config.sample.json"
)
# Plain shell scripts (not per-arch ELF binaries) — staged as-is, no ELF check.
# Driven entirely by $DOCDB_SRC / $MDB_DEST, same env vars as everything else
# in this toolkit's .env.
HELPER_SCRIPTS=(
  "fullCountVerify.sh:docdb-to-mongodb/dataVerifier/fullCountVerify.sh"
)

# The script normally lives at <repo>/docdb-to-mongodb/migration-toolkit/.
if [[ -z "$TOOLS_DIR" && -d "${SCRIPT_DIR}/../../docdb-to-mongodb" ]]; then
  TOOLS_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
fi

# ----------------------------------------------------------- staging setup ---
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BUNDLE_NAME="docdb-migration-toolkit-${TARGET_OS}-${ARCH}-${STAMP}"
STAGE="$(mktemp -d)"
PKGDIR="${STAGE}/${BUNDLE_NAME}"
trap 'rm -rf "$STAGE"' EXIT

mkdir -p "${PKGDIR}"/payload/{dsynct,images,tools,configs,helpers,cdctest} \
         "${PKGDIR}/os-packages" "${PKGDIR}/compose"
mkdir -p "$OUTDIR"
# Resolve to an absolute path: the checksum step below cd's into $OUTDIR, and a
# relative path (e.g. "./dist") would then be re-joined against the new cwd.
OUTDIR="$(cd "$OUTDIR" && pwd)"

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

# ------------------------------------------------------------- preflight -----
# Probe every artifact before downloading anything, so a bad version fails with
# a diagnosis instead of a bare 403 halfway through.
probe() {  # probe <url> -> prints HTTP status
  curl -sL -o /dev/null -w '%{http_code}' -r 0-10 "$1" 2>/dev/null || echo 000
}

log "preflight: checking artifact availability"
PREFLIGHT_FAIL=0
PREFLIGHT_URLS=(
  "mongosh ${MONGOSH_URL}"
  "jq ${JQ_URL}"
  "docdb-ca ${DOCDB_CA_URL}"
  "compose ${DOCKER_COMPOSE_URL}"
)
for spec in "${PREFLIGHT_URLS[@]}"; do
  name="${spec%% *}"; url="${spec#* }"
  code="$(probe "$url")"
  if [[ "$code" == "200" || "$code" == "206" ]]; then
    printf '    %-12s ok\n' "$name"
  else
    warn "$(printf '%-12s' "$name") HTTP ${code} — not published at ${url}"
    PREFLIGHT_FAIL=1
  fi
done

# Image mode needs docker even with --skip-images: the binary comes from the image.
if [[ "$SKIP_IMAGES" != "true" || "$DSYNCT_MODE" == "image" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    warn "docker not found — Temporal and dsynct images cannot be staged"
    PREFLIGHT_FAIL=1
  elif ! docker info >/dev/null 2>&1; then
    warn "docker daemon is not reachable — start it, or pass --skip-images"
    PREFLIGHT_FAIL=1
  else
    printf '    %-12s ok (%s)\n' "docker" "$(docker version --format '{{.Server.Version}}' 2>/dev/null)"
  fi
fi

if [[ "$PREFLIGHT_FAIL" -ne 0 ]]; then
  echo >&2
  die "preflight failed — resolve the items above before building the bundle.
     mongosh versions: https://www.mongodb.com/try/download/shell
     jq versions:      https://github.com/jqlang/jq/releases"
fi

# ---------------------------------------------------------------- payloads ---
fetch "$MONGOSH_URL"         "${PKGDIR}/payload/${MONGOSH_TGZ}"
fetch "$JQ_URL"              "${PKGDIR}/payload/${JQ_BIN}"
fetch "$DOCDB_CA_URL"        "${PKGDIR}/payload/global-bundle.pem"
fetch "$DOCKER_COMPOSE_URL"  "${PKGDIR}/payload/${DOCKER_COMPOSE_BIN}"
chmod +x "${PKGDIR}/payload/${DOCKER_COMPOSE_BIN}"

DSYNCT_IMAGE_DIGEST=""
if [[ "$DSYNCT_MODE" == "binary" ]]; then
  log "staging Enterprise dsynct from ${DSYNCT_BIN}"
  install -m 0755 "$DSYNCT_BIN" "${PKGDIR}/payload/dsynct/dsynct-linux-${GOARCH}"
else
  log "pulling ${DSYNCT_IMAGE} for ${DOCKER_PLATFORM}"
  docker pull --platform "$DOCKER_PLATFORM" "$DSYNCT_IMAGE" \
    || die "docker pull failed for ${DSYNCT_IMAGE}"
  # Pin everything below to the manifest-list digest just pulled, so a tag
  # moving on Docker Hub mid-run cannot mix two builds in one bundle.
  if docker buildx version >/dev/null 2>&1; then
    DSYNCT_IMAGE_DIGEST="$(docker buildx imagetools inspect "$DSYNCT_IMAGE" 2>/dev/null \
      | awk '/^Digest:/ {print $2; exit}')"
  fi
  if [[ -z "$DSYNCT_IMAGE_DIGEST" ]]; then
    DSYNCT_IMAGE_DIGEST="$(docker image inspect --format '{{index .RepoDigests 0}}' "$DSYNCT_IMAGE" 2>/dev/null)"
    DSYNCT_IMAGE_DIGEST="${DSYNCT_IMAGE_DIGEST##*@}"
  fi
  [[ "$DSYNCT_IMAGE_DIGEST" == sha256:* ]] || die "could not resolve the digest of ${DSYNCT_IMAGE}"
  # Strip a :tag (but not a registry :port) before appending the digest.
  DSYNCT_REPO="$DSYNCT_IMAGE"
  [[ "${DSYNCT_IMAGE##*/}" == *:* ]] && DSYNCT_REPO="${DSYNCT_IMAGE%:*}"
  DSYNCT_IMAGE_PINNED="${DSYNCT_REPO}@${DSYNCT_IMAGE_DIGEST}"
  [[ "$DSYNCT_IMAGE" == *@* ]] && DSYNCT_IMAGE_PINNED="$DSYNCT_IMAGE"
  log "dsynct image digest ${DSYNCT_IMAGE_DIGEST}"

  # The image is distroless (no shell); the binary is /dsynct. docker create
  # never runs it, so a foreign-arch image is fine here.
  DSYNCT_CID="$(docker create --platform "$DOCKER_PLATFORM" "$DSYNCT_IMAGE_PINNED")" \
    || die "docker create failed for ${DSYNCT_IMAGE_PINNED}"
  docker cp "${DSYNCT_CID}:/dsynct" "${PKGDIR}/payload/dsynct/dsynct-linux-${GOARCH}" \
    || { docker rm -f "$DSYNCT_CID" >/dev/null; die "no /dsynct binary in ${DSYNCT_IMAGE}"; }
  docker cp "${DSYNCT_CID}:/LICENSE.md" "${PKGDIR}/payload/dsynct/LICENSE.md" 2>/dev/null \
    || warn "no LICENSE.md in ${DSYNCT_IMAGE}"
  docker rm -f "$DSYNCT_CID" >/dev/null
  chmod 0755 "${PKGDIR}/payload/dsynct/dsynct-linux-${GOARCH}"
  check_dsynct_elf "${PKGDIR}/payload/dsynct/dsynct-linux-${GOARCH}" "/dsynct in ${DSYNCT_IMAGE}"
fi

# ------------------------------------------------- CDC / ops helper scripts ---
# Shipped from this script's own directory: they are part of the toolkit, not
# of the migration-utilities helper repo.
for f in cs-rate-profile.js split-hot-cold.sh dsync-progress.sh dsync-find-pauses.sh; do
  [[ -f "${SCRIPT_DIR}/helpers/${f}" ]] || die "missing ${SCRIPT_DIR}/helpers/${f}"
  install -m 0755 "${SCRIPT_DIR}/helpers/${f}" "${PKGDIR}/payload/helpers/${f}"
done
chmod 0644 "${PKGDIR}/payload/helpers/cs-rate-profile.js"
for f in run-cdc-test.sh .env.sample; do
  [[ -f "${SCRIPT_DIR}/helpers/cdctest/${f}" ]] || die "missing ${SCRIPT_DIR}/helpers/cdctest/${f}"
  install -m 0644 "${SCRIPT_DIR}/helpers/cdctest/${f}" "${PKGDIR}/payload/cdctest/${f}"
done
chmod 0755 "${PKGDIR}/payload/cdctest/run-cdc-test.sh"

# ------------------------------------------------------------- helper bins ---
for spec in "${HELPERS[@]}"; do
  name="${spec%%:*}"; bindir="${spec#*:}"
  file="${name}-linux-${GOARCH}"
  if [[ -n "$TOOLS_DIR" && -f "${TOOLS_DIR}/${bindir}/${file}" ]]; then
    log "staging ${name} from local checkout"
    install -m 0755 "${TOOLS_DIR}/${bindir}/${file}" "${PKGDIR}/payload/tools/${name}"
  else
    fetch "https://raw.githubusercontent.com/${TOOLS_REPO}/${TOOLS_REF}/${bindir}/${file}" \
          "${PKGDIR}/payload/tools/${name}"
    chmod +x "${PKGDIR}/payload/tools/${name}"
  fi
  if command -v file >/dev/null 2>&1; then
    case "$(file -b "${PKGDIR}/payload/tools/${name}")" in
      *ELF*64-bit*) ;;
      *) die "staged ${name} is not a 64-bit ELF binary — download likely served an error page" ;;
    esac
  fi
done

for spec in "${HELPER_SCRIPTS[@]}"; do
  name="${spec%%:*}"; path="${spec#*:}"
  if [[ -n "$TOOLS_DIR" && -f "${TOOLS_DIR}/${path}" ]]; then
    log "staging ${name} from local checkout"
    install -m 0755 "${TOOLS_DIR}/${path}" "${PKGDIR}/payload/tools/${name}"
  else
    fetch "https://raw.githubusercontent.com/${TOOLS_REPO}/${TOOLS_REF}/${path}" \
          "${PKGDIR}/payload/tools/${name}"
    chmod +x "${PKGDIR}/payload/tools/${name}"
  fi
done

for spec in "${HELPER_CONFIGS[@]}"; do
  name="${spec%%:*}"; path="${spec#*:}"
  if [[ -n "$TOOLS_DIR" && -f "${TOOLS_DIR}/${path}" ]]; then
    cp "${TOOLS_DIR}/${path}" "${PKGDIR}/payload/configs/${name}.config.sample.json"
  else
    fetch "https://raw.githubusercontent.com/${TOOLS_REPO}/${TOOLS_REF}/${path}" \
          "${PKGDIR}/payload/configs/${name}.config.sample.json" || \
      warn "could not stage ${name} sample config"
  fi
done

# ----------------------------------------------------------- docker images ---
if [[ "$SKIP_IMAGES" == "true" ]]; then
  log "skipping Docker images (--skip-images)"
  TEMPORAL_IMAGE_TAR=""
  DSYNCT_IMAGE_TAR=""
else
  # dsynct is statically linked, so the image is FROM + COPY only. No build step
  # executes, which is why a foreign-arch build works here without qemu.
  if [[ "$DSYNCT_MODE" == "binary" ]]; then
    cat > "${PKGDIR}/compose/Dockerfile.dsynct" <<EOF
# Generated by fetch-docdb-migration-toolkit.sh
# dsynct is a statically linked Go binary — no libc, no runtime deps.
FROM ${DSYNCT_BASE}
COPY dsynct /usr/local/bin/dsynct
COPY global-bundle.pem /certs/global-bundle.pem
ENTRYPOINT ["/usr/local/bin/dsynct"]
EOF
    cp "${PKGDIR}/payload/dsynct/dsynct-linux-${GOARCH}" "${PKGDIR}/compose/dsynct"
  else
    # The public image has no /certs/global-bundle.pem, which every
    # tlsCAFile in .env points at — add it as one COPY layer on the pinned
    # digest instead of making each compose service mount it.
    cat > "${PKGDIR}/compose/Dockerfile.dsynct" <<EOF
# Generated by fetch-docdb-migration-toolkit.sh
# ${DSYNCT_IMAGE} pinned by digest, plus the AWS DocumentDB CA bundle.
FROM ${DSYNCT_IMAGE_PINNED}
COPY global-bundle.pem /certs/global-bundle.pem
EOF
  fi
  cp "${PKGDIR}/payload/global-bundle.pem"             "${PKGDIR}/compose/global-bundle.pem"

  # A buildx container driver keeps the result in the build cache unless --load
  # is given, and `docker save` then cannot find the tag. The classic builder
  # rejects --load, so pick the invocation that matches what is available.
  if docker buildx version >/dev/null 2>&1; then
    BUILD_CMD=(docker buildx build --load)
  else
    BUILD_CMD=(docker build)
  fi

  log "building ${DSYNCT_TAG} for ${DOCKER_PLATFORM}"
  "${BUILD_CMD[@]}" --platform "$DOCKER_PLATFORM" \
    -f "${PKGDIR}/compose/Dockerfile.dsynct" \
    -t "$DSYNCT_TAG" "${PKGDIR}/compose" \
    || die "docker build failed for ${DSYNCT_TAG}"
  rm -f "${PKGDIR}/compose/dsynct" "${PKGDIR}/compose/global-bundle.pem"

  log "pulling ${TEMPORAL_IMAGE} for ${DOCKER_PLATFORM}"
  docker pull --platform "$DOCKER_PLATFORM" "$TEMPORAL_IMAGE" \
    || die "docker pull failed for ${TEMPORAL_IMAGE}"

  # Slug the image refs so the tar filenames are path-safe.
  TEMPORAL_IMAGE_TAR="temporal-$(echo "$TEMPORAL_IMAGE" | tr '/:' '--').tar"
  DSYNCT_IMAGE_TAR="dsynct-$(echo "$DSYNCT_TAG" | tr '/:' '--')-${GOARCH}.tar"

  log "saving images to payload/images/"
  docker save -o "${PKGDIR}/payload/images/${TEMPORAL_IMAGE_TAR}" "$TEMPORAL_IMAGE"
  docker save -o "${PKGDIR}/payload/images/${DSYNCT_IMAGE_TAR}"   "$DSYNCT_TAG"
fi

# --------------------------------------------------------------- compose -----
cat > "${PKGDIR}/compose/docker-compose.yml" <<EOF
# Generated by fetch-docdb-migration-toolkit.sh
#
# Multi-worker distributed copy topology (dsync Enterprise playbook §1):
#   temporal  — coordinator, holds the flow plan and task queue
#   worker    — copy workers; the unit of scale. Scale with:
#                 docker compose up -d --scale worker=N
#   runner    — submits the workflow once and serves the dashboard on :8080
#
# Fill in .env first (copy from .env.sample). Bring up in order:
#   docker compose up -d temporal
#   docker compose up -d --scale worker=\${WORKER_COUNT:-3} worker
#   docker compose up -d runner
#
# Workers deliberately run with 'app --no-progress': several dsynct processes on
# one host cannot all bind the progress port, and the runner already serves it.
#
# A second, non-overlapping flow (e.g. the cold namespaces) runs as its own
# project against this Temporal — see "Stream topology" in .env.sample:
#   docker compose -p flow-cold --env-file .env.cold up -d --no-deps \\
#       --scale worker=2 worker runner
#
# Boolean knobs are passed as --flag=\${VAR:-default}; dsynct accepts
# --flag=true / --flag=false for every boolean option.
services:
  temporal:
    image: ${TEMPORAL_IMAGE}
    command:
      - server
      - start-dev
      - --db-filename=/data/temporal.db
      - --ip=0.0.0.0
      - --dynamic-config-value=limit.numPendingActivities.error=10000
      - --dynamic-config-value=frontend.activityAPIsEnabled=true
    ports:
      - "7233:7233"   # gRPC — workers and runner connect here
      - "8233:8233"   # Temporal Web UI
    volumes:
      - temporal-data:/data
    restart: unless-stopped

  worker:
    image: ${DSYNCT_TAG}
    depends_on: [temporal]
    command:
      - worker
      - "--queue-name=\${QUEUE:?set QUEUE in .env}"
      - "--concurrent-activities=\${CONCURRENT_ACTIVITIES:-4}"
      - "--sync-writer-workers=\${SYNC_WRITER_WORKERS:-8}"
      - "--per-stream-workers=\${PER_STREAM_WORKERS:-4}"
      - --pause-on-error
      - "--attempts-before-pause=\${ATTEMPTS_BEFORE_PAUSE:-3}"
      - "\${DOCDB_SRC:?set DOCDB_SRC in .env}"
      - "--doc-partition=\${DOC_PARTITION:-500000}"
      - "--namespace-fanout=\${NAMESPACE_FANOUT:-100}"
      - "--documentdb-sampling-fanout=\${DOCUMENTDB_SAMPLING_FANOUT:-100}"
      - "--per-namespace-streams=\${PER_NAMESPACE_STREAMS:-false}"
      - "\${MDB_DEST:?set MDB_DEST in .env}"
      - temporal
      - "--host-port=\${TEMPORAL_ADDR:-temporal:7233}"
      - app
      - --no-progress
      - "--otel=\${DSYNCT_OTEL:-false}"
      - "--otel-service-name=\${OTEL_SERVICE_NAME:-dsynct}"
      - "--otel-metric-interval=\${OTEL_METRIC_INTERVAL:-10s}"
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: "\${OTEL_EXPORTER_OTLP_ENDPOINT:-}"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    restart: unless-stopped

  runner:
    image: ${DSYNCT_TAG}
    depends_on: [temporal]
    command:
      - run
      - "--workflow-id=\${WORKFLOW_ID:?set WORKFLOW_ID in .env}"
      - "--queue-name=\${QUEUE:?set QUEUE in .env}"
      - "--namespace=\${NAMESPACE:?set NAMESPACE in .env}"
      - "--skip-initial-sync=\${SKIP_INITIAL_SYNC:-false}"
      - temporal
      - "--host-port=\${TEMPORAL_ADDR:-temporal:7233}"
      - app
      - --host-port=0.0.0.0:8080
      - --persist
      - "--otel=\${DSYNCT_OTEL:-false}"
      - "--otel-service-name=\${OTEL_SERVICE_NAME:-dsynct}"
      - "--otel-metric-interval=\${OTEL_METRIC_INTERVAL:-10s}"
    environment:
      OTEL_EXPORTER_OTLP_ENDPOINT: "\${OTEL_EXPORTER_OTLP_ENDPOINT:-}"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    ports:
      - "\${DASHBOARD_PORT:-8080}:8080"   # dsynct progress dashboard
    restart: on-failure

volumes:
  temporal-data:
EOF

cat > "${PKGDIR}/compose/.env.sample" <<'EOF'
# Copy to .env and fill in before running docker compose.
#
# NOTE: tlsCAFile must point at the IN-CONTAINER path. The dsynct image ships
# the AWS bundle at /certs/global-bundle.pem.
DOCDB_SRC=mongodb://dsync_migration:PASSWORD@your-cluster.cluster-xxxx.ap-south-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=/certs/global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false
MDB_DEST=mongodb+srv://migration_user:PASSWORD@cluster.xxxxx.mongodb.net/?retryWrites=true

# One queue per migration config. Never mix different source/dest/transform
# configs on one queue — workers pull tasks interchangeably.
QUEUE=dsync-prod-svoc
WORKFLOW_ID=prod-svoc
NAMESPACE=svoc-db

# Scale knobs. Total concurrent partition copies is roughly
# CONCURRENT_ACTIVITIES x worker replicas — raise until the source, Atlas, or
# the workers saturate, then stop.
WORKER_COUNT=3
CONCURRENT_ACTIVITIES=4
SYNC_WRITER_WORKERS=8
PER_STREAM_WORKERS=4

# 500000 is the documented floor for the large MSIL clusters. Lower it only if
# a few huge collections would otherwise not split into enough tasks to fill
# the worker fleet.
DOC_PARTITION=500000
# Planning concurrency only (how many namespaces are planned at once). It does
# NOT change change-stream throughput.
NAMESPACE_FANOUT=100
DOCUMENTDB_SAMPLING_FANOUT=100

# ---------------------------------------------------------- stream topology --
# DECIDE BEFORE THE INITIAL SYNC. Stream topology cannot be changed on a
# running workflow, and a new flow cannot start CDC from a past timestamp.
#
# 1. Profile source write rates at peak first:
#      CS_SECONDS=300 mongosh "$DOCDB_SRC" --quiet --file <toolkit>/helpers/cs-rate-profile.js
# 2. With PER_NAMESPACE_STREAMS=false, ALL namespaces share ONE change stream
#    whose writer flushes whenever the namespace changes between events. With
#    many interleaved collections batches shrink to ~1 event and CDC caps at
#    ~1.9k events/s, regardless of PER_STREAM_WORKERS, worker count or Atlas
#    tier.
# 3. PER_NAMESPACE_STREAMS=true gives every collection its own stream. Each
#    stream holds 2 activity slots (StreamChanges + StreamLSN) for its whole
#    life, so CONCURRENT_ACTIVITIES x WORKER_COUNT must be >= 2 x collections
#    (plus headroom), or some collections never start streaming. Every
#    worker of a flow must use the same value.
# 4. For many collections, split into NON-OVERLAPPING flows with
#    split-hot-cold.sh: hot collections with PER_NAMESPACE_STREAMS=true, the
#    low-traffic tail on one shared stream. Each flow needs its own .env with a
#    distinct QUEUE, WORKFLOW_ID, NAMESPACE list and DASHBOARD_PORT, and its
#    own workers. Collections created later are in neither flow.
# 5. Optionally prove the choice first with the CDC test kit (<toolkit>/cdctest,
#    T0/T1/T2 against a SCRATCH cluster).
PER_NAMESPACE_STREAMS=false

# Stream-only flow: skip the initial copy and start CDC at the live head. Only
# for stream-only / reverse-sync / test flows — never for the main migration.
SKIP_INITIAL_SYNC=false

# --pause-on-error pauses an activity after this many failed attempts. 0
# pauses on the first error, and a routine worker restart counts as one.
# Always unpause with --reset-attempts (dsync-find-pauses.sh --unpause does
# this, plus --reset-heartbeats). Paused activities show only on the CHILD
# workflow (e.g. <WORKFLOW_ID>/bulk-5), not on the parent.
ATTEMPTS_BEFORE_PAUSE=3

# Additional flows on this host reach the main Temporal via the host:
#   TEMPORAL_ADDR=host.docker.internal:7233   DASHBOARD_PORT=8081
TEMPORAL_ADDR=temporal:7233
DASHBOARD_PORT=8080

# -------------------------------------------------------------- OpenTelemetry --
# Export dsynct logs + metrics over OTLP gRPC (key metrics: dsynct.read,
# dsynct.written, dsynct.write_updates, dsynct.stream_read_gauge,
# dsynct.stream_written_gauge, dsynct.read_ahead_gauge,
# dsynct.since_last_event_time). With DSYNCT_OTEL=true logs go to the
# collector as JSON. SigNoz dashboards:
#   https://github.com/adiom-data/public/tree/main/kubernetes/system/signoz_dashboards
DSYNCT_OTEL=false
# e.g. http://otel-collector.example.internal:4317 (gRPC; required when DSYNCT_OTEL=true)
OTEL_EXPORTER_OTLP_ENDPOINT=
OTEL_SERVICE_NAME=dsynct
OTEL_METRIC_INTERVAL=10s
EOF

# ------------------------------------------------------------ CDC test kit ---
# Separate compose project ("cdctest", own Temporal on 17233/18233, dashboard
# on 8082) for Adiom's stream-only T0/T1/T2 tests; driven by run-cdc-test.sh,
# which sets CDC_DEST / PER_NAMESPACE_STREAMS / QUEUE / WORKFLOW_ID per test.
# Defaults are the safe T0 values, so a bare `docker compose up` writes nowhere.
cat > "${PKGDIR}/payload/cdctest/docker-compose.yml" <<EOF
# Generated by fetch-docdb-migration-toolkit.sh — CDC throughput test kit.
# Use ./run-cdc-test.sh T0|T1|T2|status|down rather than compose directly.
name: cdctest
services:
  temporal:
    image: ${TEMPORAL_IMAGE}
    command:
      - server
      - start-dev
      - --db-filename=/data/temporal.db
      - --ip=0.0.0.0
      - --dynamic-config-value=limit.numPendingActivities.error=10000
      - --dynamic-config-value=frontend.activityAPIsEnabled=true
    ports:
      - "17233:7233"
      - "18233:8233"
    volumes:
      - temporal-data:/data
    restart: unless-stopped

  worker:
    image: ${DSYNCT_TAG}
    depends_on: [temporal]
    command:
      - worker
      - "--queue-name=\${QUEUE:-cdctest-T0}"
      - "--concurrent-activities=\${CONCURRENT_ACTIVITIES:-4}"
      - "--sync-writer-workers=\${SYNC_WRITER_WORKERS:-8}"
      - "--per-stream-workers=\${PER_STREAM_WORKERS:-4}"
      - --pause-on-error
      - "--attempts-before-pause=\${ATTEMPTS_BEFORE_PAUSE:-3}"
      - "\${DOCDB_SRC:?set DOCDB_SRC in cdctest/.env}"
      - "--doc-partition=\${DOC_PARTITION:-500000}"
      - "--namespace-fanout=\${NAMESPACE_FANOUT:-100}"
      - "--documentdb-sampling-fanout=\${DOCUMENTDB_SAMPLING_FANOUT:-100}"
      - "--per-namespace-streams=\${PER_NAMESPACE_STREAMS:-false}"
      - "\${CDC_DEST:-/dev/null}"
      - temporal
      - --host-port=temporal:7233
      - app
      - --no-progress
    restart: unless-stopped

  runner:
    image: ${DSYNCT_TAG}
    depends_on: [temporal]
    command:
      - run
      - "--workflow-id=\${WORKFLOW_ID:-cdctest-T0}"
      - "--queue-name=\${QUEUE:-cdctest-T0}"
      - "--namespace=\${NAMESPACE:?set NAMESPACE in cdctest/.env}"
      - --skip-initial-sync=true
      - temporal
      - --host-port=temporal:7233
      - app
      - --host-port=0.0.0.0:8080
      - --persist
    ports:
      - "8082:8080"
    restart: on-failure

volumes:
  temporal-data:
EOF

# ------------------------------------------------------------- OS packages ---
# tmux/screen, watch(procps-ng), curl, and the Docker engine all have
# shared-library dependencies, so unlike the tarballs above they need real
# packages.
#
# For amazon2023, these are fetched directly over HTTPS from the Amazon Linux
# repo's own metadata (repodata/primary.xml.gz) — no dnf, no matching bastion
# OS, no container required. This works from ANY host with internet access
# (verified: resolves the exact same package/version set dnf itself picks).
# rhel9/amazon2/ubuntu2204 still use their package manager's --resolve, gated
# on the bastion running the SAME OS as the target, because dependency
# resolution on a MISMATCHED or minimal-container bastion massively
# over-reports what is "missing" — verified while building this script:
# resolving tmux+screen+procps-ng+curl in a bare container pulled in 92
# packages, docker+containerd+runc pulled in 153, and every extra was a
# base-OS package (glibc, systemd, python3, rpm, selinux-policy, ...) that
# must never be shipped in this bundle and blind-installed over a live host's
# own copy.
BASTION_OS="$(detect_os 2>/dev/null || true)"
BASTION_OS="${BASTION_OS:-unknown}"
OS_MATCH="false"
[[ "$BASTION_OS" == "$TARGET_OS" ]] && OS_MATCH="true"

# Keep only the packages this toolkit actually needs from a resolved
# dependency set — never the base-OS packages a resolve pulls in when the
# bastion is missing them locally. Those are guaranteed present on a booted
# AMI; letting the target's own rpmdb satisfy them is what makes the plain
# `dnf/yum install <rpms>` in the installer safe (it will not touch a package
# that isn't in this list, so it can't downgrade something core).
RPM_STAGE_ALLOWLIST='^(tmux|screen|procps-ng|procps|curl|libcurl|docker|containerd|runc|container-selinux|libcgroup|libseccomp|libnftnl|libmnl|libnetfilter_conntrack|libnfnetlink|iptables|iptables-libs|iptables-nft|pigz|xfsprogs|device-mapper|device-mapper-libs|fuse-overlayfs|slirp4netns|criu|containers-common)-[0-9]'

filter_rpm_stage() {  # filter_rpm_stage <dir> — keep only allowlisted, native-arch rpms
  local dir="$1"
  find "$dir" -maxdepth 1 \( -name '*.i686.rpm' -o -name '*.i386.rpm' \) -delete
  find "$dir" -maxdepth 1 -name '*.rpm' -printf '%f\n' | while read -r f; do
    [[ "$f" =~ $RPM_STAGE_ALLOWLIST ]] || rm -f "${dir:?}/${f}"
  done
}

# amazon2023 packages this toolkit needs (curl-minimal, already on every
# amazon2023 host, covers the "curl" requirement — see the CURL_PKG note
# below — so it is deliberately not in this list).
AL2023_PKG_NAMES="tmux screen procps-ng docker containerd runc container-selinux libcgroup libseccomp libnftnl libmnl libnetfilter_conntrack libnfnetlink iptables-libs iptables-nft pigz xfsprogs device-mapper device-mapper-libs"

# Resolves exact package NVRs directly from the Amazon Linux 2023 repo's own
# metadata over plain HTTPS — no dnf, no container, works on any host with
# internet (verified to return the identical package/version set `dnf
# download --resolve --alldeps` does). $PKG_NAMES is a space-separated list;
# packages not needed on this run (e.g. docker when --skip-docker-engine) are
# simply absent from that list, so nothing extra is ever staged.
fetch_al2023_rpms() {  # fetch_al2023_rpms <arch> <destdir> <pkg names...>
  local arch="$1" dest="$2"; shift 2
  local mirror_list="https://cdn.amazonlinux.com/al2023/core/mirrors/latest/${arch}/mirror.list"
  local base
  base="$(curl -fsSL "$mirror_list" | head -n1)"
  [[ -n "$base" ]] || { warn "could not resolve the amazon2023 ${arch} repo mirror"; return 1; }
  local primary_gz="${dest}/.primary.xml.gz"
  fetch "${base}repodata/primary.xml.gz" "$primary_gz" || return 1

  local resolver; resolver="$(mktemp)"
  cat > "$resolver" <<'PYEOF'
import sys, gzip, xml.etree.ElementTree as ET
NS = {'c': 'http://linux.duke.edu/metadata/common'}
def vertup(ver, rel):
    def parts(s):
        return [(0, int(c)) if c.isdigit() else (1, c) for c in s.replace('-', '.').split('.')]
    return parts(ver) + parts(rel)
base, arch, primary_gz = sys.argv[1], sys.argv[2], sys.argv[3]
names = set(sys.argv[4:])
with open(primary_gz, 'rb') as f:
    root = ET.fromstring(gzip.decompress(f.read()))
best = {}
for pkg in root.findall('c:package', NS):
    name = pkg.findtext('c:name', namespaces=NS)
    if name not in names or pkg.findtext('c:arch', namespaces=NS) not in (arch, 'noarch'):
        continue
    v = pkg.find('c:version', NS)
    key = vertup(v.get('ver'), v.get('rel'))
    if name not in best or key > best[name][0]:
        best[name] = (key, base + pkg.find('c:location', NS).get('href'))
missing = names - set(best)
for name, (_, url) in sorted(best.items()):
    print(f"{name}\t{url}")
if missing:
    print("MISSING:" + ",".join(sorted(missing)), file=sys.stderr)
PYEOF
  local resolved rc=0
  resolved="$(python3 "$resolver" "$base" "$arch" "$primary_gz" "$@")" || rc=1
  rm -f "$resolver" "$primary_gz"
  [[ $rc -eq 0 ]] || { warn "amazon2023 repo metadata parse failed"; return 1; }

  while IFS=$'\t' read -r name url; do
    [[ -n "$name" ]] || continue
    fetch "$url" "${dest}/$(basename "$url")" || warn "failed to download ${name}"
  done <<< "$resolved"
}

if [[ "$SKIP_OS_PACKAGES" == "true" ]]; then
  log "skipping OS packages (--skip-os-packages)"
elif [[ "$TARGET_OS" == "amazon2023" ]]; then
  PKG_NAMES="$AL2023_PKG_NAMES"
  [[ "$SKIP_DOCKER_ENGINE" == "true" ]] && PKG_NAMES="tmux screen procps-ng"
  log "downloading amazon2023 OS packages directly from the AL2023 repo (no dnf needed)"
  # shellcheck disable=SC2086
  fetch_al2023_rpms "$ARCH" "${PKGDIR}/os-packages" $PKG_NAMES \
    || warn "amazon2023 direct package download failed; stage packages manually (see NEEDED-OS-PACKAGES.txt)"
  if ! compgen -G "${PKGDIR}/os-packages/*.rpm" > /dev/null; then
    cat > "${PKGDIR}/NEEDED-OS-PACKAGES.txt" <<EOF
Direct download from the amazon2023 repo failed (no internet on this bastion,
or the repo endpoint changed). Install these on the migration host instead:

  dnf install -y tmux screen procps-ng docker containerd runc

curl is deliberately not listed: amazon2023 ships curl-minimal by default,
which conflicts with the full curl package. curl-minimal already covers
everything this toolkit needs curl for.
EOF
  fi
elif [[ "$OS_MATCH" != "true" ]]; then
  warn "bastion OS '${BASTION_OS}' != target OS '${TARGET_OS}'."
  warn "Skipping tmux/screen/procps/curl/docker download: dependency resolution would be wrong."
  case "$TARGET_OS" in
    ubuntu2204) PKG_CMD="apt-get install -y tmux screen procps curl docker.io containerd runc" ;;
    amazon2)    PKG_CMD="amazon-linux-extras install -y docker && yum install -y tmux screen procps-ng curl" ;;
    # rhel9/amazon2023 ship curl-minimal by default, which hard-conflicts with
    # the full curl package — curl-minimal already satisfies our plain-HTTPS
    # needs, so it is deliberately left alone rather than swapped.
    *)          PKG_CMD="dnf install -y tmux screen procps-ng docker containerd runc" ;;
  esac
  cat > "${PKGDIR}/NEEDED-OS-PACKAGES.txt" <<EOF
These OS packages are NOT in this bundle and must be installed on the migration
hosts from their internal repo / Satellite / OS media. They were skipped because
the bastion that built this bundle ran '${BASTION_OS}', not '${TARGET_OS}',
so dependency resolution would have produced the wrong packages.

  tmux (or screen)         persistent session for long-running workers/runner
  procps-ng/procps         provides watch(1), used by the progress monitor loop
  curl                     dsynct progress API calls (/progress) — already
                           present as curl-minimal on rhel9/amazon2023; do not
                           install full curl there, it conflicts with it
  docker, containerd, runc container engine — required to run the
                           Temporal + dsynct multi-worker topology

On the migration host (if it has repo access):
  ${PKG_CMD}

Re-run fetch-docdb-migration-toolkit.sh on a ${TARGET_OS} bastion to stage
these properly, ideally a real EC2 instance of the target AMI family (not a
container) so dependency resolution sees the same base packages the
migration host already has.
EOF
else
  # rhel9/amazon2023 ship curl-minimal by default, which hard-conflicts with
  # the full curl package (both provide /usr/bin/curl) — dnf refuses to
  # install the two together. curl-minimal already covers everything this
  # toolkit needs curl for (plain HTTPS GETs), so it is left alone there.
  case "$TARGET_OS" in
    rhel9|amazon2023) CURL_PKG="" ;;
    *)                CURL_PKG="curl" ;;
  esac

  if [[ "$SKIP_DOCKER_ENGINE" == "true" ]]; then
    PKG_NAMES="tmux screen procps-ng ${CURL_PKG}"
    log "downloading OS packages (tmux, screen, procps-ng/procps${CURL_PKG:+, curl})"
    log "skipping Docker engine (--skip-docker-engine): target host already has docker/podman"
  else
    PKG_NAMES="tmux screen procps-ng ${CURL_PKG} docker containerd runc"
    log "downloading OS packages (tmux, screen, procps-ng/procps${CURL_PKG:+, curl}, docker engine)"
    [[ "$TARGET_OS" == "amazon2" ]] && \
      amazon-linux-extras enable docker >/dev/null 2>&1 \
      || warn "could not enable the amazon-linux-extras docker topic"
  fi

  # Staged into an isolated temp dir and filtered before landing in
  # os-packages/, so a base-OS package pulled in by --alldeps never survives
  # into the bundle regardless of which tool triggered its resolution.
  RPM_STAGE="$(mktemp -d)"
  case "$TARGET_OS" in
    rhel9)
      if command -v dnf >/dev/null 2>&1; then
        dnf -y install dnf-plugins-core >/dev/null 2>&1 || true
        # shellcheck disable=SC2086
        dnf download --resolve --alldeps --downloaddir "$RPM_STAGE" $PKG_NAMES \
          || warn "dnf download failed (needs dnf-plugins-core); stage packages manually"
      else
        warn "dnf not found; cannot stage RPMs"
      fi
      ;;
    amazon2)
      yum clean metadata >/dev/null 2>&1 || true
      if command -v yumdownloader >/dev/null 2>&1; then
        # shellcheck disable=SC2086
        yumdownloader --resolve --destdir "$RPM_STAGE" $PKG_NAMES \
          || warn "yumdownloader failed (needs yum-utils); stage packages manually"
      else
        warn "yumdownloader not found; cannot stage RPMs"
      fi
      ;;
    ubuntu2204)
      # apt's own dependency resolution already excludes always-present
      # essential packages, so no allowlist filtering is applied here.
      ( cd "$RPM_STAGE" && \
        apt-get install --reinstall --print-uris -qq -y $PKG_NAMES \
          | cut -d"'" -f2 | grep -E '^https?://' > .uris ) \
        || warn "apt-get --print-uris failed; stage packages manually"
      if [[ -s "${RPM_STAGE}/.uris" ]]; then
        while read -r u; do fetch "$u" "${RPM_STAGE}/$(basename "$u")"; done < "${RPM_STAGE}/.uris"
        rm -f "${RPM_STAGE}/.uris"
      fi
      ;;
  esac
  filter_rpm_stage "$RPM_STAGE"
  mv "$RPM_STAGE"/*.rpm "${PKGDIR}/os-packages/" 2>/dev/null || true
  mv "$RPM_STAGE"/*.deb "${PKGDIR}/os-packages/" 2>/dev/null || true
  rm -rf "$RPM_STAGE"
fi

# ------------------------------------------------------------- manifest ------
# Best effort: a foreign-arch image only runs here when binfmt/qemu is set up.
DSYNCT_VERSION="unknown"
if [[ "$SKIP_IMAGES" != "true" ]]; then
  DSYNCT_VERSION="$(docker run --rm --platform "$DOCKER_PLATFORM" "$DSYNCT_TAG" --version 2>/dev/null \
    | awk '/version/ {print $NF; exit}')" || true
  DSYNCT_VERSION="${DSYNCT_VERSION:-unknown}"
fi

cat > "${PKGDIR}/manifest.env" <<EOF
# Generated by fetch-docdb-migration-toolkit.sh on $(hostname) at ${STAMP}
BUNDLE_NAME="${BUNDLE_NAME}"
TARGET_OS="${TARGET_OS}"
ARCH="${ARCH}"
GOARCH="${GOARCH}"
DSYNCT_MODE="${DSYNCT_MODE}"
DSYNCT_SOURCE="${DSYNCT_SOURCE}"
DSYNCT_IMAGE="${DSYNCT_IMAGE}"
DSYNCT_IMAGE_DIGEST="${DSYNCT_IMAGE_DIGEST}"
DSYNCT_VERSION="${DSYNCT_VERSION}"
DSYNCT_TAG="${DSYNCT_TAG}"
DSYNCT_IMAGE_TAR="${DSYNCT_IMAGE_TAR}"
TEMPORAL_IMAGE="${TEMPORAL_IMAGE}"
TEMPORAL_IMAGE_TAR="${TEMPORAL_IMAGE_TAR}"
MONGOSH_VERSION="${MONGOSH_VERSION}"
MONGOSH_TGZ="${MONGOSH_TGZ}"
JQ_VERSION="${JQ_VERSION}"
JQ_BIN="${JQ_BIN}"
DOCKER_COMPOSE_VERSION="${DOCKER_COMPOSE_VERSION}"
DOCKER_COMPOSE_BIN="${DOCKER_COMPOSE_BIN}"
HELPER_TOOLS="fixIdTypes migrateIndexes checkChangeStreams copyMissingDocs fullCountVerify.sh"
CDC_HELPERS="cs-rate-profile.js split-hot-cold.sh dsync-progress.sh dsync-find-pauses.sh"
EOF

log "generating SHA256 checksums"
# sha256sum on Linux, shasum -a 256 on macOS; both emit "<hash>  <path>".
if command -v sha256sum >/dev/null 2>&1; then SUM=(sha256sum)
elif command -v shasum  >/dev/null 2>&1; then SUM=(shasum -a 256)
else die "no sha256sum or shasum available to checksum the bundle"
fi
( cd "${PKGDIR}" && find payload os-packages compose -type f | LC_ALL=C sort \
    | tr '\n' '\0' | xargs -0 "${SUM[@]}" > SHA256SUMS )

cp "$0" "${PKGDIR}/fetch-docdb-migration-toolkit.sh" 2>/dev/null || true
INSTALLER="${SCRIPT_DIR}/install-docdb-migration-toolkit.sh"
if [[ -f "$INSTALLER" ]]; then
  cp "$INSTALLER" "${PKGDIR}/install-docdb-migration-toolkit.sh"
  chmod +x "${PKGDIR}/install-docdb-migration-toolkit.sh"
else
  warn "install-docdb-migration-toolkit.sh not found next to this script; copy it to the host separately"
fi

BUNDLE="${OUTDIR}/${BUNDLE_NAME}.tar.gz"
log "packing ${BUNDLE}"
tar -C "$STAGE" -czf "$BUNDLE" "$BUNDLE_NAME"
( cd "$OUTDIR" && "${SUM[@]}" "$(basename "$BUNDLE")" > "${BUNDLE}.sha256" )

if [[ "$DSYNCT_MODE" == "binary" ]]; then
  DSYNCT_NOTE="This bundle contains a LICENSED Enterprise dsynct binary. Treat it as
confidential — do not publish it or commit it to a shared repository."
else
  DSYNCT_NOTE="dsynct came from ${DSYNCT_IMAGE} (${DSYNCT_IMAGE_DIGEST}), which Docker Hub
labels \"trial\"; its LICENSE.md (payload/dsynct/LICENSE.md) is an evaluation
licence. Confirm production licensing with Adiom before migrating with it, and
do not publish this bundle."
fi

cat <<EOF

Bundle ready:
  ${BUNDLE}
  ${BUNDLE}.sha256   ($(cut -d' ' -f1 < "${BUNDLE}.sha256"))
  size: $(du -h "$BUNDLE" | cut -f1)

${DSYNCT_NOTE}

Transfer to the migration host, then:
  sha256sum -c $(basename "${BUNDLE}").sha256
  tar xzf $(basename "$BUNDLE")
  sudo ./${BUNDLE_NAME}/install-docdb-migration-toolkit.sh
EOF
