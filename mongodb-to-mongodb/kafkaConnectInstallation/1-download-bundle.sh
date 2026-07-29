#!/usr/bin/env bash
#==============================================================================
# 1-download-bundle.sh
#
# STEP 1 OF 2 — run this on a machine that HAS internet access.
#
# Downloads everything needed to install Kafka Connect and the MongoDB Kafka
# Connector (source + sink) on a server with NO internet access, verifies every
# artifact, and produces a single tarball to transfer.
#
#   * No sudo / root required.
#   * Nothing is installed on this machine. Files are only downloaded here.
#   * Latest stable versions are resolved automatically at runtime.
#
# USAGE
#   ./1-download-bundle.sh [--out DIR] [--arch x86_64|aarch64] [--jdk-max N]
#                          [--kafka X.Y.Z] [--connector X.Y.Z] [--no-mongosh]
#
#   --arch is the architecture of the OFFLINE TARGET SERVER (default x86_64),
#          not of this machine. Check with `uname -m` on the target.
#
# EXAMPLES
#   ./1-download-bundle.sh
#   ./1-download-bundle.sh --out /tmp/staging --kafka 4.2.1
#   ./1-download-bundle.sh --arch aarch64        # target is ARM
#
# OUTPUT
#   <out>/kafka-connect-offline-bundle-<timestamp>.tar.gz
#   <out>/kafka-connect-offline-bundle-<timestamp>.tar.gz.sha256
#
# Transfer BOTH files to the offline server, then run 2-install-offline.sh there.
#==============================================================================
set -uo pipefail

OUT_DIR="$PWD"
KAFKA_VERSION=""          # empty = resolve latest
CONNECTOR_VERSION=""      # empty = resolve latest
SCALA_VERSION="2.13"
JDK_MAX="21"              # see note in resolve_jdk()
WANT_MONGOSH="yes"
# Architecture of the OFFLINE TARGET SERVER, not of this machine. Kafka and the
# connector are pure JVM artifacts, but the JDK and mongosh are native binaries.
TARGET_ARCH="x86_64"

while [ $# -gt 0 ]; do
  case "$1" in
    --arch)        TARGET_ARCH="$2"; shift 2 ;;
    --out)         OUT_DIR="$2"; shift 2 ;;
    --kafka)       KAFKA_VERSION="$2"; shift 2 ;;
    --connector)   CONNECTOR_VERSION="$2"; shift 2 ;;
    --scala)       SCALA_VERSION="$2"; shift 2 ;;
    --jdk-max)     JDK_MAX="$2"; shift 2 ;;
    --no-mongosh)  WANT_MONGOSH="no"; shift ;;
    -h|--help)     sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1 (try --help)"; exit 2 ;;
  esac
done

#------------------------------------------------------------------ output ----
# Map the target architecture to each project's naming convention.
case "$TARGET_ARCH" in
  x86_64|amd64|x64)  TARGET_ARCH="x86_64"; JDK_ARCH="x64";     MONGOSH_ARCH="x64" ;;
  aarch64|arm64)     TARGET_ARCH="aarch64"; JDK_ARCH="aarch64"; MONGOSH_ARCH="arm64" ;;
  *) echo "Unsupported --arch '$TARGET_ARCH' (use x86_64 or aarch64)"; exit 2 ;;
esac

C_G=$'\033[32m'; C_R=$'\033[31m'; C_Y=$'\033[33m'; C_C=$'\033[36m'; C_0=$'\033[0m'
ok()   { printf '  %s[ ok ]%s %s\n' "$C_G" "$C_0" "$*"; }
warn() { printf '  %s[warn]%s %s\n' "$C_Y" "$C_0" "$*"; }
err()  { printf '  %s[FAIL]%s %s\n' "$C_R" "$C_0" "$*"; }
step() { printf '\n%s==> %s%s\n' "$C_C" "$*" "$C_0"; }
die()  { err "$*"; echo; echo "Aborted. Nothing was installed on this machine."; exit 1; }

#------------------------------------------------------------ prerequisites ---
step "Checking prerequisites on this (internet-connected) machine"

DL=""
if command -v curl >/dev/null 2>&1; then DL=curl
elif command -v wget >/dev/null 2>&1; then DL=wget
else die "Neither curl nor wget is available. One of them is required."
fi
ok "downloader: $DL"

command -v tar >/dev/null 2>&1 || die "tar is required."
ok "tar present"

PY=""
for c in python3 python; do command -v $c >/dev/null 2>&1 && { PY=$c; break; }; done
[ -n "$PY" ] || warn "python3 not found — JAR inspection and version parsing will be limited"

# sha256: coreutils, BSD, or python fallback. No sudo, so we adapt.
if command -v sha256sum >/dev/null 2>&1; then
  _sha256() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  _sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
elif [ -n "$PY" ]; then
  _sha256() { $PY - "$1" <<'EOF'
import hashlib,sys
h=hashlib.sha256()
with open(sys.argv[1],'rb') as f:
    for b in iter(lambda: f.read(1<<20), b''): h.update(b)
print(h.hexdigest())
EOF
}
else die "No sha256 tool available (need sha256sum, shasum, or python3)."
fi
ok "sha256 tool available"

fetch() { # fetch <url> <dest>
  if [ "$DL" = curl ]; then curl -fsSL --retry 3 --connect-timeout 20 -o "$2" "$1"
  else wget -q -T 20 -t 3 -O "$2" "$1"; fi
}
fetch_stdout() {
  if [ "$DL" = curl ]; then curl -fsSL --retry 3 --connect-timeout 20 "$1"
  else wget -q -T 20 -t 3 -O - "$1"; fi
}

fetch_stdout https://repo1.maven.org/maven2/ >/dev/null 2>&1 \
  || die "No internet access from this machine (cannot reach Maven Central). Run this script on the machine that HAS internet."
ok "internet reachable"

#------------------------------------------------------- version resolution ---
step "Resolving latest stable versions"

# --- Kafka: newest release directory on the Apache CDN -----------------------
if [ -z "$KAFKA_VERSION" ]; then
  KAFKA_VERSION="$(fetch_stdout https://dlcdn.apache.org/kafka/ 2>/dev/null \
    | grep -oE '"[0-9]+\.[0-9]+\.[0-9]+/"' | tr -d '"/' | sort -V | tail -1)"
  [ -n "$KAFKA_VERSION" ] || { KAFKA_VERSION="4.3.1"; warn "could not resolve latest Kafka; falling back to $KAFKA_VERSION"; }
fi
ok "Kafka            $KAFKA_VERSION (scala $SCALA_VERSION)"

# --- Connector: <release> from Maven metadata -------------------------------
if [ -z "$CONNECTOR_VERSION" ]; then
  CONNECTOR_VERSION="$(fetch_stdout https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/maven-metadata.xml 2>/dev/null \
    | grep -oE '<release>[^<]+' | head -1 | cut -d'>' -f2)"
  [ -n "$CONNECTOR_VERSION" ] || { CONNECTOR_VERSION="3.0.1"; warn "could not resolve latest connector; falling back to $CONNECTOR_VERSION"; }
fi
ok "MongoDB connector $CONNECTOR_VERSION (source + sink in one artifact)"

# --- JDK ---------------------------------------------------------------------
# NOTE ON "LATEST": Adoptium may offer a newer LTS (e.g. 25) than Apache Kafka
# documents support for. Kafka 4.x documents Java 17 and 21. We therefore take
# the newest LTS at or below --jdk-max (default 21) rather than the absolute
# newest. Raise it with --jdk-max 25 if you have validated that combination.
resolve_jdk() {
  local json lts
  json="$(fetch_stdout https://api.adoptium.net/v3/info/available_releases 2>/dev/null)"
  if [ -n "$json" ] && [ -n "$PY" ]; then
    lts="$(printf '%s' "$json" | $PY -c "
import json,sys
d=json.load(sys.stdin)
cap=int('$JDK_MAX')
c=[v for v in d.get('available_lts_releases',[]) if v<=cap]
print(max(c) if c else '')" 2>/dev/null)"
  fi
  [ -n "${lts:-}" ] && echo "$lts" || echo "$JDK_MAX"
}
JDK_MAJOR="$(resolve_jdk)"
ok "Temurin JDK       $JDK_MAJOR (newest LTS <= $JDK_MAX; Kafka 4.x documents 17/21)"

# --- mongosh (optional, for verification and day-2 ops) ---------------------
MONGOSH_VERSION=""
if [ "$WANT_MONGOSH" = yes ]; then
  MONGOSH_VERSION="$(fetch_stdout https://api.github.com/repos/mongodb-js/mongosh/releases/latest 2>/dev/null \
    | grep -oE '"tag_name": *"v[^"]+"' | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')"
  [ -n "$MONGOSH_VERSION" ] || { MONGOSH_VERSION="2.9.2"; warn "could not resolve latest mongosh; falling back to $MONGOSH_VERSION"; }
  ok "mongosh           $MONGOSH_VERSION"
fi

#--------------------------------------------------------------- downloads ----
STAMP="$(date -u +%Y%m%d-%H%M%S)"
BUNDLE_NAME="kafka-connect-offline-bundle-${STAMP}"
WORK="${OUT_DIR}/${BUNDLE_NAME}"
mkdir -p "$WORK"/{kafka,connector,java,tools,meta} || die "cannot write to $OUT_DIR"

step "Downloading artifacts into $WORK"

KFILE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
# Prefer the Apache CDN. archive.apache.org is the archival host, throttled to
# roughly 200 KB/s, which turns a 144 MB download into a ~15 minute wait. The
# CDN serves the same file at ~16 MB/s but only keeps current releases, so fall
# back to the archive when a requested version is no longer there.
KBASE="https://dlcdn.apache.org/kafka/${KAFKA_VERSION}"
if [ "$DL" = curl ]; then
  curl -fsS -m 10 -o /dev/null -I "${KBASE}/${KFILE}" 2>/dev/null || KBASE="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}"
else
  wget -q --spider -T 10 "${KBASE}/${KFILE}" 2>/dev/null || KBASE="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}"
fi
case "$KBASE" in
  *dlcdn*)   ok "Kafka mirror: dlcdn.apache.org (CDN)" ;;
  *archive*) warn "Kafka $KAFKA_VERSION not on the CDN; using archive.apache.org — expect a slow download" ;;
esac
fetch "${KBASE}/${KFILE}"        "$WORK/kafka/${KFILE}"        || die "download failed: ${KFILE}"
fetch "${KBASE}/${KFILE}.sha512" "$WORK/kafka/${KFILE}.sha512" || die "download failed: ${KFILE}.sha512"
ok "Kafka ${KAFKA_VERSION}  ($(du -h "$WORK/kafka/${KFILE}" | cut -f1))"

# GPG material. The .asc is worthless without the KEYS file, which the offline
# server cannot fetch from a keyserver — so stage both.
fetch "${KBASE}/${KFILE}.asc" "$WORK/kafka/${KFILE}.asc" 2>/dev/null \
  && fetch "https://downloads.apache.org/kafka/KEYS" "$WORK/kafka/KEYS" 2>/dev/null \
  && ok "Kafka signature + Apache KEYS (for optional offline gpg --verify)" \
  || warn "signature material unavailable; checksum verification only"

# The connector is ONE artifact containing BOTH the source and sink connectors.
# The "-all" (shaded) build bundles the MongoDB Java driver. The plain JAR does
# not, and fails at runtime with NoClassDefFoundError. Always take "-all".
CJAR="mongo-kafka-connect-${CONNECTOR_VERSION}-all.jar"
CBASE="https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/${CONNECTOR_VERSION}"
fetch "${CBASE}/${CJAR}" "$WORK/connector/${CJAR}" || die "download failed: ${CJAR}"
fetch "${CBASE}/${CJAR}.sha1" "$WORK/connector/${CJAR}.sha1" 2>/dev/null
ok "MongoDB connector ${CONNECTOR_VERSION}  ($(du -h "$WORK/connector/${CJAR}" | cut -f1))"

JDK_TGZ="temurin-${JDK_MAJOR}-linux-${JDK_ARCH}.tar.gz"
fetch "https://api.adoptium.net/v3/binary/latest/${JDK_MAJOR}/ga/linux/${JDK_ARCH}/jdk/hotspot/normal/eclipse" \
      "$WORK/java/${JDK_TGZ}" || die "download failed: Temurin JDK ${JDK_MAJOR}"
ok "Temurin JDK ${JDK_MAJOR}  ($(du -h "$WORK/java/${JDK_TGZ}" | cut -f1))"

if [ "$WANT_MONGOSH" = yes ]; then
  SHFILE="mongosh-${MONGOSH_VERSION}-linux-${MONGOSH_ARCH}.tgz"
  fetch "https://downloads.mongodb.com/compass/${SHFILE}" "$WORK/tools/${SHFILE}" \
    && ok "mongosh ${MONGOSH_VERSION}  ($(du -h "$WORK/tools/${SHFILE}" | cut -f1))" \
    || warn "mongosh download failed — the offline server will have no MongoDB shell"
fi

#------------------------------------------------------------ verification ----
step "Verifying downloads"

# Kafka sha512 against the upstream checksum file.
verify_kafka_sha512() {
  local expected actual
  expected="$(tr -d ' \n' < "$WORK/kafka/${KFILE}.sha512" | sed 's/.*://' | tr 'A-Z' 'a-z')"
  if command -v sha512sum >/dev/null 2>&1; then actual="$(sha512sum "$WORK/kafka/${KFILE}" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1;  then actual="$(shasum -a 512 "$WORK/kafka/${KFILE}" | awk '{print $1}')"
  else return 2; fi
  [ "$expected" = "$actual" ]
}
verify_kafka_sha512
case $? in
  0) ok "Kafka tarball sha512 matches upstream" ;;
  2) warn "no sha512 tool available; skipped Kafka checksum verification" ;;
  *) die "Kafka tarball sha512 MISMATCH — do not use this bundle" ;;
esac

if [ -f "$WORK/connector/${CJAR}.sha1" ]; then
  exp="$(tr -d ' \n' < "$WORK/connector/${CJAR}.sha1")"
  if command -v sha1sum >/dev/null 2>&1; then act="$(sha1sum "$WORK/connector/${CJAR}" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then act="$(shasum -a 1 "$WORK/connector/${CJAR}" | awk '{print $1}')"
  else act=""; fi
  if [ -n "$act" ]; then
    [ "$exp" = "$act" ] && ok "connector JAR sha1 matches Maven Central" \
                        || die "connector JAR sha1 MISMATCH — do not use this bundle"
  fi
fi

# Inspect the connector JAR now, while we can still re-download. These two
# properties cannot be fixed on the offline server.
SL_MANIFESTS="unknown"
if [ -n "$PY" ]; then
  $PY - "$WORK/connector/${CJAR}" > "$WORK/meta/connector-jar-report.txt" <<'EOF'
import zipfile,sys,struct
p=sys.argv[1]; z=zipfile.ZipFile(p); names=z.namelist()
drv=[n for n in names if n.startswith('com/mongodb/client/')]
src=[n for n in names if n.endswith('connect/MongoSourceConnector.class')]
snk=[n for n in names if n.endswith('connect/MongoSinkConnector.class')]
sl=[n for n in names if n.startswith('META-INF/services/org.apache.kafka.connect')]
print("driver_classes=%d" % len(drv))
print("has_source_connector=%s" % bool(src))
print("has_sink_connector=%s" % bool(snk))
print("connect_serviceloader_manifests=%d" % len(sl))
for n in sl: print("  manifest: %s" % n)
try:
    b=z.read('com/mongodb/kafka/connect/MongoSourceConnector.class')[:8]
    print("class_major_version=%d" % struct.unpack('>H', b[6:8])[0])
except Exception as e:
    print("class_major_version=unknown")
EOF
  R="$WORK/meta/connector-jar-report.txt"
  grep -q 'has_source_connector=True' "$R" && ok "JAR contains MongoSourceConnector" || die "MongoSourceConnector missing from JAR"
  grep -q 'has_sink_connector=True'   "$R" && ok "JAR contains MongoSinkConnector"   || die "MongoSinkConnector missing from JAR"
  DRV="$(grep -oE 'driver_classes=[0-9]+' "$R" | cut -d= -f2)"
  if [ "${DRV:-0}" -gt 0 ]; then ok "MongoDB driver is bundled in the JAR ($DRV classes)"
  else die "MongoDB driver NOT bundled — wrong artifact. Expected the '-all' JAR."; fi
  SL_MANIFESTS="$(grep -oE 'connect_serviceloader_manifests=[0-9]+' "$R" | cut -d= -f2)"
  if [ "${SL_MANIFESTS:-0}" -eq 0 ]; then
    warn "JAR has no Kafka Connect ServiceLoader manifests"
    echo "         -> the installer will set plugin.discovery=hybrid_warn (required)"
  else
    ok "JAR ships $SL_MANIFESTS Connect ServiceLoader manifest(s)"
  fi
  CMV="$(grep -oE 'class_major_version=[0-9]+' "$R" | cut -d= -f2)"
  [ -n "${CMV:-}" ] && ok "connector bytecode level: major $CMV (52=Java8, 61=Java17, 65=Java21)"
fi

# Which log4j generation did this Kafka ship? Determines the JVM flag used by
# the installer. Kafka moved from log4j1 to log4j2 in the 3.9/4.x timeframe.
LOG4J_FLAVOUR="unknown"
if tar tzf "$WORK/kafka/${KFILE}" 2>/dev/null | grep -q 'config/connect-log4j2.yaml'; then
  LOG4J_FLAVOUR="log4j2"; ok "Kafka ${KAFKA_VERSION} uses log4j2 (connect-log4j2.yaml)"
elif tar tzf "$WORK/kafka/${KFILE}" 2>/dev/null | grep -q 'config/connect-log4j.properties'; then
  LOG4J_FLAVOUR="log4j1"; ok "Kafka ${KAFKA_VERSION} uses log4j1 (connect-log4j.properties)"
else
  warn "could not determine log4j generation; installer will detect it on the target"
fi

#-------------------------------------------------------- versions + manifest --
cat > "$WORK/meta/versions.env" <<EOF
# Generated by 1-download-bundle.sh — consumed by 2-install-offline.sh
KAFKA_VERSION=${KAFKA_VERSION}
SCALA_VERSION=${SCALA_VERSION}
KAFKA_TARBALL=${KFILE}
CONNECTOR_VERSION=${CONNECTOR_VERSION}
CONNECTOR_JAR=${CJAR}
CONNECTOR_SERVICELOADER_MANIFESTS=${SL_MANIFESTS}
JDK_MAJOR=${JDK_MAJOR}
JDK_TARBALL=${JDK_TGZ}
MONGOSH_VERSION=${MONGOSH_VERSION}
LOG4J_FLAVOUR=${LOG4J_FLAVOUR}
BUNDLED_AT=$(date -u +%FT%TZ)
BUNDLED_ON=$(uname -s)-$(uname -m)
BUNDLE_ARCH=${TARGET_ARCH}
EOF

{
  echo "Offline bundle for Kafka Connect + MongoDB Kafka Connector"
  echo "Created (UTC):     $(date -u +%FT%TZ)"
  echo "Created on:        $(uname -srm)"
  echo
  echo "Kafka:             ${KFILE}   (sha512 verified against upstream)"
  echo "Connector:         ${CJAR}    (source + sink, MongoDB driver bundled)"
  echo "JDK:               ${JDK_TGZ}"
  [ -n "$MONGOSH_VERSION" ] && echo "mongosh:           mongosh-${MONGOSH_VERSION}-linux-${MONGOSH_ARCH}.tgz"
  echo "log4j generation:  ${LOG4J_FLAVOUR}"
  echo "SL manifests:      ${SL_MANIFESTS} (0 => plugin.discovery must be hybrid_warn)"
  echo
  echo "Source URLs:"
  echo "  ${KBASE}/${KFILE}"
  echo "  ${CBASE}/${CJAR}"
  echo "  https://api.adoptium.net/v3/binary/latest/${JDK_MAJOR}/ga/linux/${JDK_ARCH}/jdk/hotspot/normal/eclipse"
  echo
  echo "sha256 of every artifact in this bundle:"
} > "$WORK/meta/MANIFEST.txt"

( cd "$WORK" && for f in $(find kafka connector java tools -type f | sort); do
    printf '%s  %s\n' "$(_sha256 "$f")" "$f"
  done ) >> "$WORK/meta/MANIFEST.txt"

#------------------------------------------------------------------ package ---
step "Packaging"
TARBALL="${OUT_DIR}/${BUNDLE_NAME}.tar.gz"
( cd "$OUT_DIR" && tar czf "${BUNDLE_NAME}.tar.gz" "${BUNDLE_NAME}" ) || die "failed to create tarball"
_sha256 "$TARBALL" > "${TARBALL}.sha256"
rm -rf "$WORK"
ok "bundle:   $TARBALL  ($(du -h "$TARBALL" | cut -f1))"
ok "checksum: ${TARBALL}.sha256"

cat <<EOF

${C_G}=============================================================================${C_0}
 Bundle ready.

 1. Transfer BOTH of these to the offline server (USB, scp via bastion,
    internal artifact repo — whatever your process allows):

      $(basename "$TARBALL")
      $(basename "$TARBALL").sha256

    Keep the .sha256 separate from the tarball where you can, so a corrupted
    or altered transfer is detectable.

 2. On the offline server, run step 2:

      ./2-install-offline.sh --bundle $(basename "$TARBALL") \\
                             --bootstrap <broker-host>:9092

 Neither step needs sudo or root.
${C_G}=============================================================================${C_0}
EOF
