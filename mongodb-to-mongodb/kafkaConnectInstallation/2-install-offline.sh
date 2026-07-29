#!/usr/bin/env bash
#==============================================================================
# 2-install-offline.sh
#
# STEP 2 OF 2 — run this on the server with NO internet access.
#
# Installs, entirely from the transferred bundle and with NO sudo/root:
#   * Java (Eclipse Temurin JDK)
#   * Apache Kafka (which is what provides Kafka Connect)
#   * MongoDB Kafka Connector — source AND sink (one artifact)
#   * mongosh, if it was included in the bundle
#   * A ready-to-run Connect worker config plus start/stop/status/verify scripts
#
# Everything lands under a single directory you own (default: $HOME/kafka-connect).
# No system packages, no service account, no systemd, no firewall changes, and
# no privileged ports.
#
# USAGE
#   ./2-install-offline.sh --bundle <bundle.tar.gz> --bootstrap <host:9092> [options]
#
# REQUIRED
#   --bundle FILE          the tarball produced by 1-download-bundle.sh
#                          (or use --dir if you already extracted it)
#   --bootstrap HOST:PORT  your Kafka broker(s), comma-separated
#
# COMMON OPTIONS
#   --prefix DIR           install location            (default: $HOME/kafka-connect)
#   --rest-port PORT       Connect REST port           (default: 8083)
#   --group-id ID          Connect cluster group id    (default: connect-cluster-1)
#   --replication-factor N internal topic RF           (default: 3)
#   --mongo-uri URI        stored in a 0600 secrets file, referenced indirectly
#                          by connector configs so it never appears in the REST API
#   --heap SIZE            worker heap                 (default: 2G)
#   --client-config FILE   properties file with your broker security settings
#                          (security.protocol / sasl.* / ssl.*). REQUIRED if the
#                          brokers use SASL or TLS: the CLI tools need it via
#                          --command-config, and its contents are also appended
#                          to the worker config.
#   --create-topics        create the 3 internal topics (needs broker reachable)
#   --no-start             install and configure only; do not start the worker
#   --force                overwrite an existing install at --prefix
#
# EXAMPLE
#   ./2-install-offline.sh --bundle kafka-connect-offline-bundle-*.tar.gz \
#       --bootstrap broker1:9092,broker2:9092 \
#       --mongo-uri 'mongodb://user:pass@mongo1:27017/?replicaSet=rs0' \
#       --create-topics
#==============================================================================
set -uo pipefail

BUNDLE=""; SRC_DIR=""
PREFIX="$HOME/kafka-connect"
BOOTSTRAP=""
REST_PORT="8083"
GROUP_ID="connect-cluster-1"
REPL_FACTOR="3"
MONGO_URI=""
CLIENT_CONFIG=""
HEAP="2G"
DO_TOPICS="no"
DO_START="yes"
FORCE="no"

while [ $# -gt 0 ]; do
  case "$1" in
    --bundle)             BUNDLE="$2"; shift 2 ;;
    --dir)                SRC_DIR="$2"; shift 2 ;;
    --prefix)             PREFIX="$2"; shift 2 ;;
    --bootstrap)          BOOTSTRAP="$2"; shift 2 ;;
    --rest-port)          REST_PORT="$2"; shift 2 ;;
    --group-id)           GROUP_ID="$2"; shift 2 ;;
    --replication-factor) REPL_FACTOR="$2"; shift 2 ;;
    --mongo-uri)          MONGO_URI="$2"; shift 2 ;;
    --client-config)      CLIENT_CONFIG="$2"; shift 2 ;;
    --heap)               HEAP="$2"; shift 2 ;;
    --create-topics)      DO_TOPICS="yes"; shift ;;
    --no-start)           DO_START="no"; shift ;;
    --force)              FORCE="yes"; shift ;;
    -h|--help)            sed -n '2,50p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1 (try --help)"; exit 2 ;;
  esac
done

C_G=$'\033[32m'; C_R=$'\033[31m'; C_Y=$'\033[33m'; C_C=$'\033[36m'; C_0=$'\033[0m'
ok()   { printf '  %s[ ok ]%s %s\n' "$C_G" "$C_0" "$*"; }
warn() { printf '  %s[warn]%s %s\n' "$C_Y" "$C_0" "$*"; }
err()  { printf '  %s[FAIL]%s %s\n' "$C_R" "$C_0" "$*"; }
step() { printf '\n%s==> %s%s\n' "$C_C" "$*" "$C_0"; }
die()  { err "$*"; exit 1; }

[ -n "$BOOTSTRAP" ] || die "--bootstrap <host:port> is required (your Kafka broker). See --help."
[ -n "$BUNDLE" ] || [ -n "$SRC_DIR" ] || die "--bundle <file.tar.gz> (or --dir) is required. See --help."

if [ "$(id -u)" -eq 0 ]; then
  warn "running as root. This installer does not need root; files will be owned by root."
fi

#------------------------------------------------------------ prerequisites ---
step "Checking the target server"

command -v tar >/dev/null 2>&1 || die "tar is required and was not found."
ok "tar present"

PY=""
for c in python3 python; do command -v $c >/dev/null 2>&1 && { PY=$c; break; }; done
[ -n "$PY" ] && ok "python3 present (used for checksums and JSON output)" \
             || warn "python3 not found — some verification output will be reduced"

if command -v sha256sum >/dev/null 2>&1; then _sha256() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1;    then _sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
elif [ -n "$PY" ]; then _sha256() { $PY -c "
import hashlib,sys
h=hashlib.sha256()
f=open(sys.argv[1],'rb')
for b in iter(lambda: f.read(1<<20), b''): h.update(b)
print(h.hexdigest())" "$1"; }
else warn "no sha256 tool — integrity verification will be skipped"; _sha256() { echo skip; }
fi

# This must be an offline server. If it is not, say so, but do not block.
if command -v curl >/dev/null 2>&1 && curl -sS -m 6 -o /dev/null https://repo1.maven.org/maven2/ 2>/dev/null; then
  warn "this server appears to HAVE internet access — that is fine, but this installer never uses it"
else
  ok "no outbound internet access detected (as expected)"
fi

# Disk: bundle + JDK + Kafka + logs.
AVAIL_MB=$(df -Pm "$(dirname "$PREFIX")" 2>/dev/null | awk 'NR==2{print $4}')
if [ -n "${AVAIL_MB:-}" ]; then
  [ "$AVAIL_MB" -ge 4096 ] && ok "free space at install location: ${AVAIL_MB} MB" \
                           || warn "only ${AVAIL_MB} MB free — 4096 MB or more recommended"
fi

#-------------------------------------------------------------- unpack bundle --
step "Unpacking bundle"

TMP=""
cleanup() { [ -n "$TMP" ] && rm -rf "$TMP"; }
trap cleanup EXIT

if [ -n "$BUNDLE" ]; then
  [ -f "$BUNDLE" ] || die "bundle not found: $BUNDLE"
  # Verify the transfer before trusting the contents.
  if [ -f "${BUNDLE}.sha256" ]; then
    EXP="$(awk '{print $1}' "${BUNDLE}.sha256")"
    ACT="$(_sha256 "$BUNDLE")"
    if [ "$ACT" = skip ]; then warn "cannot verify bundle checksum (no sha256 tool)"
    elif [ "$EXP" = "$ACT" ]; then ok "bundle sha256 matches — transfer intact"
    else die "bundle sha256 MISMATCH. Re-transfer the file; do not proceed."
    fi
  else
    warn "no ${BUNDLE}.sha256 alongside the bundle — transfer integrity unverified"
  fi
  TMP="$(mktemp -d)"
  tar xzf "$BUNDLE" -C "$TMP" || die "failed to extract $BUNDLE"
  SRC_DIR="$(find "$TMP" -maxdepth 1 -type d -name 'kafka-connect-offline-bundle-*' | head -1)"
  [ -n "$SRC_DIR" ] || SRC_DIR="$TMP"
fi

[ -f "$SRC_DIR/meta/versions.env" ] || die "not a valid bundle: $SRC_DIR/meta/versions.env missing"
# shellcheck disable=SC1090
. "$SRC_DIR/meta/versions.env"
ok "bundle created $BUNDLED_AT"
ok "Kafka $KAFKA_VERSION | connector $CONNECTOR_VERSION | JDK $JDK_MAJOR | log4j: $LOG4J_FLAVOUR"

# Architecture check before we extract a JDK that cannot run here. Kafka and the
# connector are pure JVM artifacts; only the JDK and mongosh are native.
HOST_ARCH="$(uname -m)"
case "${BUNDLE_ARCH:-x86_64}:$HOST_ARCH" in
  x86_64:x86_64|x86_64:amd64|aarch64:aarch64|aarch64:arm64)
      ok "bundle architecture ${BUNDLE_ARCH:-x86_64} matches this server ($HOST_ARCH)" ;;
  *)
      err "architecture mismatch: bundle is for ${BUNDLE_ARCH:-x86_64}, this server is $HOST_ARCH"
      echo "  Re-run step 1 on the internet machine with: --arch $HOST_ARCH"
      exit 1 ;;
esac

# Re-verify every artifact against the checksums recorded at download time.
if [ -f "$SRC_DIR/meta/MANIFEST.txt" ] && [ "$(_sha256 /dev/null)" != skip ]; then
  BAD=0; CHECKED=0
  while read -r sum path; do
    case "$sum" in [0-9a-f][0-9a-f]*) ;; *) continue ;; esac
    [ -f "$SRC_DIR/$path" ] || { BAD=$((BAD+1)); continue; }
    CHECKED=$((CHECKED+1))
    [ "$(_sha256 "$SRC_DIR/$path")" = "$sum" ] || { err "checksum mismatch: $path"; BAD=$((BAD+1)); }
  done < <(grep -E '^[0-9a-f]{64}  ' "$SRC_DIR/meta/MANIFEST.txt")
  [ "$BAD" -eq 0 ] && ok "all $CHECKED artifacts match their download-time checksums" \
                   || die "$BAD artifact(s) failed verification — re-transfer the bundle"
fi

#---------------------------------------------------------------- install dirs --
step "Installing to $PREFIX"

if [ -e "$PREFIX" ] && [ "$FORCE" != yes ]; then
  # Allow re-running over an existing install only with --force, to avoid
  # silently clobbering a working configuration.
  if [ -f "$PREFIX/etc/connect-distributed.properties" ]; then
    die "an install already exists at $PREFIX. Re-run with --force to overwrite (your etc/ files will be backed up)."
  fi
fi

mkdir -p "$PREFIX"/{java,kafka,plugins,etc,logs,run,bin,examples} || die "cannot create $PREFIX"

# Back up any existing config rather than losing it.
if [ -f "$PREFIX/etc/connect-distributed.properties" ]; then
  BK="$PREFIX/etc/backup-$(date -u +%Y%m%d-%H%M%S)"
  mkdir -p "$BK" && cp "$PREFIX"/etc/*.properties "$PREFIX"/etc/*.yaml "$BK"/ 2>/dev/null
  ok "existing config backed up to $BK"
fi

# --- Java --------------------------------------------------------------------
rm -rf "$PREFIX/java"; mkdir -p "$PREFIX/java"
tar xzf "$SRC_DIR/java/$JDK_TARBALL" -C "$PREFIX/java" --strip-components=1 \
  || die "failed to extract the JDK"
JAVA_HOME="$PREFIX/java"
[ -x "$JAVA_HOME/bin/java" ] || die "JDK extracted but $JAVA_HOME/bin/java is not executable"

# Actually run it. A present-but-unrunnable binary means the bundle was built for
# a different CPU architecture, which is the most likely cause of failure here.
if ! JV="$("$JAVA_HOME/bin/java" -version 2>&1)"; then
  err "the bundled JDK cannot execute on this server:"
  printf '        %s\n' "$JV"
  echo
  echo "  This server is $(uname -m); the bundle was built for ${BUNDLE_ARCH:-x86_64}."
  echo "  Re-run step 1 on the internet machine with a matching architecture, e.g.:"
  echo "      ./1-download-bundle.sh --arch $(uname -m)"
  exit 1
fi
JV="$(printf '%s' "$JV" | head -1)"
ok "Java installed and runnable: $JV"

# Kafka 4.x requires Java 17 or newer. Fail loudly rather than at first start.
JMAJ="$("$JAVA_HOME/bin/java" -version 2>&1 | head -1 | grep -oE '"[0-9]+' | tr -d '"')"
if [ "${KAFKA_VERSION%%.*}" -ge 4 ] && [ "${JMAJ:-0}" -lt 17 ]; then
  die "Kafka $KAFKA_VERSION requires Java 17+, but the bundled JDK is Java ${JMAJ}."
fi
ok "JDK satisfies the Kafka $KAFKA_VERSION requirement (Java $JMAJ)"

# --- Kafka -------------------------------------------------------------------
rm -rf "$PREFIX/kafka"; mkdir -p "$PREFIX/kafka"
tar xzf "$SRC_DIR/kafka/$KAFKA_TARBALL" -C "$PREFIX/kafka" --strip-components=1 \
  || die "failed to extract Kafka"
KAFKA_HOME="$PREFIX/kafka"
[ -x "$KAFKA_HOME/bin/connect-distributed.sh" ] || die "connect-distributed.sh missing after extract"
ok "Kafka $KAFKA_VERSION installed (provides Kafka Connect)"

# --- Connector ---------------------------------------------------------------
# One plugin per subdirectory: Kafka Connect isolates plugins by directory, and
# loose JARs in a shared folder cause classloader conflicts.
PLUGIN_DIR="$PREFIX/plugins/mongodb-kafka-connect"
rm -rf "$PLUGIN_DIR"; mkdir -p "$PLUGIN_DIR"
cp "$SRC_DIR/connector/$CONNECTOR_JAR" "$PLUGIN_DIR/" || die "failed to copy the connector JAR"
ok "MongoDB connector $CONNECTOR_VERSION installed (source + sink) -> plugins/mongodb-kafka-connect/"

# --- mongosh (optional) ------------------------------------------------------
MONGOSH_BIN=""
if [ -n "${MONGOSH_VERSION:-}" ] && ls "$SRC_DIR"/tools/mongosh-*.tgz >/dev/null 2>&1; then
  mkdir -p "$PREFIX/tools/mongosh"
  tar xzf "$SRC_DIR"/tools/mongosh-*.tgz -C "$PREFIX/tools/mongosh" --strip-components=1 2>/dev/null
  if [ -x "$PREFIX/tools/mongosh/bin/mongosh" ]; then
    MONGOSH_BIN="$PREFIX/tools/mongosh/bin/mongosh"
    ln -sf "$MONGOSH_BIN" "$PREFIX/bin/mongosh"
    ok "mongosh $MONGOSH_VERSION installed -> $PREFIX/bin/mongosh"
  fi
fi

#--------------------------------------------------------------- logging setup --
step "Configuring logging"

# Kafka moved from log4j1 to log4j2 in the 3.9/4.x timeframe. The config file
# AND the JVM flag differ. Passing the wrong flag does not fail loudly — the
# worker starts and silently logs to the console only. Detect from what actually
# shipped rather than assuming.
if [ -f "$KAFKA_HOME/config/connect-log4j2.yaml" ]; then
  cp "$KAFKA_HOME/config/connect-log4j2.yaml" "$PREFIX/etc/connect-log4j2.yaml"
  LOG4J_OPTS="-Dlog4j2.configurationFile=$PREFIX/etc/connect-log4j2.yaml"
  ok "log4j2 detected -> $LOG4J_OPTS"
elif [ -f "$KAFKA_HOME/config/connect-log4j.properties" ]; then
  cp "$KAFKA_HOME/config/connect-log4j.properties" "$PREFIX/etc/connect-log4j.properties"
  LOG4J_OPTS="-Dlog4j.configuration=file:$PREFIX/etc/connect-log4j.properties"
  ok "log4j1 detected -> $LOG4J_OPTS"
else
  LOG4J_OPTS=""
  warn "no connect log4j template found; the worker will use its built-in defaults"
fi

#--------------------------------------------------------------------- secrets --
if [ -n "$MONGO_URI" ]; then
  umask 077
  printf 'mongo.uri=%s\n' "$MONGO_URI" > "$PREFIX/etc/secrets.properties"
  chmod 600 "$PREFIX/etc/secrets.properties"
  ok "MongoDB URI stored in etc/secrets.properties (mode 0600)"
else
  umask 077
  printf '# Fill this in, then reference it from connector configs as:\n#   ${file:%s/etc/secrets.properties:mongo.uri}\nmongo.uri=mongodb://USER:PASS@HOST:27017/?replicaSet=RS\n' "$PREFIX" > "$PREFIX/etc/secrets.properties"
  chmod 600 "$PREFIX/etc/secrets.properties"
  warn "no --mongo-uri given; template written to etc/secrets.properties (edit before deploying connectors)"
fi
umask 022

#-------------------------------------------------------------- worker config --
step "Writing Connect worker configuration"

# plugin.discovery: the MongoDB uber JAR may not ship Kafka Connect
# ServiceLoader manifests. Under plugin.discovery=service_load a plugin without
# manifests is invisible to the worker. hybrid_warn scans as well, which loads
# it correctly (with a harmless warning in the log). Set explicitly so a future
# Kafka upgrade that changes the default cannot make the connector disappear.
DISCOVERY="hybrid_warn"

cat > "$PREFIX/etc/connect-distributed.properties" <<EOF
# Kafka Connect distributed worker
# Generated by 2-install-offline.sh on $(date -u +%FT%TZ)

bootstrap.servers=${BOOTSTRAP}
group.id=${GROUP_ID}

# Built-in JSON converters: no additional artifacts required.
# Avro/Protobuf converters are NOT part of Apache Kafka. They need extra JARs
# and a reachable Schema Registry, so they cannot simply be enabled here.
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false

# Internal topics. All three MUST be cleanup.policy=compact.
offset.storage.topic=connect-offsets
offset.storage.replication.factor=${REPL_FACTOR}
offset.storage.partitions=25
config.storage.topic=connect-configs
config.storage.replication.factor=${REPL_FACTOR}
config.storage.partitions=1
status.storage.topic=connect-status
status.storage.replication.factor=${REPL_FACTOR}
status.storage.partitions=5
offset.flush.interval.ms=10000

listeners=http://0.0.0.0:${REST_PORT}
rest.advertised.host.name=$(hostname -f 2>/dev/null || hostname)

# Plugin isolation. Must not point inside kafka/libs.
plugin.path=${PREFIX}/plugins
plugin.discovery=${DISCOVERY}

# Lets connector configs use \${file:...} instead of inline credentials, so
# secrets are not readable back through the REST API.
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider

# --- Broker security: uncomment and complete to match your cluster ----------
#security.protocol=SASL_SSL
#sasl.mechanism=SCRAM-SHA-512
#sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="connect" password="CHANGEME";
#ssl.truststore.location=${PREFIX}/etc/truststore.jks
#ssl.truststore.password=CHANGEME
# Producer/consumer inherit the above; override per-client if needed:
#producer.security.protocol=SASL_SSL
#consumer.security.protocol=SASL_SSL
EOF
# Merge the broker security settings into the worker config, including the
# producer./consumer./admin. prefixed copies Connect needs for its own clients.
if [ -n "$CLIENT_CONFIG" ]; then
  {
    echo ""
    echo "# --- merged from --client-config $(basename "$CLIENT_CONFIG") ---"
    grep -vE '^\s*(#|$)' "$PREFIX/etc/client.properties"
    grep -vE '^\s*(#|$)' "$PREFIX/etc/client.properties" | sed 's/^/producer./'
    grep -vE '^\s*(#|$)' "$PREFIX/etc/client.properties" | sed 's/^/consumer./'
    grep -vE '^\s*(#|$)' "$PREFIX/etc/client.properties" | sed 's/^/admin./'
  } >> "$PREFIX/etc/connect-distributed.properties"
  ok "broker security settings merged into the worker config (plus producer/consumer/admin prefixes)"
fi
chmod 640 "$PREFIX/etc/connect-distributed.properties"
ok "etc/connect-distributed.properties written"
ok "plugin.discovery=${DISCOVERY} set explicitly (connector has ${CONNECTOR_SERVICELOADER_MANIFESTS:-unknown} ServiceLoader manifests)"

#---------------------------------------------------------- helper scripts -----
step "Generating management scripts"

cat > "$PREFIX/bin/env.sh" <<EOF
# Source this to get the installed toolchain on your PATH.
export JAVA_HOME="$JAVA_HOME"
export KAFKA_HOME="$KAFKA_HOME"
export CONNECT_HOME="$PREFIX"
export CONNECT_REST="http://localhost:${REST_PORT}"
export BOOTSTRAP_SERVERS="$BOOTSTRAP"
export PATH="\$JAVA_HOME/bin:\$KAFKA_HOME/bin:$PREFIX/bin:\$PATH"
export KAFKA_HEAP_OPTS="-Xms${HEAP} -Xmx${HEAP}"
export LOG_DIR="$PREFIX/logs"
export KAFKA_LOG4J_OPTS="${LOG4J_OPTS}"
EOF

cat > "$PREFIX/bin/start.sh" <<EOF
#!/usr/bin/env bash
# Start the Kafka Connect worker in the background (no root, no systemd).
set -uo pipefail
. "$PREFIX/bin/env.sh"
PIDFILE="$PREFIX/run/connect.pid"
if [ -f "\$PIDFILE" ] && kill -0 "\$(cat "\$PIDFILE")" 2>/dev/null; then
  echo "already running, pid \$(cat "\$PIDFILE")"; exit 0
fi
mkdir -p "$PREFIX/logs" "$PREFIX/run"
# RHEL 9 ships a soft nofile limit of 1024, which Kafka Connect can exhaust under
# load (many broker connections + producer/consumer clients). systemd's
# LimitNOFILE is not available to us without root, but a non-root process may
# raise its OWN soft limit up to the hard limit. Do that here.
HARD_NOFILE="$(ulimit -Hn 2>/dev/null || echo 0)"
case "$HARD_NOFILE" in
  unlimited) ulimit -n 100000 2>/dev/null ;;
  ''|0) : ;;
  *) if [ "$HARD_NOFILE" -gt 1024 ]; then
       [ "$HARD_NOFILE" -gt 100000 ] && ulimit -n 100000 2>/dev/null || ulimit -n "$HARD_NOFILE" 2>/dev/null
     fi ;;
esac
echo "open file limit: $(ulimit -n)"
nohup "\$KAFKA_HOME/bin/connect-distributed.sh" \\
      "$PREFIX/etc/connect-distributed.properties" \\
      > "$PREFIX/logs/connect-stdout.log" 2>&1 &
echo \$! > "\$PIDFILE"
echo "started, pid \$(cat "\$PIDFILE")"
echo "logs: $PREFIX/logs/"
EOF

cat > "$PREFIX/bin/stop.sh" <<EOF
#!/usr/bin/env bash
set -uo pipefail
PIDFILE="$PREFIX/run/connect.pid"
[ -f "\$PIDFILE" ] || { echo "not running (no pid file)"; exit 0; }
PID="\$(cat "\$PIDFILE")"
if kill -0 "\$PID" 2>/dev/null; then
  kill "\$PID"
  for _ in \$(seq 1 30); do kill -0 "\$PID" 2>/dev/null || break; sleep 1; done
  kill -0 "\$PID" 2>/dev/null && { echo "did not stop; sending SIGKILL"; kill -9 "\$PID"; }
  echo "stopped"
else
  echo "stale pid file"
fi
rm -f "\$PIDFILE"
EOF

cat > "$PREFIX/bin/status.sh" <<EOF
#!/usr/bin/env bash
set -uo pipefail
. "$PREFIX/bin/env.sh"
PIDFILE="$PREFIX/run/connect.pid"
if [ -f "\$PIDFILE" ] && kill -0 "\$(cat "\$PIDFILE")" 2>/dev/null; then
  echo "worker: RUNNING (pid \$(cat "\$PIDFILE"))"
else
  echo "worker: NOT running"; exit 1
fi
echo
echo "--- worker ---"
curl -sS "\$CONNECT_REST/" || echo "REST API not responding yet"
echo
echo "--- installed connector plugins (MongoDB) ---"
curl -sS "\$CONNECT_REST/connector-plugins" | tr ',' '\n' | grep -i mongo || echo "none found"
echo
echo "--- deployed connectors ---"
for c in \$(curl -sS "\$CONNECT_REST/connectors" | tr -d '[]"' | tr ',' ' '); do
  printf '%s: ' "\$c"
  curl -sS "\$CONNECT_REST/connectors/\$c/status" | tr ',' '\n' | grep -m2 '"state"' | tr -d ' \n'
  echo
done
EOF

cat > "$PREFIX/bin/create-internal-topics.sh" <<EOF
#!/usr/bin/env bash
# Pre-create the three Connect internal topics. Required when the brokers have
# auto topic creation disabled, which is normal in locked-down clusters.
# All three MUST be compacted: a delete policy on connect-configs silently
# loses connector configuration.
set -uo pipefail
. "$PREFIX/bin/env.sh"
mk() {
  "\$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "\$BOOTSTRAP_SERVERS" ${CMD_CONFIG_ARG} \\
    --create --if-not-exists --topic "\$1" --partitions "\$2" \\
    --replication-factor ${REPL_FACTOR} --config cleanup.policy=compact
}
mk connect-offsets 25
mk connect-configs 1
mk connect-status  5
echo
"\$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "\$BOOTSTRAP_SERVERS" ${CMD_CONFIG_ARG} --list | grep '^connect-'
EOF

cat > "$PREFIX/bin/verify.sh" <<EOF
#!/usr/bin/env bash
# Post-install verification. Confirms the worker is up and that BOTH the source
# and sink connector classes were loaded from the offline plugin directory.
set -uo pipefail
. "$PREFIX/bin/env.sh"
RC=0
say() { printf '%s\n' "\$*"; }

say "== worker =="
if curl -sSf "\$CONNECT_REST/" >/dev/null 2>&1; then
  say "  OK   REST API responding at \$CONNECT_REST"
else
  say "  FAIL REST API not responding. Check $PREFIX/logs/"; exit 1
fi

say "== connector plugins =="
P="\$(curl -sS "\$CONNECT_REST/connector-plugins")"
for c in com.mongodb.kafka.connect.MongoSourceConnector com.mongodb.kafka.connect.MongoSinkConnector; do
  case "\$P" in
    *"\$c"*) say "  OK   \$c" ;;
    *) say "  FAIL \$c NOT loaded"; RC=1 ;;
  esac
done

say "== log files =="
if ls "$PREFIX"/logs/*.log >/dev/null 2>&1; then
  say "  OK   \$(ls "$PREFIX"/logs/*.log | tr '\n' ' ')"
else
  say "  WARN no log files in $PREFIX/logs — check the log4j configuration"
fi

say "== broker connectivity =="
if "\$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "\$BOOTSTRAP_SERVERS" ${CMD_CONFIG_ARG} --list >/dev/null 2>&1; then
  say "  OK   broker reachable at \$BOOTSTRAP_SERVERS"
else
  say "  WARN cannot list topics — check broker address, network, or security settings"
fi

[ \$RC -eq 0 ] && say "
RESULT: PASS — Kafka Connect and both MongoDB connectors are installed and loaded." \\
             || say "
RESULT: FAIL — see above."
exit \$RC
EOF

chmod +x "$PREFIX"/bin/*.sh
ok "bin/start.sh bin/stop.sh bin/status.sh bin/verify.sh bin/create-internal-topics.sh bin/env.sh"

#--------------------------------------------------------- example connectors --
cat > "$PREFIX/examples/source-connector.json" <<EOF
{
  "name": "mongo-source-1",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
    "connection.uri": "\${file:${PREFIX}/etc/secrets.properties:mongo.uri}",
    "database": "CHANGE_ME_DB",
    "collection": "CHANGE_ME_COLLECTION",
    "topic.prefix": "src",
    "startup.mode": "copy_existing",
    "publish.full.document.only": "true",
    "output.format.value": "json",
    "change.stream.full.document": "updateLookup",
    "errors.tolerance": "none",
    "errors.log.enable": "true",
    "errors.log.include.messages": "true",
    "tasks.max": "1"
  }
}
EOF

cat > "$PREFIX/examples/sink-connector.json" <<EOF
{
  "name": "mongo-sink-1",
  "config": {
    "connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "topics": "CHANGE_ME_TOPIC",
    "connection.uri": "\${file:${PREFIX}/etc/secrets.properties:mongo.uri}",
    "database": "CHANGE_ME_DB",
    "collection": "CHANGE_ME_COLLECTION",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "document.id.strategy": "com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy",
    "writemodel.strategy": "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy",
    "max.batch.size": "1000",
    "errors.tolerance": "all",
    "errors.log.enable": "true",
    "errors.deadletterqueue.topic.name": "dlq.mongo-sink-1",
    "errors.deadletterqueue.context.headers.enable": "true",
    "errors.deadletterqueue.topic.replication.factor": "${REPL_FACTOR}",
    "tasks.max": "1"
  }
}
EOF

cat > "$PREFIX/examples/README.txt" <<EOF
Deploying a connector
---------------------
1. Copy an example, replace every CHANGE_ME value.
2. Make sure etc/secrets.properties contains your real mongo.uri.
3. Deploy:
     . $PREFIX/bin/env.sh
     curl -sX POST \$CONNECT_REST/connectors -H 'Content-Type: application/json' \\
          -d @source-connector.json
4. Check status (connector AND every task must be RUNNING):
     curl -s \$CONNECT_REST/connectors/mongo-source-1/status

Notes
-----
* The source connector requires a MongoDB replica set or sharded cluster.
  Change streams do not work against a standalone mongod.
* MongoDB privileges: source needs 'read' on the watched database (which grants
  find + changeStream); sink needs 'readWrite' on the target database.
* If your brokers have auto topic creation disabled, pre-create the source
  target topics (topic.prefix.database.collection) and the DLQ topic as well
  as the three internal topics.
* Credentials stay in etc/secrets.properties (mode 0600) and are referenced with
  \${file:...}, so they are not retrievable through the REST API.
EOF
ok "examples/source-connector.json examples/sink-connector.json examples/README.txt"

#----------------------------------------------------------------- topics ------
if [ "$DO_TOPICS" = yes ]; then
  step "Creating Connect internal topics"
  if "$PREFIX/bin/create-internal-topics.sh" >"$PREFIX/logs/create-topics.log" 2>&1; then
    ok "connect-offsets, connect-configs, connect-status created (or already existed)"
  else
    warn "topic creation failed — see $PREFIX/logs/create-topics.log"
    warn "check the broker address, network path, and any SASL/TLS settings in etc/connect-distributed.properties"
  fi
fi

#------------------------------------------------------------------- start -----
if [ "$DO_START" = yes ]; then
  step "Starting the Connect worker"
  "$PREFIX/bin/start.sh"
  printf '  waiting for the REST API'
  UP=no
  for _ in $(seq 1 60); do
    if curl -sSf "http://localhost:${REST_PORT}/" >/dev/null 2>&1; then UP=yes; break; fi
    printf '.'; sleep 3
  done
  echo
  if [ "$UP" = yes ]; then
    ok "worker is up on port ${REST_PORT}"
    step "Verifying the installation"
    "$PREFIX/bin/verify.sh"
    VRC=$?
  else
    err "the worker did not become ready within 180s"
    echo
    echo "Last 40 lines of $PREFIX/logs/connect-stdout.log:"
    tail -40 "$PREFIX/logs/connect-stdout.log" 2>/dev/null
    echo
    echo "Most common causes:"
    echo "  * cannot reach the brokers at ${BOOTSTRAP} (network, DNS, or firewall)"
    echo "  * broker auto topic creation is disabled and the internal topics do not"
    echo "    exist yet  ->  run: $PREFIX/bin/create-internal-topics.sh"
    echo "  * broker requires SASL/TLS: complete the security section in"
    echo "    $PREFIX/etc/connect-distributed.properties"
    VRC=1
  fi
else
  step "Skipping start (--no-start)"
  VRC=0
fi

#----------------------------------------------------------------- summary -----
cat <<EOF

${C_G}=============================================================================${C_0}
 Installed under: $PREFIX

   java/       Temurin JDK $JDK_MAJOR
   kafka/      Apache Kafka $KAFKA_VERSION (provides Kafka Connect)
   plugins/    MongoDB Kafka Connector $CONNECTOR_VERSION (source + sink)
   etc/        worker config, log4j config, secrets.properties (0600)
   bin/        start.sh stop.sh status.sh verify.sh create-internal-topics.sh
   logs/       worker logs
   examples/   connector config templates

 Day-to-day use:

   . $PREFIX/bin/env.sh     # put java + kafka tools on PATH
   $PREFIX/bin/start.sh
   $PREFIX/bin/status.sh
   $PREFIX/bin/verify.sh
   $PREFIX/bin/stop.sh

 Next steps:

   1. Put your MongoDB connection string in etc/secrets.properties (0600).
   2. Deploy connectors using the templates in examples/ — see examples/README.txt.
   3. To survive a reboot without root, either add bin/start.sh to your crontab
      with @reboot, or ask your platform team for a systemd unit.

 No sudo was used. Nothing outside $PREFIX was modified.
${C_G}=============================================================================${C_0}
EOF

exit ${VRC:-0}
