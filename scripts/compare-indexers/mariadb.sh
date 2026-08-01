#!/usr/bin/env bash
#
# Provision and remove the dedicated MariaDB database used by the Robinhood
# comparison. The default names are intentionally benchmark-specific.
#
# Usage:
#   mariadb.sh setup <benchmark-tree>   root of the tree Robinhood will scan;
#                                       written as fs_path into its config, and
#                                       created if it does not exist yet
#   mariadb.sh schema                   create Robinhood's tables by scanning the
#                                       configured tree once with --alter-db;
#                                       setup does this itself
#   mariadb.sh adopt                    take ownership of an existing benchmark
#                                       database whose marker was lost, so that
#                                       setup/reset/cleanup work again
#   mariadb.sh reset
#   mariadb.sh restart
#   mariadb.sh bytes
#   mariadb.sh cleanup
#
# Environment:
#   PREFIX=$HOME/.local/indexer-compare
#   RBH_DB_NAME=rbh_indexer_compare
#   RBH_DB_USER=rbh_indexer_compare
#   RBH_DB_PASSWORD=<generated when omitted>
#   INSTALL_MARIADB_PACKAGES=1
#   PKG_ARGS=                                  extra dnf/apt-get arguments;
#                                              defaults to
#                                              --disableplugin=etckeeper on dnf
#                                              hosts (see init.sh)
#   MARIADB_ADMIN_DEFAULTS_FILE=/root/.my.cnf  (only if socket root auth is unavailable)
#   SUDO=sudo                                  (use "" when already root)
#
# setup also drops a systemd override at
# /etc/systemd/system/mariadb.service.d/indexer-compare.conf that lifts the unit's
# start rate limit, because a cold-cache run restarts the server before every
# database query. cleanup removes it.
#
set -euo pipefail

PREFIX=${PREFIX:-"$HOME/.local/indexer-compare"}
RBH_DB_NAME=${RBH_DB_NAME:-rbh_indexer_compare}
RBH_DB_USER=${RBH_DB_USER:-rbh_indexer_compare}
INSTALL_MARIADB_PACKAGES=${INSTALL_MARIADB_PACKAGES:-1}
# See init.sh: etckeeper's dnf plugin can block a transaction on an ssh password
# prompt, so it is skipped by default. Setting PKG_ARGS (even to empty) replaces
# this.
default_pkg_args() {
  local dnf_path
  dnf_path=$(command -v dnf 2>/dev/null) || return 0
  case "$(readlink -f "$dnf_path" 2>/dev/null || printf '%s' "$dnf_path")" in
    *dnf5) printf '%s' '--disable-plugin=etckeeper' ;;
    *) printf '%s' '--disableplugin=etckeeper' ;;
  esac
}
[[ -n "${PKG_ARGS+set}" ]] || PKG_ARGS=$(default_pkg_args)
read -r -a PKG_ARGS_ARR <<<"$PKG_ARGS"
# Same default as lib.sh: the config generated here has to agree with the
# thread budget the rest of the harness hands out.
THREADS=${THREADS:-16}
CONF_DIR="$PREFIX/etc/robinhood.d"
PASSWORD_FILE="$CONF_DIR/.dbpassword"
CONFIG_FILE="$CONF_DIR/indexer-compare.conf"
MARKER_FILE="$CONF_DIR/.indexer-compare-db"
LOG_DIR="$PREFIX/var/log"

if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=${SUDO:-}
else
  SUDO=${SUDO:-sudo}
fi

log() { printf '==> %s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

run_root() {
  if [[ -n "$SUDO" ]]; then
    "$SUDO" "$@"
  else
    "$@"
  fi
}

validate_settings() {
  [[ "$RBH_DB_NAME" =~ ^[A-Za-z][A-Za-z0-9_]*$ ]] ||
    die "RBH_DB_NAME must contain only letters, digits, and underscores"
  [[ "$RBH_DB_USER" =~ ^[A-Za-z][A-Za-z0-9_]*$ ]] ||
    die "RBH_DB_USER must contain only letters, digits, and underscores"
}

install_server() {
  [[ "$INSTALL_MARIADB_PACKAGES" == "1" ]] || return 0
  local args=(install -y ${PKG_ARGS_ARR[@]+"${PKG_ARGS_ARR[@]}"})
  if command -v dnf >/dev/null 2>&1; then
    run_root dnf "${args[@]}" mariadb-server mariadb-connector-c-devel
  elif command -v apt-get >/dev/null 2>&1; then
    run_root apt-get update
    run_root apt-get "${args[@]}" mariadb-server default-libmysqlclient-dev
  else
    die "cannot install MariaDB: neither dnf nor apt-get is available"
  fi
}

start_server() {
  if command -v systemctl >/dev/null 2>&1; then
    run_root systemctl enable --now mariadb
  elif command -v service >/dev/null 2>&1; then
    run_root service mariadb start
  else
    die "MariaDB is installed, but no systemctl/service command is available"
  fi
}

# systemd refuses to start a unit more than StartLimitBurst times in
# StartLimitIntervalSec (5 in 10s by default), and a cold-cache benchmark
# restarts the server before every database row. Once the limit is hit the
# restart fails, the server stays down, and every remaining Robinhood row fails
# on a missing socket. Lifting the limit for this unit is the whole fix; the
# drop-in is removed by cleanup.
SYSTEMD_DROPIN_DIR=/etc/systemd/system/mariadb.service.d
SYSTEMD_DROPIN="$SYSTEMD_DROPIN_DIR/indexer-compare.conf"

allow_frequent_restarts() {
  command -v systemctl >/dev/null 2>&1 || return 0
  [[ ! -f "$SYSTEMD_DROPIN" ]] || return 0
  run_root mkdir -p "$SYSTEMD_DROPIN_DIR" || return 0
  run_root tee "$SYSTEMD_DROPIN" >/dev/null <<'EOF'
# Added by scripts/compare-indexers/mariadb.sh. A cold-cache benchmark restarts
# MariaDB before each database query, which trips the default start rate limit
# and leaves the server down. Removed by 'mariadb.sh cleanup'.
[Unit]
StartLimitIntervalSec=0
EOF
  run_root systemctl daemon-reload || true
  log "allowed frequent restarts via $SYSTEMD_DROPIN"
}

remove_frequent_restarts() {
  command -v systemctl >/dev/null 2>&1 || return 0
  [[ -f "$SYSTEMD_DROPIN" ]] || return 0
  run_root rm -f "$SYSTEMD_DROPIN"
  run_root rmdir --ignore-fail-on-non-empty "$SYSTEMD_DROPIN_DIR" 2>/dev/null || true
  run_root systemctl daemon-reload || true
  log "removed $SYSTEMD_DROPIN"
}

# Used by cold-cache benchmarking: dropping the page cache leaves InnoDB's
# buffer pool populated, so the server has to be cycled to match the other tools.
restart_server() {
  if ! command -v systemctl >/dev/null 2>&1; then
    if command -v service >/dev/null 2>&1; then
      run_root service mariadb restart
    else
      die "no systemctl/service command available to restart MariaDB"
    fi
  else
    local attempt=0 started=0
    while ((attempt < 2)); do
      attempt=$((attempt + 1))
      # A unit left in the failed state (rate limit, or a crash on the previous
      # cycle) refuses to start again until it is cleared.
      run_root systemctl reset-failed mariadb >/dev/null 2>&1 || true
      if run_root systemctl restart mariadb; then
        started=1
        break
      fi
      sleep 5
    done
    ((started == 1)) ||
      die "MariaDB failed to restart twice; see 'systemctl status mariadb'"
  fi
  local waited
  for waited in $(seq 1 60); do
    if admin_sql --batch --skip-column-names -e 'SELECT 1;' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  die "MariaDB did not accept connections within 60s of restarting"
}

mysql_client() {
  if command -v mariadb >/dev/null 2>&1; then
    printf '%s\n' mariadb
  elif command -v mysql >/dev/null 2>&1; then
    printf '%s\n' mysql
  else
    die "MariaDB client not found"
  fi
}

admin_sql() {
  local client
  client=$(mysql_client)
  if [[ -n "${MARIADB_ADMIN_DEFAULTS_FILE:-}" ]]; then
    run_root "$client" --defaults-extra-file="$MARIADB_ADMIN_DEFAULTS_FILE" "$@"
  else
    # Fresh RHEL/Debian installs normally authenticate root through this socket.
    run_root "$client" --protocol=socket "$@"
  fi
}

write_marker() {
  {
    printf 'database=%s\n' "$RBH_DB_NAME"
    printf 'user=%s\n' "$RBH_DB_USER"
  } >"$MARKER_FILE"
}

require_marker() {
  [[ -f "$MARKER_FILE" ]] ||
    die "$MARKER_FILE is absent; refusing to modify a database not created by this helper"
  grep -qx "database=$RBH_DB_NAME" "$MARKER_FILE" ||
    die "database name does not match the provision marker"
  grep -qx "user=$RBH_DB_USER" "$MARKER_FILE" ||
    die "database user does not match the provision marker"
}

require_names_available_or_marked() {
  if [[ -f "$MARKER_FILE" ]]; then
    require_marker
    return 0
  fi
  local db_exists user_exists
  db_exists=$(admin_sql --batch --skip-column-names -e \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '$RBH_DB_NAME';")
  user_exists=$(admin_sql --batch --skip-column-names -e \
    "SELECT COUNT(*) FROM mysql.user WHERE User = '$RBH_DB_USER' AND Host = 'localhost';")
  [[ "$db_exists" == "0" && "$user_exists" == "0" ]] || die "$(
    printf "database '%s' or user '%s' already exists, but %s does not, so this helper cannot prove it owns them.\n" \
      "$RBH_DB_NAME" "$RBH_DB_USER" "$MARKER_FILE"
    printf '       If they are left over from an earlier benchmark run, take ownership with:\n'
    printf '         PREFIX=%s %s adopt\n' "$PREFIX" "$0"
    printf '       then rerun setup, or drop them with '\''%s cleanup'\''.\n' "$0"
    printf '       If PREFIX simply differs from the run that created them, set PREFIX to that path instead.'
  )"
}

# Recovery for a database this helper created but can no longer prove it owns,
# which is what an interrupted setup used to leave behind. Deliberately explicit:
# it makes cleanup able to drop these names, so it must be asked for by name.
adopt() {
  start_server
  local db_exists
  db_exists=$(admin_sql --batch --skip-column-names -e \
    "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = '$RBH_DB_NAME';")
  [[ "$db_exists" != "0" ]] ||
    die "database '$RBH_DB_NAME' does not exist; run '$0 setup <benchmark-tree>' instead"
  mkdir -p "$CONF_DIR"
  write_marker
  log "adopted database '$RBH_DB_NAME' and user '$RBH_DB_USER'"
  log "wrote $MARKER_FILE"
  if [[ ! -f "$CONFIG_FILE" ]]; then
    log "no Robinhood config yet; rerun '$0 setup <benchmark-tree>' to generate one"
  fi
}

generate_password() {
  if [[ -n "${RBH_DB_PASSWORD:-}" ]]; then
    [[ "$RBH_DB_PASSWORD" =~ ^[A-Za-z0-9._~-]+$ ]] ||
      die "RBH_DB_PASSWORD may contain only letters, digits, '.', '_', '~', and '-'"
    printf '%s\n' "$RBH_DB_PASSWORD"
  elif command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 24
  else
    python3 - <<'PY'
import secrets
print(secrets.token_hex(24))
PY
  fi
}

create_database() {
  local password=$1
  admin_sql <<SQL
CREATE DATABASE IF NOT EXISTS \`$RBH_DB_NAME\`;
CREATE USER IF NOT EXISTS '$RBH_DB_USER'@'localhost' IDENTIFIED BY '$password';
ALTER USER '$RBH_DB_USER'@'localhost' IDENTIFIED BY '$password';
GRANT ALL PRIVILEGES ON \`$RBH_DB_NAME\`.* TO '$RBH_DB_USER'@'localhost';
FLUSH PRIVILEGES;
SQL
}

drop_database() {
  admin_sql <<SQL
DROP DATABASE IF EXISTS \`$RBH_DB_NAME\`;
DROP USER IF EXISTS '$RBH_DB_USER'@'localhost';
FLUSH PRIVILEGES;
SQL
}

setup() {
  [[ $# -eq 1 ]] || die "usage: $0 setup <benchmark-tree>"
  local fs_path fs_type password
  # The tree need not be populated yet; prepare-synth.sh can run after this.
  # Only create the leaf: a missing parent means a mistyped mount point, and
  # creating that would point Robinhood at the root filesystem instead of the
  # storage under test.
  if [[ ! -d "$1" ]]; then
    local parent
    parent=$(dirname "$1")
    [[ -d "$parent" ]] ||
      die "neither $1 nor its parent $parent exists; check the mount point"
    log "creating benchmark tree root $1"
    mkdir -p "$1"
  fi
  fs_path=$(cd "$1" && pwd)
  [[ "$fs_path" != *\"* && "$fs_path" != *$'\n'* ]] ||
    die "filesystem path cannot contain a quote or newline"

  install_server
  allow_frequent_restarts
  start_server
  require_names_available_or_marked
  password=$(generate_password)

  # Claim the names before creating anything. Writing the marker last means a
  # failure anywhere in the rest of setup leaves a database this helper owns but
  # can no longer prove it owns, which blocks every later setup, reset and
  # cleanup. The marker records intent, not completion.
  mkdir -p "$CONF_DIR"
  write_marker

  create_database "$password"

  if command -v findmnt >/dev/null 2>&1; then
    fs_type=$(findmnt -n -o FSTYPE -T "$fs_path" | awk 'NR == 1 { print; exit }')
  else
    fs_type=$(stat -f -c '%T' "$fs_path")
  fi
  [[ "$fs_type" =~ ^[A-Za-z0-9._-]+$ ]] ||
    die "could not determine a safe filesystem type for $fs_path"

  mkdir -p "$CONF_DIR" "$LOG_DIR"
  umask 077
  printf '%s\n' "$password" >"$PASSWORD_FILE"

  # Robinhood takes its thread counts from the config, and its defaults (2 scan
  # threads) are far below what the other tools are given, so the comparison
  # would measure the default rather than the tool. Scan and pipeline threads
  # run concurrently, so they split the same budget the harness gives everyone.
  local scan_threads pipeline_threads
  scan_threads=$((THREADS / 2))
  ((scan_threads >= 1)) || scan_threads=1
  pipeline_threads=$((THREADS - scan_threads))
  ((pipeline_threads >= 1)) || pipeline_threads=1

  cat >"$CONFIG_FILE" <<EOF
General {
    fs_path = "$fs_path";
    fs_type = $fs_type;
}

FS_Scan {
    nb_threads_scan = $scan_threads;
}

EntryProcessor {
    nb_threads = $pipeline_threads;
}

Log {
    log_file = "$LOG_DIR/robinhood.log";
    report_file = "$LOG_DIR/robinhood-actions.log";
    alert_file = "$LOG_DIR/robinhood-alerts.log";
}

ListManager {
    MySQL {
        server = localhost;
        db = $RBH_DB_NAME;
        user = $RBH_DB_USER;
        password_file = $PASSWORD_FILE;
    }
}
EOF

  verify_config_syntax
  create_schema

  log "MariaDB database '$RBH_DB_NAME' is ready"
  log "Robinhood config: $CONFIG_FILE"
  log "Robinhood threads: $scan_threads scan + $pipeline_threads pipeline (THREADS=$THREADS)"
}

robinhood_bin() {
  local bin=${RBH_SCAN:-} c
  [[ -n "$bin" && -x "$bin" ]] && {
    printf '%s' "$bin"
    return 0
  }
  bin=$(command -v rbh-scan 2>/dev/null || command -v robinhood 2>/dev/null || true)
  # init.sh installs into PREFIX, which is not necessarily on PATH here.
  if [[ -z "$bin" ]]; then
    for c in "$PREFIX/sbin/robinhood" "$PREFIX/bin/robinhood" \
      "$PREFIX/bin/rbh-scan" "$PREFIX/sbin/rbh-scan"; do
      if [[ -x "$c" ]]; then
        bin=$c
        break
      fi
    done
  fi
  [[ -n "$bin" ]] || return 1
  printf '%s' "$bin"
}

schema_present() {
  local n
  n=$(admin_sql --batch --skip-column-names -e \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '$RBH_DB_NAME' AND table_name = 'ENTRIES';" 2>/dev/null) || return 1
  [[ "$n" == "1" ]]
}

# Robinhood does not ship a schema: it builds ENTRIES, NAMES and the rest on its
# first run with --alter-db, which in this harness is the index step. When that
# step is skipped -- no config, or a run that only asks questions -- every query
# hits "table ENTRIES does not exist" instead of returning an empty answer. So
# setup finishes the job itself with one scan. It runs before the tree is
# populated, so there is nothing to walk and it costs a moment.
create_schema() {
  require_marker
  local bin log_file
  if ! bin=$(robinhood_bin); then
    log "WARN: no robinhood binary found, so the database has no schema yet"
    log "      the first indexed run will create it, but queries before then will fail"
    return 0
  fi
  if schema_present; then
    log "Robinhood schema already present in '$RBH_DB_NAME'"
    return 0
  fi
  mkdir -p "$LOG_DIR"
  log_file="$LOG_DIR/schema-scan.log"
  log "creating the Robinhood schema (scan of an empty tree)"
  if ! "$bin" -f "$CONFIG_FILE" --scan --once --alter-db >"$log_file" 2>&1; then
    log "WARN: the schema scan failed; see $log_file"
    return 0
  fi
  if schema_present; then
    log "Robinhood schema created in '$RBH_DB_NAME'"
  else
    log "WARN: the schema scan succeeded but '$RBH_DB_NAME' still has no ENTRIES table; see $log_file"
  fi
}

# Block names differ across Robinhood builds and backends, so a config that
# names threads can be rejected outright by a build that does not know the
# block. Better to lose the thread pinning (and say so) than to leave a config
# that makes every Robinhood row fail.
verify_config_syntax() {
  local bin help
  bin=$(robinhood_bin) || return 0
  help=$("$bin" --help 2>&1 || true)
  case "$help" in
    *--test-syntax*) ;;
    *) return 0 ;;
  esac
  if "$bin" -f "$CONFIG_FILE" --test-syntax >/dev/null 2>&1; then
    return 0
  fi
  log "WARN: this Robinhood build rejects the thread settings; writing a config without them"
  log "      Robinhood will scan at its own default thread count, unlike the other tools"
  # Drop the two blocks, keeping everything else byte for byte.
  awk '
    /^(FS_Scan|EntryProcessor) \{$/ { skip = 1 }
    skip && /^\}$/ { skip = 0; getline; next }
    !skip { print }
  ' "$CONFIG_FILE" >"$CONFIG_FILE.tmp" && mv "$CONFIG_FILE.tmp" "$CONFIG_FILE"
  "$bin" -f "$CONFIG_FILE" --test-syntax >/dev/null 2>&1 ||
    die "Robinhood rejects $CONFIG_FILE even without the thread settings; run '$bin -f $CONFIG_FILE --test-syntax' to see why"
}

reset_database() {
  require_marker
  local password
  password=$(<"$PASSWORD_FILE")
  drop_database
  create_database "$password"
  log "reset MariaDB database '$RBH_DB_NAME'"
}

database_bytes() {
  require_marker
  admin_sql --batch --skip-column-names -e \
    "SELECT COALESCE(SUM(data_length + index_length), 0) FROM information_schema.tables WHERE table_schema = '$RBH_DB_NAME';"
}

cleanup() {
  require_marker
  drop_database
  remove_frequent_restarts
  rm -f "$PASSWORD_FILE" "$CONFIG_FILE" "$MARKER_FILE"
  rm -f "$LOG_DIR"/robinhood.log "$LOG_DIR"/robinhood-actions.log "$LOG_DIR"/robinhood-alerts.log
  log "dropped MariaDB database/user and removed generated Robinhood files"
  log "MariaDB packages and service were left installed"
}

validate_settings
case "${1:-}" in
  setup) shift; setup "$@" ;;
  schema) shift; [[ $# -eq 0 ]] || die "usage: $0 schema"; create_schema ;;
  adopt) shift; [[ $# -eq 0 ]] || die "usage: $0 adopt"; adopt ;;
  reset) shift; [[ $# -eq 0 ]] || die "usage: $0 reset"; reset_database ;;
  restart) shift; [[ $# -eq 0 ]] || die "usage: $0 restart"; restart_server ;;
  bytes) shift; [[ $# -eq 0 ]] || die "usage: $0 bytes"; database_bytes ;;
  cleanup) shift; [[ $# -eq 0 ]] || die "usage: $0 cleanup"; cleanup ;;
  *) die "usage: $0 {setup <benchmark-tree>|schema|adopt|reset|restart|bytes|cleanup}" ;;
esac
