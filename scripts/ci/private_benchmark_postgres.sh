#!/bin/sh
# Start, then separately promote, a receipt-bound disposable PG15 database copy.
set -eu
umask 027
export LC_ALL=C

action=$1
work=${QT_CLONE_WORK:-/qt-work}
receipts=${QT_CLONE_RECEIPTS:?}
pgdata=$work/pgdata
history=$work/history
control=$work/control
socket=$control/socket
database=${QT_CLONE_SOURCE_DATABASE:?}
role=${QT_CLONE_ROLE:?}
system_id=${QT_CLONE_SYSTEM_ID:?}
database_oid=${QT_CLONE_DATABASE_OID:?}
role_oid=${QT_CLONE_ROLE_OID:?}
tablespace_oid=${QT_CLONE_TABLESPACE_OID:?}
tablespace=${QT_CLONE_TABLESPACE:?}
pgdata_inode=${QT_CLONE_PGDATA_INODE:?}
history_inode=${QT_CLONE_HISTORY_INODE:?}
control_sha=${QT_CLONE_CONTROL_SHA256:?}
target=${QT_CLONE_TARGET_LSN:?}
replay=${QT_CLONE_REPLAY_LSN:?}
watermark=${QT_CLONE_COMMIT_SEQ:?}
wal_first=${QT_CLONE_FIRST_WAL:?}
wal_last=${QT_CLONE_LAST_WAL:?}
terminal_sha=${QT_CLONE_TERMINAL_WAL_SHA1:?}
port=${QT_CLONE_PORT:?}
benchmark=${QT_CLONE_BENCHMARK_DATABASE:?}
hook=${QT_CLONE_RESTORE_HOOK:-/qt-verify-wal}
startup_seconds=${QT_CLONE_STARTUP_SECONDS:-7200}

fail() { printf 'qt_private_benchmark_postgres: %s\n' "$1" >&2; exit 1; }
matches() { printf '%s\n' "$1" | grep -Eq "$2"; }
case "$action" in boot|promote) ;; *) fail "unknown action" ;; esac
for name in "$database" "$role"; do
    matches "$name" '^[a-z_][a-z0-9_]{0,62}$' || fail "invalid database or role name"
done
matches "$tablespace" '^[a-z_][a-z0-9_-]{0,62}$' || fail "invalid tablespace name"
matches "$benchmark" '^qt_bench_[a-z0-9_]{1,48}$' && test "$database" != "$benchmark" || fail "invalid benchmark database"
for number in "$system_id" "$database_oid" "$role_oid" "$tablespace_oid" "$pgdata_inode" "$history_inode" "$port"; do
    matches "$number" '^[1-9][0-9]{0,19}$' || fail "invalid native identity"
done
matches "$watermark" '^(0|[1-9][0-9]{0,18})$' && matches "$startup_seconds" '^[1-9][0-9]{0,4}$' \
    && test "$startup_seconds" -le 14400 && test "$port" -le 65535 || fail "invalid watermark or startup limit"
for lsn in "$target" "$replay"; do matches "$lsn" '^[0-9A-F]{1,8}/[0-9A-F]{1,8}$' || fail "invalid WAL LSN"; done
for segment in "$wal_first" "$wal_last"; do matches "$segment" '^00000001[0-9A-F]{16}$' || fail "invalid WAL segment"; done
matches "$control_sha" '^[a-f0-9]{64}$' && matches "$terminal_sha" '^[a-f0-9]{40}$' || fail "invalid native digest"
for path in "$work" "$receipts" "$hook"; do
    matches "$path" '^/[A-Za-z0-9_./-]+$' && test "$(readlink -f "$path")" = "$path" || fail "noncanonical private path"
done
test "$(id -u):$(id -g)" = 70:70 && test -z "$(awk 'NR>2 && $1!="lo:" {print $1}' /proc/net/dev)" || fail "isolation changed"
test "$(stat -c %i "$pgdata")" = "$pgdata_inode" && test "$(stat -c %i "$history")" = "$history_inode" \
    && test "$pgdata_inode" != "$history_inode" \
    && test "$(readlink "$pgdata/pg_tblspc/$tablespace_oid")" = "$history" || fail "clone directory identity changed"
test "$(stat -c '%u:%g %a' "$control")" = '70:70 700' || fail "private config directory changed"

export PGCONNECT_TIMEOUT=2 PGOPTIONS='-c statement_timeout=35000 -c lock_timeout=5000'
psql_private() {
    psql -X -qAt -w -h "$socket" -p "$port" -U "$role" -v ON_ERROR_STOP=1 -F ' ' "$@"
}
snapshot_body="
SELECT extract(epoch FROM pg_postmaster_start_time())::text AS started,
       COALESCE(max(head.market_commit_seq), 0) AS watermark
FROM market.series AS series
CROSS JOIN LATERAL (
    SELECT fact.market_commit_seq FROM market.fact_versions AS fact
    WHERE fact.series_id=series.id ORDER BY fact.market_commit_seq DESC LIMIT 1
) AS head
HAVING inet_server_addr() IS NULL
   AND (SELECT system_identifier::text FROM pg_control_system())='$system_id'
   AND (SELECT oid FROM pg_database WHERE datname=current_database())=$database_oid
   AND (SELECT oid FROM pg_roles WHERE rolname=current_user)=$role_oid
   AND current_setting('data_directory')='$pgdata'
   AND current_setting('unix_socket_directories')='$socket'
   AND current_setting('listen_addresses')=''
   AND current_setting('archive_mode')='off'
   AND current_setting('primary_conninfo')=''
   AND pg_tablespace_location($tablespace_oid)='$history'
   AND (SELECT spcname FROM pg_tablespace WHERE oid=$tablespace_oid)='$tablespace'
   AND (SELECT count(*) FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global'))=1
   AND NOT EXISTS (SELECT 1 FROM pg_stat_wal_receiver)"
pause_query="$snapshot_body
   AND pg_is_in_recovery() AND pg_get_wal_replay_pause_state()='paused'
   AND (SELECT timeline_id FROM pg_control_checkpoint())=1
   AND pg_last_wal_replay_lsn()='$replay'::pg_lsn
   AND current_setting('recovery_target_lsn')='$target'
   AND current_setting('recovery_target_action')='pause'
   AND current_setting('recovery_target_inclusive')='on'"

if [ "$action" = boot ]; then
    test ! -e "$pgdata/postmaster.pid" && test -f "$pgdata/recovery.signal" && test ! -L "$pgdata/recovery.signal" \
        && test ! -e "$pgdata/standby.signal" && test ! -s "$pgdata/postgresql.auto.conf" \
        && test ! -L "$pgdata/postgresql.auto.conf" || fail "clone is not a stopped recovery copy"
    test "$(sha256sum "$pgdata/global/pg_control" | cut -d' ' -f1)" = "$control_sha" || fail "copied native control changed"
    native=$(pg_controldata "$pgdata")
    field() {
        printf '%s\n' "$native" | awk -v name="$1" 'index($0,name ":")==1 {
            value=substr($0,length(name)+2); sub(/^[ \t]+/,"",value); print value; seen++
        } END {exit !(seen==1)}'
    }
    test "$(field 'Database cluster state')" = 'shut down in recovery' \
        && test "$(field 'Database system identifier')" = "$system_id" \
        && test "$(field "Latest checkpoint's TimeLineID")" = 1 \
        && test "$(field 'Bytes per WAL segment')" = 16777216 || fail "native control scope changed"
    for path in "$socket" "$control/pg_hba.conf" "$control/pg_ident.conf" "$control/clone.conf" "$receipts/benchmark-clone-paused"; do
        test ! -e "$path" && test ! -L "$path" || fail "private startup files already exist"
    done
    mkdir -m 0700 "$socket"
    printf 'local "%s" "%s" trust\nlocal "%s" "%s" trust\nlocal postgres "%s" trust\nlocal all all reject\n' \
        "$database" "$role" "$benchmark" "$role" "$role" >"$control/pg_hba.conf"
    : >"$control/pg_ident.conf"
    conf=$control/clone.conf
    cat >"$conf" <<CONF
data_directory = '$pgdata'
hba_file = '$control/pg_hba.conf'
ident_file = '$control/pg_ident.conf'
listen_addresses = ''
unix_socket_directories = '$socket'
unix_socket_permissions = 0700
port = $port
ssl = off
archive_mode = off
archive_command = ''
archive_library = ''
primary_conninfo = ''
primary_slot_name = ''
recovery_target_lsn = '$target'
recovery_target_inclusive = on
recovery_target_timeline = '1'
recovery_target_action = 'pause'
hot_standby = on
fsync = on
full_page_writes = on
synchronous_commit = on
shared_preload_libraries = 'timescaledb,pg_stat_statements'
timescaledb.telemetry_level = 'off'
default_transaction_read_only = on
shared_buffers = '256MB'
max_parallel_workers_per_gather = 0
CONF
    printf "restore_command = '/bin/sh %s %s %s %%f \"%%p\"'\n" "$hook" "$wal_first" "$wal_last" >>"$conf"
    numeric() {
        value=$(field "$2")
        matches "$value" '^[0-9]+$' || fail "invalid native startup setting"
        printf '%s = %s\n' "$1" "$value" >>"$conf"
    }
    numeric max_connections 'max_connections setting'
    numeric max_worker_processes 'max_worker_processes setting'
    numeric max_wal_senders 'max_wal_senders setting'
    numeric max_prepared_transactions 'max_prepared_xacts setting'
    numeric max_locks_per_transaction 'max_locks_per_xact setting'
    wal_level=$(field 'wal_level setting')
    track=$(field 'track_commit_timestamp setting')
    matches "$wal_level" '^(replica|logical)$' && matches "$track" '^(on|off)$' || fail "invalid native WAL settings"
    printf "wal_level = '%s'\ntrack_commit_timestamp = '%s'\n" "$wal_level" "$track" >>"$conf"
    # A clone may replay copied resident WAL. Requests that reach the archive
    # still use the authenticated wrapper and must honor its terminal pin.
    export QT_REHEARSAL_WAL_PHASE=later QT_REHEARSAL_TERMINAL_WAL_SHA1="$terminal_sha"
    postgres -D "$pgdata" -c config_file="$conf" &
    child=$!
    trap 'kill -TERM "$child" 2>/dev/null || true; wait "$child" || true' EXIT
    trap 'exit 143' TERM HUP INT
    limit=$(($(date +%s) + startup_seconds))
    while kill -0 "$child" 2>/dev/null; do
        test ! -e "$control/later-wal-failure" || fail "clone WAL verification failed"
        state=$(psql_private -d "$database" -c "$pause_query" 2>/dev/null || true)
        if matches "$state" "^[0-9]+[.][0-9]+ $watermark$"; then
            started=${state% *}
            pending=$(mktemp "$receipts/.benchmark-clone-paused.XXXXXXXX")
            printf 'system_id=%s database_oid=%s role_oid=%s timeline=1 replay_lsn=%s accepted_commit_seq=%s postmaster_epoch=%s\n' \
                "$system_id" "$database_oid" "$role_oid" "$replay" "$watermark" "$started" >"$pending"
            chmod 0640 "$pending"
            ln "$pending" "$receipts/benchmark-clone-paused"
            rm "$pending"
            wait "$child"
            exit
        fi
        test "$(date +%s)" -lt "$limit" || fail "private clone replay deadline"
        sleep 2
    done
    fail "postmaster ended before the verified pause"
fi

started=${QT_CLONE_VERIFIED_POSTMASTER_EPOCH:?}
verified_sha=${QT_CLONE_VERIFIED_RECEIPT_SHA256:?}
socket_inode=${QT_CLONE_VERIFIED_SOCKET_INODE:?}
matches "$started" '^[0-9]+[.][0-9]+$' && matches "$verified_sha" '^[a-f0-9]{64}$' || fail "invalid archive verification binding"
test -S "$socket/.s.PGSQL.$port" && test "$(stat -c %i "$socket/.s.PGSQL.$port")" = "$socket_inode" \
    || fail "verified private socket changed"
verified=$receipts/benchmark-clone-verified
test -f "$verified" && test ! -L "$verified" \
    && test "$(sha256sum "$verified" | cut -d' ' -f1)" = "$verified_sha" || fail "archive verification receipt changed"
test ! -e "$control/later-wal-failure" || fail "clone WAL verification failed"
intent=$receipts/benchmark-clone-promotion
intent_value="$started $verified_sha"
if [ -e "$intent" ] || [ -L "$intent" ]; then
    test -f "$intent" && test ! -L "$intent" && test "$(cat "$intent")" = "$intent_value" || fail "promotion intent changed"
else
    state=$(psql_private -d "$database" -c "$pause_query")
    test "$state" = "$started $watermark" || fail "verified clone pause changed"
    pending=$(mktemp "$receipts/.benchmark-clone-promotion.XXXXXXXX")
    printf '%s\n' "$intent_value" >"$pending"
    chmod 0640 "$pending"
    ln "$pending" "$intent"
    rm "$pending"
fi
mode=$(psql_private -d postgres -c "SELECT pg_is_in_recovery() WHERE extract(epoch FROM pg_postmaster_start_time())::text='$started'")
case "$mode" in
    t)
        resumed=$(psql_private -d "$database" -c "
            WITH verified AS ($pause_query)
            SELECT 'resumed', pg_wal_replay_resume() FROM verified
            WHERE started='$started' AND watermark=$watermark")
        test "$resumed" = 'resumed ' || fail "exact pause changed before resume"
        ;;
    f) ;;
    *) fail "private postmaster changed before resume" ;;
esac
limit=$(($(date +%s) + 60))
while :; do
    promoted=$(psql_private -d postgres -c "
        SELECT 'ready' WHERE NOT pg_is_in_recovery()
          AND pg_last_wal_replay_lsn()='$replay'::pg_lsn
          AND extract(epoch FROM pg_postmaster_start_time())::text='$started'" 2>/dev/null || true)
    test "$promoted" = ready && break
    test "$(date +%s)" -lt "$limit" || fail "private promotion did not complete"
    sleep 1
done
export PGOPTIONS='-c statement_timeout=30000 -c lock_timeout=5000 -c default_transaction_read_only=off'
psql_private -d postgres -c 'CHECKPOINT'
name=$(psql_private -d postgres -c "SELECT datname FROM pg_database WHERE oid=$database_oid")
if [ "$name" = "$database" ]; then
    psql_private -d postgres -c "ALTER DATABASE \"$database\" RENAME TO \"$benchmark\""
fi
state=$(psql_private -d "$benchmark" -c "$snapshot_body
   AND NOT pg_is_in_recovery() AND (SELECT timeline_id FROM pg_control_checkpoint())=2
   AND pg_last_wal_replay_lsn()='$replay'::pg_lsn")
test "$state" = "$started $watermark" || fail "promoted database identity or watermark changed"
printf 'benchmark_database=%s database_oid=%s timeline=2 replay_lsn=%s accepted_commit_seq=%s postmaster_epoch=%s\n' \
    "$benchmark" "$database_oid" "$replay" "$watermark" "$started"
