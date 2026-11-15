#!/bin/sh
# Two native PostgreSQL boots for the explicitly isolated signed receiver fixture.
set -eu
umask 077
export LC_ALL=C
test "${QT_NAMED_REPLAYED_FIXTURE:-}" = 1
test "$(id -u)" = 70
test "$(postgres --version)" = 'postgres (PostgreSQL) 15.6'

control=/qt-restore/rolling-control
socket=/qt-restore/receiver-workspace/rolling-socket
pgdata=/qt-restore/receiver-workspace/pgdata
port=55432
booted=no

cleanup() {
    status=$?
    if [ "$booted" = yes ] && [ -f "$pgdata/postmaster.pid" ]; then
        pg_ctl -D "$pgdata" -m fast -w -t 30 stop || true
    fi
    if [ "$status" != 0 ]; then
        for phase in prefix final; do
            if [ -f "$control/$phase.postgres.log" ]; then tail -30 "$control/$phase.postgres.log" >&2; fi
        done
    fi
}
trap cleanup EXIT
trap 'exit 1' INT TERM

wait_for() {
    deadline=$(($(date +%s) + 240))
    until [ -f "$control/$1" ] && [ ! -L "$control/$1" ]; do
        test "$(date +%s)" -lt "$deadline"
        sleep 1
    done
}

ack() {
    printf '%s\n' "$1" > "$control/.ack"
    mv "$control/.ack" "$control/$1"
}

assert_paths() {
    for path in "$control" "$socket" "$pgdata"; do
        test "$(readlink -f "$path")" = "$path"
        test "$(stat -c '%u:%a' "$path")" = 70:700
    done
    test -f "$pgdata/recovery.signal" && test ! -L "$pgdata/recovery.signal"
    test ! -e "$pgdata/postmaster.pid" && test ! -e "$pgdata/standby.signal"
}

base_options="-c listen_addresses= -c unix_socket_directories=$socket -c unix_socket_permissions=0700 -c port=$port -c hba_file=$control/pg_hba.conf -c fsync=on -c archive_mode=off -c timescaledb.telemetry_level=off -c shared_preload_libraries=timescaledb,pg_stat_statements -c primary_conninfo="

wait_for prefix.start
assert_paths
IFS= read -r target < "$control/prefix.start"
printf '%s\n' "$target" | grep -Eq '^[0-9A-F]{1,8}/[0-9A-F]{1,8}$'
booted=yes
pg_ctl -D "$pgdata" -l "$control/prefix.postgres.log" -w -t 30 \
    -o "$base_options -c default_transaction_read_only=on -c recovery_target_name= -c recovery_target_lsn=$target -c recovery_target_timeline=1 -c recovery_target_action=pause -c recovery_target_inclusive=on" start
ack prefix.started

wait_for prefix.stop
pg_ctl -D "$pgdata" -m fast -w -t 30 stop
booted=no
assert_paths
test ! -e "$pgdata/backup_label"
pg_controldata "$pgdata" > "$control/stopped-control" 2> "$control/stopped-control.stderr"
test ! -s "$control/stopped-control.stderr"
grep -Eq 'Database cluster state:[[:space:]]+shut down in recovery$' "$control/stopped-control"
sha256sum "$pgdata/global/pg_control" | cut -d' ' -f1 > "$control/stopped-control.sha256"
ack prefix.stopped

wait_for final.start
assert_paths
booted=yes
pg_ctl -D "$pgdata" -l "$control/final.postgres.log" -w -t 30 -o "$base_options" start
ack final.started

wait_for final.stop
pg_ctl -D "$pgdata" -m fast -w -t 30 stop
booted=no
ack final.stopped
