#!/usr/bin/env bash
# Own one explicitly disposable, private local cluster and scratch archive.
set -euo pipefail

if [ "$(id -u)" -eq 0 ]; then
    echo 'Run the disposable scale fixture as an unprivileged PostgreSQL OS user.' >&2
    exit 2
fi

pgbin=${QT_SCALE_PG_BIN:-$(pg_config --bindir)}
if [ ! -x "$pgbin/initdb" ] || [ ! -x "$pgbin/pg_ctl" ]; then
    echo 'Local PostgreSQL server tools are required.' >&2
    exit 2
fi

root=$(mktemp -d /tmp/qt-native-cold-scale-XXXXXXXX)
cleanup() {
    if [ -f "$root/pgdata/postmaster.pid" ]; then
        "$pgbin/pg_ctl" -D "$root/pgdata" -w -t 30 -m fast stop >&2 || return 1
    fi
    case "$root" in
        /tmp/qt-native-cold-scale-????????) rm -rf -- "$root" ;;
        *) echo "Refusing unexpected scratch directory $root" >&2; return 1 ;;
    esac
    echo "Disposed local PostgreSQL cluster and objects at $root" >&2
}
trap cleanup EXIT

mkdir -m 700 "$root/socket" "$root/objects" "$root/staging" "$root/history"
"$pgbin/initdb" -D "$root/pgdata" --no-locale --encoding=UTF8 -U qt_scale_fixture --auth-local=trust --auth-host=reject >&2
"$pgbin/pg_ctl" -D "$root/pgdata" -w -t 30 -l "$root/pg.log" \
    -o "-c listen_addresses= -c unix_socket_directories=$root/socket -c unix_socket_permissions=0700 \
    -c port=55498 -c shared_buffers=128MB -c max_wal_size=512MB -c max_connections=12" start >&2

export PG_DSN="postgresql+psycopg2://qt_scale_fixture@/qt_bench_native_scale?host=$root/socket&port=55498"
export QT_DISABLE_DOTENV=1 MARKET_STRUCTURE_STORAGE_ROOT="$root" QT_MARKET_DATA_EXPECTED_UUID=
export PYTHONPATH=src:.
python -m scripts.db.canonical_cold_scale --root "$root" "$@"
