#!/bin/sh
# Fetch one timeline-1 WAL segment from the pinned rehearsal repository.
# PostgreSQL receives the file only after the decrypted bytes match the archive name.
set -eu
umask 077
export LC_ALL=C

test "$#" -eq 4 || exit 1
first=$1
last=$2
segment=$3
requested=$4
if ! printf '%s\n' "$first" "$last" "$segment" | awk '
    length($0) != 24 || $0 !~ /^00000001[0-9A-F]+$/ { invalid=1 }
    NR==1 { first="x" $0 }
    NR==2 { last="x" $0 }
    NR==3 { requested="x" $0 }
    END { exit (invalid || NR!=3 || first>requested || requested>last) }
'; then
    exit 1
fi

case "${QT_REHEARSAL_WAL_PHASE:-initial}" in
    initial) ledger=/qt-work/control/fetched-wal.sha1 ;;
    later) ledger=/qt-work/control/later-fetched-wal.sha1 ;;
    *) exit 1 ;;
esac
required_failure() {
    if [ "${QT_REHEARSAL_WAL_PHASE:-initial}" = later ]; then
        printf '%s\n' "$segment" >>/qt-work/control/later-wal-failure || true
        exit 126
    fi
    exit 1
}
wal=/qt-work/pgdata/pg_wal
destination=$wal/RECOVERYXLOG
case "$requested" in pg_wal/RECOVERYXLOG|"$destination") ;; *) required_failure ;; esac
test -d "$wal" && test ! -L "$wal" && test ! -L "$destination" || required_failure
prefix=$(printf '%.16s' "$segment")
directory=/repository/database/archive/qt/15-1/$prefix
test "$(readlink -f "$directory")" = "$directory" && test -d "$directory" || required_failure
match=
for source in "$directory/$segment"-*; do
    suffix=${source##*/}
    suffix=${suffix#"$segment"-}
    if printf '%s\n' "$suffix" | grep -Eq '^[0-9a-f]{40}(\.(zst|gz|bz2|lz4))?$'; then
        test -z "$match" && test -f "$source" && test ! -L "$source" || required_failure
        match="$suffix"
    fi
done
test -n "$match" || required_failure
expected=${match%%.*}

key=
test "$(stat -c '%u %a %h' /database-key)" = '70 600 1' && test ! -L /database-key || required_failure
IFS= read -r key < /database-key || test -n "$key" || required_failure
test "${#key}" -eq 64 || required_failure
case "$key" in *[!a-f0-9]*) required_failure ;; esac

staged=$(mktemp "$wal/.qt-verified-wal-XXXXXXXX") || required_failure
trap 'rm -f -- "$staged"' EXIT
trap required_failure HUP INT TERM
if ! PGBACKREST_REPO1_CIPHER_PASS=$key /usr/local/bin/pgbackrest --no-config --stanza=qt \
    --repo1-path=/repository/database --repo1-cipher-type=aes-256-cbc \
    --pg1-path=/qt-work/pgdata --lock-path=/qt-work/control/lock --archive-async=n \
    --log-level-file=off --log-level-console=off archive-get "$segment" "$staged" >/dev/null 2>&1; then
    required_failure
fi
unset key
test "$(stat -c %s "$staged")" -eq 16777216 || required_failure
digest=$(sha1sum "$staged") || required_failure
test "${digest%% *}" = "$expected" || required_failure
mv -f -- "$staged" "$destination" || required_failure
printf '%s %s\n' "$segment" "$expected" >>"$ledger" || required_failure
