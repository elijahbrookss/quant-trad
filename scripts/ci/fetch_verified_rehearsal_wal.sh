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

wal=/qt-work/pgdata/pg_wal
destination=$wal/RECOVERYXLOG
case "$requested" in pg_wal/RECOVERYXLOG|"$destination") ;; *) exit 1 ;; esac
test -d "$wal" && test ! -L "$wal" && test ! -L "$destination" || exit 1
prefix=$(printf '%.16s' "$segment")
directory=/repository/database/archive/qt/15-1/$prefix
test "$(readlink -f "$directory")" = "$directory" && test -d "$directory" || exit 1
match=
for source in "$directory/$segment"-*; do
    suffix=${source##*/}
    suffix=${suffix#"$segment"-}
    if printf '%s\n' "$suffix" | grep -Eq '^[0-9a-f]{40}(\.(zst|gz|bz2|lz4))?$'; then
        test -z "$match" && test -f "$source" && test ! -L "$source" || exit 1
        match="$suffix"
    fi
done
test -n "$match" || exit 1
expected=${match%%.*}

key=
test "$(stat -c '%u %a %h' /database-key)" = '70 600 1' && test ! -L /database-key || exit 1
IFS= read -r key < /database-key || test -n "$key"
test "${#key}" -eq 64 || exit 1
case "$key" in *[!a-f0-9]*) exit 1 ;; esac

staged=$(mktemp "$wal/.qt-verified-wal-XXXXXXXX")
trap 'rm -f -- "$staged"' EXIT
trap 'exit 1' HUP INT TERM
if ! PGBACKREST_REPO1_CIPHER_PASS=$key /usr/local/bin/pgbackrest --no-config --stanza=qt \
    --repo1-path=/repository/database --repo1-cipher-type=aes-256-cbc \
    --pg1-path=/qt-work/pgdata --lock-path=/qt-work/control/lock --archive-async=n \
    --log-level-file=off --log-level-console=off archive-get "$segment" "$staged" >/dev/null 2>&1; then
    exit 1
fi
unset key
test "$(stat -c %s "$staged")" -eq 16777216 || exit 1
digest=$(sha1sum "$staged")
test "${digest%% *}" = "$expected" || exit 1
mv -f -- "$staged" "$destination"
printf '%s %s\n' "$segment" "$expected" >>/qt-work/control/fetched-wal.sha1
