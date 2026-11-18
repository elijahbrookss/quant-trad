#!/bin/sh
# Build pinned native tools in a disposable Docker builder with its own libc.
set -eu
test "$(uname -m)" = x86_64
mkdir -p /build/qt-backup-tools /opt/qt-backup-tools
cd /build/qt-backup-tools
qt_pgbackrest_version=${QT_PGBACKREST_VERSION:-2.59.3}
case "$qt_pgbackrest_version" in
  2.59.1) qt_pgbackrest_sha256=ca1e75c7490989a2fb39b8266c0f3c518dd3c873ddc0c3a346ca9b5dccc16455 ;;
  2.59.3) qt_pgbackrest_sha256=b2a95535a68e326a0f316c2d8d3243bc30092cf1ce9de9be5d142e204d2d4263 ;;
  *) echo 'unsupported pinned pgBackRest version' >&2; exit 1 ;;
esac
curl --fail --location --retry 3 \
  "https://github.com/pgbackrest/pgbackrest/archive/refs/tags/release/$qt_pgbackrest_version.tar.gz" \
  --output pgbackrest.tar.gz
echo "$qt_pgbackrest_sha256  pgbackrest.tar.gz" | sha256sum -c -
tar -xzf pgbackrest.tar.gz
meson setup --buildtype=release build "pgbackrest-release-$qt_pgbackrest_version"
ninja -C build -j 2
install -m 0755 build/src/pgbackrest /opt/qt-backup-tools/pgbackrest
curl --fail --location --retry 3 \
  https://github.com/restic/restic/releases/download/v0.19.1/restic_0.19.1_linux_amd64.bz2 \
  --output restic.bz2
echo 'f415415624dcc452f2a02b8c33641791a8c6d6d3b65bbb3543fcf9a25151585c  restic.bz2' | sha256sum -c -
bunzip2 restic.bz2
install -m 0755 restic /opt/qt-backup-tools/restic
/opt/qt-backup-tools/pgbackrest version
/opt/qt-backup-tools/restic version
