FROM timescale/timescaledb:2.14.2-pg15 AS backup-tools
USER root
RUN apk add --no-cache build-base meson ninja pkgconf openssl-dev libxml2-dev \
    lz4-dev zstd-dev bzip2-dev yaml-dev postgresql15-dev curl
COPY docker/build-backup-tools.sh /build-backup-tools.sh
RUN sh /build-backup-tools.sh

FROM timescale/timescaledb:2.14.2-pg15

USER root

RUN apk add --no-cache libxml2 lz4-libs zstd-libs libbz2 yaml libpq
COPY --from=backup-tools /opt/qt-backup-tools/pgbackrest /usr/local/bin/pgbackrest
RUN mkdir -p /var/log/postgresql \
    && chown -R postgres:postgres /var/log/postgresql

COPY docker/postgres/initdb.d/ /docker-entrypoint-initdb.d/
