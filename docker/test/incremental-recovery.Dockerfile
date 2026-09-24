# Disposable physical-backup proof using the same Timescale/PG major as QT.
# No production volume, network, credentials or configuration are required.
FROM timescale/timescaledb:2.14.2-pg15 AS tools
USER root
RUN apk add --no-cache build-base meson ninja pkgconf openssl-dev libxml2-dev \
    lz4-dev zstd-dev bzip2-dev yaml-dev postgresql15-dev curl
COPY docker/build-backup-tools.sh /build-backup-tools.sh
RUN sh /build-backup-tools.sh

FROM timescale/timescaledb:2.14.2-pg15
USER root
RUN apk add --no-cache python3 libxml2 lz4-libs zstd-libs libbz2 yaml libpq
COPY --from=tools /opt/qt-backup-tools/pgbackrest /usr/local/bin/pgbackrest
COPY --from=tools /opt/qt-backup-tools/restic /usr/local/bin/restic
COPY scripts/ci/rehearse_incremental_recovery.py /opt/qt/rehearse_incremental_recovery.py
USER postgres
WORKDIR /tmp
ENTRYPOINT ["python3", "/opt/qt/rehearse_incremental_recovery.py"]
CMD ["--pg-bin", "/usr/local/bin", "--pgbackrest", "/usr/local/bin/pgbackrest", "--restic", "/usr/local/bin/restic", "--require-timescale"]
