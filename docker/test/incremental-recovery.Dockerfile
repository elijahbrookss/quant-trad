# Disposable physical-backup proof using the same Timescale/PG major as QT.
# No production volume, network, credentials or configuration are required.
FROM timescale/timescaledb:2.14.2-pg15 AS tools
USER root
RUN apk add --no-cache build-base meson ninja pkgconf openssl-dev libxml2-dev \
    lz4-dev zstd-dev bzip2-dev yaml-dev postgresql15-dev curl
WORKDIR /build
RUN curl --fail --location --retry 3 \
      https://github.com/pgbackrest/pgbackrest/archive/refs/tags/release/2.59.1.tar.gz \
      --output pgbackrest.tar.gz \
    && echo 'ca1e75c7490989a2fb39b8266c0f3c518dd3c873ddc0c3a346ca9b5dccc16455  pgbackrest.tar.gz' | sha256sum -c - \
    && tar -xzf pgbackrest.tar.gz \
    && meson setup --buildtype=release build pgbackrest-release-2.59.1 \
    && ninja -C build -j 2
RUN curl --fail --location --retry 3 \
      https://github.com/restic/restic/releases/download/v0.19.1/restic_0.19.1_linux_amd64.bz2 \
      --output restic.bz2 \
    && echo 'f415415624dcc452f2a02b8c33641791a8c6d6d3b65bbb3543fcf9a25151585c  restic.bz2' | sha256sum -c - \
    && bunzip2 restic.bz2 && chmod 0755 restic

FROM timescale/timescaledb:2.14.2-pg15
USER root
RUN apk add --no-cache python3 libxml2 lz4-libs zstd-libs bzip2-libs yaml libpq
COPY --from=tools /build/build/src/pgbackrest /usr/local/bin/pgbackrest
COPY --from=tools /build/restic /usr/local/bin/restic
COPY scripts/ci/rehearse_incremental_recovery.py /opt/qt/rehearse_incremental_recovery.py
USER postgres
WORKDIR /tmp
ENTRYPOINT ["python3", "/opt/qt/rehearse_incremental_recovery.py"]
CMD ["--pg-bin", "/usr/local/bin", "--pgbackrest", "/usr/local/bin/pgbackrest", "--restic", "/usr/local/bin/restic", "--require-timescale"]
