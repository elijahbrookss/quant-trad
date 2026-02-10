FROM timescale/timescaledb:2.14.2-pg15

USER root

RUN mkdir -p /var/log/postgresql \
    && chown -R postgres:postgres /var/log/postgresql

COPY docker/postgres/initdb.d/ /docker-entrypoint-initdb.d/
