FROM timescale/timescaledb:2.14.2-pg15

USER root

RUN apk add --no-cache postgresql15-system-stats || echo "system_stats package not available"

RUN mkdir -p /var/log/postgresql \
    && chown -R postgres:postgres /var/log/postgresql

COPY docker/postgres/entrypoint.sh /usr/local/bin/pg-entrypoint.sh
COPY docker/postgres/initdb.d/ /docker-entrypoint-initdb.d/

RUN chmod +x /usr/local/bin/pg-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/pg-entrypoint.sh"]
