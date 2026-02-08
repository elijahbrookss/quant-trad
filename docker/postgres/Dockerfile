FROM timescale/timescaledb:2.14.2-pg15

USER root

RUN apt-get update \
    && (apt-get install -y --no-install-recommends postgresql-15-system-stats || echo "system_stats package not available") \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/log/postgresql \
    && chown -R postgres:postgres /var/log/postgresql

COPY entrypoint.sh /usr/local/bin/pg-entrypoint.sh
COPY initdb.d/ /docker-entrypoint-initdb.d/

RUN chmod +x /usr/local/bin/pg-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/pg-entrypoint.sh"]
