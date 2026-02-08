FROM timescale/timescaledb:2.14.2-pg15

USER root

RUN if command -v apt-get >/dev/null 2>&1; then \
      apt-get update \
      && (apt-get install -y --no-install-recommends postgresql-15-system-stats || echo "system_stats package not available") \
      && rm -rf /var/lib/apt/lists/*; \
    elif command -v apk >/dev/null 2>&1; then \
      apk add --no-cache postgresql15-system-stats || echo "system_stats package not available"; \
    else \
      echo "system_stats package not available"; \
    fi

RUN mkdir -p /var/log/postgresql \
    && chown -R postgres:postgres /var/log/postgresql

COPY docker/postgres/entrypoint.sh /usr/local/bin/pg-entrypoint.sh
COPY docker/postgres/initdb.d/ /docker-entrypoint-initdb.d/

RUN chmod +x /usr/local/bin/pg-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/pg-entrypoint.sh"]
