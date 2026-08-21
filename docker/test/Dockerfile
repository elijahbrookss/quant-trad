# docker/test/Dockerfile
FROM python:3.12.3

ENV PYTHONPATH=/app/src
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

# The supported DB test runner gates pytest on pg_isready. Keep that probe in
# the image so a healthy disposable database cannot be misreported as absent.
RUN apt-get update \
    && apt-get install --no-install-recommends -y postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install deps with uv's pip-compatible installer. The cache mount keeps local
# rebuilds fast without changing the runtime package set.
COPY requirements.lock .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-deps -r requirements.lock \
    && python -m pip check

# Copy everything needed for testing
COPY . .
COPY scripts/wait-for-db.sh /app/wait-for-db.sh

# Ensure output folders exist
RUN mkdir -p output/integration_tests
RUN chmod +x /app/wait-for-db.sh

# Default CMD: basic tests (can be overridden in GH Actions)
CMD ["pytest", "-v", "tests/"]
