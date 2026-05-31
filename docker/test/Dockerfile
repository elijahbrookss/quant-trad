# docker/test/Dockerfile
FROM python:3.12.3

ENV PYTHONPATH=/app/src
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

# Install deps with uv's pip-compatible installer. The cache mount keeps local
# rebuilds fast without changing the runtime package set.
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r requirements.txt

# Copy everything needed for testing
COPY . .
COPY scripts/wait-for-db.sh /app/wait-for-db.sh

# Ensure output folders exist
RUN mkdir -p output/integration_tests
RUN chmod +x /app/wait-for-db.sh

# Default CMD: basic tests (can be overridden in GH Actions)
CMD ["pytest", "-v", "tests/"]
