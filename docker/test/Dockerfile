# docker/test/Dockerfile
FROM python:3.12.3

ARG QT_SOURCE_REVISION
ARG QT_SOURCE_TREE_HASH

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

RUN actual_hash="$(python /app/scripts/provenance/source_tree_hash.py --root /app)" \
    && test -n "$QT_SOURCE_REVISION" \
    && test -n "$QT_SOURCE_TREE_HASH" \
    && test "$actual_hash" = "$QT_SOURCE_TREE_HASH" \
    && printf '{"schema_version":"qt_source_attestation.v1","source_revision":"%s","source_tree_hash":"%s"}\n' \
       "$QT_SOURCE_REVISION" "$actual_hash" > /app/.qt-source-attestation.json

ENV QT_IMAGE_SOURCE_REVISION="$QT_SOURCE_REVISION" \
    QT_IMAGE_SOURCE_TREE_HASH="$QT_SOURCE_TREE_HASH"

# Ensure output folders exist
RUN mkdir -p output/integration_tests
RUN chmod +x /app/wait-for-db.sh

# Default CMD: basic tests (can be overridden in GH Actions)
CMD ["pytest", "-v", "tests/"]
