# syntax=docker/dockerfile:1
FROM python:3.12.3-slim

ARG QT_SOURCE_REVISION
ARG QT_SOURCE_TREE_HASH

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:${PYTHONPATH}"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev docker.io \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r /app/requirements.txt

COPY config/ /app/config
COPY src/ /app/src
COPY portal/ /app/portal
COPY scripts/provenance/ /app/scripts/provenance/

RUN actual_hash="$(python /app/scripts/provenance/source_tree_hash.py --root /app)" \
    && test -n "$QT_SOURCE_REVISION" \
    && test -n "$QT_SOURCE_TREE_HASH" \
    && test "$actual_hash" = "$QT_SOURCE_TREE_HASH" \
    && printf '{"schema_version":"qt_source_attestation.v1","source_revision":"%s","source_tree_hash":"%s"}\n' \
       "$QT_SOURCE_REVISION" "$actual_hash" > /app/.qt-source-attestation.json

ENV QT_IMAGE_SOURCE_REVISION="$QT_SOURCE_REVISION" \
    QT_IMAGE_SOURCE_TREE_HASH="$QT_SOURCE_TREE_HASH"

RUN ls -la /app && ls -la /app/src || true


EXPOSE 8000

CMD ["python", "-m", "portal.backend.run_backend"]
