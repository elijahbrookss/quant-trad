# syntax=docker/dockerfile:1
FROM python:3.12.3-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:${PYTHONPATH}"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev docker.io postgresql-common \
    && printf 'create_main_cluster = false\n' > /etc/postgresql-common/createcluster.conf \
    && apt-get install -y --no-install-recommends postgresql-15 postgresql-client-15 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements.lock /app/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-deps -r /app/requirements.lock \
    && python -m pip check

COPY config/ /app/config
COPY src/ /app/src
COPY cli/ /app/cli
COPY portal/ /app/portal
COPY scripts/qt /app/scripts/qt
COPY scripts/provenance/ /app/scripts/provenance/
# Explicit operator entry points only; no startup migration or manual SQL bundle.
COPY scripts/db/fact_header_v2_handoff.py \
     scripts/db/fact_header_v2_capture.py \
     scripts/db/fact_header_v2_copy.py \
     scripts/db/fact_header_v2_admission.py \
     scripts/db/fact_header_v2_placement.py \
     scripts/db/fact_header_v2_references.py \
     scripts/db/raw_mapping_v2_copy.py \
     scripts/db/archive_root_v2_copy.py \
     scripts/db/archive_reference_v2_placement.py /app/scripts/db/

# The held worker performs only the bounded legacy ownership entry point,
# then permanently drops to the database UID before opening a connection.
COPY scripts/automation/storage_host_prepare.py \
     scripts/automation/storage_device_audit.py /app/scripts/automation/

# The opt-in fixed storage layout uses the database UID. Application scratch
# paths remain usable without changing ownership of source code or host data.
RUN install -d -o 70 -g 70 -m 0750 /app/logs /app/reports

RUN chmod +x /app/scripts/qt

# Revision changes invalidate attestation, not unchanged dependency installs.
ARG QT_SOURCE_REVISION
ARG QT_SOURCE_TREE_HASH

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

# Exercise the deployable code and tools in the existing disposable storage
# topology. Test-only files never enter the default production target.
FROM runtime AS storage-test
COPY tests/ /app/tests/
COPY scripts/ /app/scripts/
COPY pytest.ini pyproject.toml /app/
RUN chmod +x /app/scripts/wait-for-db.sh

FROM runtime AS production
