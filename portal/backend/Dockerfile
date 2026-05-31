# syntax=docker/dockerfile:1
FROM python:3.12.3-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:${PYTHONPATH}"

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /uvx /bin/

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev docker.io \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r /tmp/requirements.txt

COPY config/ /app/config
COPY src/ /app/src
COPY portal/ /app/portal

RUN ls -la /app && ls -la /app/src || true


EXPOSE 8000

CMD ["python", "-m", "portal.backend.run_backend"]
