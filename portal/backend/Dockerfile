# syntax=docker/dockerfile:1
FROM python:3.12.3-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src:${PYTHONPATH}"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev docker.io \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src/ /app/src
COPY portal/ /app/portal

RUN ls -la /app && ls -la /app/src || true


EXPOSE 8000

CMD ["python", "-m", "portal.backend.run_backend"]
