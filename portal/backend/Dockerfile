# syntax=docker/dockerfile:1
FROM python:3.12.3-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY portal /app/portal

EXPOSE 8000

CMD ["uvicorn", "portal.backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
