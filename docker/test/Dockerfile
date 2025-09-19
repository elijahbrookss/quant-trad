# docker/test/Dockerfile
FROM python:3.12.3

ENV PYTHONPATH=/app/src
WORKDIR /app

# Upgrade pip (and build helpers), then install deps
COPY requirements.txt .
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Copy everything needed for testing
COPY . .
COPY scripts/wait-for-db.sh /app/wait-for-db.sh

# Ensure output folders exist
RUN mkdir -p output/integration_tests
RUN chmod +x /app/wait-for-db.sh

# Default CMD: basic tests (can be overridden in GH Actions)
CMD ["pytest", "-v", "tests/"]
