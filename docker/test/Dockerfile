# docker/test/Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything needed for testing
COPY . .

# Ensure output folders exist
RUN mkdir -p output/integration_tests

# Default CMD: basic tests (can be overridden in GH Actions)
CMD ["pytest", "-v", "tests/"]
