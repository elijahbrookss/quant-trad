# Agent Guidelines

## Logging
- Logging exists primarily for Grafana dashboards—keep entries concise, structured, and human-readable.
- Include tracking or correlation identifiers whenever flows span external services or asynchronous tasks.
- Prefer contextual metadata (service name, action, outcome) over raw dumps; avoid logging sensitive data.

## Comments
- Comment only when needed for clarity and future maintenance.
- Each function should have a brief summary of its purpose; complex loops or algorithms may include a high-level note plus any external services or components touched.
- Remove redundant or outdated comments and favour explanations of intent over step-by-step narration.
