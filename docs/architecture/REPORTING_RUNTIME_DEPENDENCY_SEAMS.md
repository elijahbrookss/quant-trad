---
component: reporting-runtime-dependency-seams
subsystem: reporting
layer: service
doc_type: architecture
status: active
tags:
  - reporting
  - runtime
  - seams
code_paths:
  - portal/backend/service/reports/report_service.py
  - portal/backend/service/bots/container_runtime.py
  - portal/backend/service/bots/runtime_composition.py
---

# Reporting Runtime Dependency Seams

Reporting remains a downstream collaborator of runtime execution.

## Seam Guidance

- Runtime orchestration emits canonical runtime artifacts/events first.
- Reporting derives from persisted runtime outputs and should not drive runtime decisions.
- Wiring points for report persistence should be explicit at composition/runtime orchestration boundaries, not hidden in unrelated imports.
