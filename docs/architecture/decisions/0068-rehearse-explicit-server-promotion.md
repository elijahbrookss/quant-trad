---
component: adr-rehearsed-server-promotion
subsystem: system
layer: decision
doc_type: adr
status: accepted
tags:
  - adr
  - deployment
  - recovery
code_paths:
  - scripts/automation/server_deploy.sh
  - scripts/automation/check_release_ci.py
  - scripts/automation/pin_deploy_recovery.py
  - scripts/ci/test_server_promotion.py
  - scripts/ci/test_server_core_recreation.py
  - .github/workflows/test.yaml
  - tests/test_release_ci.py
  - tests/test_server_promotion.py
---
# ADR 0068: Rehearse Explicit Server Promotion

## Status

Accepted on 2026-09-07 for manually initiated, compatible single-node updates.

## Context

The deployment helper already builds exact-commit application images and checks
health, initialization, and image identity. Previously a failed update could
leave partially replaced services, and overlapping helper invocations could
change the same checkout. Local passing tests and PR review alone do not qualify
the actual merged commit or establish that restoring old code remains safe.

## Decision

Keep one operator deployment helper. Add an explicit `promote` action requiring
a full candidate SHA, exact successful develop push CI, and an operator's
compatibility acknowledgement naming the currently deployed SHA. Check every
required job in the same latest run attempt, including disposable rehearsal.
Serialize helper mutations using one host lock.

Before replacement, retain the running image identities under private recovery
tags and save the rendered Compose configuration. Recover compatible failures
with those local artifacts without rebuilding, fetching, or pulling. Never
roll back database contents automatically. Failed promotion remains a failed
operation even when recovery succeeds. An interrupted or failed recovery keeps
its marker and blocks new deployments until explicit `recover` succeeds.

Use two evidence layers: synthetic Docker services to inject deployment
failures into the real controller, and actual QT core images on isolated storage
to exercise bootstrap, clean collector stop/restart, health, and retained data.
No provider enrollment, real credentials, runtime egress, production volumes,
or remote host is admitted by these rehearsals.

## Consequences

An operator or agent still chooses when to deploy. No automatic merge trigger,
new always-running environment, or GitHub-to-host credential is required.
Compatibility is a release-specific decision, not inferred from tests or a
migration filename scan. Initial installation and incompatible schema/storage
cutovers remain explicit operator procedures. The existing `deploy` action is
an intentional manual escape hatch, not proof of CI qualification.

Retained images consume disk until deliberately retired; private snapshots may
contain resolved secrets and must not be published. Host loss, disk failure,
active provider reconnect, broker/observability behavior, and arbitrary schema
compatibility are outside the disposable promotion evidence. The first host
cutover still requires actual-state admission and post-deploy acquisition checks.

See [server deployment](../../engineering/server-deployment.md) for commands,
rehearsal scope, and first-cutover requirements.
