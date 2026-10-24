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
  - scripts/automation/release_package.py
  - scripts/automation/routine_promotion.py
  - scripts/automation/routine_schema_probe.py
  - scripts/automation/routine_configuration.py
  - scripts/automation/check_release_ci.py
  - scripts/automation/pin_deploy_recovery.py
  - scripts/automation/storage_handoff_pause.py
  - scripts/ci/test_storage_handoff_pause.py
  - scripts/ci/test_storage_database_preparation.py
  - tests/test_storage_handoff_pause.py
  - scripts/ci/test_server_promotion.py
  - scripts/ci/test_server_core_recreation.py
  - docker/docker-compose.storage-server.yml
  - tests/test_server_storage_config.py
  - scripts/ci/deployment_proofs.py
  - scripts/ci/proof_pytest.py
  - scripts/ci/online_proof_selection.json
  - scripts/ci/online_proof_allocation.json
  - scripts/ci/proof_alerts.sh
  - scripts/ci/proof_lifecycle.sh
  - scripts/ci/proof_restore.sh
  - scripts/ci/proof_core.sh
  - tests/test_deployment_proofs.py
  - .github/workflows/test.yaml
  - tests/test_release_ci.py
  - tests/test_server_promotion.py
---
# ADR 0068: Rehearse Explicit Server Promotion

Accepted September 7, 2026 for manually initiated compatible single-node updates.
This decision defines deployment ownership and evidence; it does not activate an
unattended production trigger. [Server deployment](../../engineering/server-deployment.md)
owns supported commands and the current release procedure.

## Qualify the source that will actually run

Local tests and PR review do not qualify the merged release. Explicit promotion
requires the candidate commit and the exact successful develop-push workflow
attempt, including every required expanded proof job. One green wrapper or an
iteration-only CI selection is insufficient.

Serialize mutations with the host deployment lock. Before replacement, retain
the running immutable image identities and private rendered recovery
configuration. Compatible failure recovery uses those artifacts without
rebuilding or pulling substitutes. Failure remains failure even when recovery
succeeds; database contents are never automatically rolled back.

An interrupted or failed recovery retains its durable marker and blocks new
deployment until the owning explicit recovery completes. Compatibility remains
a release-specific decision, not an inference from a filename scan or a healthy
process.

## Rehearsal has bounded authority

Use synthetic services to inject rollout failures into the actual controller,
and actual QT core images on isolated storage to exercise bootstrap, clean
shutdown, health and retained data. Synthetic metadata can test admission
behavior; it cannot become a real release receipt.

Proof runners bind source, run/attempt, image IDs, transported archive checksum,
collected cases and coverage. Image transport is provisional until all required
groups and recovery variants pass aggregation. Parallel scheduling changes where
proofs run, not their coverage, isolation or production concurrency.

A disposable rehearsal does not establish physical disk capacity, real provider
availability, actual backup custody or arbitrary schema compatibility.
[CI topology](../../engineering/testing/ci-test-topology.md) describes the
supported test boundary and retained diagnostics.

## Storage transitions retain their own owner

An incompatible storage switch must not enter compatible promotion recovery.
The storage host procedure records durable intent before pausing publishers,
admits actual containers, mounts, images, networks and filesystem identities,
and retains its hold through authoritative database outcome and runtime recovery.

Hold presence, including corrupt or partial state, blocks ordinary deployment,
rollback and relaunch. Process death and loss of the host lock cannot erase that
refusal. Manual Docker/SQL or an older helper remain privileged bypasses that
operator admission must exclude.

Preparing a PostgreSQL history mount preserves PGDATA, image and cluster identity
through a clean stop and explicitly reconciled replacement. The storage recipe
cannot pull/build another database, create fallback bind directories or select
unowned resources. Uncertain creation, start or COMMIT replies resolve from the
same durable identity; they do not justify repeating mutations.

Source-compatible file ownership preparation is an explicit bounded phase under
publisher exclusion. It preserves bytes and file identity, refuses links/foreign
mounts/owners, then permanently drops privilege before database work. It never
runs as an ordinary startup repair.

Only the complete owning transition may record the applied storage layout,
activate the matching runtime and recovery, and finish its hold. Readiness,
mounts or a successful SQL handoff alone cannot grant those authorities.

## Routine promotion is deliberately narrower than CI iteration

The routine path admits only documentation and exact reviewed presentation
changes while backend read/write, schemas, registries, serialized results,
dependencies, configuration and storage source remain unchanged. It consumes
qualified image IDs exported by the exact develop attempt, pins current
infrastructure and never rebuilds or pulls during activation/recovery.

Admission also needs an operator-qualified backup/restore record and fresh
read-only schema/cluster checks. A checksum proves identity, not restoration.
Private configuration fingerprints bind effective Compose, operator environment,
mounted files and actual runtime inventory before recovery capture and again
before activation. Missing baselines or configuration drift refuse.

Compose configs/secrets without covered payload identity remain unsupported.
A storage-layout deployment retains its external resource bindings, immutable
database image and recorded layout; environment overrides cannot silently revert
them or retain a source-only interlock setting.

The tradeoff is retained image/configuration storage and explicit refusal when
compatibility or outcome is uncertain. This is preferable to a convenient
rollback that could restart an incompatible reader or lose durable state.

See [online storage preparation](0073-prepare-storage-migrations-with-live-collection.md)
and [encrypted paired recovery](0072-use-encrypted-incremental-recovery-sets.md).
