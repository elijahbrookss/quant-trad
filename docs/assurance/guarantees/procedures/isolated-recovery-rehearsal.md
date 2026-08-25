# Isolated Recovery Rehearsal

## Purpose And Authority Ceiling

This procedure defines the evidence required by `QT-PROOF-014`. It is an
assurance procedure, not a production runbook, product contract, proof result,
remediation closure, or guarantee-activation decision.

The rehearsal may use only disposable, non-production resources. A production,
live, shared-development, or externally managed database is never an acceptable
source or restore target. Missing prerequisites produce `UNAVAILABLE`; they do
not justify substituting a less isolated environment.

## Admission Manifest

Before any command runs, record a manifest containing:

- the exact clean source commit and attestation session ID;
- operator identity and, when a final PASS or FAIL is requested, a distinct
  independent reviewer identity;
- separately generated source and restore-target identities;
- host, container image, image digest, PostgreSQL, TimescaleDB, backup-tool,
  and restore-tool versions;
- the source and target network boundaries and an assertion that neither is a
  production, live, or shared-development resource;
- the approved fixture definition and expected representative records;
- the exact commands that will create, back up, restore, validate, and clean up
  the resources; and
- start time, maximum duration, and cleanup owner.

The source and target identities must differ. The target must be empty before
restore. The manifest must fail admission if either identity is missing, if the
target is not empty, or if the environment classification is ambiguous.

## Representative Source

Create a new disposable source containing only synthetic fixtures. The fixture
must exercise:

- the required `timescaledb` and `pgcrypto` extensions;
- current application schema creation from the bound source commit;
- representative relational tables, constraints, indexes, triggers, and
  time-series data;
- stable row counts and content digests for the selected representative data;
- one checksummed archive-move candidate; and
- one independently identified active retention pin.

Record the source database identity, schema inventory, extension versions, row
counts, content digests, and fixture-generation output before backup.

## Backup And Restore

1. Produce a logically consistent backup from the disposable source with the
   admitted backup tool and exact recorded arguments.
2. Record the backup path or object identity, byte size, and SHA-256 digest.
3. Verify the digest before restore.
4. Restore only into the separately identified empty disposable target using
   the admitted restore tool and exact recorded arguments.
5. Record command start/end times, exit codes, stdout, and stderr.

A checksum mismatch, non-empty target, missing extension, or source/target
identity collision must stop the positive path. The operator may preserve the
failed disposable resources long enough to collect evidence, but must not
continue as though the precondition passed.

## Positive Validation

After restore, capture and compare:

- PostgreSQL and extension versions;
- schema, table, column, constraint, index, and trigger inventories;
- representative row counts and content digests;
- time-series reads over the seeded interval;
- application-level reads through the bound repository code; and
- the restored backup identity and digest.

All selected comparisons must be exact or have an explicit reviewed tolerance
defined in the fixture before execution. A later interpretation cannot weaken a
failed comparison.

## Negative Destructive-Operation Cases

Using disposable copies only:

1. alter a copy of the archive-move candidate so its observed checksum differs
   from the recorded checksum and verify that deletion remains blocked;
2. restore an unmodified candidate and verify that matching evidence is still
   required before deletion can proceed; and
3. exercise the active-retention-pin case and verify that the pin blocks
   deletion even when the object checksum matches.

Record object identities, expected and observed checksums, pin identity, exact
commands or test selectors, and the resulting retained/deleted state. These
negative cases must not target an object outside the disposable fixture.

## Cleanup Evidence

After evidence capture, remove the exact source and target resources named in
the admission manifest. Record cleanup start/end times, exit state, and a final
inventory showing that both resources and any temporary backup object are
absent. Cleanup failure is reported; it is never omitted from the evidence.

## Result Semantics

- `PASS` requires all positive and negative checks, cleanup evidence, and
  independent reviewer acceptance.
- `FAIL` records a contradiction of an acceptance criterion and requires
  independent reviewer acceptance of the failure evidence.
- `MANUAL` records a completed or attempted rehearsal whose evidence has not
  received the independent acceptance required for PASS or FAIL.
- `PARTIAL` records an attempted automated or manual portion with incomplete
  required coverage and no observed contradiction.
- `UNAVAILABLE` identifies a concrete missing admission prerequisite, such as
  the separate disposable source, restore target, representative fixture, or
  required tool/version.
- `NOT_RUN` means no rehearsal step was attempted.

No result changes guarantee activation. `QT-REM-009` remains open until its
acceptance and review requirements are separately evaluated.

## Required Evidence Bundle

The proof-scoped evidence bundle must contain the admission manifest, exact
commands, timestamps, tool and service identities, backup checksum, source and
target inventories, comparison results, application-read output, negative-case
results, cleanup evidence, operator identity, and any independent review. Every
artifact is hashed and bound to the exact attestation session and source commit.
