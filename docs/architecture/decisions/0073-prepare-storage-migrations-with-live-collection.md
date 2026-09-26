---
component: adr-online-storage-migration
subsystem: persistence
layer: decision
doc_type: adr
status: draft
tags:
  - adr
  - storage
  - migration
code_paths:
  - scripts/db/fact_header_v2_online.py
  - scripts/db/fact_header_v2_online_proof.py
  - scripts/db/fact_header_v2_capture.py
  - scripts/db/fact_header_v2_copy.py
  - scripts/db/raw_mapping_v2_copy.py
  - scripts/db/fact_header_v2_handoff.py
  - scripts/automation/storage_handoff_pause.py
  - tests/test_market_data/test_fact_header_online_db.py
  - tests/test_market_data/test_fact_header_online_proof_db.py
---
# ADR 0073: Prepare storage migrations with live collection

Proposed September 26, 2026 after the user rejected a multi-day collector outage.
This decision records the required replacement workflow. Bounded copying and protected exact SQL page proofs
are implemented; a complete online operator and short final switch
remain unqualified. The existing whole-migration host hold is not the authorized
production release path.

## Problem

The fixed v1-to-v2 migration has transactional insert capture and resumable
copies, but its host operator holds clients throughout preparation. Final
verification also scans all header, identity, raw lookup and archive content
under writer fences. The saved constrained-fixture projection is over 50 hours
for only some phases. Increasing the database increases this outage.

Removing the host hold alone is insufficient. Verification, reference
relocation, archive publication, source/runtime compatibility and recovery
activation also need explicitly qualified concurrency boundaries.

## Decision

Retain one authoritative serving source throughout preparation. Prepare the
existing fixed SSD/HDD destination with an explicit operator; application startup
must not migrate it. Preserve current, historical, correction and frozen reads.

Separate elapsed migration work from collector downtime:

1. Install transactionally committed capture at a short, nonwaiting writer
   boundary. Corrections remain appended immutable revisions. An allocated
   sequence watermark cannot substitute for committed change capture.
2. Copy a finite baseline in bounded transactions. Commit each verified page
   with its durable cursor. Interruption retains committed progress and leaves
   the source authoritative. Process headers and raw lookup catch-up fairly;
   an endless header tail must not prevent raw lookup work.
3. Qualify exact background verification whose proof remains valid under later
   writes. Track or reject changes that invalidate a checked range. Counts,
   sampled hashes, a stale empty queue, and an earlier scan alone cannot prove
   final equality. Complete large reference and archive work while collecting.
4. Admit a final switch only when the remaining delta, locks, reference work,
   archive reconciliation and runtime activation fit a measured short outage
   budget. Stop/drain publishers, reconcile the bounded committed tail, switch
   atomically, and restart the matching runtime. Exceeding the pre-switch
   budget must leave the original source serving through a qualified abort.
   Once the switch commits, use explicit outcome reconciliation rather than
   blindly starting the old runtime.
5. Continue regular history placement and incremental recovery under their
   existing policy, resource guards and maintenance ownership. This is a fixed
   migration workflow, not a generic placement or rebalancing framework.

Total duration and final pause have separate reported budgets. Every retry uses
the original saved attempt deadline; no expired capture is revived. The present
96-hour ceiling is not a promised completion time. Any further prospective
change requires evidence and explicit bounded admission.

Online capacity must account for continuing source/target growth, transactional
capture backlog, WAL archival, temporary/index allocations, retained originals,
and initial/replacement encrypted baselines. The previous held-writer growth
sensitivity is not an online capacity approval. Preserve the saved reserve and
retention. Slow or nonconverging catch-up prevents switch eligibility rather than
stopping collection indefinitely.

## Implemented slice

The internal fact_header_v2_online.copy_pass accepts an already prepared,
physically bound copy and its existing resource limits. It commits a bounded
number of pages, finishes finite baselines before live tails, and alternates the
two tails. Private identity/raw relocation remains a separately admitted phase
boundary, preserving the tested SSD allocation order. The immutable retained
rollback table must already be observed on HDD when it exists.

The pass reuses exact page comparison, source change queues, filesystem
inspection, resource cancellation and the original capture clock. Its report
always says migration_ready=false and final_switch_authorized=false.
Observing both queues empty in separate transactions is explicitly not an atomic
readiness observation.

The opt-in fact_header_v2_online_proof is installed before the first copied
row, only on empty fixed shadows. Header, identity and raw inserts must exactly
match their authoritative source row. Row updates/deletes and parent/direct-leaf
truncation are refused. Routing can only expand. Immutable partition metadata,
recorded leaf object identities and exact function/trigger/definition checks
preserve each committed page's proof. Existing source immutability, transactional
capture, exact page comparison and cursor/queue retirement establish coverage;
the saved row counts report that proof, rather than substitute for comparison.

At the final source/shadow fence, completed baselines and empty committed queues
close coverage. Bounded partition/catalog/physical checks replace rereading all
header, identity and raw rows. The raw verifier accepts only the live enclosing
header proof context. The existing SQL switch removes temporary guards in that
same transaction; a failed switch restores both guards and old serving source.
An unprotected older shadow keeps the original full verifier and cannot be
retroactively certified. Privileged operators must not disable protections or
edit progress; this is not protection against a malicious database owner.

This slice does not install a production supervisor, expose a new user command,
remove the archive final scan or authorize the old host hold. Additional indexed
source lookups for protected inserts need measured throughput and collector/query
impact; earlier unchanged-copy projections do not qualify that new cost.
Complete reference/archive preparation and a measured end-to-end short switch
remain required. This is implementation work, not a new user approval request.

## Required evidence

Use disposable actual QT data with current/correction/frozen/archive readers,
concurrent publication, out-of-order transaction commits, controller/process
interruption, lost commit replies and same-attempt resume. Test proof invalidation,
bounded locks, nonconverging tails and abort before the final switch. Measure
concurrent collector progress and disk/WAL pressure separately from copy speed.
A complete rehearsal must include the matching runtime restart and encrypted
paired recovery publication.

The user-waived full-size logical restore remains deferred and unverified. It is
not reintroduced here as a gate. Preserve the completed backup, partial restore,
keys and historical failed/canceled receipts.
