# Activate cold history on the private rehearsal clone

Run after promotion, private placement binding, and a complete migration audit.
The audit needs the plan and every forward and inverse batch for the exact
retirement prefix. Capture and review the original exact-ID, frozen, and as-of
read evidence at the clone's verified watermark. Run the source benchmarks
before activation and leave benchmark ingest until the post-activation reads
finish.

Use the `qt_bench_*` database through its native private Unix socket. In the
network-disabled capsule, bind the actual private PGDATA and history on the
admitted HDD parent and the seeded candidate archive at the path recorded in
the placement receipt. The archive mount must be writable and the operation
opens its objects for verification only. Keep the clone proof, placement,
execution scope, audit receipts, witness and capacity limits on read-only
mounts. Give the activation a separate fresh path in a private journal
directory. Use the exact native `pg_controldata` from the rehearsal runtime.

The execution scope uses `qt.private_canonical_cold_activate.v1`. Its source
and target are the same clone database. It names the exact accepted commit,
exclusive cutoff, audit and loaded inventory hashes, retiring payload child
OIDs, native raw index and custody move bounds, and original and prepared
identity registry OIDs. It also carries the reviewed clone proof, placement
and witness SHA-256 values. It is distinct from a production approval package.

```sh
python -m scripts.db.canonical_cold_private_activate \
  --proof /inputs/clone.json --proof-sha256 "$CLONE_SHA" \
  --placement /inputs/placement.json --placement-sha256 "$PLACEMENT_SHA" \
  --scope /inputs/scope.json --scope-sha256 "$SCOPE_FILE_SHA" \
  --audit-receipts /inputs/audit.json --audit-receipts-sha256 "$AUDIT_FILE_SHA" \
  --witness /inputs/witness.json --limits /inputs/limits.json --limits-sha256 "$LIMITS_SHA" \
  --pg-controldata "$PG_CONTROLDATA" --journal /private-state/activation.jsonl \
  --max-audit-bytes "$AUDIT_BYTES" --max-archive-objects "$OBJECT_LIMIT" \
  --max-archive-bytes "$ARCHIVE_BYTES" --max-pages "$PAGE_LIMIT" --timeout-seconds "$SECONDS_LIMIT"
```

The command verifies candidate page bytes before closing database logins.
It then checks the original witness, runs the native authority transaction,
and reads the candidate witness in that same transaction. The filesystem and
WAL watch finishes before commit. The prior identity registry remains on the
private copy. PostgreSQL logins reopen only after a confirmed outcome. A
private postmaster configured with zero Timescale workers must continue to
report zero workers through closure, activation, and reopen.

The fsynced journal records the native fence and transaction. If execution
stops with an unresolved outcome, preserve the journal and use its execution
hash and native transaction ID to inspect the private database before retrying.
After a confirmed commit, verify the complete candidate archive to produce the
B1 mount scope and then run the read workloads.
