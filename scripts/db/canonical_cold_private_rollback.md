# Rehearse rollback after the private candidate has accepted new writes

Complete the source and candidate read benchmarks first. Run the reviewed
private ingestion workload. Close its worker, reconcile its pending raw event
files, and record the candidate's last accepted commit. The rollback command
will fence that exact commit. Restart the original-layout donor at the
pre-cutover point with the stopped-copy and suffix receipts.

Run inside the promoted candidate's isolated PostgreSQL process namespace.
Mount the candidate's PGDATA, native tablespace and private Unix socket at the
paths recorded in the placement proof. Bind the donor workspace read-only at
`/qt-rollback-donor`. Mount both archive views read-only. They may share a
configured root on the same HDD. For separate mounts, leave the generic
`QT_MARKET_DATA_EXPECTED_UUID` unset in this private worker. Its explicit
clone and donor guards verify the exact device, paths, and inodes. Keep the
journal in a separate private, writable directory on that HDD. The command
uses the journal directory for temporary binary COPY files.

Review one request file using schema
`qt.private_canonical_cold_rollback_request.v1`. Set `latest_commit` to the
last accepted post-cutover commit and `recurring` to the exact day to manifest
digest map for days retired after activation. Set `stream_ids` to the reviewed
raw stream definition IDs and `witness_cases` to the exact-ID, frozen revision,
and as-of selections. Include the new candle and raw trade among the IDs.
Provide native copy, witness, and archive limits in `limits` using the
`RollbackLimits` fields. Use the shared activation capacity fields in
`capacity` and set `relation_bytes` to the reviewed peak relation allocation
for the reconstruction. Include the largest binary COPY file and other worker
allocations in `capacity.other_transaction_bytes`. Use the observed native
WAL, filesystem and archive measurements for these limits.

```sh
python -m scripts.db.canonical_cold_private_rollback \
  --proof /review/clone.json --proof-sha256 "$CLONE" \
  --placement /review/placement.json --placement-sha256 "$PLACEMENT" \
  --scope /review/activation-scope.json --scope-sha256 "$SCOPE" \
  --audit-receipts /review/audit-receipts.json --audit-receipts-sha256 "$AUDIT" \
  --seal-receipts /review/seal-receipts.json --seal-receipts-sha256 "$SEALS" \
  --request /review/rollback.json --request-sha256 "$ROLLBACK" \
  --copy /review/benchmark-copy-ready --copy-sha256 "$COPY" \
  --suffix /review/suffix-ready --suffix-sha256 "$SUFFIX" \
  --donor-mount /qt-rollback-donor --donor-archive "$DONOR_ARCHIVE" \
  --donor-tablespace qt_history_hdd-01 --pg-controldata "$PG_CONTROLDATA" \
  --journal /private-state/rollback.jsonl --max-audit-bytes "$AUDIT_BYTES" \
  --timeout-seconds "$REVIEWED_SECONDS"
```

The command captures the candidate's accepted boundary, stream custody, and
next sequence value. It projects the new direct-cold records back to SQL,
reconstructs the audited legacy and dated cohorts from the donor, restores
later retired days, restores custody and indexes, and repeats the data and
stream witnesses before committing. A private seal receipt binds any date
that remained open on the donor. The log records the native transaction ID
and WAL position before commit. Reopen is conditional on the durable layout
and its receipt matching the last accepted commit. Verify the old release can
start and accept its first isolated write at the recorded next sequence value.
If a native connection is lost and the outcome cannot be verified, preserve
the journal and the fence for private reconciliation.
