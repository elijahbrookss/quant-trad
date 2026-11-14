# Raw archive primary index rehearsal

Run both trials on the same isolated Linux host against two fresh database and object-store copies from the same verified restore seed. Keep the raw mapping heap and its other indexes, archive objects, live spool, PostgreSQL settings and runtime fixed. Use the existing placement operator separately to put the mapping primary index on SSD in one copy and HDD in the other. The runner observes index placement and never moves it.

Specify historical raw IDs from the restored seed. Include IDs representative of all-age and compacted lookup use. The exact source bytes, object hashes, mapping order, and all live placements returned by the production raw lineage SQL must match in both copies. A missing record, changed mapping, timed run without real commit/lookup overlap, retry that inserts a mapping, or nonempty final spool fails the run. The SHA-256 digest of the restore receipt identifies the input, and the operator still needs to verify the restore itself.

Plan example:

```json
{
  "seed_sha256": "<64 hex characters from the restore receipt>",
  "historical_record_ids": ["raw_<64 hex characters from the restored catalog>"],
  "segments": 80,
  "rows_per_segment": 256,
  "backlog_segments": 8,
  "records_per_second": 1000,
  "historical_lookup_rows": 1,
  "historical_lookups": 3000,
  "retry_every_segments": 10,
  "timeout_seconds": 300
}
```

Pick the rate, segment size, historical witness mix, lookup batch size, and retry interval from observed collector traffic. A rate of zero drives spool input as quickly as local writes permit. Keep enough historical samples to cover the required write interval. The report counts measured overlaps.

Use QT's configured roots and a private Unix-socket-only PostgreSQL cluster with a unique `qt_bench_*` database name. Use the exact OID, system identifier and current fact layout of that restore. Run from a clean application checkout. Supply both archive and working UUIDs through the environment used by QT and through the explicit trial arguments. Place the working root on SSD and the archive root on HDD. Physical paths must be visible in the same mount namespace as the runner and database, including the PostgreSQL data directory and the tablespace symlink destination.

```bash
PYTHONPATH=src:. python -m scripts.db.canonical_cold_raw_benchmark run \
  --plan raw-workload.json --seed-receipt independent-restore.json \
  --variant ssd --disposable-database qt_bench_raw_ssd \
  --system-identifier "$RESTORE_SYSTEM_IDENTIFIER" --database-oid "$RESTORE_DATABASE_OID" \
  --revision "$QT_APPLICATION_SHA" --runtime-digest "$QT_RUNTIME_IMAGE_DIGEST" \
  --layout "$RESTORED_FACT_LAYOUT" --index-tablespace "$INDEX_TABLESPACE" \
  --index-mount "$INDEX_MOUNT" --index-uuid "$INDEX_UUID" \
  --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" --archive-uuid "$QT_MARKET_DATA_EXPECTED_UUID" \
  --working-root "$MARKET_STRUCTURE_WORKING_ROOT" --working-uuid "$QT_MARKET_DATA_WORKING_EXPECTED_UUID" \
  > raw-ssd.json
# Repeat with the independently restored HDD variant and its own identity.
PYTHONPATH=src:. python -m scripts.db.canonical_cold_raw_benchmark compare \
  --ssd raw-ssd.json --hdd raw-hdd.json > raw-comparison.json
```

The writer uses native stream leases, per-frame spool fsync, verified Parquet publication, the archive manifest and mapping transaction, and durable spool acknowledgement and release. Synthetic frames exercise both trade and L2 channel namespaces and carry a deterministic payload. They do not traverse provider parsers or canonical trade and book derivation. Qualify ordered frozen research, canonical trade, L2 and custody parity separately on these restored copies.

The read lane executes the production SQL for exact raw IDs and validates the ordered results against the pretrial catalog. Before and after the timed workload, it verifies the complete selected objects and mapping ordinals. The final audit verifies every new row, including the raw ID, its manifest membership, exact object position and frame hash. Native commit, verified object upload, retry and spool release each have sample counts, p50, p95 and p99. The report includes outer and publication-window throughput, spool bytes sampled after awaiting each append, sealed work awaiting enqueue or consumption, producer backpressure, and zero final backlog. Peak RSS and process I/O refer to the measurement process. PostgreSQL I/O counters can lag at the end of the interval. Linux block-device counters are shared with other processes. WAL position and resident bytes are endpoint readings. Measure peak resident WAL with the separate storage pressure observer during the deployment rehearsal.

The single-device smoke flag permits testing the command with a co-located fixture. The comparison carries the smoke flag through in its result. Scale acceptance requires the real mount run and separate measurement of the physical move and recovery.
