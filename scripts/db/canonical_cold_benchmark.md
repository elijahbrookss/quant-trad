# Compare reads on the paired offline restore

Run both trials on one Linux host after independent restore and lineage verification, using the restored source database and an isolated candidate after activation. Use the storage media and PostgreSQL settings the comparison is meant to represent. Stop writers, collectors, retention, and other workload on both copies. Use the same script bytes and Python dependencies in both trials. Each trial imports the exact clean QT checkout declared on the command line. Use a private PostgreSQL socket directory with `listen_addresses=''` and a restored `qt_bench_*` database. The connection uses `PG_DSN` and starts every transaction read-only. The connection user needs read access to the selected market tables and permission to execute `pg_control_system()`.

Build one JSON plan from the independently verified paired restore. The `restore_pair_hash` names that external verification record. `commit_seq` is the maximum accepted commit across the whole restored database, including retained hot facts. `retire_before` is the exclusive UTC storage-day cutoff used for candidate activation. `storage_witnesses` are exact accepted fact-version IDs and their original accepted storage days. The runner checks where these identities are stored on each side and requires them to appear in their query results. The plan must witness both retired and retained hot data. A single mixed-window case can witness both. Dates in observation filters can differ from the days used to place accepted facts. Use separate recent, retired, and mixed cases where the restored history supports them.

Example plan shape with placeholder identities, clocks and hashes. Replace them with values from the retained archive and frozen dataset. Add a `latest_as_of` case with the same keys as `all_revisions` to test revision selection. Either mode accepts an optional `as_of_commit_seq` at or below the restored watermark. By default both read as of the watermark. Frozen reads use the dataset's pinned series commit.

```json
{
  "restore_pair_hash": "1111111111111111111111111111111111111111111111111111111111111111",
  "commit_seq": 87654321,
  "retire_before": "2026-08-10",
  "repeats": 20,
  "timeout_seconds": 30,
  "max_rows": 50000,
  "max_archive_bytes": 268435456,
  "cases": [
    {
      "name": "mixed-revisions",
      "mode": "all_revisions",
      "series_id": 1,
      "start": "2026-08-09T00:00:00Z",
      "end": "2026-08-11T00:00:00Z",
      "known_at_lte": "2026-08-11T00:00:00Z",
      "source_identity_keys": [],
      "storage_witnesses": {"retired-fact-id": "2026-08-09", "hot-fact-id": "2026-08-10"}
    },
    {
      "name": "frozen-replay-input",
      "mode": "frozen_revisions",
      "dataset_id": "frozen-dataset-id",
      "dataset_hash": "2222222222222222222222222222222222222222222222222222222222222222",
      "series_id": 1,
      "start": "2026-08-09T00:00:00Z",
      "end": "2026-08-10T00:00:00Z",
      "known_at_lte": "2026-08-11T00:00:00Z",
      "source_identity_keys": [],
      "storage_witnesses": {"retired-fact-id": "2026-08-09"}
    },
    {
      "name": "historical-book-replay",
      "mode": "book_replay",
      "definition_id": "retained-definition-id",
      "session_id": "retained-session-id"
    }
  ]
}
```

From each respective QT checkout, use `MARKET_STRUCTURE_STORAGE_ROOT` for the restored archive root, matching the explicit option. Set `QT_MARKET_DATA_EXPECTED_UUID` when using the mounted production-like device so QT verifies filesystem identity during reads. Keep the writable QT history cache disabled. Provide the matching database OID, PostgreSQL system identifier, Git SHA, and image digest or environment lock digest for each trial. Save outputs to an operator evidence directory. The source application can run the same script from a separate path.

```sh
cd /restore/source-checkout
PYTHONPATH=src:. python /review/canonical_cold_benchmark.py run \
  --side source --plan /review/workload.json --revision "$SOURCE_REVISION" \
  --runtime-digest "$SOURCE_RUNTIME_DIGEST" --system-identifier "$SOURCE_SYSTEM" \
  --database-oid "$SOURCE_OID" --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" \
  > /review/source-read-trial.json

cd /restore/candidate-checkout
PYTHONPATH=src:. python /review/canonical_cold_benchmark.py run \
  --side candidate --plan /review/workload.json --revision "$CANDIDATE_REVISION" \
  --runtime-digest "$CANDIDATE_RUNTIME_DIGEST" --system-identifier "$CANDIDATE_SYSTEM" \
  --database-oid "$CANDIDATE_OID" --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" \
  > /review/candidate-read-trial.json

python /review/canonical_cold_benchmark.py compare \
  --source /review/source-read-trial.json --candidate /review/candidate-read-trial.json
```

Each sample reports application read or replay time and total time including the receipt hash. The first measured read follows scope and placement checks. Later application read times give the repeated p50 and nearest-rank p95. PostgreSQL and OS cache temperatures are uncontrolled. The single-process workload measures serial reads. `process_read_bytes` tracks physical reads attributed to the benchmark process and excludes PostgreSQL backend processes. `archive_counters` are emitted by instrumented QT archive paths. Book replay also reports the raw and checkpoint file sizes. Independent restore evidence, load and ingestion performance, retention, and capacity measurements each need their own receipts.

Book replay verifies the requested session's raw objects, ordered event identities, checkpoints, final book state and derived features through QT. Frozen reads compare canonical revisions under the pinned dataset series watermark. Full raw ordinal inventory, custody holds, coverage, and all frozen lineage references require separate audits. Concurrent ingestion, hardware throughput and SSD/HDD capacity require separate measurements.
