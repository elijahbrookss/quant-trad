# Compare native QT ingestion during historical reads

Use writable, disposable duplicates of the verified source and activated candidate. Keep the original restore pair for read and custody checks. Run both trials on the same Linux host with the same PostgreSQL configuration and Python dependencies. Stop other application writers, collector and retention processes on both duplicates. Only use private Unix sockets and `qt_bench_*` databases. QT reads retain a separate read-only pool and the normal shared lifecycle lease. The writer uses the existing QT canonical ingestion API and retains its normal ownership and publication checks.

Wrap the [read benchmark plan](canonical_cold_benchmark.md) under `reads`. Choose latest, revision and frozen fact reads that include witnesses on both sides of the verified exclusive storage-day cutoff. Add `batches` (8–80), `batch_rows` (1–128), `interval_ms` (0–1000) and `observation_start` (an explicit UTC timestamp). Use a multiple of four batches for full cycles. Each cycle inserts new synthetic funding observations, retries those same facts, corrects the same observations, and retries the corrected facts. The observation timestamp for each new key is `observation_start` plus one second per position. Choose a starting time that places every synthetic observation before the trial. This timestamp controls deterministic observation identity. QT receives its actual acceptance and known-at clocks from the workload process at dispatch. The acceptance clocks can therefore differ between two trials.

Run the read and custody evidence on fresh copies first. These trials add a dedicated synthetic instrument, source, series, and facts to each writable clone. Start from an exact restore watermark with today’s hot storage already open. A repeat trial needs a fresh writable copy. Retain the receipts and discard both trial databases after collecting evidence.

```sh
PYTHONPATH=src:. python /review/canonical_cold_ingest_benchmark.py run \
  --side source --plan /review/mixed-plan.json --disposable-database "$SOURCE_CLONE" \
  --revision "$SOURCE_REVISION" --runtime-digest "$SOURCE_RUNTIME_DIGEST" --system-identifier "$SOURCE_SYSTEM" \
  --database-oid "$SOURCE_OID" --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" \
  > /review/mixed-source.json

PYTHONPATH=src:. python /review/canonical_cold_ingest_benchmark.py run \
  --side candidate --plan /review/mixed-plan.json --disposable-database "$CANDIDATE_CLONE" \
  --revision "$CANDIDATE_REVISION" --runtime-digest "$CANDIDATE_RUNTIME_DIGEST" --system-identifier "$CANDIDATE_SYSTEM" \
  --database-oid "$CANDIDATE_OID" --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" \
  > /review/mixed-candidate.json

PYTHONPATH=src:. python /review/canonical_cold_ingest_benchmark.py compare \
  --source /review/mixed-source.json --candidate /review/mixed-candidate.json
```

Set the matching `PG_DSN` and archive environment in each application's own clean checkout. Keep both operator scripts side by side. The runner records per-call wall latency, write outcomes, and I/O counters emitted by the QT archive reader. It reports p50 and nearest-rank p95 over all calls and separately over calls whose elapsed intervals actually overlap. It fails if there is no measured overlap. Cases without an overlapping observation show that explicitly. Compare sample counts along with each latency estimate.

Each trial verifies actual acceptance clocks, exact stored fact IDs and hashes, ordered commit sequence, persisted writer outcomes and current hot header, payload and identity placement. Paired comparison requires exact pinned historical results and equal synthetic payloads and observations after normalizing out measured acceptance clocks and local identities. The isolated exercise covers one QT canonical writer and one concurrent historical reader. Provider polling, fenced live stream collection, wire latency, concurrent collector fleets, maintenance traffic, and full-device throughput require separate workloads.
