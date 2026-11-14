# Native cold lookup scale fixture

From the repo root, run the fixture with the QT Python environment and local PostgreSQL server tools installed. The runner creates its own Unix-socket-only PostgreSQL cluster and the database and tablespace it will populate. It ignores the existing `PG_DSN`. Scratch data lives under a newly allocated `/tmp/qt-native-cold-scale-*` directory. The runner drops the generated database and tablespace, then stops the server and removes the scratch directory. It records the affected path on stderr. It leaves the JSON receipt on stdout. Use a nonroot OS user and an isolated host with at least 5 GiB free on the local filesystem throughout the run.

```sh
bash scripts/db/canonical_cold_scale.sh --rows 1000000 --page-rows 10000 \
  --repeats 24 --max-seconds 4200 > /review/native-cold-scale.json
```

The fixture creates synthetic funding-rate rows across 16 series, with two revisions per observation and a deterministic invalidation subset. It uses the native canonical Fact codec, lossless Parquet publication and readback, PostgreSQL catalog creation with registered lookup placement, and `PostgresColdHistory.admit`. Each page commits its lookup updates before the simulated retirement receipt makes it visible. The separate audit regenerates every input and checks every locator's exact fact ID, hash, key digest, revision, series, page ordinal and commit. It also compares per-page series metadata and checks the inverse locator count. Seeded sparse reads use the same native lookup methods and exact Parquet selection code as the cold repository. The first query against any given file may already benefit from publication and the full readback audit.

The receipt reports relation and index allocation, compressed object bytes, PostgreSQL WAL inserted during the isolated publication, and elapsed page timings for native encode/publish, checked catalog admission and audit. It measures only the simulated day visibility update. The fixture does not exercise source cohort scans, a real full-day retirement, custody generation or HDD/SSD contention. The PostgreSQL tablespaces and files share one fixture device. Hash and byte totals describe this input exactly. Use a representative paired restore and separate physical devices to measure deployment throughput and capacity.
