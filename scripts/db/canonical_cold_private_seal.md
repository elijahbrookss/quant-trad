# Seal one day on the private rehearsal database

Use this when the restored catalog still shows a dated cohort as `open` and
migration needs the exact sealed row count. The paused restore stays available
as the unchanged replay reference. Seal only its verified, promoted benchmark
copy before benchmark ingest changes the accepted commit watermark.

Use the `benchmark-clone-verified` receipt from the private copy's archive
verification and its reviewed SHA-256. Mount the same archive `objects`
directory read-only. Use the private Unix socket backed by the promoted
postmaster and the `qt_bench_*` database named in its promotion receipt. Query
the dated header and payload child OIDs from the restored copy and pin both.
For example, a reviewed rehearsal of October 14 can use these values if the
restored copy independently shows them.

```sh
export PG_DSN='postgresql+psycopg2://quanttrad@/qt_bench_gkikfhwg?host=/qt-work/control/socket&port=5432'
python -m scripts.db.canonical_cold_private_seal \
  --proof /qt-receipts/benchmark-clone-verified --proof-sha256 "$REVIEWED_SHA" \
  --day 2026-10-14 --expected-state open --header-oid 70880790 --payload-oid 70880848 \
  --archive-object-root /suffix/objects --max-seconds 180 --count-statement-timeout-ms 60000
```

The command checks the promoted copy against the verified socket, database,
postmaster, replay LSN, storage certificate, and accepted commit. It rechecks
both attached children and applies the clone's saved hot window and existing
family eligibility check. The normal seal operation counts headers and payloads
under separate deadlines and commits only if their counts match. The receipt
records the exact count, sealed timestamp, and child OIDs. A stale or already
sealed marker is refused. After an interrupted call, inspect the marker before
retrying because a committed seal can outlive its command output.

The sealed marker is valid for a bounded migration stage on this same clone.
Production retirement requires the serving database's own sealed marker and
its separately verified source audit.
