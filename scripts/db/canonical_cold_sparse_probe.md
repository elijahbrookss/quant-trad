# Selective decoding for exact Fact IDs

The serving repository resolves the visible SQL locator in its existing database snapshot. A locator supplies an ordinal into an immutable Parquet page that already passed full admission. The reader checks every byte of the selected file against the admitted SHA-256, then decodes the row groups needed by the requested ordinals.

The hash and Arrow reads use the same descriptor. The reader checks that the file is regular and that its device, inode, size, mode, modification and change stamps are stable through the operation. Source reads also check that the selected pathname still names that inode. The optional SSD cache retains its active-reader lock and checks the same hash. A corrupt copy is invalidated and the read falls back to the authoritative source. Cache readers can share a copy without rewriting its access marker during verification.

The authenticated footer supplies ordered group sizes and schema metadata. The codec checks row counts, inline compressed chunks, uncompressed byte limits and the selected rows' canonical hashes and manifest bounds. The repository compares the selected ID, series, key digest, revision, commit and row hash with the SQL locator. A selected group wider than the configured decode bound uses the existing full-page decoder. Range reads, first publication, retirement dependency verification and full audits still use the whole-page codec.

```sh
PYTHONPATH=src:. python -m scripts.db.canonical_cold_sparse_benchmark --probes 16
```

The local benchmark alternates the two native readers for seeded exact ordinals on one 10,000-row synthetic Parquet page after full admission. It compares complete returned rows and records bytes and decoded rows. It uses one filesystem, no QT SSD cache, and an operating-system cache warmed by admission. It excludes live PostgreSQL timings and the physical SSD/HDD placement difference.
