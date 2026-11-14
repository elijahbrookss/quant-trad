# Selective decoding for exact Fact IDs

The full offline catalog run took about 2.44 seconds to return a sampled exact row from a 10,000-row page. Its SQL locator took about 2.69 milliseconds. This prototype keeps full file authentication and decodes the row groups needed by exact locator claims.

The caller must already have a visible SQL locator and a manifest from a completed full-page admission. Both bind the original immutable file. The prototype opens one read-only, nonblocking descriptor with symlink rejection, checks that it is a regular file, and hashes every byte before passing that same descriptor to Arrow. It checks pathname and descriptor device, inode, size, mode, modification and change stamps before and after decoding. An in-place mutation or replacement fails closed. These checks assume the local authorized object store and its existing immutability rules. They do not replace an exclusive publication/expiry lease or defend against a privileged filesystem attacker.

The object hash also authenticates the original Parquet footer. The reader checks exact schema, footer row count, compression, inline chunks, and declared whole-file and row-group limits. Ordinals use cumulative footer group sizes. For each selected row it checks exact ID, series, observation key digest, revision, commit, canonical row hash, decoded document hashes, and its admitted series and clock bounds. First publication and full retained-source audit still need all rows decoded and checked against manifest contents. A range query has only per-series page bounds in the current manifest. It would need independently admitted row-group bounds or a verified header-only discovery mechanism before it can skip groups without changing selection.

The cache-free probe is separate from live runtime. Integrating it into exact repository reads requires preserving the mounted-file check, cache copy and eviction lock, full cached copy hash, checksum-failure invalidation and authoritative-file fallback. It also requires the SQL query to expose the complete locator claim. Current cache and range behavior retain their existing path.

```sh
PYTHONPATH=src:. python -m scripts.db.canonical_cold_sparse_benchmark --probes 16
```

The benchmark alternates the native and probe order for 16 seeded exact ordinals on one entirely synthetic, 10,000-row native Parquet page, after full admission. It compares complete returned rows, records actual archive bytes attributed to each path, and shows actual decoded group rows. It does not flush operating system caches. It measures codec behavior on one device and makes no production latency claim.
