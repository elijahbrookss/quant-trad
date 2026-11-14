# Stage canonical history for the one-time cutover

Use a separate, current recovery pair and disposable PostgreSQL restore for rehearsal. Run the source read against the old application/schema revision. The tool stages immutable Fact archive pages and writes a local progress inventory. Source retirement requires the integrated cutover procedure and its fresh checks.

## Staging one cohort

Use the existing `PG_DSN` and provide the expected PostgreSQL system identifier from the approved recovery inventory. Staging works on one physical `storage_day` at a time. The day must have a retention row with a sealed source count. That includes reclaimed payload days for which the immutable SQL headers and all corresponding archive manifests still exist. Inventory legacy days by their authoritative storage day; the archival tool does not infer them from observation date or file age. An absent or open day needs explicit source reconciliation before staging.

```text
python -m scripts.db.canonical_cold_migration \
  --source-system-identifier <expected-id> \
  --day 2026-09-28 \
  --source-object-root /rehearsal/archive/objects \
  --staging-object-root /rehearsal/stage/objects \
  --temporary-directory /rehearsal/stage/work \
  --checkpoint /rehearsal/stage/progress.sqlite3 \
  --max-pages 1 --max-seconds 120
```

Repeat the command with the same checkpoint to resume. The checkpoint binds the source system, database and parent relation identities, the physical day, seal timestamp, expected count, initial archive prefix, source and destination roots, and codec limits. Each page advances a keyset cursor only after verified bytes and full source comparison. A missing staged object is caught by `verify_completed_cohort`; keep the source and a second valid copy until that check and the integrated gate have passed. Reuse-in-place is available for an explicitly approved shared object root via `--reuse-in-place`. Distinct roots copy the same content-addressed bytes. A new object may remain staged after an interruption before journal commit. Re-running the exact page can reuse those bytes.

SQL reads use short repeatable, read-only transactions. Each transaction limits server-side statements and lock waits. The wall budget is checked between pages. Individual encoding or file writes can run past the elapsed budget after a page starts. A completed cohort records exhaustion and the exact sealed row count. `MigrationCheckpoint.iter_completed_pages()` yields only completed cohorts and includes the prior exclusive source cursor, manifest descriptor with object key/hash, and whether it was reused. `verify_completed_cohort()` checks current staged file contents, ordering, counts, and checkpoint membership without an SSD cache. Depend on the paired source snapshot and a separately verified final owner fence for serving activation.

## Prepare the cold catalog

Install the cold schema and registered HDD tablespace first. Configure the target layout's archive root so its `objects` directory is the checkpoint's staged object root. With `PG_DSN` set to the approved target database, supply its current system identifier, database name and OID separately. A mismatch stops the command before any catalog admission.

```text
python -m scripts.db.canonical_cold_load \
  --target-system-identifier <target-system-id> \
  --target-database <target-name> --target-database-oid <target-oid> \
  --day 2026-09-28 --checkpoint /rehearsal/stage/progress.sqlite3 \
  --max-pages 1 --max-seconds 120
```

The loader validates each page's current mounted bytes through the serving codec, acquires a state-table lock and checks for an inactive cold layout before each admission transaction. It never changes that layout flag or source relations. Record `last_processed_ordinal`. On retry, reread the same page idempotently or provide `--after-ordinal` with the previously committed value. An end-of-checkpoint receipt only describes the batch traversal. The activation procedure must verify every page, loaded memberships, dependency holds and the final writer fence independently.

## Data decisions

| Inventory evidence | Disposition |
| --- | --- |
| Accepted Fact version, frozen pin, custody hold, or hard-to-reconstruct dependencies | Give the existing source bytes and all revisions an exact durable placement. Keep reception, acceptance, availability, revisions, external keys, quality and provenance. This includes captured raw stream inputs, receive ordinals, L2 snapshots and deltas, checkpoints and dependent artifacts. |
| Provider-native Coinbase historical candle gap with explicit missing coverage and no accepted/held evidence for that range | Reacquire under its own run once provider product coverage and retained source meaning are established. Compare boundary overlap against existing exact versions, record empty intervals and errors, and use QT's admission rules for new observations. A provider value match supplies no evidence about QT's earlier availability. |
| Rebuildable cold locator, typed candle projection or read cache with verified source inputs and no independent identity/custody | Rebuild from authoritative inputs and verify its output, exact lookup and eviction/failure behavior. A legacy SQL index used by a still-active caller stays until the replacement has proven coverage and that caller has migrated. |
| Ambiguous operator history, stale retention evidence, unresolved hold, or unsupported contract | Preserve exactly and escalate the missing evidence before changing its lifecycle. |

QT's advanced-trade adapter requests at most 300 candles and advances by 299 intervals to account for a provider that may include both endpoints. Coinbase's current public product-candle endpoint documents a maximum of 350. Planning uses the installed adapter's observed behavior. Five 365-day years for one symbol need at least 8,790 requests for 1m, 147 for 1h, or 7 for 1d, before retries, coverage holes and any product retention limits. Ten symbols at 1m need at least 87,900 requests. Count each requested half-open contiguous window separately. Window data or count parity does not establish accepted-row parity. Benchmark the same API product and adapter away from production, with the same no-trade and error handling. Record successful ranges, throttling, elapsed publication, gaps, and capacity effects before using any ETA or replacing copy work with a provider workflow. [QT Coinbase adapter](../../src/data_providers/providers/coinbase.py) · [Coinbase Advanced Trade candles](https://docs.cdp.coinbase.com/api-reference/advanced-trade-api/rest-api/public/get-public-product-candles) · [Coinbase WebSocket channels](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-endpoints)

## Readback evidence before handoff

Capture ordered, deterministic result streams from the old and new runtime paths using matching source boundary bindings. Use `verify_parity()` on those streams. The required cases cover all revisions, as-of selection, frozen dataset readback, coverage, raw ordinals, L2 replay and raw holds. Include exact versions and hash, decoded clocks and metadata when relevant, invalidation, correction timing, cross-tier boundaries, absent intervals, and lead/successor replay. For frozen data compare current dataset identity, commit watermark, ordered consumed IDs, material/provenance/quality and coverage before comparing higher-level outputs. Separately rehydrate raw records using their exact object hashes and decode ranges, then compare sequence and receive ordinal through each L2 checkpoint or replay witness. Check the inventory of raw and checkpoint holds, including retained frozen, replay and migration recovery generations, before any expiry changes. Restore and verify a separately retrievable pair at the actual boundary.

The harness's output binds its case, source boundary, ordered comparison digest and row count. Scope and input adapters belong to the actual rehearsal. Content mismatch, coverage mismatch, mismatched boundary, an unexpected empty result or an untested required domain fail the gate. A cohort checkpoint alone proves migrated Fact bytes and headers for that cohort. Integrated readback, dependency availability, frozen replay and physical retirement remain separately owned acceptance checks.
