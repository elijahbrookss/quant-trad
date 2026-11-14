# Preserve the next accepted commit allocation through a reverse-projection fixture

The accepted market watermark and the PostgreSQL sequence serve different purposes. The watermark identifies committed facts. A rolled-back writer can still consume a sequence value, so restarting the allocator from the watermark can reuse a previously allocated value. This fixture captures both the visible commit and the native sequence tuple under the live PostgreSQL connection gate. It validates a permanent bigint sequence with increment and cache both set to one and cycling disabled.

The disposable original-layout clone starts at a consistent pre-cutover checkpoint with an accepted row already in place. The candidate accepts a later cold-only row and consumes further commit numbers in an aborted transaction. The fixture copies the selected missing row and its ingestion-run reference, then uses `synchronize_allocator()` while the destination's connection gate is still closed. The call requires the captured candidate and baseline hashes, exact database and sequence OIDs, the expected v2 layout, and an accepted watermark equal to the captured candidate boundary.

PostgreSQL `ALTER SEQUENCE RESTART WITH` is transactional. It writes the captured candidate's exact next value and leaves that value available for the next call. An aborted synchronization restores the destination's previous sequence state. Repeating the synchronization at the same normalized boundary makes no change. A different sequence state or newer visible accepted watermark fails the check. `setval()` would remain visible after transaction rollback and is unsuitable for this step.

The returned receipt describes the in-transaction result. The caller must retain the writer fence, commit, and independently confirm the durable target boundary before reopening the database. This primitive checks the commit allocator and accepted watermark. Full accepted-row, raw custody, archive, arbitrary operational state and other sequence reconciliation still need separate proof.

```sh
pytest -q tests/test_market_data/test_canonical_cold_reverse_projection_db.py
```
