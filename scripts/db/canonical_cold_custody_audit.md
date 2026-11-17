# Compare held archive evidence on paired offline restores

Use the same verified restore pair, accepted commit watermark and exclusive retirement day as the read workload. Run each side with its exact clean application checkout, mounted restored archive, and private Unix-socket-only `qt_bench_*` database. Stop all writers on both copies. Use `PG_DSN` and `MARKET_STRUCTURE_STORAGE_ROOT` as in the read benchmark. The audit takes QT’s shared lifecycle lease and a PostgreSQL repeatable, read-only snapshot on each side.

Use one JSON plan for both runs. Replace the example identifiers with selected objects from the exact restore pair. Include published and prepared generation holds on representative targets. Select a retained Level 2 checkpoint to check its byte custody alongside the raw session objects.

```json
{
  "restore_pair_hash": "1111111111111111111111111111111111111111111111111111111111111111",
  "commit_seq": 87654321,
  "retire_before": "2026-08-10",
  "targets": [
    {"kind": "raw_manifest", "id": "raw-manifest-id", "required_holds": ["explicit", "published", "prepared"]},
    {"kind": "book_checkpoint", "id": "checkpoint-id", "required_holds": ["published"]}
  ]
}
```

Other supported required hold witnesses are `dataset` and `prefix`. A required explicit hold must be active at the restored boundary. The receipt hashes all explicit revisions for the object, including releases, along with its ordered frozen dataset refs, published generation holds with active pointer identities, prepared generation holds with lifecycle state, prefix holds, and both directions of compaction lineage. It also compares effective protection from the native QT retention status. Hot backlog presence is reported per side because the backing location can legitimately change during retirement. Each selected object checksum must match the currently mounted bytes. For a selected raw object, every physical row must have the exact database mapping, frame hash, receive ordinal, source scope, and global object row index. The archive decoder checks strict ordinal order. An object can have legitimate ordinal gaps between its first and last record. The plan can include several objects if it needs to cover an entire session.

Run each trial from its own application checkout with the audit and benchmark scripts in the same operator tooling directory. Use the same selected plan and budgets. Supply the explicit PostgreSQL system identifier and database OID alongside the application Git SHA.

```sh
PYTHONPATH=src:. python /review/canonical_cold_custody_audit.py run \
  --side source --plan /review/custody-plan.json --revision "$SOURCE_REVISION" \
  --system-identifier "$SOURCE_SYSTEM" --database-oid "$SOURCE_OID" \
  --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" > /review/custody-source.json

PYTHONPATH=src:. python /review/canonical_cold_custody_audit.py run \
  --side candidate --plan /review/custody-plan.json --revision "$CANDIDATE_REVISION" \
  --system-identifier "$CANDIDATE_SYSTEM" --database-oid "$CANDIDATE_OID" \
  --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" > /review/custody-candidate.json

PYTHONPATH=src:. python /review/canonical_cold_custody_audit.py compare \
  --source /review/custody-source.json --candidate /review/custody-candidate.json
```

Defaults allow 32 targets per plan, 50,000 raw rows per object, 10,000 rows per target and hold relation, and 256 MiB of total physical archive bytes per side. The selected witness inventory and resulting receipts cover those objects. Full custody closure and extent placement, unselected ordinal and acquisition coverage, freeze identity, and checkpoint reconstruction need their own evidence. The read benchmark can exercise native raw-to-book and checkpoint replay for a selected session.
