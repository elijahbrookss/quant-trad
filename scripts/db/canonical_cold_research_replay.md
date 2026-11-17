# Compare complete frozen research outputs on the offline restore pair

Run one trial on the verified source and another on the isolated activated candidate. Use the same plan, matching PostgreSQL settings and Python dependencies, and each application's exact clean Git checkout. Use the same private socket, disposable `qt_bench_*` database conventions, and mounted archive root as the [read benchmark](canonical_cold_benchmark.md). The rehearsals open PostgreSQL transactions read-only and use QT's ordinary lifecycle read lease. Supply the independent restore pair evidence digest in the boundary plan. Disable the optional writable history read cache.

Use a plan with `read_boundary` containing the [read benchmark plan](canonical_cold_benchmark.md), and `checks` containing one to eight entries. Each Check entry has `name`, `dataset_hash`, `witness` and `request`. Set `witness` to a named fact read case listed in `read_boundary.cases`. It must read the same series inside the frozen dataset range and use that series' pinned commit. An explicit `all_revisions` case at the pinned commit can witness a dataset whose public frozen all-revision API exposes another record selection. Set `request` to an existing full research Check evidence request with `mode` equal to `evidence` and its retained `dataset_id`. Exclude preparation and acquisition flags. Select representative frozen requests for all needed evaluator families and data dependencies. A request without an installed evaluator cannot establish replay.

```sh
PYTHONPATH=src:. python /review/canonical_cold_research_replay.py run \
  --side source --plan /review/frozen-checks.json --revision "$SOURCE_REVISION" \
  --runtime-digest "$SOURCE_RUNTIME_DIGEST" --system-identifier "$SOURCE_SYSTEM" \
  --database-oid "$SOURCE_OID" --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" \
  > /review/frozen-checks-source.json

PYTHONPATH=src:. python /review/canonical_cold_research_replay.py run \
  --side candidate --plan /review/frozen-checks.json --revision "$CANDIDATE_REVISION" \
  --runtime-digest "$CANDIDATE_RUNTIME_DIGEST" --system-identifier "$CANDIDATE_SYSTEM" \
  --database-oid "$CANDIDATE_OID" --archive-root "$MARKET_STRUCTURE_STORAGE_ROOT" \
  > /review/frozen-checks-candidate.json

PYTHONPATH=src:. python /review/canonical_cold_research_replay.py compare \
  --source /review/frozen-checks-source.json --candidate /review/frozen-checks-candidate.json
```

Keep this script beside `canonical_cold_benchmark.py`. The runner calls the native computation used by `qt research check run` and omits the persistence step. Every receipt retains the complete native request, resolved plan, frozen input, evaluator output and original hashes. Each side independently verifies its producing commit. The paired comparison covers all ordered arrays and full native result contents. It compares the selected fact receipt, definition, request, plan, frozen dataset/source binding and indicator, fact, gap and quality hashes. The comparison ignores only `code_revision` and the evidence/result digest fields that incorporate it. Saved Check replay via QT still requires its exact original producing revision.

This is a selected research Check rehearsal. It does not rerun every saved Check, backtest or strategy family. Use separate receipts for native Level 2, raw custody, independent restore, and physical space measurements.
