"""Frozen quality is immutable evidence, not an input to re-expansion."""
from types import SimpleNamespace

import pytest

from market_data.canonical import build_canonical_fact_provenance_hash
from market_data.canonical_adapters import decode_market_trade_record, decode_trade_flow_record
from market_data.canonical_storage import record_from_storage_row
from market_data.contracts import build_provenance_hash, build_quality_hash
from portal.backend.service.market.backtest_dataset_service import validate_frozen_dataset_series
from portal.backend.service.storage.repos.market_data import _build_material_hash, _preserves_canonical_revision_history
from tests.test_market_data.test_fact_flow_admission import _fixture


@pytest.mark.parametrize("history", [False, True])
@pytest.mark.parametrize("family", ["market.trade_flow", "market.trade"])
def test_trade_dataset_validation_uses_exact_pinned_quality_once(family, history):
    root, rows, _ = _fixture()
    row = root.row if family == "market.trade_flow" else rows[0]
    canonical = record_from_storage_row(row)
    typed = (decode_trade_flow_record if family == "market.trade_flow" else decode_market_trade_record)(canonical)
    records = [canonical if history else typed]
    if family == "market.trade":
        quality = [{"classification": "covered_trade", "provider_product_id": typed.fact.provider_product_id,
            "provider_trade_id": typed.fact.provider_trade_id, "raw_record_id": typed.fact.raw_record_id,
            "coverage_interval_id": typed.fact.coverage_interval_id}]
    else:
        quality = [{"classification": "complete", "bucket_start": typed.fact.bucket_start.isoformat(),
            "archive_complete": typed.fact.archive_complete, "canonicalization_complete": typed.fact.canonicalization_complete,
            "coverage_interval_id": typed.fact.coverage_interval_id, "coverage_revision": typed.fact.coverage_revision}]
    identity = dict(identity_key="trade-history-fixture", instrument_id=root.instrument_id, fact_type=family,
        timeframe_seconds=1 if family == "market.trade_flow" else None, contract_version=f"{family}.v1")
    entry = {**identity, "dataset_id": "fixture-dataset", "series_id": row["series_id"],
        "range_start": root.fact.bucket_start, "range_end": root.fact.bucket_end, "max_commit_seq": root.row["market_commit_seq"],
        "row_count": 1, "quality_evidence": quality, "quality_hash": build_quality_hash(quality),
        "material_hash": _build_material_hash(fact_type=family, series_identity=identity, records=records),
        "provenance_hash": (build_canonical_fact_provenance_hash if history else build_provenance_hash)(records),
        "source_summary": {"record_selection": "all_canonical_revisions.v1"} if history else {}}
    called = []
    def selected(**kwargs):
        called.append(kwargs)
        return records
    def wrong(**kwargs):
        raise AssertionError("frozen revision selection changed")
    store = SimpleNamespace(read_dataset_fact_revisions=selected if history else wrong,
                            read_dataset_series=wrong if history else selected)
    validated, checked_quality, _ = validate_frozen_dataset_series(store=store, entry=entry)
    assert checked_quality == quality and called
    assert validated["quality_hash"] == entry["quality_hash"]
    assert entry["quality_evidence"] == quality
    assert _preserves_canonical_revision_history(identity["contract_version"])
    with pytest.raises(RuntimeError, match="hash_disagreement.*quality_hash"):
        validate_frozen_dataset_series(store=store, entry={**entry, "quality_evidence": []})
