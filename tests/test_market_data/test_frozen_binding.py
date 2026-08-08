from __future__ import annotations

import pytest

from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION
from market_data.frozen import (
    CAUSAL_KNOWN_AT_SEMANTICS_VERSION,
    FROZEN_MARKET_DATA_READ_BINDING_VERSION,
    bound_frozen_series_for_request,
    build_frozen_market_data_read_binding,
    normalize_frozen_market_data_read_binding,
)


def _series(*, alias: str = "primary", source: str = "source-a") -> dict:
    return {
        "alias": alias,
        "series_id": 7,
        "identity_key": "series-7",
        "instrument_id": "instrument-1",
        "fact_type": CANDLE_FACT_TYPE,
        "contract_version": CANDLE_FACT_VERSION,
        "timeframe_seconds": 3600,
        "range_start": "2026-01-01T00:00:00Z",
        "range_end": "2026-01-03T00:00:00Z",
        "row_count": 47,
        "max_commit_seq": 73,
        "material_hash": "material-hash",
        "provenance_hash": "provenance-hash",
        "quality_hash": "quality-hash",
        "source_summary": {
            "counts": {source: 47},
            "sources": {
                source: {
                    "provider": "provider-a",
                    "adapter_version": "adapter.v1",
                }
            },
        },
    }


def _binding(**changes) -> dict:
    dataset_hash = "a" * 64
    values = {
        "dataset_id": f"mds_{dataset_hash[:32]}",
        "dataset_hash": dataset_hash,
        "max_commit_seq": 73,
        "series": [_series()],
        "subjects": [
            {
                "instrument_id": "instrument-1",
                "snapshot_hash": "subject-hash",
                "snapshot": {
                    "id": "instrument-1",
                    "symbol": "ETH-USD",
                    "datasource": "provider-a",
                    "exchange": "venue-a",
                },
            }
        ],
        "recorded_gaps": [
            {
                "series_id": 7,
                "start": "2026-01-02T00:00:00Z",
                "end": "2026-01-02T01:00:00Z",
                "classification": "provider_missing_data",
            }
        ],
        "quality": {"status": "recorded", "evidence_count": 1},
    }
    values.update(changes)
    return build_frozen_market_data_read_binding(**values)


def test_frozen_binding_is_strategy_independent_and_provider_free() -> None:
    binding = _binding()

    assert binding["schema_version"] == FROZEN_MARKET_DATA_READ_BINDING_VERSION
    assert binding["known_at_semantics"] == CAUSAL_KNOWN_AT_SEMANTICS_VERSION
    assert binding["provider_access"] == "disabled"
    assert binding["provider_call_performed"] is False
    assert "strategy_id" not in binding
    assert binding["series"][0]["series_id"] == 7
    assert binding["series"][0]["source_binding_hash"]
    assert binding["recorded_gaps"][0]["classification"] == "provider_missing_data"


def test_frozen_binding_reads_only_resolved_series_and_range() -> None:
    binding = _binding()

    selected = bound_frozen_series_for_request(
        binding,
        alias="primary",
        instrument_id="instrument-1",
        fact_type=CANDLE_FACT_TYPE,
        contract_version=CANDLE_FACT_VERSION,
        timeframe_seconds=3600,
        start="2026-01-01T12:00:00Z",
        end="2026-01-02T12:00:00Z",
    )
    assert selected["series_id"] == 7

    with pytest.raises(ValueError, match="range_expansion_forbidden"):
        bound_frozen_series_for_request(
            binding,
            alias="primary",
            instrument_id="instrument-1",
            fact_type=CANDLE_FACT_TYPE,
            contract_version=CANDLE_FACT_VERSION,
            timeframe_seconds=3600,
            start="2025-12-31T23:00:00Z",
            end="2026-01-02T12:00:00Z",
        )


def test_frozen_binding_hash_covers_quality_and_source_binding() -> None:
    original = _binding()
    changed_quality = _binding(quality={"status": "recorded", "evidence_count": 2})
    changed_source = _binding(series=[_series(source="source-b")])

    assert changed_quality["binding_hash"] != original["binding_hash"]
    assert changed_source["binding_hash"] != original["binding_hash"]


def test_frozen_binding_rejects_provider_transport_and_hash_substitution() -> None:
    binding = _binding()
    with pytest.raises(ValueError, match="provider access must be disabled"):
        normalize_frozen_market_data_read_binding(
            {**binding, "provider_access": "enabled"}
        )
    with pytest.raises(ValueError, match="binding hash disagreement"):
        normalize_frozen_market_data_read_binding(
            {**binding, "quality": {"status": "changed"}}
        )
