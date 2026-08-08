from __future__ import annotations

from portal.backend.service.research.planning import plan_research_check
from portal.backend.service.research.registry import normalize_check_request


class _Store:
    def current_commit_seq(self):
        return 77

    def list_series(self, *, instrument_id=None):
        assert instrument_id == "instrument-1"
        return [
            {
                "series_id": 1,
                "instrument_id": instrument_id,
                "fact_type": "candle.ohlcv",
                "contract_version": "candle.ohlcv.v1",
                "timeframe_seconds": 1800,
                "dimensions": {},
            },
            {
                "series_id": 2,
                "instrument_id": instrument_id,
                "fact_type": "market.reference_price",
                "contract_version": "market.reference_price.v1",
                "timeframe_seconds": None,
                "dimensions": {"quote_currency": "USD"},
            },
        ]

    def read_series_records(self, **_kwargs):
        return [object()]

    def list_gap_evidence(self, **kwargs):
        if kwargs["series_id"] == 1:
            return [
                {
                    "classification": "provider_missing_data",
                    "start": "2026-01-10T00:00:00Z",
                    "end": "2026-01-10T00:30:00Z",
                }
            ]
        return []


def _indicator_plan(*_args, **_kwargs):
    return {
        "schema_version": "indicator_requirement_plan.v1",
        "root_indicator_ids": ["indicator-1"],
        "warmup_bars": 72,
        "graph_hash": "graph-hash",
        "indicators": [
            {
                "indicator_id": "indicator-1",
                "indicator_type": "market_profile",
                "configuration_hash": "indicator-hash",
            }
        ],
        "requirements": [
            {
                "consumer_id": "indicator-1",
                "required_start": "2025-12-31T00:00:00Z",
                "input": {
                    "key": "reference_price",
                    "fact_type": "market.reference_price",
                    "contract_version": "market.reference_price.v1",
                    "timeframe_seconds": None,
                    "instrument_role": "primary",
                    "instrument_ref": None,
                    "dimensions": {"quote_currency": "USD"},
                    "alignment": "latest_known",
                    "max_staleness_seconds": 21600,
                    "required": True,
                    "allow_gaps": True,
                    "known_at_required": True,
                    "required_fields": ["known_at", "value"],
                    "lookback_bars": None,
                    "lookback_seconds": 21600,
                },
            }
        ],
    }


def test_check_plan_exposes_transitive_inputs_warmup_staleness_sources_and_quality(
    monkeypatch,
) -> None:
    from portal.backend.service.research import planning

    monkeypatch.setattr(
        planning.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {"id": instrument_id, "symbol": "ETH-USD"},
    )
    definition, request = normalize_check_request(
        {
            "mode": "evidence",
            "title": "Indicator evidence",
            "check_family": "indicator_forward_outcome",
            "dataset_id": "mds_" + "a" * 32,
            "scope": {
                "indicator_id": "indicator-1",
                "instrument_id": "instrument-1",
                "timeframe": "30m",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-02-01T00:00:00Z",
            },
            "detector": {"type": "record_match", "output_name": "breakout"},
            "outcomes": {"forward_bars": [2, 6, 12]},
            "gap_policy": "reset_rewarm",
        },
        mode="evidence",
    )

    plan = plan_research_check(
        definition,
        request,
        store=_Store(),
        indicator_planner=_indicator_plan,
    )

    assert plan.warmup == {
        "bars": 72,
        "seconds": 129600,
        "timeframe_seconds": 1800,
    }
    assert plan.outcome_tail == {
        "horizons": [2, 6, 12],
        "bars": 12,
        "seconds": 21600,
        "horizon_kind": "bars",
    }
    transitive = next(
        row
        for row in plan.market_data_requirements
        if row["alias"] == "indicator:indicator-1:reference_price"
    )
    assert transitive["max_staleness_seconds"] == 21600
    assert transitive["source_policy"] == {"mode": "exact"}
    assert transitive["required_start"] == "2025-12-31T00:00:00Z"
    assert plan.materialization_range["as_of_commit_seq"] == 77
    assert plan.quality_evidence[0]["classification"] == "provider_missing_data"
    assert plan.missing_coverage == ()


def test_evidence_requires_explicit_gap_policy() -> None:
    import pytest

    with pytest.raises(ValueError, match="gap_policy_required"):
        normalize_check_request(
            {
                "title": "Evidence",
                "check_family": "raw_forward_outcome",
                "dataset_id": "mds_" + "a" * 32,
                "scope": {
                    "instrument_id": "instrument-1",
                    "timeframe": "30m",
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-02-01T00:00:00Z",
                },
                "detector": {
                    "type": "raw_condition",
                    "field": "close",
                    "operator": "gt",
                    "value": 1,
                },
            },
            mode="evidence",
        )
