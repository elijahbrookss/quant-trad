from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from portal.backend.service.research.planning import plan_research_check
from portal.backend.service.research.planning import _coverage_for_requirement
from portal.backend.service.research.registry import normalize_check_request
from market_data.frozen import semantic_hash


def _iso(value) -> str:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _coverage_row(*, start, end, source_key: str = "source-a") -> dict:
    material = {
        "schema_version": "market.fact_acquisition_coverage.v1",
        "series_id": 2,
        "source_id": 1,
        "binding_id": "binding-1",
        "manifest_hash": "a" * 64,
        "interface_version": "test.v1",
        "confirmation_depth": 12,
        "range_start": _iso(start),
        "range_end": _iso(end),
        "source_positions": {"start": "1", "end": "2", "head": "2"},
        "status": "complete",
        "evidence": {"response_count": 0},
    }
    return {
        **material,
        "identity_key": semantic_hash(material),
        "source_identity_key": source_key,
        "source_position_start": "1",
        "source_position_end": "2",
        "source_position_head": "2",
        "created_at": "2026-02-01T00:00:00.000000Z",
    }


class _Store:
    def current_commit_seq(self):
        return 77

    def list_series(self, *, instrument_id=None):
        assert instrument_id == "instrument-1"
        return [
            {
                "id": 1,
                "series_id": None,
                "instrument_id": instrument_id,
                "fact_type": "candle.ohlcv",
                "contract_version": "candle.ohlcv.v1",
                "timeframe_seconds": 1800,
                "dimensions": {},
            },
            {
                "id": 2,
                "series_id": None,
                "instrument_id": instrument_id,
                "fact_type": "market.reference_price",
                "contract_version": "market.reference_price.v1",
                "timeframe_seconds": None,
                "dimensions": {"quote_currency": "USD"},
            },
        ]

    def read_series_records(self, **kwargs):
        source = SimpleNamespace(
            identity_key="source-a",
            provider="provider-a",
            venue="venue-a",
            source_kind="test",
            adapter_version="test.v1",
        )
        if int(kwargs["series_id"]) == 1:
            cursor = kwargs["start"]
            records = []
            while cursor < kwargs["end"]:
                records.append(
                    SimpleNamespace(
                        source_identity_key="source-a",
                        source=source,
                        fact=SimpleNamespace(open_time=cursor),
                    )
                )
                cursor += timedelta(minutes=30)
            return records
        return [
            SimpleNamespace(
                source_identity_key="source-a",
                source=source,
                fact=SimpleNamespace(effective_at=kwargs["start"]),
            )
        ]

    def list_gap_evidence(self, **kwargs):
        if kwargs["series_id"] == 1:
            return [
                {
                    "classification": "provider_missing_data",
                    "start": "2026-01-10T00:00:00Z",
                    "end": "2026-01-10T00:30:00Z",
                    "source_identity_key": "source-a",
                }
            ]
        return []

    def list_source_acquisition_coverage(self, **kwargs):
        return [_coverage_row(start=kwargs["start"], end=kwargs["end"])]


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
            "check_family": "signal_audit",
            "dataset_id": "mds_" + "a" * 32,
            "scope": {
                "indicator_id": "indicator-1",
                "instrument_id": "instrument-1",
                "timeframe": "30m",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-02-01T00:00:00Z",
            },
            "detector": {
                "type": "signal_audit",
                "source_output": "state",
                "source_field": "state_key",
                "from": "inside",
                "to": "outside",
                "signal_output": "breakout",
                "event_key": "breakout",
            },
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
        "required_horizons": [2, 6, 12],
        "bars": 12,
        "seconds": 21600,
        "horizon_kind": "bars",
        "entry_lag_bars": 0,
        "invalidation_max_bars": 0,
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


def test_sparse_numeric_fact_does_not_prove_full_source_coverage() -> None:
    store = _Store()
    required_start = datetime(2026, 1, 1, tzinfo=UTC)
    required_end = datetime(2026, 2, 1, tzinfo=UTC)

    def incomplete_coverage(**_kwargs):
        return [
            _coverage_row(
                start=required_start,
                end=required_start + timedelta(days=1),
            )
        ]

    store.list_source_acquisition_coverage = incomplete_coverage
    missing, _quality = _coverage_for_requirement(
        {
            "alias": "reference",
            "instrument_id": "instrument-1",
            "fact_type": "market.reference_price",
            "contract_version": "market.reference_price.v1",
            "timeframe_seconds": None,
            "dimensions": {"quote_currency": "USD"},
            "alignment": "latest_known",
            "required_start": _iso(required_start),
            "required_end": _iso(required_end),
            "source_policy": {
                "mode": "exact",
                "source_identity_key": "source-a",
            },
        },
        store=store,
        as_of_commit_seq=77,
    )

    assert any(row["reason"] == "source_acquisition_coverage_missing" for row in missing)


def test_complete_acquisition_manifest_can_prove_zero_event_numeric_range() -> None:
    class EmptyNumericStore(_Store):
        def read_series_records(self, **kwargs):
            if int(kwargs["series_id"]) == 2:
                return []
            return super().read_series_records(**kwargs)

    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = datetime(2026, 2, 1, tzinfo=UTC)
    missing, quality = _coverage_for_requirement(
        {
            "alias": "reference",
            "instrument_id": "instrument-1",
            "fact_type": "market.reference_price",
            "contract_version": "market.reference_price.v1",
            "timeframe_seconds": None,
            "dimensions": {"quote_currency": "USD"},
            "alignment": "latest_known",
            "required_start": _iso(start),
            "required_end": _iso(end),
            "source_policy": {
                "mode": "exact",
                "source_identity_key": "source-a",
            },
        },
        store=EmptyNumericStore(),
        as_of_commit_seq=77,
    )

    assert missing == []
    assert quality[0]["classification"] == "source_acquisition_coverage"


def test_requirement_planning_enforces_exact_series_identity() -> None:
    missing, quality = _coverage_for_requirement(
        {
            "alias": "reference",
            "instrument_id": "instrument-1",
            "fact_type": "market.reference_price",
            "contract_version": "market.reference_price.v1",
            "timeframe_seconds": None,
            "dimensions": {"quote_currency": "USD"},
            "alignment": "latest_known",
            "required_start": "2026-01-01T00:00:00Z",
            "required_end": "2026-02-01T00:00:00Z",
            "source_policy": {
                "mode": "exact",
                "series_id": 999,
                "source_identity_key": "source-a",
            },
        },
        store=_Store(),
        as_of_commit_seq=77,
    )

    assert quality == []
    assert missing == [
        {
            "alias": "reference",
            "reason": "source_series_binding_unresolved",
            "requested_series_id": 999,
            "candidate_series_ids": [2],
        }
    ]


def test_l2_fact_snapshot_plans_no_indicator_and_stops_facts_at_last_decision() -> None:
    definition, request = normalize_check_request(
        {
            "mode": "preview",
            "check_family": "event_fact_analysis",
            "scope": {
                "instrument_id": "instrument-1",
                "timeframe": "30m",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-02T00:00:00Z",
            },
            "detector": {"type": "fact_snapshot", "input_alias": "bbo"},
            "outcomes": {"horizons": [6]},
            "inputs": [
                {
                    "alias": "bbo",
                    "fact_type": "market.bbo",
                    "contract_version": "market.bbo.v1",
                    "timeframe_seconds": 1,
                    "max_staleness_seconds": 120,
                    "source_policy": {"mode": "current"},
                }
            ],
            "gap_policy": "continue_degraded",
        },
        mode="preview",
    )
    seen_indicator_ids: list[list[str]] = []

    def empty_indicator_plan(indicator_ids, **_kwargs):
        seen_indicator_ids.append(list(indicator_ids))
        return {
            "schema_version": "indicator_requirement_plan.v1",
            "root_indicator_ids": [],
            "warmup_bars": 0,
            "graph_hash": semantic_hash({"indicators": []}),
            "indicators": [],
            "requirements": [],
        }

    plan = plan_research_check(
        definition,
        request,
        store=_Store(),
        indicator_planner=empty_indicator_plan,
        instrument_loader=lambda instrument_id: {"id": instrument_id},
        inspect_coverage=False,
    )

    bbo = next(
        row for row in plan.market_data_requirements if row["alias"] == "bbo"
    )
    primary = next(
        row
        for row in plan.market_data_requirements
        if row["alias"] == "primary_bars"
    )
    assert seen_indicator_ids == [[]]
    assert plan.indicator_graph == ()
    assert plan.execution["event_source"] == "check_fact_snapshot"
    assert plan.execution["fact_history_required"] is True
    assert bbo["alignment"] == "exact_interval"
    assert bbo["required_end"] == "2026-01-02T00:00:00.000000Z"
    assert primary["required_end"] == "2026-01-02T03:00:00.000000Z"
