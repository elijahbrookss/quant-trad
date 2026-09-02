from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Mapping, Sequence

import pandas as pd
import pytest

from market_data.canonical import CanonicalFactRecord, FactState
from market_data.canonical_adapters import (
    canonicalize_bbo_feature,
    canonicalize_depth_feature,
)
from market_data.contracts import SourceIdentity
from market_data.market_state import BboFeatureFact, DepthFeatureFact
from market_data.order_book import BookSourcePosition
from research_science.check import CHECK_PLAN_SCHEMA_VERSION, ResolvedCheckPlan

import portal.backend.service.research.event_fact_evaluator as evaluator_module
from portal.backend.service.research.event_fact_evaluator import (
    EventFactEvaluator,
    normalize_event_fact_configuration,
)


_BASE = datetime(2026, 1, 1, tzinfo=UTC)
_SOURCE = SourceIdentity(
    provider="COINBASE",
    venue="COINBASE_DIRECT",
    source_kind="websocket_l2",
    adapter_version="coinbase_advanced_trade.l2.v1",
)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _digest(value: int) -> str:
    return format(value % 16, "x") * 64


def _position(ordinal: int) -> BookSourcePosition:
    return BookSourcePosition(
        definition_id="btc-usd-l2",
        session_id="fact-snapshot-test",
        connection_epoch=0,
        provider_product_id="BTC-USD",
        provider_sequence_num=ordinal,
        receive_ordinal=ordinal,
        event_ordinal=0,
    )


def _bbo_record(
    *,
    bucket_end: datetime,
    known_at: datetime,
    commit_seq: int,
    revision: int = 1,
    mid_price: str = "100",
) -> CanonicalFactRecord:
    mid = Decimal(mid_price)
    spread = Decimal("2")
    feature = BboFeatureFact(
        series_id=51,
        source_l2_series_id=41,
        bucket_start=bucket_end - timedelta(seconds=1),
        bucket_end=bucket_end,
        source_effective_at=bucket_end - timedelta(milliseconds=500),
        known_at=known_at,
        source_position=_position(commit_seq),
        validity_interval_id="validity-1",
        product_definition_version_id="coinbase.BTC-USD.v1",
        provider_size_unit="base",
        source_state_hash=_digest(commit_seq),
        bid_price=mid - spread / 2,
        bid_quantity=Decimal("2"),
        bid_base_quantity=Decimal("2"),
        ask_price=mid + spread / 2,
        ask_quantity=Decimal("3"),
        ask_base_quantity=Decimal("3"),
        mid_price=mid,
        spread=spread,
        spread_bps=Decimal("10000") * spread / mid,
        input_fingerprint=_digest(commit_seq + 1),
    )
    return CanonicalFactRecord(
        series_id=feature.series_id,
        source_id=1,
        revision=revision,
        market_commit_seq=commit_seq,
        fact=canonicalize_bbo_feature(feature, source=_SOURCE),
    )


def _depth_record(
    *,
    bucket_end: datetime,
    known_at: datetime,
    band_bps: int,
    commit_seq: int,
    bid_quantity: str,
    ask_quantity: str,
) -> CanonicalFactRecord:
    bid = Decimal(bid_quantity)
    ask = Decimal(ask_quantity)
    feature = DepthFeatureFact(
        series_id=52,
        source_l2_series_id=41,
        bucket_start=bucket_end - timedelta(seconds=1),
        bucket_end=bucket_end,
        source_effective_at=bucket_end - timedelta(milliseconds=500),
        known_at=known_at,
        source_position=_position(commit_seq),
        validity_interval_id="validity-1",
        source_state_hash=_digest(commit_seq),
        bbo_input_fingerprint=_digest(commit_seq + 1),
        provider_size_unit="base",
        band_bps=band_bps,
        mid_price=Decimal("100"),
        bid_quantity=bid,
        ask_quantity=ask,
        bid_base_quantity=bid,
        ask_base_quantity=ask,
        bid_notional=bid * Decimal("99"),
        ask_notional=ask * Decimal("101"),
        imbalance=(bid - ask) / (bid + ask),
        input_fingerprint=_digest(commit_seq + 2),
    )
    return CanonicalFactRecord(
        series_id=feature.series_id,
        source_id=1,
        revision=1,
        market_commit_seq=commit_seq,
        fact=canonicalize_depth_feature(feature, source=_SOURCE),
    )


def _tombstone(
    record: CanonicalFactRecord,
    *,
    known_at: datetime,
    commit_seq: int,
) -> CanonicalFactRecord:
    return CanonicalFactRecord(
        series_id=record.series_id,
        source_id=record.source_id,
        revision=record.revision + 1,
        market_commit_seq=commit_seq,
        fact=replace(
            record.fact,
            state=FactState.INVALIDATED,
            received_at=known_at,
            accepted_at=known_at,
            known_at=known_at,
        ),
    )


def _candles(event_count: int) -> list[dict[str, Any]]:
    closes = [100.0, 101.0, 99.0, 102.0, 98.0, 103.0]
    rows: list[dict[str, Any]] = []
    for index in range(event_count + 1):
        open_time = _BASE + timedelta(minutes=index)
        close = closes[index % len(closes)]
        rows.append(
            {
                "time": _iso(open_time),
                "open_time": _iso(open_time),
                "close_time": _iso(open_time + timedelta(minutes=1)),
                "known_at": _iso(open_time + timedelta(minutes=1)),
                "open": close - 0.25,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 10.0 + index,
            }
        )
    return rows


def _plan(*, event_count: int, gap_policy: str) -> ResolvedCheckPlan:
    evaluation_end = _BASE + timedelta(minutes=event_count)
    return ResolvedCheckPlan(
        schema_version=CHECK_PLAN_SCHEMA_VERSION,
        request_hash="fact-snapshot-request-hash",
        market_data_requirements=(),
        indicator_graph=(),
        evaluation_range={
            "start": _iso(_BASE),
            "end_exclusive": _iso(evaluation_end),
        },
        materialization_range={
            "start": _iso(_BASE),
            "end_exclusive": _iso(evaluation_end + timedelta(minutes=1)),
        },
        warmup={"bars": 0, "seconds": 0, "timeframe_seconds": 60},
        outcome_tail={
            "horizons": [1],
            "horizon_kind": "bars",
            "bars": 1,
            "seconds": 60,
        },
        gap_policy=gap_policy,
        quality_evidence=(),
    )


def _evaluate(
    *,
    alias: str,
    fact_type: str,
    records: Sequence[CanonicalFactRecord],
    event_count: int,
    alignment: str,
    where: Mapping[str, Any] | None = None,
    enriched_features: Sequence[Mapping[str, Any]] = (),
    gap_policy: str = "continue_degraded",
    recorded_gaps: Sequence[Mapping[str, Any]] = (),
    candles: Any | None = None,
) -> dict[str, Any]:
    detector, outcomes, statistics = normalize_event_fact_configuration(
        detector={
            "type": "fact_snapshot",
            "input_alias": alias,
            "sampling": "primary_bar_close",
            "where": dict(where or {}),
        },
        outcomes={"horizons": [1], "primary_horizon": 1},
        statistics={
            "features": {
                "baseline": [],
                "enriched": [dict(row) for row in enriched_features],
            },
            "eligibility": {"min_samples": 0},
        },
    )
    return EventFactEvaluator().evaluate(
        plan=_plan(event_count=event_count, gap_policy=gap_policy),
        inputs={
            "detector": detector,
            "outcomes": outcomes,
            "statistics": statistics,
            "candles": (
                candles if candles is not None else _candles(event_count)
            ),
            "fact_records_by_alias": {alias: list(records)},
            "fact_requirements_by_alias": {
                alias: {
                    "fact_type": fact_type,
                    "alignment": alignment,
                    "timeframe_seconds": 1,
                    "max_staleness_seconds": 600,
                }
            },
            "data_quality": {
                "status": "degraded" if recorded_gaps else "clean",
                "recorded_gaps": [dict(row) for row in recorded_gaps],
            },
        },
    )


def _detector_version_ids(result: Mapping[str, Any]) -> list[str | None]:
    return [
        (
            str(reference["fact_version_id"])
            if isinstance(reference := row.get("detector_fact_reference"), Mapping)
            else None
        )
        for row in result["events"]
    ]


def test_fact_snapshot_accepts_canonical_dataframe_candles() -> None:
    rows = _candles(1)
    frame = pd.DataFrame(
        [
            {
                **{
                    key: value
                    for key, value in row.items()
                    if key not in {"time", "open_time"}
                },
                "timestamp": pd.Timestamp(row["open_time"]),
            }
            for row in rows
        ]
    )
    frame.set_index("timestamp", inplace=True, drop=False)
    record = _bbo_record(
        bucket_end=_BASE + timedelta(minutes=1),
        known_at=_BASE + timedelta(minutes=1),
        commit_seq=1,
    )

    result = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=[record],
        event_count=1,
        alignment="exact_interval",
        candles=frame,
    )

    assert result["status"] == "completed"
    assert result["candidate_count"] == 1
    assert result["sample_count"] == 1
    assert _detector_version_ids(result) == [record.fact_version_id]


def test_fact_snapshot_accepts_empty_dataframe_candles() -> None:
    frame = pd.DataFrame(
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "known_at",
        ]
    )

    result = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=[],
        event_count=1,
        alignment="exact_interval",
        candles=frame,
    )

    assert result["candidate_count"] == 0
    assert result["sample_count"] == 0
    assert result["events"] == []


def test_fact_snapshot_rejects_unsupported_candle_container() -> None:
    with pytest.raises(
        ValueError,
        match=(
            r"event_fact_check_invalid: candles must be a pandas DataFrame "
            r"or a sequence of objects; received_type=dict"
        ),
    ):
        _evaluate(
            alias="bbo",
            fact_type="market.bbo",
            records=[],
            event_count=1,
            alignment="exact_interval",
            candles={"timestamp": _BASE},
        )


def test_exact_interval_requires_the_exact_bucket_and_preserves_prior_decisions() -> None:
    first_close = _BASE + timedelta(minutes=1)
    second_close = _BASE + timedelta(minutes=2)
    third_close = _BASE + timedelta(minutes=3)
    first = _bbo_record(
        bucket_end=first_close,
        known_at=first_close,
        commit_seq=1,
    )
    second = _bbo_record(
        bucket_end=second_close,
        known_at=second_close,
        commit_seq=2,
        mid_price="101",
    )
    future_exact = _bbo_record(
        bucket_end=third_close,
        known_at=third_close + timedelta(microseconds=1),
        commit_seq=5,
        mid_price="103",
    )
    corrected_first = _bbo_record(
        bucket_end=first_close,
        known_at=second_close + timedelta(seconds=30),
        commit_seq=3,
        revision=2,
        mid_price="102",
    )
    tombstoned_second = _tombstone(
        second,
        known_at=second_close + timedelta(seconds=30),
        commit_seq=4,
    )

    original = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=[first, second, future_exact],
        event_count=3,
        alignment="exact_interval",
    )
    revised = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=[
            first,
            second,
            corrected_first,
            tombstoned_second,
            future_exact,
        ],
        event_count=3,
        alignment="exact_interval",
    )

    assert _detector_version_ids(revised) == [
        first.fact_version_id,
        second.fact_version_id,
        None,
    ]
    assert revised["events"][2]["exclusion_reasons"] == [
        "detector_fact_missing:bbo"
    ]
    assert revised["events"][2]["event"]["metadata"]["selection_rule"] == (
        "exact_source_interval_known_at_decision.v1"
    )
    assert revised["hashes"]["selected_facts_hash"] == original["hashes"][
        "selected_facts_hash"
    ]
    assert revised["hashes"]["event_population_hash"] == original["hashes"][
        "event_population_hash"
    ]


def test_latest_snapshot_causally_applies_corrections_and_tombstones() -> None:
    first_close = _BASE + timedelta(minutes=1)
    second_close = _BASE + timedelta(minutes=2)
    third_close = _BASE + timedelta(minutes=3)
    fourth_close = _BASE + timedelta(minutes=4)
    first = _bbo_record(
        bucket_end=first_close,
        known_at=first_close,
        commit_seq=1,
    )
    second = _bbo_record(
        bucket_end=second_close,
        known_at=second_close,
        commit_seq=2,
        mid_price="101",
    )
    corrected_first = _bbo_record(
        bucket_end=first_close,
        known_at=second_close + timedelta(seconds=30),
        commit_seq=3,
        revision=2,
        mid_price="102",
    )
    tombstoned_second = _tombstone(
        second,
        known_at=third_close + timedelta(seconds=30),
        commit_seq=4,
    )
    future_tombstone = _tombstone(
        corrected_first,
        known_at=fourth_close + timedelta(microseconds=1),
        commit_seq=5,
    )

    visible = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=[first, second, corrected_first, tombstoned_second],
        event_count=4,
        alignment="latest_known",
    )
    with_future = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=[
            first,
            second,
            corrected_first,
            tombstoned_second,
            future_tombstone,
        ],
        event_count=4,
        alignment="latest_known",
    )

    assert _detector_version_ids(with_future) == [
        first.fact_version_id,
        second.fact_version_id,
        second.fact_version_id,
        corrected_first.fact_version_id,
    ]
    assert with_future["events"][2]["detector_fact_reference"][
        "fact_version_id"
    ] == second.fact_version_id
    assert with_future["events"][3]["detector_fact_reference"][
        "fact_version_id"
    ] == corrected_first.fact_version_id
    assert with_future["hashes"] == visible["hashes"]


def test_exact_snapshot_keeps_distinct_closes_with_one_shared_known_at() -> None:
    first_close = _BASE + timedelta(minutes=1)
    second_close = _BASE + timedelta(minutes=2)
    shared_known_at = second_close
    candles = _candles(2)
    candles[0]["known_at"] = _iso(shared_known_at)
    candles[1]["known_at"] = _iso(shared_known_at)
    first = _bbo_record(
        bucket_end=first_close,
        known_at=first_close,
        commit_seq=1,
    )
    second = _bbo_record(
        bucket_end=second_close,
        known_at=second_close,
        commit_seq=2,
        mid_price="101",
    )

    result = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=[first, second],
        event_count=2,
        alignment="exact_interval",
        candles=candles,
    )

    assert _detector_version_ids(result) == [
        first.fact_version_id,
        second.fact_version_id,
    ]
    assert [
        datetime.fromisoformat(
            row["event"]["metadata"]["sample_time"].replace("Z", "+00:00")
        )
        for row in result["events"]
    ] == [first_close, second_close]


def test_depth_band_filter_selects_one_band_and_unfiltered_snapshot_is_ambiguous() -> None:
    sample_time = _BASE + timedelta(minutes=1)
    depth_5 = _depth_record(
        bucket_end=sample_time,
        known_at=sample_time,
        band_bps=5,
        commit_seq=1,
        bid_quantity="2",
        ask_quantity="3",
    )
    depth_10 = _depth_record(
        bucket_end=sample_time,
        known_at=sample_time,
        band_bps=10,
        commit_seq=2,
        bid_quantity="3",
        ask_quantity="1",
    )
    where = {"payload.band_bps": 5}

    filtered = _evaluate(
        alias="depth",
        fact_type="market.depth_observation",
        records=[depth_5, depth_10],
        event_count=1,
        alignment="exact_interval",
        where=where,
        enriched_features=[
            {
                "name": "imbalance_5bps",
                "operator": "latest_payload_number",
                "input_alias": "depth",
                "path": "payload.imbalance",
                "where": where,
            }
        ],
    )

    event = filtered["events"][0]
    assert event["detector_fact_reference"]["fact_version_id"] == (
        depth_5.fact_version_id
    )
    assert event["direction"] == "neutral"
    assert event["outcomes"]["1"]["forward_return"] == pytest.approx(0.01)
    assert event["outcomes"]["1"]["direction_signed_forward_return"] == (
        pytest.approx(0.01)
    )
    assert event["outcomes"]["1"]["positive"] is True
    assert event["features"]["imbalance_5bps"] == pytest.approx(-0.2)
    assert event["fact_references"]["depth"][0]["fact_version_id"] == (
        depth_5.fact_version_id
    )
    with pytest.raises(
        RuntimeError,
        match=(
            r"event_fact_snapshot_ambiguous: alias=depth .*candidate_count=2"
        ),
    ):
        _evaluate(
            alias="depth",
            fact_type="market.depth_observation",
            records=[depth_5, depth_10],
            event_count=1,
            alignment="exact_interval",
        )


def test_fact_snapshot_gap_policy_rejects_or_excludes_exact_missing_sample() -> None:
    first_close = _BASE + timedelta(minutes=1)
    second_close = _BASE + timedelta(minutes=2)
    third_close = _BASE + timedelta(minutes=3)
    records = [
        _bbo_record(
            bucket_end=first_close,
            known_at=first_close,
            commit_seq=1,
        ),
        _bbo_record(
            bucket_end=third_close,
            known_at=third_close,
            commit_seq=2,
            mid_price="102",
        ),
    ]
    gap = {
        "alias": "bbo",
        "start": _iso(second_close - timedelta(seconds=1)),
        "end": _iso(second_close),
        "classification": "deployment_restart_gap",
    }

    rejected = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=records,
        event_count=3,
        alignment="exact_interval",
        gap_policy="reject",
        recorded_gaps=[gap],
    )
    assert rejected["status"] == "blocked"
    assert rejected["event_ownership"] == "check"
    assert rejected["sample_count"] == 0
    assert rejected["events"] == []
    assert rejected["gap_decision"]["check_action"] == (
        "rejected_before_sample_emission"
    )
    assert "indicator_action" not in rejected["gap_decision"]

    continued = _evaluate(
        alias="bbo",
        fact_type="market.bbo",
        records=records,
        event_count=3,
        alignment="exact_interval",
        gap_policy="continue_degraded",
        recorded_gaps=[gap],
    )
    assert continued["status"] == "completed"
    assert continued["candidate_count"] == 3
    assert continued["sample_count"] == 2
    assert continued["events"][1]["exclusion_reasons"] == [
        "detector_fact_gap:bbo"
    ]
    assert continued["events"][2]["eligible"] is True
    assert continued["eligibility"]["exclusions"] == {
        "detector_fact_gap:bbo": 1
    }
    assert continued["gap_decision"]["check_action"] == "fact_samples_excluded"
    assert continued["gap_decision"]["degraded"] is True
    assert "indicator_action" not in continued["gap_decision"]


def test_snapshot_sweep_record_visits_are_linear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_count = 256
    records = [
        _bbo_record(
            bucket_end=_BASE + timedelta(seconds=index + 1),
            known_at=_BASE + timedelta(seconds=index + 1),
            commit_seq=index + 1,
            mid_price=str(100 + index / 1000),
        )
        for index in range(record_count)
    ]
    decision_times = [record.fact.known_at for record in records]
    calls = {"known_at": 0, "freshness": 0, "matches": 0}
    original_known_at = evaluator_module._record_known_at
    original_freshness = evaluator_module._record_freshness_time
    original_matches = evaluator_module._record_matches_where

    def counted_known_at(record: Any) -> datetime:
        calls["known_at"] += 1
        return original_known_at(record)

    def counted_freshness(record: Any) -> datetime:
        calls["freshness"] += 1
        return original_freshness(record)

    def counted_matches(record: Any, where: Mapping[str, Any]) -> bool:
        calls["matches"] += 1
        return original_matches(record, where)

    monkeypatch.setattr(evaluator_module, "_record_known_at", counted_known_at)
    monkeypatch.setattr(
        evaluator_module,
        "_record_freshness_time",
        counted_freshness,
    )
    monkeypatch.setattr(
        evaluator_module,
        "_record_matches_where",
        counted_matches,
    )

    snapshots = evaluator_module._causal_snapshot_series(
        records,
        snapshot_requests=[(value, None) for value in decision_times],
        where={},
        alias="bbo",
    )

    assert len(snapshots) == record_count
    assert snapshots[(decision_times[-1], None)] is records[-1]
    assert calls["known_at"] <= 2 * record_count + len(decision_times)
    assert calls["freshness"] <= 2 * record_count
    assert calls["matches"] <= 2 * record_count
