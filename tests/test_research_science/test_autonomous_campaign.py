from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from portal.backend.service.research.campaign_runner import (
    _gate_failures,
    _protocol_manifest,
)
from research_science import (
    CampaignCharter,
    CampaignExecutionCosts,
    FrozenCampaignBar,
    build_campaign_features,
    build_campaign_graph_manifest,
    campaign_graph_specs,
    evaluate_campaign_graph,
    full_scoring_indexes,
    resolve_campaign_charter,
    validation_scoring_indexes,
)
from strategies.typed_graph import TypedStrategyGraph, compile_typed_strategy_graph

CHARTER_PATH = (
    Path(__file__).parents[2]
    / "config"
    / "research_campaigns"
    / "btc_perp_market_structure_v1.json"
)


def _charter_raw() -> dict:
    return json.loads(CHARTER_PATH.read_text(encoding="utf-8"))


def _charter() -> CampaignCharter:
    return resolve_campaign_charter(
        _charter_raw(),
        sealed_holdout_binding={
            "dataset_id": "sealed-holdout-id",
            "dataset_hash": "sealed-holdout-hash",
            "window_start": "2026-08-05T15:30:00Z",
            "window_end": "2026-08-05T16:33:00Z",
        },
    )


def _bars(count: int = 100) -> tuple[FrozenCampaignBar, ...]:
    start = datetime(2026, 8, 5, 0, 0, tzinfo=UTC)
    rows = []
    price = 60_000.0
    for index in range(count):
        prior = price
        direction = 1.0 if (index // 20) % 2 == 0 else -1.0
        price = prior * (1.0 + direction * 0.002)
        bucket_start = start + timedelta(minutes=index)
        rows.append(
            FrozenCampaignBar(
                bucket_start=bucket_start,
                bucket_end=bucket_start + timedelta(minutes=1),
                known_at=bucket_start + timedelta(minutes=1, milliseconds=50),
                open_price=prior,
                high_price=max(prior, price) * 1.0001,
                low_price=min(prior, price) * 0.9999,
                close_price=price,
                trade_count=20 + index % 5,
                base_volume=100.0,
                quote_notional=6_000_000.0 + index * 1000.0,
                cvd_delta=50.0 * direction,
                open_interest=100_000.0 + index,
                funding_rate=0.00001,
                source_hashes=(f"source-{index}",),
            )
        )
    return tuple(rows)


def _execution(charter: CampaignCharter) -> CampaignExecutionCosts:
    return CampaignExecutionCosts(
        market_slippage_bps=charter.market_slippage_bps,
        taker_fee_rate=charter.taker_fee_rate,
        execution_quality_class="X2",
        execution_model_hash="execution-model-hash",
        fee_schedule_hash="fee-schedule-hash",
        stress_scenarios=charter.cost_stress_scenarios,
    )


def test_campaign_charter_is_immutable_and_fail_closed() -> None:
    raw = _charter_raw()
    charter = _charter()
    assert charter.graph_budget == 24
    assert charter.economic_claim_intent == "selection"
    assert charter.instrument_class == "perpetual_style_future"
    assert charter.instrument_economics_class == "incomplete"
    assert len(charter.charter_hash) == 64
    assert charter.dataset("holdout").blind_alias == "btc-perp-final-session-v1"
    public_holdout = next(
        row for row in raw["datasets"] if row["role"] == "holdout"
    )
    assert public_holdout == {
        "role": "holdout",
        "blind_alias": "btc-perp-final-session-v1",
        "sealed": True,
    }

    disclosed = deepcopy(raw)
    next(row for row in disclosed["datasets"] if row["role"] == "holdout")[
        "dataset_id"
    ] = "leaked"
    with pytest.raises(ValueError, match="must not disclose"):
        resolve_campaign_charter(
            disclosed,
            sealed_holdout_binding={
                "dataset_id": "sealed-holdout-id",
                "dataset_hash": "sealed-holdout-hash",
                "window_start": "2026-08-05T15:30:00Z",
                "window_end": "2026-08-05T16:33:00Z",
            },
        )

    pinned = charter.to_dict()
    pinned["objective"] = "changed after activation"
    with pytest.raises(ValueError, match="charter hash mismatch"):
        CampaignCharter.from_dict(pinned)

    unsafe = deepcopy(raw)
    unsafe["provider_fetch_allowed"] = True
    with pytest.raises(ValueError, match="cannot fetch providers"):
        resolve_campaign_charter(
            unsafe,
            sealed_holdout_binding={
                "dataset_id": "sealed-holdout-id",
                "dataset_hash": "sealed-holdout-hash",
                "window_start": "2026-08-05T15:30:00Z",
                "window_end": "2026-08-05T16:33:00Z",
            },
        )

    spot = deepcopy(raw)
    spot["instrument_class"] = "spot"
    with pytest.raises(ValueError, match="perpetual-class"):
        resolve_campaign_charter(
            spot,
            sealed_holdout_binding={
                "dataset_id": "sealed-holdout-id",
                "dataset_hash": "sealed-holdout-hash",
                "window_start": "2026-08-05T15:30:00Z",
                "window_end": "2026-08-05T16:33:00Z",
            },
        )


def test_campaign_graph_space_is_exactly_bounded_and_compilable() -> None:
    specs = campaign_graph_specs()
    assert len(specs) == 24
    assert [row["ordinal"] for row in specs] == list(range(1, 25))
    hashes = set()
    for spec in specs:
        manifest = build_campaign_graph_manifest(
            campaign_id="campaign",
            family_id="family",
            protocol_hash="protocol-hash",
            spec=spec,
        )
        graph = TypedStrategyGraph.from_dict(manifest)
        compiled = compile_typed_strategy_graph(graph)
        hashes.add(graph.graph_hash)
        assert compiled.graph.graph_id.endswith(f"{spec['ordinal']:02d}")
    assert len(hashes) == 24


def test_campaign_features_are_prefix_invariant() -> None:
    bars = _bars(30)
    prefix = build_campaign_features(bars[:20], lookback_bars=10)
    full = build_campaign_features(bars, lookback_bars=10)
    assert [row.feature_hash for row in prefix] == [
        row.feature_hash for row in full[:20]
    ]
    assert prefix[-1].facts == full[19].facts


def test_late_known_bar_is_excluded_from_earlier_feature_prefix() -> None:
    bars = list(_bars(20))
    delayed = bars[10]
    bars[10] = FrozenCampaignBar(
        **{
            **delayed.__dict__,
            "known_at": delayed.known_at + timedelta(minutes=5),
        }
    )
    features = build_campaign_features(bars, lookback_bars=10)
    expected = (
        float(bars[11].close_price) / float(bars[9].close_price) - 1.0
    ) * 10_000.0
    assert features[11].facts["market.return_1_bps"] == pytest.approx(expected)


def test_walk_forward_indexes_apply_declared_gaps() -> None:
    charter = _charter()
    indexes = validation_scoring_indexes(charter, 87)
    assert indexes[:3] == (30, 31, 32)
    assert indexes[12:15] == (42, 43, 44)
    assert indexes[24:27] == (54, 55, 56)
    assert len(indexes) == 36
    with pytest.raises(ValueError, match="do not cover"):
        validation_scoring_indexes(charter, 75)


def test_campaign_evaluation_is_deterministic_and_costed() -> None:
    charter = _charter()
    features = build_campaign_features(_bars(), lookback_bars=charter.feature_lookback_bars)
    manifest = build_campaign_graph_manifest(
        campaign_id=charter.campaign_id,
        family_id="family",
        protocol_hash="protocol-hash",
        spec=campaign_graph_specs()[0],
    )
    graph = TypedStrategyGraph.from_dict(manifest)
    indexes = full_scoring_indexes(charter, len(features))
    left = evaluate_campaign_graph(
        charter=charter,
        graph=graph,
        rows=features,
        execution=_execution(charter),
        scoring_indexes=indexes,
    )
    right = evaluate_campaign_graph(
        charter=charter,
        graph=graph,
        rows=features,
        execution=_execution(charter),
        scoring_indexes=indexes,
    )
    assert left == right
    assert left.trade_count > 0
    assert left.execution_model_hash == "execution-model-hash"
    assert set(left.benchmark_metric_results) == set(charter.benchmark_ids)
    assert left.metric_results["mean_net_return_bps"] < 100.0
    assert left.artifact_hash == right.artifact_hash
    evidence = left.to_attempt_evidence(charter=charter, validation=True)
    assert evidence["walk_forward_fold_count"] == 3
    assert evidence["purge_bars"] == 10
    assert evidence["instrument_economics_class"] == "incomplete"
    assert evidence["promotion_eligible"] is False


def test_zero_trade_bars_can_only_carry_prior_causal_price() -> None:
    rows = list(_bars(15))
    source = rows[8]
    rows[8] = FrozenCampaignBar(
        bucket_start=source.bucket_start,
        bucket_end=source.bucket_end,
        known_at=source.known_at,
        open_price=None,
        high_price=None,
        low_price=None,
        close_price=None,
        trade_count=0,
        base_volume=0.0,
        quote_notional=0.0,
        cvd_delta=0.0,
        open_interest=source.open_interest,
        funding_rate=source.funding_rate,
        source_hashes=source.source_hashes,
    )
    features = build_campaign_features(rows, lookback_bars=10)
    assert features[8].reference_price == features[7].reference_price
    assert features[8].facts["market.has_trade"] is False


def test_campaign_protocol_manifest_pins_every_scientific_boundary() -> None:
    charter = _charter()
    manifest = _protocol_manifest(charter, code_revision="abcdef123456")
    assert manifest["economic_claim_intent"] == "selection"
    assert manifest["policy_versions"]["code_revision"] == "abcdef123456"
    assert manifest["leakage"]["purge_bars"] == 10
    assert manifest["leakage"]["embargo_bars"] == 10
    assert manifest["walk_forward"]["fold_count"] == 3
    assert manifest["minimum_execution_quality_class"] == "X2"
    assert manifest["budget"] == {
        "max_attempts": 40,
        "max_runtime_seconds": 3600.0,
        "max_compute_units": 40.0,
        "max_validation_feedback_uses": 4,
    }
    holdout = next(row for row in manifest["datasets"] if row["role"] == "holdout")
    assert holdout["dataset_id"] == "sealed-holdout-id"
    assert holdout["blind_alias"] == "btc-perp-final-session-v1"


def test_campaign_validation_gate_is_mandatory_and_fail_closed() -> None:
    charter = _charter()
    features = build_campaign_features(
        _bars(), lookback_bars=charter.feature_lookback_bars
    )
    graph = TypedStrategyGraph.from_dict(
        build_campaign_graph_manifest(
            campaign_id=charter.campaign_id,
            family_id="family",
            protocol_hash="protocol-hash",
            spec=campaign_graph_specs()[0],
        )
    )
    evaluation = evaluate_campaign_graph(
        charter=charter,
        graph=graph,
        rows=features,
        execution=_execution(charter),
        scoring_indexes=full_scoring_indexes(charter, len(features)),
    )
    passing_metrics = {
        **evaluation.metric_results,
        charter.primary_metric: 1.0,
        "directional_accuracy": 0.6,
        "median_net_return_bps": 0.5,
        "signal_rate": 0.2,
        "max_drawdown_bps": 5.0,
        "worst_trade_bps": 2.0,
        "cost_stress_min_return_bps": 0.1,
    }
    passing_benchmarks = {
        benchmark_id: {charter.primary_metric: 0.0}
        for benchmark_id in charter.benchmark_ids
    }
    eligible = replace(
        evaluation,
        sample_count=charter.minimum_sample_count,
        trade_count=charter.minimum_trade_count,
        calendar_days=charter.minimum_calendar_days,
        exposure=charter.minimum_exposure,
        metric_results=passing_metrics,
        benchmark_metric_results=passing_benchmarks,
        execution_stress_ids_passed=charter.execution_stress_ids,
    )
    assert _gate_failures(charter, eligible) == ()
    underpowered = replace(eligible, sample_count=charter.minimum_sample_count - 1)
    assert _gate_failures(charter, underpowered) == (
        "sample_count_below_minimum",
    )
    unstressed = replace(eligible, execution_stress_ids_passed=())
    assert _gate_failures(charter, unstressed) == (
        "cost_stress_survival_failed",
    )
