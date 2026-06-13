from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("pandas")

import pandas as pd
from fastapi.testclient import TestClient

from portal.backend.controller import research as research_controller
from portal.backend.main import app
from portal.backend.service.research import checks, service


def _candles() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0, 102.5, 104.0, 105.0],
            "high": [101.0, 101.4, 103.0, 103.0, 106.0, 107.0],
            "low": [99.0, 100.8, 101.8, 102.1, 103.5, 104.5],
            "close": [101.0, 101.2, 102.6, 102.8, 105.5, 106.0],
            "volume": [10.0, 9.0, 20.0, 19.0, 30.0, 32.0],
        },
        index=pd.to_datetime(
            [
                "2026-01-01T00:00:00Z",
                "2026-01-01T01:00:00Z",
                "2026-01-01T02:00:00Z",
                "2026-01-01T03:00:00Z",
                "2026-01-01T04:00:00Z",
                "2026-01-01T05:00:00Z",
            ],
            utc=True,
        ),
    )


def test_raw_event_check_summarizes_forward_outcomes() -> None:
    payload = checks.evaluate_raw_event_check(
        _candles(),
        detector={"type": "raw_condition", "field": "close", "operator": "gt", "value_field": "previous_close"},
        outcomes={"forward_bars": [1, 2], "min_sample_count": 1},
        data_quality={"status": "clean"},
    )

    assert payload["schema_version"] == "research_check_result.v1"
    assert payload["check_family"] == "raw_forward_outcome"
    assert payload["status"] == "completed"
    assert payload["sample_count"] == 5
    assert payload["outcomes"]["summary"]["1"]["sample_count"] == 4
    assert payload["outcomes"]["summary"]["2"]["sample_count"] == 3
    assert payload["recommendation"] == "promote_to_hypothesis"
    assert payload["events"][0]["event_time"] == "2026-01-01T01:00:00Z"


def test_raw_event_check_honors_max_examples() -> None:
    payload = checks.evaluate_raw_event_check(
        _candles(),
        detector={"type": "raw_condition", "field": "close", "operator": "gt", "value_field": "previous_close"},
        outcomes={"forward_bars": [1], "min_sample_count": 1, "max_examples": 1},
        data_quality={"status": "clean"},
    )

    assert payload["sample_count"] == 5
    assert len(payload["events"]) == 1
    assert payload["event_count_truncated"] == 4


def test_research_check_service_creates_observation_check_and_link(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []

    def fake_create_item(**kwargs):
        item_id = kwargs.get("item_id") or f"item-{len(created) + 1}"
        item = {
            "id": item_id,
            "kind": kwargs["kind"],
            "status": kwargs["status"],
            "title": kwargs["title"],
            "payload": dict(kwargs.get("payload") or {}),
        }
        item.update({key: kwargs.get(key) for key in ("symbol", "timeframe", "instrument_id")})
        created.append(item)
        return item

    def fake_create_link(**kwargs):
        link = {"id": "link-1", **kwargs}
        links.append(link)
        return link

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", fake_create_item)
    monkeypatch.setattr(service.repository, "create_link", fake_create_link)
    monkeypatch.setattr(
        service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )
    monkeypatch.setattr(
        service.candle_service,
        "preflight_candle_coverage_by_instrument",
        lambda *args: {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": "inst-eth",
            "symbol": "ETH/USD",
            "provider": "CCXT",
            "exchange": "coinbase",
            "timeframe": "1h",
            "status": "ok",
            "continuity": {"final_status": "clean"},
            "row_count": 6,
            "missing_ranges": [],
        },
    )
    monkeypatch.setattr(service.candle_service, "fetch_ohlcv_by_instrument", lambda *args: _candles())

    payload = service.run_research_check(
        {
            "title": "ETH contraction follow-through",
            "scope": {
                "instrument_id": "inst-eth",
                "timeframe": "1h",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T06:00:00Z",
            },
            "detector": {"type": "raw_condition", "field": "close", "operator": "gt", "value_field": "previous_close"},
            "outcomes": {"forward_bars": [1], "min_sample_count": 1},
        }
    )

    assert payload["schema_version"] == "research_check_run.v1"
    assert payload["status"] == "completed"
    assert created[0]["kind"] == "observation"
    assert created[1]["kind"] == "research_check"
    assert created[1]["payload"]["result"]["status"] == "completed"
    assert links == [
        {
            "id": "link-1",
            "source_item_id": created[1]["id"],
            "target_type": "research_item",
            "target_id": created[0]["id"],
            "relation": "tests",
            "metadata": {"target_kind": "observation"},
        }
    ]


def test_research_check_service_fails_loud_for_invalid_detector_before_blocked_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[dict[str, Any]] = []

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", lambda **kwargs: created.append(kwargs) or kwargs)
    monkeypatch.setattr(
        service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )
    monkeypatch.setattr(
        service.candle_service,
        "preflight_candle_coverage_by_instrument",
        lambda *args: {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": "inst-eth",
            "symbol": "ETH/USD",
            "provider": "CCXT",
            "exchange": "coinbase",
            "timeframe": "1h",
            "status": "error",
            "continuity": {"final_status": "missing"},
            "row_count": 0,
            "missing_ranges": [],
            "message": "missing source candles",
        },
    )

    with pytest.raises(ValueError, match="unsupported raw detector field"):
        service.run_research_check(
            {
                "title": "ETH bad field",
                "scope": {
                    "instrument_id": "inst-eth",
                    "timeframe": "1h",
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-01T06:00:00Z",
                },
                "detector": {"type": "raw_condition", "field": "future_close", "operator": "lt", "value": 0.01},
            }
        )

    assert created == []


def test_research_check_sweep_ranks_metric_contract_and_reuses_scope_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[dict[str, Any]] = []
    coverage_calls: list[tuple[Any, ...]] = []
    candle_calls: list[tuple[Any, ...]] = []
    evidence_calls: list[dict[str, Any]] = []

    monkeypatch.setattr(service.repository, "create_item", lambda **kwargs: created.append(kwargs) or kwargs)
    monkeypatch.setattr(
        service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )

    def fake_coverage(*args):
        coverage_calls.append(args)
        return {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": "inst-eth",
            "symbol": "ETH/USD",
            "provider": "CCXT",
            "exchange": "coinbase",
            "timeframe": "1h",
            "status": "ok",
            "continuity": {"final_status": "clean"},
            "row_count": 6,
            "missing_ranges": [],
        }

    def fake_candles(*args):
        candle_calls.append(args)
        return _candles()

    def fake_evidence(
        inst_id,
        start,
        end,
        interval,
        *,
        indicator_param_overrides=None,
        candle_frame=None,
        source_frame_cache=None,
        **_kwargs,
    ):
        assert inst_id == "indicator-1"
        assert start == "2026-01-01T00:00:00Z"
        assert end == "2026-01-01T06:00:00Z"
        assert interval == "1h"
        assert candle_frame is not None
        assert source_frame_cache is not None
        overrides = dict(indicator_param_overrides or {})
        evidence_calls.append(overrides)
        sample_count = 3 if overrides.get("touch_tolerance") == 0.4 else 1
        outputs = [
            {
                "bar_index": index + 1,
                "time": f"2026-01-01T0{index + 1}:00:00Z",
                "indicator_id": "indicator-1",
                "indicator_type": "generic",
                "output_name": "entry",
                "output_type": "signal",
                "event_key": "entry_long",
                "event": {"key": "entry_long"},
                "value": {"events": [{"key": "entry_long"}]},
            }
            for index in range(sample_count)
        ]
        return {
            "schema_version": "indicator_output_evidence.v1",
            "indicator": {"id": "indicator-1", "type": "generic", "params": overrides},
            "runtime_path": "typed_indicator_engine.v1",
            "ready_counts": {"entry": sample_count},
            "not_ready_counts": {},
            "candles": [
                {"time": str(timestamp).replace("+00:00", "Z"), **{field: float(row[field]) for field in ["open", "high", "low", "close", "volume"]}}
                for timestamp, row in _candles().iterrows()
            ],
            "outputs": outputs,
        }

    monkeypatch.setattr(service.candle_service, "preflight_candle_coverage_by_instrument", fake_coverage)
    monkeypatch.setattr(service.candle_service, "fetch_ohlcv_by_instrument", fake_candles)
    monkeypatch.setattr(service, "collect_runtime_output_evidence_for_instance", fake_evidence)

    payload = service.sweep_research_checks(
        {
            "title": "Entry variant sweep",
            "check_family": "indicator_forward_outcome",
            "scope": {
                "indicator_id": "indicator-1",
                "instrument_id": "inst-eth",
                "timeframe": "1h",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T06:00:00Z",
            },
            "detector": {"type": "record_match", "output_name": "entry"},
            "outcomes": {"forward_bars": [1], "min_sample_count": 1},
            "variants": [
                {"id": "base", "param_overrides": {}},
                {"id": "tol04", "label": "touch 0.4", "param_overrides": {"touch_tolerance": 0.4}},
            ],
            "ranking": {
                "rank_by": "sample_count",
                "direction": "desc",
                "display_metrics": ["eligible_events"],
            },
        }
    )

    assert payload["schema_version"] == "research_check_sweep.v1"
    assert payload["evaluation_count"] == 2
    assert payload["leaderboard"]["rows"][0]["variant_id"] == "tol04"
    assert payload["leaderboard"]["rows"][0]["rank_metric"] == {"path": "sample_count", "value": 3.0}
    assert payload["leaderboard"]["rows"][1]["variant_id"] == "base"
    assert payload["cache"]["stats"] == {
        "candle_hits": 1,
        "candle_misses": 1,
        "coverage_hits": 1,
        "coverage_misses": 1,
    }
    assert len(coverage_calls) == 1
    assert len(candle_calls) == 1
    assert evidence_calls == [{}, {"touch_tolerance": 0.4}]
    assert created == []


def test_research_check_sweep_fails_when_rank_metric_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )
    monkeypatch.setattr(
        service.candle_service,
        "preflight_candle_coverage_by_instrument",
        lambda *args: {
            "status": "ok",
            "continuity": {"final_status": "clean"},
            "row_count": 6,
            "missing_ranges": [],
        },
    )
    monkeypatch.setattr(service.candle_service, "fetch_ohlcv_by_instrument", lambda *args: _candles())
    monkeypatch.setattr(
        service,
        "collect_runtime_output_evidence_for_instance",
        lambda *args, **kwargs: {
            "indicator": {"id": "indicator-1", "type": "generic"},
            "runtime_path": "typed_indicator_engine.v1",
            "ready_counts": {},
            "not_ready_counts": {},
            "candles": [
                {"time": str(timestamp).replace("+00:00", "Z"), **{field: float(row[field]) for field in ["open", "high", "low", "close", "volume"]}}
                for timestamp, row in _candles().iterrows()
            ],
            "outputs": [],
        },
    )

    with pytest.raises(ValueError, match="rank metric missing"):
        service.sweep_research_checks(
            {
                "check_family": "indicator_forward_outcome",
                "scope": {
                    "indicator_id": "indicator-1",
                    "instrument_id": "inst-eth",
                    "timeframe": "1h",
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-01T06:00:00Z",
                },
                "detector": {"type": "record_match", "output_name": "entry"},
                "variants": [{"id": "base", "param_overrides": {}}],
                "ranking": {"rank_by": "outcomes.summary.12.median_forward_return_pct", "direction": "desc"},
            }
        )


def test_run_signal_summary_check_summarizes_report_signals() -> None:
    payload = checks.evaluate_run_signal_summary(
        {
            "signals": [
                {
                    "signal_id": "sig-1",
                    "symbol": "BTCUSDT",
                    "output_name": "confirmed_balance_breakout",
                    "event_key": "confirmed_balance_breakout_long",
                },
                {
                    "signal_id": "sig-2",
                    "symbol": "ETHUSDT",
                    "output_name": "balance_retest",
                    "event_key": "balance_retest_long",
                },
            ],
            "decisions": [{"decision_id": "decision-1", "signal_id": "sig-1", "decision_state": "accepted"}],
            "trades": [{"id": "trade-1", "decision_id": "decision-1", "net_pnl": 12.0}],
        },
        detector={"type": "run_signal_match", "output_name": "confirmed_balance_breakout"},
        outcomes={"bucket_by": ["symbol", "event_key"], "min_sample_count": 1},
        data_quality={"status": "clean"},
    )

    assert payload["check_family"] == "run_signal_summary"
    assert payload["sample_count"] == 1
    assert payload["outcomes"]["decision_states"] == {"accepted": 1}
    assert payload["outcomes"]["trade_counts"] == {"with_trade": 1}
    assert payload["recommendation"] == "promote_to_hypothesis"


def test_run_signal_summary_normalizes_report_signal_context_fields() -> None:
    payload = checks.evaluate_run_signal_summary(
        {
            "signals": [
                {
                    "signal_id": "sig-1",
                    "symbol": "BTCUSDT",
                    "context": {
                        "event_key": "confirmed_balance_breakout_long",
                        "decision_artifact": {
                            "decision_context": {
                                "trigger_output_ref": "profile-1.confirmed_balance_breakout",
                                "event_key": "confirmed_balance_breakout_long",
                            },
                            "referenced_outputs": {
                                "profile-1.confirmed_balance_breakout": {
                                    "type": "signal",
                                    "event_keys": [
                                        "confirmed_balance_breakout_long",
                                        "confirmed_balance_breakout_short",
                                    ],
                                }
                            },
                        },
                    },
                    "indicator_context": {
                        "outputs": {
                            "profile-1.confirmed_balance_breakout": {
                                "output_name": "confirmed_balance_breakout",
                                "event_keys": ["confirmed_balance_breakout_long"],
                            }
                        }
                    },
                },
                {
                    "signal_id": "sig-2",
                    "symbol": "ETHUSDT",
                    "context": {
                        "event_key": "balance_retest_long",
                        "decision_artifact": {
                            "decision_context": {
                                "trigger_output_ref": "profile-1.balance_retest",
                                "event_key": "balance_retest_long",
                            }
                        },
                    },
                },
            ],
            "decisions": [],
            "trades": [],
        },
        detector={"type": "run_signal_match", "output_name": "confirmed_balance_breakout"},
        outcomes={"bucket_by": ["symbol", "output_name", "event_key"], "min_sample_count": 1},
        data_quality={"status": "clean"},
    )

    assert payload["sample_count"] == 1
    assert payload["outcomes"]["buckets"] == {
        "symbol=BTCUSDT|output_name=confirmed_balance_breakout|event_key=confirmed_balance_breakout_long": 1
    }
    assert payload["events"][0]["output_name"] == "confirmed_balance_breakout"
    assert payload["events"][0]["event_key"] == "confirmed_balance_breakout_long"
    assert payload["recommendation"] == "promote_to_hypothesis"


def test_run_check_matching_preserves_falsey_values() -> None:
    payload = checks.evaluate_run_signal_summary(
        {
            "signals": [
                {"signal_id": "sig-1", "symbol": "BTCUSDT", "linked_trade_count": 0},
                {"signal_id": "sig-2", "symbol": "ETHUSDT"},
            ],
            "decisions": [],
            "trades": [],
        },
        detector={"type": "run_signal_match", "linked_trade_count": 0},
        outcomes={"min_sample_count": 1},
        data_quality={"status": "clean"},
    )

    assert payload["sample_count"] == 1
    assert payload["events"][0]["signal_id"] == "sig-1"


def test_report_backed_check_rejects_mismatched_detector_family() -> None:
    with pytest.raises(ValueError, match="unsupported run check detector type for run_signal_summary"):
        checks.evaluate_run_signal_summary(
            {"signals": [], "decisions": [], "trades": []},
            detector={"type": "run_decision_match", "decision_state": "accepted"},
            outcomes={},
            data_quality={"status": "clean"},
        )

    with pytest.raises(ValueError, match="unsupported run check detector type for run_decision_trade_comparison"):
        checks.evaluate_run_decision_trade_comparison(
            {"signals": [], "decisions": [], "trades": []},
            detector={"type": "run_signal_match", "output_name": "confirmed_balance_breakout"},
            outcomes={},
            data_quality={"status": "clean"},
        )


def test_report_backed_research_check_rejects_mismatched_detector_before_persistence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[dict[str, Any]] = []

    monkeypatch.setattr(service.repository, "create_item", lambda **kwargs: created.append(kwargs) or kwargs)

    with pytest.raises(ValueError, match="unsupported run check detector type for run_signal_summary"):
        service.run_research_check(
            {
                "title": "Mismatched detector",
                "check_family": "run_signal_summary",
                "scope": {"run_id": "run-1"},
                "detector": {"type": "run_decision_match", "decision_state": "accepted"},
            }
        )

    assert created == []


def test_report_backed_research_check_links_observation_and_run(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []

    def fake_create_item(**kwargs):
        item_id = kwargs.get("item_id") or f"item-{len(created) + 1}"
        item = {
            "id": item_id,
            "kind": kwargs["kind"],
            "status": kwargs["status"],
            "title": kwargs["title"],
            "payload": dict(kwargs.get("payload") or {}),
            "symbol": kwargs.get("symbol"),
            "timeframe": kwargs.get("timeframe"),
        }
        created.append(item)
        return item

    def fake_create_link(**kwargs):
        link = {"id": f"link-{len(links) + 1}", **kwargs}
        links.append(link)
        return link

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", fake_create_item)
    monkeypatch.setattr(service.repository, "create_link", fake_create_link)
    monkeypatch.setattr(
        service.reports_contract,
        "get_run_research_dataset",
        lambda run_id: {
            "metadata": {
                "run_id": run_id,
                "bot_id": "bot-1",
                "strategy_id": "strategy-1",
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "simulated_window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            },
            "readiness": {"dataset_status": "ready", "safe_to_compare": True, "caveats": []},
            "diagnostics": {"summary": {}},
            "signals": [
                {
                    "signal_id": "sig-1",
                    "symbol": "BTCUSDT",
                    "output_name": "confirmed_balance_breakout",
                    "event_key": "confirmed_balance_breakout_long",
                }
            ],
            "decisions": [{"decision_id": "decision-1", "signal_id": "sig-1", "decision_state": "accepted"}],
            "trades": [{"id": "trade-1", "decision_id": "decision-1", "net_pnl": 1.0}],
        },
    )

    payload = service.run_research_check(
        {
            "title": "Run signal check",
            "check_family": "run_signal_summary",
            "scope": {"run_id": "run-1"},
            "detector": {"type": "run_signal_match", "output_name": "confirmed_balance_breakout"},
            "outcomes": {"min_sample_count": 1},
        }
    )

    assert payload["status"] == "completed"
    assert payload["result"]["check_family"] == "run_signal_summary"
    assert created[0]["kind"] == "observation"
    assert created[0]["symbol"] == "BTCUSDT"
    assert created[0]["timeframe"] == "5m"
    assert created[0]["payload"]["scope"]["run_id"] == "run-1"
    assert created[0]["payload"]["scope"]["strategy_id"] == "strategy-1"
    assert created[0]["payload"]["scope"]["bot_id"] == "bot-1"
    assert created[0]["payload"]["scope"]["start"] == "2026-01-01T00:00:00Z"
    assert created[0]["payload"]["scope"]["end"] == "2026-01-02T00:00:00Z"
    assert created[1]["payload"]["request"]["scope"]["run_id"] == "run-1"
    assert [link["target_type"] for link in links] == ["research_item", "run"]
    assert links[1]["relation"] == "analyzes"


def test_report_backed_research_check_blocks_on_report_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []

    def fake_create_item(**kwargs):
        item_id = kwargs.get("item_id") or f"item-{len(created) + 1}"
        item = {
            "id": item_id,
            "kind": kwargs["kind"],
            "status": kwargs["status"],
            "title": kwargs["title"],
            "payload": dict(kwargs.get("payload") or {}),
            "symbol": kwargs.get("symbol"),
            "timeframe": kwargs.get("timeframe"),
        }
        created.append(item)
        return item

    def fake_create_link(**kwargs):
        link = {"id": f"link-{len(links) + 1}", **kwargs}
        links.append(link)
        return link

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", fake_create_item)
    monkeypatch.setattr(service.repository, "create_link", fake_create_link)
    monkeypatch.setattr(
        service.reports_contract,
        "get_run_research_dataset",
        lambda run_id: {
            "metadata": {
                "run_id": run_id,
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "simulated_window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            },
            "readiness": {"dataset_status": "failed", "safe_to_compare": False, "caveats": ["run_failed"]},
            "diagnostics": {"summary": {"errors": 1}},
            "signals": [],
            "decisions": [],
            "trades": [],
        },
    )

    payload = service.run_research_check(
        {
            "title": "Failed run signal check",
            "check_family": "run_signal_summary",
            "scope": {"run_id": "run-2"},
            "detector": {"type": "run_signal_match", "output_name": "confirmed_balance_breakout"},
            "outcomes": {"min_sample_count": 1},
        }
    )

    assert payload["status"] == "blocked"
    assert created[0]["kind"] == "observation"
    assert created[1]["kind"] == "research_check"
    assert created[1]["status"] == "blocked"
    assert created[1]["payload"]["result"]["data_quality"]["status"] == "blocked"
    assert created[1]["payload"]["result"]["data_quality"]["readiness_status"] == "failed"
    assert [link["target_type"] for link in links] == ["research_item", "run"]


def test_report_backed_research_check_fails_loud_for_unsupported_detector(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", lambda **kwargs: created.append(kwargs) or kwargs)
    monkeypatch.setattr(
        service.reports_contract,
        "get_run_research_dataset",
        lambda run_id: {
            "metadata": {
                "run_id": run_id,
                "symbols": ["BTCUSDT"],
                "timeframe": "5m",
                "simulated_window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            },
            "readiness": {"dataset_status": "ready", "safe_to_compare": True, "caveats": []},
            "diagnostics": {"summary": {}},
            "signals": [{"signal_id": "sig-1", "symbol": "BTCUSDT"}],
            "decisions": [],
            "trades": [],
        },
    )

    with pytest.raises(ValueError, match="unsupported run check detector type"):
        service.run_research_check(
            {
                "title": "Bad run signal check",
                "check_family": "run_signal_summary",
                "scope": {"run_id": "run-3"},
                "detector": {"type": "unsupported_detector"},
            }
        )

    assert created == []


def test_indicator_forward_outcome_matches_output_fields() -> None:
    payload = checks.evaluate_indicator_forward_outcome(
        {
            "indicator": {"id": "stats-1", "type": "candle_stats"},
            "runtime_path": "typed_indicator_engine.v1",
            "ready_counts": {"candle_stats": 3},
            "not_ready_counts": {},
            "candles": [
                {"time": "2026-01-01T00:00:00Z", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 10},
                {"time": "2026-01-01T01:00:00Z", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 11},
                {"time": "2026-01-01T02:00:00Z", "open": 102, "high": 104, "low": 101, "close": 103, "volume": 12},
            ],
            "outputs": [
                {
                    "bar_index": 0,
                    "time": "2026-01-01T00:00:00Z",
                    "indicator_id": "stats-1",
                    "indicator_type": "candle_stats",
                    "output_name": "candle_stats",
                    "output_type": "metric",
                    "value": {"range_pct": 0.01},
                },
                {
                    "bar_index": 1,
                    "time": "2026-01-01T01:00:00Z",
                    "indicator_id": "stats-1",
                    "indicator_type": "candle_stats",
                    "output_name": "candle_stats",
                    "output_type": "metric",
                    "value": {"range_pct": 0.03},
                },
            ],
        },
        detector={
            "type": "indicator_output_match",
            "output_name": "candle_stats",
            "field": "range_pct",
            "operator": "gt",
            "value": 0.02,
        },
        outcomes={"forward_bars": [1], "min_sample_count": 1},
        data_quality={"status": "clean"},
    )

    assert payload["check_family"] == "indicator_forward_outcome"
    assert payload["sample_count"] == 1
    assert payload["events"][0]["output_name"] == "candle_stats"
    assert payload["events"][0]["field_value"] == 0.03
    assert payload["recommendation"] == "refine"


def test_signal_audit_reconciles_public_output_expectations() -> None:
    payload = checks.evaluate_signal_audit(
        {
            "indicator": {"id": "indicator-1", "type": "generic_state"},
            "runtime_path": "typed_indicator_engine.v1",
            "ready_counts": {"state": 4, "break": 1},
            "not_ready_counts": {},
            "outputs": [
                {
                    "bar_index": 0,
                    "time": "2026-01-01T00:00:00Z",
                    "indicator_id": "indicator-1",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"state_key": "inside", "group_key": "A"},
                },
                {
                    "bar_index": 1,
                    "time": "2026-01-01T01:00:00Z",
                    "indicator_id": "indicator-1",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"state_key": "outside", "group_key": "A"},
                },
                {
                    "bar_index": 1,
                    "time": "2026-01-01T01:00:00Z",
                    "indicator_id": "indicator-1",
                    "output_name": "break",
                    "output_type": "signal",
                    "event_key": "break_out",
                    "event": {"key": "break_out"},
                    "value": {"events": [{"key": "break_out"}]},
                },
                {
                    "bar_index": 2,
                    "time": "2026-01-01T02:00:00Z",
                    "indicator_id": "indicator-1",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"state_key": "inside", "group_key": "A"},
                },
                {
                    "bar_index": 3,
                    "time": "2026-01-01T03:00:00Z",
                    "indicator_id": "indicator-1",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"state_key": "outside", "group_key": "B"},
                },
            ],
        },
        detector={
            "type": "signal_audit",
            "name": "generic break",
            "source_output": "state",
            "source_field": "state_key",
            "from": "inside",
            "to": "outside",
            "same_group_by": ["group_key"],
            "signal_output": "break",
            "event_key": "break_out",
        },
        outcomes={"max_examples": 10},
        data_quality={"status": "clean"},
    )

    assert payload["check_family"] == "signal_audit"
    assert payload["recommendation"] == "review_contract"
    assert payload["outcomes"]["summary"] == {
        "expected_count": 1,
        "emitted_count": 1,
        "matched_count": 1,
        "missing_expected_count": 0,
        "invalid_emitted_count": 0,
        "excluded_candidate_count": 1,
    }
    assert payload["events"][0]["classification"] == "excluded_candidate"
    assert payload["events"][0]["group_values"] == {"group_key": "B"}


def test_signal_audit_surfaces_missing_and_invalid_events() -> None:
    payload = checks.evaluate_signal_audit(
        {
            "indicator": {"id": "indicator-1", "type": "generic_state"},
            "outputs": [
                {
                    "bar_index": 0,
                    "time": "2026-01-01T00:00:00Z",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"state_key": "inside"},
                },
                {
                    "bar_index": 1,
                    "time": "2026-01-01T01:00:00Z",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"state_key": "outside"},
                },
                {
                    "bar_index": 2,
                    "time": "2026-01-01T02:00:00Z",
                    "output_name": "break",
                    "output_type": "signal",
                    "event_key": "break_out",
                    "event": {"key": "break_out"},
                    "value": {"events": [{"key": "break_out"}]},
                },
            ],
        },
        detector={
            "type": "signal_audit",
            "source_output": "state",
            "source_field": "state_key",
            "from": "inside",
            "to": "outside",
            "signal_output": "break",
            "event_key": "break_out",
        },
        outcomes={"max_examples": 10},
        data_quality={"status": "clean"},
    )

    assert payload["recommendation"] == "repair_signal"
    assert payload["outcomes"]["summary"]["missing_expected_count"] == 1
    assert payload["outcomes"]["summary"]["invalid_emitted_count"] == 1
    assert [event["classification"] for event in payload["events"]] == [
        "missing_expected",
        "invalid_emitted",
    ]


def test_signal_audit_condition_preserves_null_expected_value() -> None:
    payload = checks.evaluate_signal_audit(
        {
            "indicator": {"id": "indicator-1", "type": "generic_state"},
            "outputs": [
                {
                    "bar_index": 1,
                    "time": "2026-01-01T01:00:00Z",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"candidate_id": None},
                },
                {
                    "bar_index": 1,
                    "time": "2026-01-01T01:00:00Z",
                    "output_name": "entry",
                    "output_type": "signal",
                    "event_key": "entry_long",
                    "event": {"key": "entry_long"},
                    "value": {"events": [{"key": "entry_long"}]},
                },
            ],
        },
        detector={
            "type": "signal_audit",
            "expectation_type": "condition",
            "source_output": "state",
            "source_field": "candidate_id",
            "operator": "eq",
            "value": None,
            "signal_output": "entry",
            "event_key": "entry_long",
        },
        outcomes={"max_examples": 10},
        data_quality={"status": "clean"},
    )

    assert payload["recommendation"] == "contract_holds"
    assert payload["outcomes"]["summary"]["matched_count"] == 1
    assert payload["outcomes"]["summary"]["missing_expected_count"] == 0
    assert payload["outcomes"]["summary"]["invalid_emitted_count"] == 0


def test_candidate_lifecycle_summarizes_generic_funnel_and_signal_links() -> None:
    outputs = [
        {
            "bar_index": 0,
            "event_index": 0,
            "time": "2026-01-01T00:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "formed",
            "event": {
                "candidate_id": "candidate-a",
                "family": "retest",
                "side": "long",
                "stage": "formed",
                "status": "active",
                "group_key": "group-1",
                "known_at": 1767225600,
                "reason": "source_confirmed",
            },
        },
        {
            "bar_index": 1,
            "event_index": 0,
            "time": "2026-01-01T01:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "eligible",
            "event": {
                "candidate_id": "candidate-a",
                "family": "retest",
                "side": "long",
                "stage": "eligible",
                "status": "active",
                "group_key": "group-1",
                "known_at": 1767229200,
                "reason": "threshold_met",
            },
        },
        {
            "bar_index": 2,
            "event_index": 0,
            "time": "2026-01-01T02:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "touched",
            "event": {
                "candidate_id": "candidate-a",
                "family": "retest",
                "side": "long",
                "stage": "touched",
                "status": "active",
                "group_key": "group-1",
                "known_at": 1767232800,
                "reason": "reference_touched",
            },
        },
        {
            "bar_index": 3,
            "event_index": 0,
            "time": "2026-01-01T03:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "confirmed",
            "event": {
                "candidate_id": "candidate-a",
                "family": "retest",
                "side": "long",
                "stage": "confirmed",
                "status": "closed",
                "group_key": "group-1",
                "signal_output": "entry",
                "signal_event_key": "entry_long",
                "known_at": 1767236400,
                "reason": "signal_emitted",
            },
        },
        {
            "bar_index": 3,
            "event_index": 0,
            "time": "2026-01-01T03:00:00Z",
            "output_name": "entry",
            "output_type": "signal",
            "event_key": "entry_long",
            "event": {"key": "entry_long", "pattern_id": "candidate-a"},
            "value": {"events": [{"key": "entry_long", "pattern_id": "candidate-a"}]},
        },
        {
            "bar_index": 0,
            "event_index": 1,
            "time": "2026-01-01T00:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "formed",
            "event": {
                "candidate_id": "candidate-b",
                "family": "retest",
                "side": "short",
                "stage": "formed",
                "status": "active",
                "group_key": "group-2",
                "known_at": 1767225600,
                "reason": "source_confirmed",
            },
        },
        {
            "bar_index": 1,
            "event_index": 1,
            "time": "2026-01-01T01:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "eligible",
            "event": {
                "candidate_id": "candidate-b",
                "family": "retest",
                "side": "short",
                "stage": "eligible",
                "status": "active",
                "group_key": "group-2",
                "known_at": 1767229200,
                "reason": "threshold_met",
            },
        },
        {
            "bar_index": 4,
            "event_index": 0,
            "time": "2026-01-01T04:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "expired",
            "event": {
                "candidate_id": "candidate-b",
                "family": "retest",
                "side": "short",
                "stage": "expired",
                "status": "closed",
                "group_key": "group-2",
                "known_at": 1767240000,
                "reason": "window_elapsed",
            },
        },
    ]

    payload = checks.evaluate_candidate_lifecycle(
        {"indicator": {"id": "indicator-1", "type": "generic"}, "outputs": outputs},
        detector={"type": "candidate_lifecycle", "family": "retest"},
        outcomes={"max_examples": 10},
        data_quality={"status": "clean"},
    )

    assert payload["check_family"] == "candidate_lifecycle"
    assert payload["recommendation"] == "contract_holds"
    assert payload["sample_count"] == 2
    assert payload["outcomes"]["funnel"]["formed"]["candidate_count"] == 2
    assert payload["outcomes"]["funnel"]["eligible"]["candidate_count"] == 2
    assert payload["outcomes"]["funnel"]["touched"]["candidate_count"] == 1
    assert payload["outcomes"]["funnel"]["confirmed"]["candidate_count"] == 1
    assert payload["outcomes"]["terminal_counts"] == {"confirmed": 1, "expired": 1}
    assert payload["outcomes"]["summary"]["matched_signal_count"] == 1
    assert payload["outcomes"]["summary"]["missing_signal_count"] == 0
    assert payload["outcomes"]["summary"]["invalid_signal_count"] == 0


def test_candidate_lifecycle_signal_defaults_do_not_filter_lifecycle_rows() -> None:
    outputs = [
        {
            "bar_index": 0,
            "event_index": 0,
            "time": "2026-01-01T00:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "formed",
            "event": {
                "candidate_id": "candidate-a",
                "family": "retest",
                "side": "long",
                "stage": "formed",
                "status": "active",
                "known_at": 1767225600,
                "reason": "source_confirmed",
            },
        },
        {
            "bar_index": 1,
            "event_index": 0,
            "time": "2026-01-01T01:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "eligible",
            "event": {
                "candidate_id": "candidate-a",
                "family": "retest",
                "side": "long",
                "stage": "eligible",
                "status": "active",
                "known_at": 1767229200,
                "reason": "threshold_met",
            },
        },
        {
            "bar_index": 2,
            "event_index": 0,
            "time": "2026-01-01T02:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "confirmed",
            "event": {
                "candidate_id": "candidate-a",
                "family": "retest",
                "side": "long",
                "stage": "confirmed",
                "status": "closed",
                "known_at": 1767232800,
                "reason": "signal_emitted",
            },
        },
        {
            "bar_index": 2,
            "event_index": 0,
            "time": "2026-01-01T02:00:00Z",
            "output_name": "entry",
            "output_type": "signal",
            "event_key": "entry_long",
            "event": {"key": "entry_long", "pattern_id": "candidate-a"},
            "value": {"events": [{"key": "entry_long", "pattern_id": "candidate-a"}]},
        },
    ]

    payload = checks.evaluate_candidate_lifecycle(
        {"indicator": {"id": "indicator-1", "type": "generic"}, "outputs": outputs},
        detector={
            "type": "candidate_lifecycle",
            "family": "retest",
            "signal_output": "entry",
            "signal_event_key": "entry_long",
        },
        outcomes={"max_examples": 10},
        data_quality={"status": "clean"},
    )

    assert payload["recommendation"] == "contract_holds"
    assert payload["sample_count"] == 1
    assert payload["outcomes"]["funnel"]["formed"]["candidate_count"] == 1
    assert payload["outcomes"]["funnel"]["eligible"]["candidate_count"] == 1
    assert payload["outcomes"]["funnel"]["confirmed"]["candidate_count"] == 1
    assert payload["outcomes"]["summary"]["matched_signal_count"] == 1
    assert payload["outcomes"]["summary"]["missing_signal_count"] == 0
    assert payload["outcomes"]["summary"]["invalid_signal_count"] == 0


def test_candidate_lifecycle_surfaces_missing_and_invalid_signal_links() -> None:
    outputs = [
        {
            "bar_index": 2,
            "event_index": 0,
            "time": "2026-01-01T02:00:00Z",
            "output_name": "candidate_lifecycle",
            "output_type": "lifecycle",
            "event_key": "confirmed",
            "event": {
                "candidate_id": "candidate-a",
                "family": "retest",
                "side": "long",
                "stage": "confirmed",
                "status": "closed",
                "signal_output": "entry",
                "signal_event_key": "entry_long",
                "known_at": 1767232800,
                "reason": "signal_emitted",
            },
        },
        {
            "bar_index": 2,
            "event_index": 0,
            "time": "2026-01-01T02:00:00Z",
            "output_name": "entry",
            "output_type": "signal",
            "event_key": "entry_long",
            "event": {"key": "entry_long", "pattern_id": "candidate-b"},
            "value": {"events": [{"key": "entry_long", "pattern_id": "candidate-b"}]},
        },
    ]

    payload = checks.evaluate_candidate_lifecycle(
        {"indicator": {"id": "indicator-1", "type": "generic"}, "outputs": outputs},
        detector={"type": "candidate_lifecycle", "family": "retest"},
        outcomes={"max_examples": 10},
        data_quality={"status": "clean"},
    )

    assert payload["recommendation"] == "repair_lifecycle"
    assert payload["outcomes"]["summary"]["missing_signal_count"] == 1
    assert payload["outcomes"]["summary"]["invalid_signal_count"] == 1
    assert [event["classification"] for event in payload["events"]] == [
        "missing_signal",
        "invalid_signal",
    ]


def test_indicator_research_check_uses_persisted_indicator_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []
    links: list[dict[str, Any]] = []

    def fake_create_item(**kwargs):
        item_id = kwargs.get("item_id") or f"item-{len(created) + 1}"
        item = {
            "id": item_id,
            "kind": kwargs["kind"],
            "status": kwargs["status"],
            "title": kwargs["title"],
            "payload": dict(kwargs.get("payload") or {}),
            "symbol": kwargs.get("symbol"),
            "timeframe": kwargs.get("timeframe"),
            "instrument_id": kwargs.get("instrument_id"),
        }
        created.append(item)
        return item

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", fake_create_item)
    monkeypatch.setattr(service.repository, "create_link", lambda **kwargs: links.append(kwargs) or kwargs)
    monkeypatch.setattr(
        service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )
    monkeypatch.setattr(
        service.candle_service,
        "preflight_candle_coverage_by_instrument",
        lambda *args: {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": "inst-eth",
            "symbol": "ETH/USD",
            "provider": "CCXT",
            "exchange": "coinbase",
            "timeframe": "1h",
            "status": "ok",
            "continuity": {"final_status": "clean"},
            "row_count": 3,
            "missing_ranges": [],
        },
    )
    monkeypatch.setattr(
        service,
        "collect_runtime_output_evidence_for_instance",
        lambda *args, **kwargs: {
            "indicator": {"id": "stats-1", "type": "candle_stats"},
            "runtime_path": "typed_indicator_engine.v1",
            "ready_counts": {"candle_stats": 2},
            "not_ready_counts": {},
            "candles": [
                {"time": "2026-01-01T00:00:00Z", "open": 100, "high": 102, "low": 99, "close": 101, "volume": 10},
                {"time": "2026-01-01T01:00:00Z", "open": 101, "high": 103, "low": 100, "close": 102, "volume": 11},
            ],
            "outputs": [
                {
                    "bar_index": 0,
                    "time": "2026-01-01T00:00:00Z",
                    "indicator_id": "stats-1",
                    "indicator_type": "candle_stats",
                    "output_name": "candle_stats",
                    "output_type": "metric",
                    "value": {"range_pct": 0.03},
                }
            ],
        },
    )

    payload = service.run_research_check(
        {
            "title": "Indicator range follow-through",
            "check_family": "indicator_forward_outcome",
            "scope": {
                "indicator_id": "stats-1",
                "instrument_id": "inst-eth",
                "timeframe": "1h",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T02:00:00Z",
            },
            "detector": {
                "type": "indicator_output_match",
                "output_name": "candle_stats",
                "field": "range_pct",
                "operator": "gt",
                "value": 0.02,
            },
            "outcomes": {"forward_bars": [1], "min_sample_count": 1},
        }
    )

    assert payload["status"] == "completed"
    assert created[1]["payload"]["request"]["scope"]["indicator_id"] == "stats-1"
    assert created[1]["payload"]["result"]["check_family"] == "indicator_forward_outcome"
    assert links[0]["relation"] == "tests"


def test_signal_audit_research_check_uses_persisted_indicator_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []

    def fake_create_item(**kwargs):
        item_id = kwargs.get("item_id") or f"item-{len(created) + 1}"
        item = {
            "id": item_id,
            "kind": kwargs["kind"],
            "status": kwargs["status"],
            "title": kwargs["title"],
            "payload": dict(kwargs.get("payload") or {}),
            "symbol": kwargs.get("symbol"),
            "timeframe": kwargs.get("timeframe"),
            "instrument_id": kwargs.get("instrument_id"),
        }
        created.append(item)
        return item

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", fake_create_item)
    monkeypatch.setattr(service.repository, "create_link", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )
    monkeypatch.setattr(
        service.candle_service,
        "preflight_candle_coverage_by_instrument",
        lambda *args: {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": "inst-eth",
            "symbol": "ETH/USD",
            "provider": "CCXT",
            "exchange": "coinbase",
            "timeframe": "1h",
            "status": "ok",
            "continuity": {"final_status": "clean"},
            "row_count": 2,
            "missing_ranges": [],
        },
    )
    monkeypatch.setattr(
        service,
        "collect_runtime_output_evidence_for_instance",
        lambda *args, **kwargs: {
            "indicator": {"id": "indicator-1", "type": "generic_state"},
            "runtime_path": "typed_indicator_engine.v1",
            "ready_counts": {"state": 2, "break": 1},
            "not_ready_counts": {},
            "outputs": [
                {
                    "bar_index": 0,
                    "time": "2026-01-01T00:00:00Z",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"state_key": "inside"},
                },
                {
                    "bar_index": 1,
                    "time": "2026-01-01T01:00:00Z",
                    "output_name": "state",
                    "output_type": "context",
                    "value": {"state_key": "outside"},
                },
                {
                    "bar_index": 1,
                    "time": "2026-01-01T01:00:00Z",
                    "output_name": "break",
                    "output_type": "signal",
                    "event_key": "break_out",
                    "event": {"key": "break_out"},
                    "value": {"events": [{"key": "break_out"}]},
                },
            ],
        },
    )

    payload = service.run_research_check(
        {
            "title": "Signal audit",
            "check_family": "signal_audit",
            "scope": {
                "indicator_id": "indicator-1",
                "instrument_id": "inst-eth",
                "timeframe": "1h",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T02:00:00Z",
            },
            "detector": {
                "type": "signal_audit",
                "source_output": "state",
                "source_field": "state_key",
                "from": "inside",
                "to": "outside",
                "signal_output": "break",
                "event_key": "break_out",
            },
        }
    )

    assert payload["status"] == "completed"
    assert created[1]["payload"]["request"]["scope"]["indicator_id"] == "indicator-1"
    assert created[1]["payload"]["result"]["check_family"] == "signal_audit"
    assert created[1]["payload"]["result"]["recommendation"] == "contract_holds"


def test_candidate_lifecycle_research_check_uses_persisted_indicator_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []

    def fake_create_item(**kwargs):
        item_id = kwargs.get("item_id") or f"item-{len(created) + 1}"
        item = {
            "id": item_id,
            "kind": kwargs["kind"],
            "status": kwargs["status"],
            "title": kwargs["title"],
            "payload": dict(kwargs.get("payload") or {}),
            "symbol": kwargs.get("symbol"),
            "timeframe": kwargs.get("timeframe"),
            "instrument_id": kwargs.get("instrument_id"),
        }
        created.append(item)
        return item

    monkeypatch.setattr(service, "source_revision", lambda: "abc123")
    monkeypatch.setattr(service.repository, "create_item", fake_create_item)
    monkeypatch.setattr(service.repository, "create_link", lambda **kwargs: kwargs)
    monkeypatch.setattr(
        service.instrument_service,
        "get_instrument_record",
        lambda instrument_id: {
            "id": instrument_id,
            "symbol": "ETH/USD",
            "datasource": "CCXT",
            "exchange": "coinbase",
        },
    )
    monkeypatch.setattr(
        service.candle_service,
        "preflight_candle_coverage_by_instrument",
        lambda *args: {
            "schema_version": "candle_coverage_preflight.v1",
            "instrument_id": "inst-eth",
            "symbol": "ETH/USD",
            "provider": "CCXT",
            "exchange": "coinbase",
            "timeframe": "1h",
            "status": "ok",
            "continuity": {"final_status": "clean"},
            "row_count": 2,
            "missing_ranges": [],
        },
    )
    monkeypatch.setattr(
        service,
        "collect_runtime_output_evidence_for_instance",
        lambda *args, **kwargs: {
            "indicator": {"id": "indicator-1", "type": "generic_state"},
            "runtime_path": "typed_indicator_engine.v1",
            "ready_counts": {"candidate_lifecycle": 1, "entry": 1},
            "not_ready_counts": {},
            "outputs": [
                {
                    "bar_index": 1,
                    "event_index": 0,
                    "time": "2026-01-01T01:00:00Z",
                    "output_name": "candidate_lifecycle",
                    "output_type": "lifecycle",
                    "event_key": "confirmed",
                    "event": {
                        "candidate_id": "candidate-a",
                        "family": "retest",
                        "side": "long",
                        "stage": "confirmed",
                        "status": "closed",
                        "signal_output": "entry",
                        "signal_event_key": "entry_long",
                        "known_at": 1767229200,
                        "reason": "signal_emitted",
                    },
                },
                {
                    "bar_index": 1,
                    "event_index": 0,
                    "time": "2026-01-01T01:00:00Z",
                    "output_name": "entry",
                    "output_type": "signal",
                    "event_key": "entry_long",
                    "event": {"key": "entry_long", "pattern_id": "candidate-a"},
                    "value": {"events": [{"key": "entry_long", "pattern_id": "candidate-a"}]},
                },
            ],
        },
    )

    payload = service.run_research_check(
        {
            "title": "Lifecycle audit",
            "check_family": "candidate_lifecycle",
            "scope": {
                "indicator_id": "indicator-1",
                "instrument_id": "inst-eth",
                "timeframe": "1h",
                "start": "2026-01-01T00:00:00Z",
                "end": "2026-01-01T02:00:00Z",
            },
            "detector": {
                "type": "candidate_lifecycle",
                "family": "retest",
            },
        }
    )

    assert payload["status"] == "completed"
    assert created[1]["payload"]["request"]["scope"]["indicator_id"] == "indicator-1"
    assert created[1]["payload"]["result"]["check_family"] == "candidate_lifecycle"
    assert created[1]["payload"]["result"]["recommendation"] == "contract_holds"


def test_run_research_evidence_summarizes_checkable_report_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        service.reports_contract,
        "get_run_research_dataset",
        lambda run_id: {
            "metadata": {
                "run_id": run_id,
                "bot_id": "bot-1",
                "strategy_id": "strategy-1",
                "symbols": ["BTCUSDT"],
                "instrument_ids": ["inst-btc"],
                "timeframe": "5m",
                "simulated_window": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            },
            "readiness": {"dataset_status": "ready", "safe_to_compare": True, "caveats": []},
            "summary": {"accepted_decisions": 1, "rejected_decisions": 1, "closed_trades": 1, "open_trades": 0},
            "signals": [
                {
                    "signal_id": "sig-1",
                    "symbol": "BTCUSDT",
                    "context": {"event_key": "confirmed_balance_breakout_long"},
                    "indicator_context": {
                        "outputs": {
                            "profile-1.confirmed_balance_breakout": {
                                "output_name": "confirmed_balance_breakout",
                                "event_keys": ["confirmed_balance_breakout_long"],
                            }
                        }
                    },
                }
            ],
            "decisions": [
                {"decision_id": "decision-1", "decision_state": "accepted", "reason_code": "ENTRY"},
                {"decision_id": "decision-2", "decision_state": "rejected", "reason_code": "FILTER"},
            ],
            "trades": [{"id": "trade-1", "decision_id": "decision-1"}],
        },
    )

    payload = service.get_run_research_evidence("run-1")

    assert payload["schema_version"] == "run_research_evidence.v1"
    assert payload["counts"]["signals"] == 1
    assert payload["signals"]["output_names"] == {"confirmed_balance_breakout": 1}
    assert payload["signals"]["event_keys"] == {"confirmed_balance_breakout_long": 1}
    assert payload["decisions"]["states"] == {"accepted": 1, "rejected": 1}
    assert payload["supported_checks"][0]["command"] == "qt research check signal"


def test_research_trail_collects_related_items_and_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    items = {
        "obs-1": {"id": "obs-1", "kind": "observation", "title": "Observation", "payload": {}},
        "check-1": {
            "id": "check-1",
            "kind": "research_check",
            "title": "Check",
            "payload": {"result": {"check_family": "run_signal_summary", "sample_count": 1}},
        },
    }
    links = [
        {
            "id": "link-1",
            "source_item_id": "check-1",
            "target_type": "research_item",
            "target_id": "obs-1",
            "relation": "tests",
            "metadata": {},
        },
        {
            "id": "link-2",
            "source_item_id": "check-1",
            "target_type": "run",
            "target_id": "run-1",
            "relation": "analyzes",
            "metadata": {},
        },
    ]
    links_by_item = {
        "obs-1": [links[0]],
        "check-1": links,
    }

    monkeypatch.setattr(service.repository, "get_item", lambda item_id: items[item_id])
    monkeypatch.setattr(
        service.repository,
        "list_links",
        lambda item_id, include_inbound=True: links_by_item[item_id],
    )
    monkeypatch.setattr(
        service,
        "_run_evidence_summary",
        lambda run_id: {"schema_version": "run_research_evidence.v1", "run_id": run_id},
    )

    payload = service.get_research_trail("obs-1")

    assert payload["schema_version"] == "research_trail.v1"
    assert payload["item"]["id"] == "obs-1"
    assert payload["checks"][0]["id"] == "check-1"
    assert payload["runs"] == [{"schema_version": "run_research_evidence.v1", "run_id": "run-1"}]
    assert {link["id"] for link in payload["links"]} == {"link-1", "link-2"}
    assert payload["summary"]["check_count"] == 1


def test_research_check_compare_uses_emitted_forward_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    items = {
        "left": {
            "id": "left",
            "kind": "research_check",
            "title": "Left",
            "payload": {
                "result": {
                    "check_family": "indicator_forward_outcome",
                    "status": "completed",
                    "sample_count": 2,
                    "outcomes": {
                        "summary": {
                            "1": {
                                "median_forward_return_pct": 0.01,
                                "positive_rate": 0.5,
                                "mean_mfe_pct": 0.02,
                                "ignored_label": "left",
                            }
                        }
                    },
                }
            },
        },
        "right": {
            "id": "right",
            "kind": "research_check",
            "title": "Right",
            "payload": {
                "result": {
                    "check_family": "indicator_forward_outcome",
                    "status": "completed",
                    "sample_count": 5,
                    "outcomes": {
                        "summary": {
                            "1": {
                                "median_forward_return_pct": 0.03,
                                "positive_rate": 0.75,
                                "mean_mfe_pct": 0.04,
                                "ignored_label": "right",
                            }
                        }
                    },
                }
            },
        },
    }
    monkeypatch.setattr(service.repository, "get_item", lambda item_id: items[item_id])

    payload = service.compare_research_checks("left", "right")

    assert payload["schema_version"] == "research_check_comparison.v1"
    assert payload["deltas"]["sample_count"] == {"left": 2, "right": 5, "delta": 3.0}
    assert payload["deltas"]["forward_summary"]["1"]["median_forward_return_pct"]["delta"] == 0.019999999999999997
    assert payload["deltas"]["forward_summary"]["1"]["positive_rate"]["delta"] == 0.25
    assert payload["deltas"]["forward_summary"]["1"]["mean_mfe_pct"]["delta"] == 0.02
    assert "ignored_label" not in payload["deltas"]["forward_summary"]["1"]


def test_research_check_compare_includes_decision_denominator(monkeypatch: pytest.MonkeyPatch) -> None:
    items = {
        "left": {
            "id": "left",
            "kind": "research_check",
            "title": "Left",
            "payload": {
                "result": {
                    "check_family": "run_decision_trade_comparison",
                    "status": "completed",
                    "sample_count": 2,
                    "eligible_decisions": 10,
                    "outcomes": {"by_decision_state": {}},
                }
            },
        },
        "right": {
            "id": "right",
            "kind": "research_check",
            "title": "Right",
            "payload": {
                "result": {
                    "check_family": "run_decision_trade_comparison",
                    "status": "completed",
                    "sample_count": 3,
                    "eligible_decisions": 12,
                    "outcomes": {"by_decision_state": {}},
                }
            },
        },
    }
    monkeypatch.setattr(service.repository, "get_item", lambda item_id: items[item_id])

    payload = service.compare_research_checks("left", "right")

    assert payload["left"]["eligible_decisions"] == 10
    assert payload["right"]["eligible_decisions"] == 12
    assert payload["deltas"]["eligible_decisions"] == {"left": 10, "right": 12, "delta": 2.0}


def test_research_check_route_delegates_to_service(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_run_research_check(payload):
        observed["payload"] = payload
        return {"schema_version": "research_check_run.v1", "status": "completed"}

    monkeypatch.setattr(research_controller.research_service, "run_research_check", fake_run_research_check)

    response = TestClient(app).post(
        "/api/research/checks/run",
        json={
            "title": "Quick check",
            "scope": {"instrument_id": "inst-1", "timeframe": "1h", "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            "detector": {"type": "raw_condition", "field": "close", "operator": "gt", "value_field": "previous_close"},
        },
    )

    assert response.status_code == 201
    assert response.json()["status"] == "completed"
    assert observed["payload"]["title"] == "Quick check"


def test_research_check_preview_and_sweep_routes_delegate_to_service(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_evaluate_research_check(payload):
        observed["evaluate"] = payload
        return {"schema_version": "research_check_evaluation.v1", "status": "completed"}

    def fake_sweep_research_checks(payload):
        observed["sweep"] = payload
        return {"schema_version": "research_check_sweep.v1", "evaluation_count": 1}

    monkeypatch.setattr(research_controller.research_service, "evaluate_research_check", fake_evaluate_research_check)
    monkeypatch.setattr(research_controller.research_service, "sweep_research_checks", fake_sweep_research_checks)

    client = TestClient(app)
    evaluate_response = client.post(
        "/api/research/checks/evaluate",
        json={
            "title": "Preview check",
            "scope": {"instrument_id": "inst-1", "timeframe": "1h", "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            "detector": {"type": "raw_condition", "field": "close", "operator": "gt", "value_field": "previous_close"},
        },
    )
    sweep_response = client.post(
        "/api/research/checks/sweep",
        json={
            "check_family": "candidate_lifecycle",
            "scope": {"indicator_id": "indicator-1", "instrument_id": "inst-1", "timeframe": "1h", "start": "2026-01-01T00:00:00Z", "end": "2026-01-02T00:00:00Z"},
            "detector": {"type": "candidate_lifecycle"},
            "variants": [{"id": "base", "param_overrides": {}}],
            "ranking": {"rank_by": "sample_count", "direction": "desc"},
        },
    )

    assert evaluate_response.status_code == 200
    assert sweep_response.status_code == 200
    assert observed["evaluate"]["title"] == "Preview check"
    assert observed["sweep"]["check_family"] == "candidate_lifecycle"


def test_research_read_routes_delegate_to_service_exports(monkeypatch: pytest.MonkeyPatch) -> None:
    observed = {}

    def fake_run_research_evidence(run_id: str):
        observed["run_id"] = run_id
        return {"schema_version": "run_research_evidence.v1", "run_id": run_id}

    def fake_research_trail(item_id: str):
        observed["trail_item_id"] = item_id
        return {"schema_version": "research_trail.v1", "item_id": item_id, "items": []}

    def fake_compare(left_check_id: str, right_check_id: str):
        observed["compare"] = (left_check_id, right_check_id)
        return {
            "schema_version": "research_check_comparison.v1",
            "left": {"check_id": left_check_id},
            "right": {"check_id": right_check_id},
            "deltas": {},
        }

    monkeypatch.setattr(research_controller.research_service, "get_run_research_evidence", fake_run_research_evidence)
    monkeypatch.setattr(research_controller.research_service, "get_research_trail", fake_research_trail)
    monkeypatch.setattr(research_controller.research_service, "compare_research_checks", fake_compare)

    client = TestClient(app)

    run_response = client.get("/api/research/runs/run-1/evidence")
    trail_response = client.get("/api/research/items/obs-1/trail")
    compare_response = client.get(
        "/api/research/checks/compare",
        params={"left_check_id": "check-a", "right_check_id": "check-b"},
    )

    assert run_response.status_code == 200
    assert run_response.json()["run_id"] == "run-1"
    assert trail_response.status_code == 200
    assert trail_response.json()["item_id"] == "obs-1"
    assert compare_response.status_code == 200
    assert compare_response.json()["left"]["check_id"] == "check-a"
    assert observed == {
        "run_id": "run-1",
        "trail_item_id": "obs-1",
        "compare": ("check-a", "check-b"),
    }
