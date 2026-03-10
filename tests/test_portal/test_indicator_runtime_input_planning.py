from __future__ import annotations
import pytest
pytest.importorskip("pandas")

from datetime import datetime, timedelta, timezone

from portal.backend.service.indicators.indicator_factory import IndicatorFactory
from portal.backend.service.indicators.indicator_service.runtime_contract import (
    SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT,
)
from portal.backend.service.strategies.strategy_service import indicator_signal_service


def _parse_utc(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def test_market_profile_runtime_input_plan_uses_native_timeframe_and_days_back():
    factory = IndicatorFactory()
    end = datetime(2026, 2, 10, 12, 0, tzinfo=timezone.utc)
    start = end - timedelta(hours=2)
    meta = {
        "id": "mp-1",
        "type": "market_profile",
        "params": {"days_back": 30},
    }

    plan = factory.build_runtime_input_plan(
        meta,
        strategy_interval="5m",
        start=start.isoformat(),
        end=end.isoformat(),
    )

    assert plan["source_timeframe"] == "30m"
    assert plan["normalization"] == "project_to_strategy_timeframe"
    assert _parse_utc(plan["start"]) <= end - timedelta(days=30)


def test_pivot_runtime_input_plan_uses_param_timeframe_and_days_back():
    factory = IndicatorFactory()
    end = datetime(2026, 2, 10, 18, 0, tzinfo=timezone.utc)
    start = end - timedelta(hours=1)
    meta = {
        "id": "pivot-1",
        "type": "pivot_level",
        "params": {"timeframe": "1h", "days_back": 14},
    }

    plan = factory.build_runtime_input_plan(
        meta,
        strategy_interval="5m",
        start=start.isoformat(),
        end=end.isoformat(),
    )

    assert plan["source_timeframe"] == "1h"
    assert plan["lookback_days"] == 14
    assert _parse_utc(plan["start"]) <= end - timedelta(days=14)


def test_generate_indicator_payloads_uses_runtime_input_plan(monkeypatch):
    captured: dict[str, object] = {}

    def fake_runtime_input_plan(inst_id: str, *, strategy_interval: str, start: str, end: str):
        return {
            "indicator_id": inst_id,
            "indicator_type": "market_profile",
            "strategy_interval": strategy_interval,
            "source_timeframe": "30m",
            "start": "2026-01-01T00:00:00+00:00",
            "end": end,
            "session_scope": "global",
            "alignment": "closed_bar_only",
            "normalization": "project_to_strategy_timeframe",
            "lookback_bars": None,
            "lookback_days": 30,
            "lookback_seconds": None,
        }

    def fake_generate_signals_for_instance(
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        symbol: str | None = None,
        datasource: str | None = None,
        exchange: str | None = None,
        config: dict | None = None,
    ):
        captured["inst_id"] = inst_id
        captured["start"] = start
        captured["end"] = end
        captured["interval"] = interval
        captured["config"] = dict(config or {})
        return {"signals": [], "runtime_path": SIGNAL_RUNTIME_PATH_ENGINE_SNAPSHOT}

    monkeypatch.setattr(
        indicator_signal_service,
        "runtime_input_plan_for_instance",
        fake_runtime_input_plan,
    )
    monkeypatch.setattr(
        indicator_signal_service,
        "generate_signals_for_instance",
        fake_generate_signals_for_instance,
    )

    payloads, missing, total = indicator_signal_service.generate_indicator_payloads(
        strategy_id="strat-1",
        instrument_id="inst-1",
        indicator_ids=["ind-1"],
        indicator_rule_map={},
        start="2026-02-10T00:00:00+00:00",
        end="2026-02-10T01:00:00+00:00",
        interval="5m",
        symbol="ES",
        datasource="ALPACA",
        exchange=None,
        base_config={},
        run_id="run-1",
    )

    assert missing == []
    assert total == 0
    assert captured["interval"] == "5m"
    assert captured["start"] == "2026-01-01T00:00:00+00:00"
    assert "runtime_input_plan" in captured["config"]
    assert payloads["ind-1"]["runtime_input_plan"]["source_timeframe"] == "30m"


def test_generate_indicator_payloads_rejects_non_engine_runtime_path(monkeypatch):
    def fake_runtime_input_plan(inst_id: str, *, strategy_interval: str, start: str, end: str):
        return {
            "indicator_id": inst_id,
            "indicator_type": "market_profile",
            "strategy_interval": strategy_interval,
            "source_timeframe": "30m",
            "start": start,
            "end": end,
        }

    def fake_generate_signals_for_instance(
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        symbol: str | None = None,
        datasource: str | None = None,
        exchange: str | None = None,
        config: dict | None = None,
    ):
        return {"signals": [], "runtime_path": "legacy"}

    monkeypatch.setattr(
        indicator_signal_service,
        "runtime_input_plan_for_instance",
        fake_runtime_input_plan,
    )
    monkeypatch.setattr(
        indicator_signal_service,
        "generate_signals_for_instance",
        fake_generate_signals_for_instance,
    )

    payloads, missing, total = indicator_signal_service.generate_indicator_payloads(
        strategy_id="strat-1",
        instrument_id="inst-1",
        indicator_ids=["ind-1"],
        indicator_rule_map={},
        start="2026-02-10T00:00:00+00:00",
        end="2026-02-10T01:00:00+00:00",
        interval="5m",
        symbol="ES",
        datasource="ALPACA",
        exchange=None,
        base_config={},
        run_id="run-1",
    )

    assert missing == []
    assert total == 0
    assert "runtime_path_mismatch" in str(payloads["ind-1"].get("error") or "")
