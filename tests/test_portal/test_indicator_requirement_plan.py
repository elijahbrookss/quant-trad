from __future__ import annotations

from types import SimpleNamespace

from market_data.contracts import MarketDataRequirement
from portal.backend.service.indicators.indicator_service import requirements


class _Definition:
    @staticmethod
    def resolve_config(params, *, strict_unknown):
        assert strict_unknown is True
        return dict(params or {})


class _Factory:
    @staticmethod
    def build_runtime_input_plan(meta, *, strategy_interval, start, end):
        _ = meta, end
        return {
            "source_timeframe": strategy_interval,
            "start": start,
            "lookback_bars": 48,
            "lookback_days": None,
        }


class _Input:
    fact_type = "market.reference_price"

    @staticmethod
    def to_requirement(**kwargs):
        return MarketDataRequirement(
            key="reference_price",
            fact_type="market.reference_price",
            contract_version="market.reference_price.v1",
            instrument_role="primary",
            dimensions={"quote_currency": "USD"},
            alignment="latest_known",
            max_staleness_seconds=21600,
            required=True,
            allow_gaps=True,
            known_at_required=True,
            lookback_bars=kwargs.get("lookback_bars"),
            lookback_seconds=kwargs.get("lookback_seconds"),
        )


def test_indicator_requirement_plan_includes_transitive_graph_warmup_and_staleness(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        requirements,
        "collect_runtime_indicator_metas",
        lambda roots, ctx: {
            "dependency-1": {
                "id": "dependency-1",
                "type": "dependency",
                "params": {"warmup_bars": 24},
                "dependencies": [],
            },
            "root-1": {
                "id": "root-1",
                "type": "root",
                "params": {"warmup_bars": 72},
                "dependencies": [
                    {
                        "indicator_id": "dependency-1",
                        "indicator_type": "dependency",
                        "output_name": "state",
                    }
                ],
            },
        },
    )
    monkeypatch.setattr(requirements, "get_indicator_definition", lambda _type: _Definition())
    monkeypatch.setattr(
        requirements,
        "get_indicator_manifest",
        lambda _type, version=None: SimpleNamespace(market_inputs=(_Input(),)),
    )
    monkeypatch.setattr(
        requirements,
        "serialize_indicator_manifest",
        lambda manifest: {
            "type": "test",
            "market_inputs": [item.fact_type for item in manifest.market_inputs],
        },
    )

    plan = requirements.plan_runtime_requirements_for_indicators(
        ["root-1"],
        timeframe="30m",
        start="2026-01-01T00:00:00Z",
        end="2026-02-01T00:00:00Z",
        ctx=SimpleNamespace(factory=_Factory()),
    )

    assert plan["root_indicator_ids"] == ["root-1"]
    assert [row["indicator_id"] for row in plan["indicators"]] == [
        "dependency-1",
        "root-1",
    ]
    assert plan["warmup_bars"] == 72
    assert len(plan["requirements"]) == 2
    assert plan["requirements"][0]["input"]["lookback_bars"] == 48
    assert plan["requirements"][0]["input"]["max_staleness_seconds"] == 21600
    assert plan["graph_hash"]


def _origin_meta():
    from portal.backend.service.indicators.indicator_factory import IndicatorFactory
    return IndicatorFactory().build_meta_from_record({
        "id": "reversal-plan", "type": "reversal", "version": "v1",
        "params": {"computation_origin": "2022-01-01T00:00:00Z"},
    })


def _origin_context(**changes):
    from indicators.config import IndicatorExecutionContext
    values = dict(symbol="BTC/USD", start="2022-01-01T01:00:00Z",
        end="2022-01-01T02:00:00Z", interval="15m",
        datasource="COINBASE", exchange="coinbase", instrument_id="canonical-btc-instrument")
    return IndicatorExecutionContext(**{**values, **changes})


def test_origin_producer_real_requirement_plan_and_runtime_source_preparation(monkeypatch):
    from datetime import datetime, timedelta, timezone
    import pandas as pd
    from engines.bot_runtime.core.domain.candle_factory import build_candles_from_dataframe
    from portal.backend.service.indicators.indicator_service import runtime_graph

    meta = _origin_meta()
    context = _origin_context()
    plan = requirements.plan_runtime_requirements_for_indicators(["reversal-plan"],
        timeframe="15m", start=context.start, end=context.end, execution_context=context,
        preloaded_metas={"reversal-plan": meta})
    origin = datetime(2022, 1, 1, tzinfo=timezone.utc)
    assert plan["warmup_bars"] == 4
    assert plan["requirements"][0]["required_start"] == "2022-01-01T00:00:00Z"
    assert plan["requirements"][0]["input"]["timeframe_seconds"] == 900
    assert plan["indicators"][0]["runtime_input_plan"]["start"] == "2022-01-01T00:00:00Z"

    prices = [10., 9., 9.2, 8.8, 8.7, 9.6, 9.9, 10.]
    rows = []
    for i, close in enumerate(prices):
        opened = prices[i-1] if i else close+.1
        rows.append({"timestamp": origin+timedelta(minutes=15*i),
            "close_time": origin+timedelta(minutes=15*(i+1)),
            "known_at": origin+timedelta(minutes=15*(i+1)),
            "open": opened, "high": max(opened, close)+.1, "low": min(opened, close)-.1,
            "close": close, "source_identity_key": "canonical-coinbase-candle-source",
            "instrument_id": context.instrument_id, "interval": "15m"})
    source = pd.DataFrame(rows).set_index("timestamp", drop=False)
    calls = []
    def frozen_source_read(request, **kwargs):
        calls.append(request)
        assert request.start_utc().to_pydatetime() == origin
        assert request.end == context.end
        assert request.instrument_id == "canonical-btc-instrument"
        assert kwargs["frozen_alias"] == "indicator:reversal-plan:primary_bars"
        return source
    monkeypatch.setattr(runtime_graph.candle_service, "fetch_ohlcv_for_context", frozen_source_read)
    _, runtimes = runtime_graph.build_runtime_indicator_graph(["reversal-plan"],
        execution_context=context, preloaded_metas={"reversal-plan": meta},
        require_preloaded_metas=True)
    runtime, = runtimes
    assert calls and runtime._recognizer.snapshot().processed_bars == 0
    events = []
    for bar in build_candles_from_dataframe(source, timeframe="15m"):
        runtime.apply_bar(bar, {})
        events.extend(runtime.snapshot()["provisional_reversals"].value["events"])
    assert events
    assert all(event["metadata"]["recognition"]["context"]["instrument_id"] == context.instrument_id
               for event in events)
    assert all(event["metadata"]["recognition"]["context"]["source_identity_key"] ==
               "canonical-coinbase-candle-source" for event in events)


def test_origin_producer_planning_requires_real_context_and_native_aligned_window():
    import pytest
    meta = _origin_meta()
    kwargs = dict(timeframe="15m", start="2022-01-01T01:00:00Z",
        end="2022-01-01T02:00:00Z", preloaded_metas={"reversal-plan": meta})
    with pytest.raises(ValueError, match="actual execution context"):
        requirements.plan_runtime_requirements_for_indicators(["reversal-plan"], **kwargs)
    with pytest.raises(ValueError, match="instrument_id"):
        requirements.plan_runtime_requirements_for_indicators(["reversal-plan"], **kwargs,
            execution_context=_origin_context(instrument_id=None))
    with pytest.raises(ValueError, match="differs from planned window"):
        requirements.plan_runtime_requirements_for_indicators(["reversal-plan"], **kwargs,
            execution_context=_origin_context(start="2022-01-01T01:15:00Z"))
    with pytest.raises(ValueError, match="aligned"):
        requirements.plan_runtime_requirements_for_indicators(["reversal-plan"], **{
            **kwargs, "start": "2022-01-01T01:01:00Z"},
            execution_context=_origin_context(start="2022-01-01T01:01:00Z"))


def test_non_origin_producers_keep_prechange_graph_hashes_and_ignore_new_context():
    from portal.backend.service.indicators.indicator_factory import IndicatorFactory
    factory = IndicatorFactory()
    baseline = {
        "market_profile": ("e66eb5324caff30446dde83bb68d0530f5c0515ba50194140008de5a9e1f7e82", 0, {"days_back": 3}),
        "candle_stats": ("f0ec97831464a6faa663212b0019a3038299590247d40fab7e70cf5312ee8118", 200, {}),
    }
    for kind, (expected_hash, warmup, params) in baseline.items():
        meta = factory.build_meta_from_record({"id": "existing-1", "type": kind,
                                               "version": "v1", "params": params})
        kwargs = dict(timeframe="15m", start="2022-01-04T00:00:00Z", end="2022-01-04T03:00:00Z",
                      preloaded_metas={"existing-1": meta})
        before = requirements.plan_runtime_requirements_for_indicators(["existing-1"], **kwargs)
        after = requirements.plan_runtime_requirements_for_indicators(["existing-1"], **kwargs,
            execution_context=_origin_context(start=kwargs["start"], end=kwargs["end"]))
        assert after == before
        assert after["graph_hash"] == expected_hash
        assert after["warmup_bars"] == warmup
