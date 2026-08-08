import pytest

pd = pytest.importorskip("pandas", reason="pandas required for indicator runtime import graph")

from types import SimpleNamespace

from indicators.config import DataContext, IndicatorExecutionContext

from portal.backend.service.indicators.indicator_service import runtime_graph


class _FakeRuntimeIndicator:
    @classmethod
    def resolve_config(cls, params, *, strict_unknown=False):
        _ = strict_unknown
        return dict(params or {})

    @classmethod
    def build_runtime_data_request(cls, *, resolved_params, execution_context):
        _ = resolved_params
        return DataContext(
            symbol=execution_context.symbol,
            start=execution_context.start,
            end=execution_context.end,
            interval="30m",
            instrument_id=execution_context.instrument_id,
        )

    @classmethod
    def build_runtime_source_facts(cls, *, resolved_params, execution_context, source_frame):
        return {
            "source_rows": getattr(source_frame, "rows", None),
            "timeframe": execution_context.interval,
            "params": dict(resolved_params or {}),
        }


def test_runtime_indicator_source_fetch_uses_canonical_candle_service(monkeypatch):
    source_frame = SimpleNamespace(empty=False, rows=12)
    captured = {}

    def fake_fetch(ctx, *, datasource=None, exchange=None, frozen_alias=None):
        captured["ctx"] = ctx
        captured["datasource"] = datasource
        captured["exchange"] = exchange
        captured["frozen_alias"] = frozen_alias
        return source_frame

    def fake_builder(**kwargs):
        captured["builder_kwargs"] = kwargs
        return {"built": True}

    monkeypatch.setitem(runtime_graph._INDICATOR_MAP, "fake_runtime_indicator", _FakeRuntimeIndicator)
    monkeypatch.setattr(runtime_graph, "runtime_indicator_builder_for_type", lambda indicator_type: fake_builder)
    monkeypatch.setattr(runtime_graph.candle_service, "fetch_ohlcv_for_context", fake_fetch)

    result = runtime_graph.build_runtime_indicator_instance(
        "indicator-1",
        meta={
            "id": "indicator-1",
            "type": "fake_runtime_indicator",
            "params": {},
            "datasource": "COINBASE",
            "exchange": "coinbase_direct",
        },
        execution_context=IndicatorExecutionContext(
            symbol="BTCUSD",
            start="2026-01-01T00:00:00Z",
            end="2026-01-02T00:00:00Z",
            interval="1h",
            datasource="COINBASE",
            exchange="coinbase_direct",
            instrument_id="instrument-1",
        ),
    )

    assert result == {"built": True}
    assert captured["ctx"].instrument_id == "instrument-1"
    assert captured["ctx"].interval == "30m"
    assert captured["datasource"] == "COINBASE"
    assert captured["exchange"] == "coinbase_direct"
    assert captured["frozen_alias"] == "indicator:indicator-1:primary_bars"
    assert captured["builder_kwargs"]["source_facts"] == {
        "source_rows": 12,
        "timeframe": "1h",
        "params": {},
    }


def test_runtime_indicator_source_fetch_uses_explicit_source_frame_cache(monkeypatch):
    source_frame = SimpleNamespace(empty=False, rows=12)
    fetch_count = 0
    cache = {}
    stats = {}

    def fake_fetch(ctx, *, datasource=None, exchange=None, frozen_alias=None):
        nonlocal fetch_count
        _ = ctx, datasource, exchange, frozen_alias
        fetch_count += 1
        return source_frame

    def fake_builder(**kwargs):
        return {"source_facts": kwargs["source_facts"]}

    monkeypatch.setitem(runtime_graph._INDICATOR_MAP, "fake_runtime_indicator", _FakeRuntimeIndicator)
    monkeypatch.setattr(runtime_graph, "runtime_indicator_builder_for_type", lambda indicator_type: fake_builder)
    monkeypatch.setattr(runtime_graph.candle_service, "fetch_ohlcv_for_context", fake_fetch)

    execution_context = IndicatorExecutionContext(
        symbol="BTCUSD",
        start="2026-01-01T00:00:00Z",
        end="2026-01-02T00:00:00Z",
        interval="1h",
        datasource="COINBASE",
        exchange="coinbase_direct",
        instrument_id="instrument-1",
    )
    meta = {
        "id": "indicator-1",
        "type": "fake_runtime_indicator",
        "params": {},
        "datasource": "COINBASE",
        "exchange": "coinbase_direct",
    }

    first = runtime_graph.build_runtime_indicator_instance(
        "indicator-1",
        meta=meta,
        execution_context=execution_context,
        source_frame_cache=cache,
        source_frame_cache_stats=stats,
    )
    second = runtime_graph.build_runtime_indicator_instance(
        "indicator-1",
        meta=meta,
        execution_context=execution_context,
        source_frame_cache=cache,
        source_frame_cache_stats=stats,
    )

    assert fetch_count == 1
    assert len(cache) == 1
    assert stats == {"source_frame_hits": 1, "source_frame_misses": 1}
    assert first == second == {"source_facts": {"source_rows": 12, "timeframe": "1h", "params": {}}}


def test_collect_runtime_indicator_diagnostics_keeps_indicator_identity() -> None:
    indicator = SimpleNamespace(
        runtime_spec=SimpleNamespace(
            instance_id="indicator-1",
            manifest_type="market_profile",
        ),
        runtime_diagnostics=lambda: {
            "source_candle_continuity": {
                "status": "warning",
                "acceptability": "acceptable_with_caveat",
            }
        },
    )

    diagnostics = runtime_graph.collect_runtime_indicator_diagnostics([indicator])

    assert diagnostics == [
        {
            "indicator_id": "indicator-1",
            "indicator_type": "market_profile",
            "source_candle_continuity": {
                "status": "warning",
                "acceptability": "acceptable_with_caveat",
            },
        }
    ]


def test_runtime_indicator_source_fetch_attaches_generic_source_diagnostics(monkeypatch):
    class _DiagnosticIndicator(_FakeRuntimeIndicator):
        @classmethod
        def build_runtime_source_facts(
            cls,
            *,
            resolved_params,
            execution_context,
            source_frame,
            source_diagnostics,
        ):
            return {"diagnostics": source_diagnostics}

    index = pd.to_datetime(
        [
            "2026-01-01T00:00:00Z",
            "2026-01-01T01:00:00Z",
            "2026-01-01T01:30:00Z",
        ],
        utc=True,
    )
    source_frame = pd.DataFrame(
        {
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [10.0, 11.0, 12.0],
        },
        index=index,
    )
    source_frame.attrs["gap_classification"] = [
        {
            "start": int(pd.Timestamp("2026-01-01T00:30:00Z").timestamp()),
            "end": int(pd.Timestamp("2026-01-01T01:00:00Z").timestamp()),
            "classification": "provider_missing_data",
            "reason_code": "source_sparse",
        }
    ]

    def fake_builder(**kwargs):
        return SimpleNamespace(
            runtime_spec=SimpleNamespace(
                instance_id=kwargs["indicator_id"],
                manifest_type="diagnostic_runtime_indicator",
            )
        )

    monkeypatch.setitem(runtime_graph._INDICATOR_MAP, "diagnostic_runtime_indicator", _DiagnosticIndicator)
    monkeypatch.setattr(runtime_graph, "runtime_indicator_builder_for_type", lambda indicator_type: fake_builder)
    monkeypatch.setattr(
        runtime_graph.candle_service,
        "fetch_ohlcv_for_context",
        lambda *args, **kwargs: source_frame,
    )

    indicator = runtime_graph.build_runtime_indicator_instance(
        "indicator-1",
        meta={
            "id": "indicator-1",
            "type": "diagnostic_runtime_indicator",
            "params": {},
            "datasource": "COINBASE",
            "exchange": "coinbase_direct",
        },
        execution_context=IndicatorExecutionContext(
            symbol="BTCUSD",
            start="2026-01-01T00:00:00Z",
            end="2026-01-01T02:00:00Z",
            interval="1h",
            datasource="COINBASE",
            exchange="coinbase_direct",
            instrument_id="instrument-1",
        ),
    )

    diagnostics = runtime_graph.collect_runtime_indicator_diagnostics([indicator])

    source_continuity = diagnostics[0]["source_candle_continuity"]
    assert source_continuity["status"] == "warning"
    assert source_continuity["acceptability"] == "acceptable_with_caveat"
    assert source_continuity["continuity"]["gap_count_by_type"]["provider_missing_data"] == 1
