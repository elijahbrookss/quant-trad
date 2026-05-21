import pytest

pytest.importorskip("pandas", reason="pandas required for indicator runtime import graph")

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

    def fake_fetch(ctx, *, datasource=None, exchange=None):
        captured["ctx"] = ctx
        captured["datasource"] = datasource
        captured["exchange"] = exchange
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
    assert captured["builder_kwargs"]["source_facts"] == {
        "source_rows": 12,
        "timeframe": "1h",
        "params": {},
    }
