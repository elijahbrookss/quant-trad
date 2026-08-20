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
        lambda _type: SimpleNamespace(market_inputs=(_Input(),)),
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
