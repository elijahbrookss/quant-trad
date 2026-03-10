from __future__ import annotations

from portal.backend.service.bots.bot_runtime.strategy.models import (
    Strategy,
    StrategyIndicatorLink,
    StrategyInstrumentLink,
)


def test_strategy_to_dict_includes_rules_for_runtime_meta() -> None:
    strategy = Strategy(
        id="strategy-1",
        name="Strategy 1",
        timeframe="1m",
        datasource="demo",
        exchange="demo",
        atm_template_id=None,
        atm_template={},
        base_risk_per_trade=None,
        global_risk_multiplier=None,
        indicator_links=[
            StrategyIndicatorLink(
                id="link-1",
                strategy_id="strategy-1",
                indicator_id="ind-1",
            )
        ],
        instrument_links=[
            StrategyInstrumentLink(
                id="inst-link-1",
                strategy_id="strategy-1",
                instrument_id="instrument-1",
                instrument_snapshot={"symbol": "BTC/USDT"},
            )
        ],
        rules={
            "rule-1": {
                "id": "rule-1",
                "name": "Buy breakout",
                "action": "buy",
                "conditions": [
                    {"indicator_id": "ind-1", "signal_type": "breakout", "direction": "long"}
                ],
            }
        },
    )

    payload = strategy.to_dict()
    assert "rules" in payload
    assert payload["rules"]["rule-1"]["action"] == "buy"


def test_strategy_to_dict_rules_are_copied() -> None:
    strategy = Strategy(
        id="strategy-1",
        name="Strategy 1",
        timeframe="1m",
        datasource="demo",
        exchange="demo",
        atm_template_id=None,
        atm_template={},
        base_risk_per_trade=None,
        global_risk_multiplier=None,
        indicator_links=[],
        instrument_links=[],
        rules={"rule-1": {"id": "rule-1", "action": "buy", "conditions": []}},
    )

    payload = strategy.to_dict()
    payload["rules"]["rule-1"]["action"] = "sell"

    assert strategy.rules["rule-1"]["action"] == "buy"
