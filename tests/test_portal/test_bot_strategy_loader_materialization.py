from __future__ import annotations

from contextlib import contextmanager

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from portal.backend.db.models import (
    ATMTemplateRecord,
    Base,
    StrategyInstrumentLink,
    StrategyRecord,
    StrategyRuleRecord,
    StrategyVariantRecord,
)
from portal.backend.service.bots.strategy_loader import StrategyLoader
from risk import normalise_risk_config


class _SqliteDb:
    available = True

    def __init__(self) -> None:
        self._engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self._engine)
        self._session_factory = sessionmaker(
            bind=self._engine,
            expire_on_commit=False,
            autoflush=False,
            future=True,
        )

    @contextmanager
    def session(self):
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


def test_strategy_loader_materializes_variant_atm_and_risk_config_without_bot_atm_override(monkeypatch) -> None:
    db = _SqliteDb()
    monkeypatch.setattr("portal.backend.service.bots.strategy_loader.db", db)

    with db.session() as session:
        session.add_all(
            [
                ATMTemplateRecord(
                    id="atm-base",
                    name="Base ATM",
                    template={
                        "name": "Base ATM",
                        "initial_stop": {"mode": "atr", "atr_period": 14, "atr_multiplier": 1.0},
                        "take_profit_orders": [{"id": "tp-1", "r_multiple": 1.0, "size_fraction": 1.0}],
                    },
                ),
                ATMTemplateRecord(
                    id="atm-variant",
                    name="Variant ATM",
                    template={
                        "name": "Variant ATM",
                        "initial_stop": {"mode": "atr", "atr_period": 10, "atr_multiplier": 2.0},
                        "take_profit_orders": [{"id": "tp-1", "r_multiple": 2.0, "size_fraction": 1.0}],
                    },
                ),
                StrategyRecord(
                    id="strategy-1",
                    name="Strategy 1",
                    description=None,
                    timeframe="1m",
                    datasource="demo",
                    exchange="demo",
                    atm_template_id="atm-base",
                    risk_config={"base_risk_per_trade": 100.0, "global_risk_multiplier": 1.0},
                ),
                StrategyVariantRecord(
                    id="variant-1",
                    strategy_id="strategy-1",
                    name="aggressive",
                    description=None,
                    param_overrides={"conviction_min": 0.55},
                    atm_template_id="atm-variant",
                    is_default=False,
                ),
                StrategyInstrumentLink(
                    id="link-1",
                    strategy_id="strategy-1",
                    instrument_id="instrument-1",
                    instrument_snapshot={"symbol": "BTC/USDT"},
                ),
                StrategyRuleRecord(
                    id="rule-1",
                    strategy_id="strategy-1",
                    name="Rule 1",
                    action="buy",
                    match="all",
                    description=None,
                    enabled=True,
                    conditions={
                        "id": "rule-1",
                        "name": "Rule 1",
                        "intent": "enter_long",
                        "priority": 0,
                        "trigger": {
                            "type": "signal_match",
                            "indicator_id": "ind-1",
                            "output_name": "signal",
                            "event_key": "breakout_long",
                        },
                        "guards": [],
                    },
                ),
            ]
        )

    strategy = StrategyLoader.fetch_strategy(
        "strategy-1",
        {
            "strategy_variant_id": "variant-1",
            "strategy_variant_name": "aggressive",
            "resolved_params": {"conviction_min": 0.55},
            "atm_template_id": "atm-bot-override-should-be-ignored",
            "risk_config": {"base_risk_per_trade": 250.0, "global_risk_multiplier": 1.2},
        },
    )

    assert strategy.atm_template_id == "atm-variant"
    assert strategy.atm_template["name"] == "Variant ATM"
    assert strategy.variant_name == "aggressive"
    assert strategy.resolved_params == {"conviction_min": 0.55}
    assert strategy.risk_config == normalise_risk_config(
        {"base_risk_per_trade": 250.0, "global_risk_multiplier": 1.2}
    )
