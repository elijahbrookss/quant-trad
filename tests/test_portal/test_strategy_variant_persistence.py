from __future__ import annotations

from contextlib import contextmanager

import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from portal.backend.db.models import Base, BotRecord, StrategyRecord
from portal.backend.service.storage.repos import bots as bot_repos
from portal.backend.service.storage.repos import strategies as strategy_repos


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


def _seed_strategy(db: _SqliteDb, strategy_id: str = "strategy-1") -> None:
    with db.session() as session:
        session.add(
            StrategyRecord(
                id=strategy_id,
                name="Strategy 1",
                description=None,
                timeframe="1m",
                datasource="demo",
                exchange="demo",
            )
        )


def test_default_variant_creation_for_strategy(monkeypatch) -> None:
    db = _SqliteDb()
    _seed_strategy(db)
    monkeypatch.setattr(strategy_repos, "db", db)

    created = strategy_repos.ensure_default_strategy_variant("strategy-1")
    listed = strategy_repos.list_strategy_variants("strategy-1")

    assert created["strategy_id"] == "strategy-1"
    assert created["name"] == "default"
    assert created["param_overrides"] == {}
    assert created["is_default"] is True
    assert [item["name"] for item in listed] == ["default"]


def test_create_list_update_delete_variant_behavior(monkeypatch) -> None:
    db = _SqliteDb()
    _seed_strategy(db)
    monkeypatch.setattr(strategy_repos, "db", db)

    strategy_repos.ensure_default_strategy_variant("strategy-1")
    created = strategy_repos.upsert_strategy_variant(
        {
            "strategy_id": "strategy-1",
            "name": "aggressive",
            "description": "Looser threshold",
            "param_overrides": {"conviction_min": 0.5},
            "atm_template_id": "atm-fast",
            "is_default": False,
        }
    )

    listed = strategy_repos.list_strategy_variants("strategy-1")
    assert [item["name"] for item in listed] == ["default", "aggressive"]
    assert listed[1]["atm_template_id"] == "atm-fast"

    updated = strategy_repos.upsert_strategy_variant(
        {
            "id": created["id"],
            "strategy_id": "strategy-1",
            "name": "aggressive",
            "description": "Updated description",
            "param_overrides": {"conviction_min": 0.55},
            "atm_template_id": "atm-slower",
            "is_default": False,
        }
    )
    fetched = strategy_repos.get_strategy_variant(created["id"])

    assert updated["description"] == "Updated description"
    assert fetched is not None
    assert fetched["param_overrides"] == {"conviction_min": 0.55}
    assert fetched["atm_template_id"] == "atm-slower"

    strategy_repos.delete_strategy_variant(created["id"])

    remaining = strategy_repos.list_strategy_variants("strategy-1")
    assert [item["name"] for item in remaining] == ["default"]


def test_default_variant_cannot_be_deleted(monkeypatch) -> None:
    db = _SqliteDb()
    _seed_strategy(db)
    monkeypatch.setattr(strategy_repos, "db", db)

    default_variant = strategy_repos.ensure_default_strategy_variant("strategy-1")

    with pytest.raises(ValueError, match="Default strategy variant cannot be deleted"):
        strategy_repos.delete_strategy_variant(default_variant["id"])


def test_bot_persistence_includes_variant_provenance_and_resolved_params(monkeypatch) -> None:
    db = _SqliteDb()
    monkeypatch.setattr(bot_repos, "db", db)

    bot_repos.upsert_bot(
        {
            "id": "bot-1",
            "name": "Bot 1",
            "strategy_id": "strategy-1",
            "strategy_variant_id": "variant-1",
            "strategy_variant_name": "aggressive",
            "atm_template_id": "atm-fast",
            "resolved_params": {"conviction_min": 0.5},
            "risk_config": {"base_risk_per_trade": 250.0, "global_risk_multiplier": 1.0},
            "mode": "instant",
            "run_type": "backtest",
            "wallet_config": {"balances": {"USD": 1000.0}},
            "snapshot_interval_ms": 250,
        }
    )

    persisted = bot_repos.get_bot("bot-1")

    assert persisted is not None
    assert persisted["strategy_variant_id"] == "variant-1"
    assert persisted["strategy_variant_name"] == "aggressive"
    assert persisted["atm_template_id"] == "atm-fast"
    assert persisted["resolved_params"] == {"conviction_min": 0.5}
    assert persisted["risk_config"]["base_risk_per_trade"] == 250.0


def test_existing_bot_flow_is_unchanged_without_variant_provenance(monkeypatch) -> None:
    db = _SqliteDb()
    monkeypatch.setattr(bot_repos, "db", db)

    bot_repos.upsert_bot(
        {
            "id": "bot-1",
            "name": "Bot 1",
            "strategy_id": "strategy-1",
            "mode": "instant",
            "run_type": "backtest",
            "wallet_config": {"balances": {"USD": 1000.0}},
            "snapshot_interval_ms": 250,
        }
    )

    persisted = bot_repos.get_bot("bot-1")

    assert persisted is not None
    assert persisted["strategy_id"] == "strategy-1"
    assert persisted["strategy_variant_id"] is None
    assert persisted["strategy_variant_name"] is None
    assert persisted["atm_template_id"] is None
    assert persisted["resolved_params"] == {}
    assert persisted["risk_config"] == {}


def test_strategy_rule_conditions_persist_as_canonical_mapping(monkeypatch) -> None:
    db = _SqliteDb()
    _seed_strategy(db)
    monkeypatch.setattr(strategy_repos, "db", db)

    strategy_repos.upsert_strategy_rule(
        {
            "id": "rule-1",
            "strategy_id": "strategy-1",
            "name": "Breakout Long",
            "action": "buy",
            "match": "all",
            "description": "Canonical typed rule",
            "enabled": True,
            "conditions": {
                "intent": "enter_long",
                "priority": 5,
                "trigger": {
                    "type": "signal_match",
                    "indicator_id": "indicator-1",
                    "output_name": "breakout",
                    "event_key": "breakout_long",
                },
                "guards": [
                    {
                        "type": "metric_match",
                        "indicator_id": "indicator-1",
                        "output_name": "profile_stats",
                        "field": "width",
                        "operator": ">=",
                        "value": 10,
                    }
                ],
            },
        }
    )

    loaded = strategy_repos.load_strategies()

    assert loaded[0]["rules_raw"][0]["conditions"] == {
        "intent": "enter_long",
        "priority": 5,
        "trigger": {
            "type": "signal_match",
            "indicator_id": "indicator-1",
            "output_name": "breakout",
            "event_key": "breakout_long",
        },
        "guards": [
            {
                "type": "metric_match",
                "indicator_id": "indicator-1",
                "output_name": "profile_stats",
                "field": "width",
                "operator": ">=",
                "value": 10,
            }
        ],
    }
