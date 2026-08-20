from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from indicators.manifest import IndicatorManifest, IndicatorMarketInput
from market_data.contracts import (
    NumericFact,
    NumericFactRecord,
    NumericFactState,
    SourceIdentity,
)
from portal.backend.service.market.backtest_dataset_service import (
    derive_backtest_dataset_plan,
)
from portal.backend.service.market.runtime_market_data import RuntimeMarketDataResolver


REFERENCE_PRICE_FACT_TYPE = "market.reference_price"
REFERENCE_PRICE_FACT_VERSION = "market.reference_price.v1"
EVALUATION_START = datetime(2026, 1, 1, tzinfo=UTC)
EVALUATION_END = EVALUATION_START + timedelta(hours=2)


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(
        id="strategy-1",
        name="Typed numeric requirement proof",
        timeframe="1h",
        indicator_ids=["reference-price-proof"],
        instrument_links=[SimpleNamespace(instrument_id="eth-usd")],
        rules={},
        resolved_params={},
        run_strategy_snapshot={"effective_strategy_config_hash": "config-hash-1"},
        effective_strategy_config={"effective_strategy_config_hash": "config-hash-1"},
        variant_id=None,
        variant_name=None,
    )


def _instrument(instrument_id: str) -> dict[str, Any]:
    return {
        "id": instrument_id,
        "symbol": "ETH/USD",
        "datasource": "CCXT",
        "exchange": "COINBASE",
        "instrument_type": "spot",
    }


def test_dataset_plan_keeps_numeric_dimensions_in_typed_series_identity(
    monkeypatch,
) -> None:
    import portal.backend.service.market.backtest_dataset_service as service

    manifest = IndicatorManifest(
        type="reference_price_proof",
        version="1.0.0",
        label="Reference price proof",
        description="Offline typed numeric planning fixture.",
        market_inputs=(
            IndicatorMarketInput(
                key="reference_price_usd",
                fact_type=REFERENCE_PRICE_FACT_TYPE,
                contract_version=REFERENCE_PRICE_FACT_VERSION,
                dimensions={"quote_currency": "usd"},
                alignment="latest_known",
                max_staleness_seconds=3600,
                required_fields=("effective_at", "value", "unit", "known_at"),
            ),
            IndicatorMarketInput(
                key="reference_price_eur",
                fact_type=REFERENCE_PRICE_FACT_TYPE,
                contract_version=REFERENCE_PRICE_FACT_VERSION,
                dimensions={"quote_currency": "eur"},
                alignment="latest_known",
                max_staleness_seconds=3600,
                required_fields=("effective_at", "value", "unit", "known_at"),
            ),
        ),
    )
    monkeypatch.setattr(service, "get_indicator_manifest", lambda _type: manifest)

    plan = derive_backtest_dataset_plan(
        bot={"id": "bot-1", "run_type": "backtest"},
        strategy=_strategy(),
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        indicator_meta_loader=lambda _indicator_id: {
            "id": "reference-price-proof",
            "type": "reference_price_proof",
            "params": {},
            "enabled": True,
        },
        indicator_input_plan_loader=lambda *_args, **_kwargs: {
            "source_timeframe": "1h",
            "start": EVALUATION_START,
        },
        instrument_loader=_instrument,
    )

    numeric_series = [
        row for row in plan["series"] if row["fact_type"] == REFERENCE_PRICE_FACT_TYPE
    ]
    assert {tuple(sorted(row["dimensions"].items())) for row in numeric_series} == {
        (("quote_currency", "EUR"),),
        (("quote_currency", "USD"),),
    }
    assert all(row["timeframe_seconds"] is None for row in numeric_series)
    assert {
        row["bindings"][0]["input"]["key"]: row["bindings"][0]["input"][
            "dimensions"
        ]
        for row in numeric_series
    } == {
        "reference_price_eur": {"quote_currency": "EUR"},
        "reference_price_usd": {"quote_currency": "USD"},
    }
    assert all(
        "provider" not in row["bindings"][0]["input"] for row in numeric_series
    )


class _FrozenOnlyStore:
    def __init__(self, records: list[NumericFactRecord]) -> None:
        self.records = records
        self.dataset_reads = 0

    def read_dataset_series(self, *, dataset_id: str, series_id: int):
        raise AssertionError("numeric frozen runtime must read commit-pinned revisions")

    def read_numeric_fact_revisions(
        self,
        *,
        series_id: int,
        start: datetime,
        end: datetime,
        as_of_commit_seq: int,
    ):
        assert series_id == 31
        assert as_of_commit_seq == max(
            record.market_commit_seq for record in self.records
        )
        self.dataset_reads += 1
        return [
            record
            for record in self.records
            if start <= record.fact.effective_at < end
            and record.market_commit_seq <= as_of_commit_seq
        ]


def _numeric_record(
    *,
    effective_at: datetime,
    known_at: datetime,
    value: str,
    commit_seq: int,
    revision: int = 1,
    event_key: str | None = None,
    state: NumericFactState = NumericFactState.ACTIVE,
) -> NumericFactRecord:
    source = SourceIdentity(
        provider="CHAINLINK",
        venue="ethereum-mainnet",
        source_kind="aggregator_v3",
        adapter_version="chainlink.numeric.v1",
    )
    return NumericFactRecord(
        series_id=31,
        revision=revision,
        market_commit_seq=commit_seq,
        ingestion_run_id=f"numeric-{commit_seq}",
        source_identity_key=source.identity_key,
        source=source,
        provenance={"fixture": "provider-free-runtime"},
        fact=NumericFact(
            fact_type=REFERENCE_PRICE_FACT_TYPE,
            contract_version=REFERENCE_PRICE_FACT_VERSION,
            value=value,
            raw_value=value.replace(".", ""),
            unit="USD",
            dimensions={"quote_currency": "usd"},
            effective_at=effective_at,
            effective_at_method="chainlink_answer_computation",
            source_published_at=effective_at + timedelta(seconds=5),
            accepted_at=effective_at + timedelta(seconds=10),
            known_at=known_at,
            known_at_method="block_confirmation_policy",
            source_event_key=event_key or f"ethereum:proxy:round:{commit_seq}",
            state=state,
        ),
    )


def test_frozen_numeric_runtime_is_provider_free_and_causally_selects_latest_known(
    monkeypatch,
) -> None:
    import portal.backend.service.market.runtime_market_data as runtime_market_data

    decision = datetime(2026, 1, 1, 12, 10, tzinfo=UTC)
    visible = _numeric_record(
        effective_at=decision - timedelta(minutes=10),
        known_at=decision - timedelta(minutes=8),
        value="4321.123456789012345678",
        commit_seq=41,
        event_key="ethereum:proxy:round:1",
    )
    future = _numeric_record(
        effective_at=decision - timedelta(minutes=5),
        known_at=decision + timedelta(minutes=1),
        value="9999.999999999999999999",
        commit_seq=42,
        revision=2,
        event_key="ethereum:proxy:round:1",
    )
    invalidated = _numeric_record(
        effective_at=decision - timedelta(minutes=5),
        known_at=decision + timedelta(minutes=2),
        value="9999.999999999999999999",
        commit_seq=43,
        revision=3,
        event_key="ethereum:proxy:round:1",
        state=NumericFactState.INVALIDATED,
    )
    store = _FrozenOnlyStore([visible, future, invalidated])
    requirement = {
        "key": "reference_price",
        "fact_type": REFERENCE_PRICE_FACT_TYPE,
        "contract_version": REFERENCE_PRICE_FACT_VERSION,
        "instrument_role": "primary",
        "dimensions": {"quote_currency": "usd"},
        "alignment": "latest_known",
        "max_staleness_seconds": 900,
        "required": True,
        "required_fields": ["effective_at", "value", "unit", "known_at"],
    }
    binding = {
        "dataset_id": "mds_numeric",
        "series": [
            {
                "series_id": 31,
                "fact_type": REFERENCE_PRICE_FACT_TYPE,
                "contract_version": REFERENCE_PRICE_FACT_VERSION,
                "dimensions": {"quote_currency": "USD"},
                "range_start": (decision - timedelta(hours=1)).isoformat(),
                "range_end": (decision + timedelta(hours=1)).isoformat(),
                "max_commit_seq": 43,
                "bindings": [
                    {
                        "consumer_id": "reference-price-proof",
                        "primary_instrument_id": "eth-usd",
                        "input": requirement,
                    }
                ],
            }
        ],
    }

    class _MutableReadTrap:
        def __init__(self, *, store) -> None:
            self.store = store

        def __getattr__(self, name: str):
            raise AssertionError(f"frozen runtime attempted mutable/provider read: {name}")

    monkeypatch.setattr(
        runtime_market_data,
        "normalize_backtest_dataset_binding",
        lambda payload: dict(payload),
    )
    monkeypatch.setattr(
        runtime_market_data,
        "MarketDataCollectorService",
        _MutableReadTrap,
    )
    resolver = RuntimeMarketDataResolver(store=store, dataset_binding=binding)

    first = resolver.resolve(
        requirements_by_consumer={"reference-price-proof": (requirement,)},
        primary_instrument_id="eth-usd",
        evaluation_time=decision,
    )
    second = resolver.resolve(
        requirements_by_consumer={"reference-price-proof": (requirement,)},
        primary_instrument_id="eth-usd",
        evaluation_time=decision,
    )

    selected = first["reference-price-proof"]["reference_price"]
    assert selected is visible
    assert selected.fact.value == Decimal("4321.123456789012345678")
    assert second["reference-price-proof"]["reference_price"] is visible
    planned_requirement = {**requirement, "alias": requirement["key"]}
    planned_requirement.pop("key")
    assert resolver.causal_history(
        consumer_id="reference-price-proof",
        requirement=planned_requirement,
        primary_instrument_id="eth-usd",
        start=decision - timedelta(hours=1),
        end=decision + timedelta(minutes=1),
        evaluation_time=decision,
    ) == (visible,)
    corrected = resolver.resolve(
        requirements_by_consumer={"reference-price-proof": (requirement,)},
        primary_instrument_id="eth-usd",
        evaluation_time=decision + timedelta(seconds=90),
    )
    assert corrected["reference-price-proof"]["reference_price"] is future
    with pytest.raises(RuntimeError, match="market_data_required_unavailable"):
        resolver.resolve(
            requirements_by_consumer={"reference-price-proof": (requirement,)},
            primary_instrument_id="eth-usd",
            evaluation_time=decision + timedelta(minutes=3),
        )
    assert store.dataset_reads == 1
