from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    CandleFact,
    DatasetSeriesRequest,
    NumericFact,
    NumericFactRecord,
    NumericFactState,
    SourceIdentity,
)
from portal.backend.db import InstrumentRecord, db
from portal.backend.service.market.backtest_dataset_service import (
    _validate_dataset_series,
)
from portal.backend.service.storage.repos.market_data import market_data_repo


pytestmark = pytest.mark.db

_DAY = datetime(2026, 8, 7, tzinfo=UTC)
_END = _DAY + timedelta(days=1)
_VERIFIED_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "tests"
    / "fixtures"
    / "providers"
    / "chainlink"
    / "aggregator_v3"
    / "verified_current.json"
)
_FACT_CONFIG = {
    "eth-usd": {
        "fact_type": "market.reference_price",
        "contract_version": "market.reference_price.v1",
        "unit": "USD",
        "dimensions": {"quote_currency": "USD"},
        "effective_at": datetime(2026, 8, 7, 22, 3, 23, tzinfo=UTC),
    },
    "tusd-reserves": {
        "fact_type": "market.reserve_balance",
        "contract_version": "market.reserve_balance.v1",
        "unit": "USD",
        "dimensions": {"reserve_unit": "USD"},
        "effective_at": datetime(2026, 8, 7, 2, 49, 59, tzinfo=UTC),
    },
}


@pytest.fixture
def numeric_series() -> dict[str, int | str]:
    token = uuid.uuid4().hex
    eth_instrument_id = f"numeric-eth-{token[:20]}"
    tusd_instrument_id = f"numeric-tusd-{token[:19]}"
    with db.session() as session:
        session.add_all(
            [
                InstrumentRecord(
                    id=eth_instrument_id,
                    datasource="CHAINLINK",
                    exchange="ETHEREUM_MAINNET",
                    symbol=f"ETH-{token[:8].upper()}",
                    instrument_type="spot",
                    can_short=False,
                    short_requires_borrow=False,
                    has_funding=False,
                    extra_metadata={"fixture": "numeric-fact-repository"},
                ),
                InstrumentRecord(
                    id=tusd_instrument_id,
                    datasource="CHAINLINK",
                    exchange="ETHEREUM_MAINNET",
                    symbol=f"TUSD-{token[:8].upper()}",
                    instrument_type="spot",
                    can_short=False,
                    short_requires_borrow=False,
                    has_funding=False,
                    extra_metadata={"fixture": "numeric-fact-repository"},
                ),
            ]
        )

    chainlink_source_id = market_data_repo.register_source(
        SourceIdentity(
            provider="CHAINLINK",
            venue="ETHEREUM_MAINNET",
            source_kind="public_evm_contract",
            adapter_version=f"chainlink_aggregator_v3.fixture.{token}",
        ),
        lineage={
            "fixture": "tests/fixtures/providers/chainlink/aggregator_v3/verified_current.json"
        },
    )
    candle_source_id = market_data_repo.register_source(
        SourceIdentity(
            provider="TEST",
            venue="ISOLATED",
            source_kind="fixture",
            adapter_version=f"numeric-fact-candle.{token}",
        ),
        lineage={"fixture": "test_numeric_fact_repository_db.py"},
    )
    eth_series_id = market_data_repo.register_series(
        instrument_id=eth_instrument_id,
        fact_type="market.reference_price",
        timeframe_seconds=None,
        contract_version="market.reference_price.v1",
        dimensions={"quote_currency": "USD"},
    )
    tusd_series_id = market_data_repo.register_series(
        instrument_id=tusd_instrument_id,
        fact_type="market.reserve_balance",
        timeframe_seconds=None,
        contract_version="market.reserve_balance.v1",
        dimensions={"reserve_unit": "USD"},
    )
    candle_series_id = market_data_repo.register_series(
        instrument_id=eth_instrument_id,
        fact_type=CANDLE_FACT_TYPE,
        timeframe_seconds=3600,
        contract_version=CANDLE_FACT_VERSION,
    )
    return {
        "eth_instrument_id": eth_instrument_id,
        "tusd_instrument_id": tusd_instrument_id,
        "chainlink_source_id": chainlink_source_id,
        "candle_source_id": candle_source_id,
        "eth_series_id": eth_series_id,
        "tusd_series_id": tusd_series_id,
        "candle_series_id": candle_series_id,
    }


def test_numeric_event_revisions_are_idempotent_causal_and_auditable(
    numeric_series: dict[str, int | str],
) -> None:
    series_id = int(numeric_series["eth_series_id"])
    source_id = int(numeric_series["chainlink_source_id"])
    original = _numeric_fact("eth-usd", material="block-a")

    inserted = _ingest_numeric(
        series_id=series_id,
        source_id=source_id,
        fact=original,
        provenance={"block_hash": "block-a"},
    )
    duplicate = _ingest_numeric(
        series_id=series_id,
        source_id=source_id,
        fact=original,
        provenance={"block_hash": "duplicate-rpc-observation"},
    )

    assert inserted.inserted_count == 1
    assert inserted.corrected_count == 0
    assert duplicate.noop_count == 1
    assert duplicate.inserted_count == duplicate.corrected_count == 0

    value_corrected = replace(
        original,
        value=Decimal("1915.00000001"),
        raw_value="191500000001",
        known_at=original.known_at + timedelta(minutes=5),
        accepted_at=original.accepted_at + timedelta(minutes=5),
        source_event_material_hash=_material_hash("block-a-value-corrected"),
    )
    value_outcome = _ingest_numeric(
        series_id=series_id,
        source_id=source_id,
        fact=value_corrected,
        provenance={"block_hash": "block-a", "correction": "answer"},
    )
    assert value_outcome.corrected_count == 1

    material_corrected = replace(
        value_corrected,
        source_event_material_hash=_material_hash("block-b-same-answer"),
    )
    material_outcome = _ingest_numeric(
        series_id=series_id,
        source_id=source_id,
        fact=material_corrected,
        provenance={"block_hash": "block-b", "correction": "source_material"},
    )
    assert material_outcome.corrected_count == 1

    before_invalidation = market_data_repo.read_numeric_facts(
        series_id=series_id,
        start=_DAY,
        end=_END,
    )
    at_original_known = market_data_repo.read_numeric_facts(
        series_id=series_id,
        start=_DAY,
        end=_END,
        known_at_lte=original.known_at,
    )
    assert before_invalidation[0].revision == 3
    assert before_invalidation[0].fact.value == Decimal("1915.00000001")
    assert before_invalidation[0].provenance["block_hash"] == "block-b"
    assert at_original_known[0].revision == 1
    assert at_original_known[0].fact.value == Decimal("1914.28523541")

    invalidated = replace(
        material_corrected,
        state=NumericFactState.INVALIDATED,
        known_at=material_corrected.known_at + timedelta(minutes=5),
        accepted_at=material_corrected.accepted_at + timedelta(minutes=5),
        source_event_material_hash=_material_hash("block-b-removed"),
    )
    invalidation_outcome = _ingest_numeric(
        series_id=series_id,
        source_id=source_id,
        fact=invalidated,
        provenance={"block_hash": "block-b", "removed": True},
    )
    assert invalidation_outcome.corrected_count == 1

    assert market_data_repo.read_numeric_facts(
        series_id=series_id,
        start=_DAY,
        end=_END,
    ) == []

    pre_invalidation_snapshot = market_data_repo.read_numeric_facts(
        series_id=series_id,
        start=_DAY,
        end=_END,
        as_of_commit_seq=material_outcome.max_commit_seq,
    )
    assert pre_invalidation_snapshot[0].revision == 3
    assert pre_invalidation_snapshot[0].fact.state is NumericFactState.ACTIVE

    revisions = market_data_repo.read_numeric_fact_revisions(
        series_id=series_id,
        start=_DAY,
        end=_END,
    )
    assert [record.revision for record in revisions] == [1, 2, 3, 4]
    assert [record.fact.state for record in revisions] == [
        NumericFactState.ACTIVE,
        NumericFactState.ACTIVE,
        NumericFactState.ACTIVE,
        NumericFactState.INVALIDATED,
    ]
    assert [record.market_commit_seq for record in revisions] == sorted(
        {record.market_commit_seq for record in revisions}
    )
    assert revisions[-1].provenance["removed"] is True


def test_verified_numeric_facts_share_commit_clock_and_freeze_provider_free(
    numeric_series: dict[str, int | str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eth = _numeric_fact("eth-usd", material="eth-dataset")
    tusd = _numeric_fact("tusd-reserves", material="tusd-dataset")
    eth_outcome = _ingest_numeric(
        series_id=int(numeric_series["eth_series_id"]),
        source_id=int(numeric_series["chainlink_source_id"]),
        fact=eth,
        provenance={"fixture": "verified_current", "binding_id": "eth-usd"},
    )
    candle_outcome = market_data_repo.ingest_candles(
        series_id=int(numeric_series["candle_series_id"]),
        source_id=int(numeric_series["candle_source_id"]),
        facts=[_candle()],
        request={"fixture": "numeric-shared-commit-clock"},
        source_revision="candle-v1",
    )
    tusd_outcome = _ingest_numeric(
        series_id=int(numeric_series["tusd_series_id"]),
        source_id=int(numeric_series["chainlink_source_id"]),
        fact=tusd,
        provenance={
            "fixture": "verified_current",
            "binding_id": "tusd-reserves",
        },
    )
    tusd_retry = _ingest_numeric(
        series_id=int(numeric_series["tusd_series_id"]),
        source_id=int(numeric_series["chainlink_source_id"]),
        fact=tusd,
        provenance={
            "fixture": "verified_current_retry",
            "binding_id": "tusd-reserves",
        },
    )
    assert tusd_retry.noop_count == 1
    assert tusd_retry.inserted_count == tusd_retry.corrected_count == 0
    assert (
        eth_outcome.max_commit_seq
        < candle_outcome.max_commit_seq
        < tusd_outcome.max_commit_seq
    )

    requests = [
        DatasetSeriesRequest(int(numeric_series[key]), _DAY, _END)
        for key in ("eth_series_id", "tusd_series_id", "candle_series_id")
    ]
    frozen = market_data_repo.freeze_dataset(requests, purpose="backtest")
    repeated = market_data_repo.freeze_dataset(requests, purpose="backtest")

    assert frozen.reused_existing is False
    assert repeated.reused_existing is True
    assert repeated.dataset_id == frozen.dataset_id
    assert repeated.dataset_hash == frozen.dataset_hash
    assert repeated.max_commit_seq == frozen.max_commit_seq
    assert repeated.series == frozen.series
    assert len(frozen.series) == 3
    assert all(len(str(item["material_hash"])) == 64 for item in frozen.series)
    assert all(len(str(item["provenance_hash"])) == 64 for item in frozen.series)
    assert all(len(str(item["quality_hash"])) == 64 for item in frozen.series)

    def network_forbidden(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("frozen numeric reads must not contact a provider")

    monkeypatch.setattr("requests.sessions.Session.request", network_forbidden)
    frozen_eth = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=int(numeric_series["eth_series_id"]),
    )
    frozen_tusd = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=int(numeric_series["tusd_series_id"]),
    )
    assert isinstance(frozen_eth[0], NumericFactRecord)
    assert isinstance(frozen_tusd[0], NumericFactRecord)
    assert frozen_eth[0].fact.value == Decimal("1914.28523541")
    assert frozen_eth[0].fact.raw_value == "191428523541"
    assert frozen_tusd[0].fact.value == Decimal("501928900.88")
    assert frozen_tusd[0].fact.raw_value == "501928900880000000000000000"
    assert frozen_eth[0].source.provider == "CHAINLINK"
    assert frozen_tusd[0].source.venue == "ETHEREUM_MAINNET"

    corrected = replace(
        eth,
        value=Decimal("1916.25"),
        raw_value="191625000000",
        known_at=eth.known_at + timedelta(minutes=5),
        accepted_at=eth.accepted_at + timedelta(minutes=5),
        source_event_material_hash=_material_hash("eth-post-freeze-correction"),
    )
    _ingest_numeric(
        series_id=int(numeric_series["eth_series_id"]),
        source_id=int(numeric_series["chainlink_source_id"]),
        fact=corrected,
        provenance={"fixture": "post-freeze-correction"},
    )
    corrected_tusd = replace(
        tusd,
        value=Decimal("501928901"),
        raw_value="501928901000000000000000000",
        known_at=tusd.known_at + timedelta(minutes=5),
        accepted_at=tusd.accepted_at + timedelta(minutes=5),
        source_event_material_hash=_material_hash("tusd-post-freeze-correction"),
    )
    _ingest_numeric(
        series_id=int(numeric_series["tusd_series_id"]),
        source_id=int(numeric_series["chainlink_source_id"]),
        fact=corrected_tusd,
        provenance={"fixture": "post-freeze-correction"},
    )
    latest_eth = market_data_repo.read_numeric_facts(
        series_id=int(numeric_series["eth_series_id"]),
        start=_DAY,
        end=_END,
    )
    latest_tusd = market_data_repo.read_numeric_facts(
        series_id=int(numeric_series["tusd_series_id"]),
        start=_DAY,
        end=_END,
    )
    frozen_eth_again = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=int(numeric_series["eth_series_id"]),
    )
    frozen_tusd_again = market_data_repo.read_dataset_series(
        dataset_id=frozen.dataset_id,
        series_id=int(numeric_series["tusd_series_id"]),
    )
    corrected_freeze = market_data_repo.freeze_dataset(requests, purpose="backtest")

    assert latest_eth[0].revision == 2
    assert latest_eth[0].fact.value == Decimal("1916.25")
    assert latest_tusd[0].revision == 2
    assert latest_tusd[0].fact.value == Decimal("501928901")
    assert frozen_eth_again[0].revision == 1
    assert frozen_eth_again[0].fact.value == Decimal("1914.28523541")
    assert frozen_tusd_again[0].revision == 1
    assert frozen_tusd_again[0].fact.value == Decimal("501928900.88")
    assert corrected_freeze.dataset_id != frozen.dataset_id
    assert corrected_freeze.dataset_hash != frozen.dataset_hash
    for series_key in ("eth_series_id", "tusd_series_id"):
        series_id = int(numeric_series[series_key])
        frozen_revisions = market_data_repo.read_dataset_series(
            dataset_id=corrected_freeze.dataset_id,
            series_id=series_id,
        )
        assert [record.revision for record in frozen_revisions] == [1, 2]
        entry = next(
            item
            for item in corrected_freeze.series
            if int(item["series_id"]) == series_id
        )
        validated, _quality, admitted_revisions = _validate_dataset_series(
            store=market_data_repo,
            entry={**dict(entry), "dataset_id": corrected_freeze.dataset_id},
        )
        assert [record.revision for record in admitted_revisions] == [1, 2]
        assert validated["material_hash"] == entry["material_hash"]


def test_meaning_changing_dimensions_separate_numeric_series_identity(
    numeric_series: dict[str, int | str],
) -> None:
    instrument_id = str(numeric_series["eth_instrument_id"])
    usd = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="market.reference_price",
        timeframe_seconds=None,
        contract_version="market.reference_price.v1",
        dimensions={"quote_currency": "usd"},
    )
    repeated_usd = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="market.reference_price",
        timeframe_seconds=None,
        contract_version="market.reference_price.v1",
        dimensions={"quote_currency": "USD"},
    )
    eur = market_data_repo.register_series(
        instrument_id=instrument_id,
        fact_type="market.reference_price",
        timeframe_seconds=None,
        contract_version="market.reference_price.v1",
        dimensions={"quote_currency": "EUR"},
    )

    assert usd == repeated_usd == int(numeric_series["eth_series_id"])
    assert eur != usd
    assert market_data_repo.resolve_series_id(
        instrument_id=instrument_id,
        fact_type="market.reference_price",
        timeframe_seconds=None,
        contract_version="market.reference_price.v1",
        dimensions={"quote_currency": "eur"},
    ) == eur


def _ingest_numeric(
    *,
    series_id: int,
    source_id: int,
    fact: NumericFact,
    provenance: dict[str, Any],
):
    return market_data_repo.ingest_numeric_facts(
        series_id=series_id,
        source_id=source_id,
        facts=[fact],
        request={"fixture": "numeric-fact-repository-db"},
        provenance_by_event={fact.source_event_key: provenance},
        source_revision="chainlink-fixture.v1",
    )


def _numeric_fact(binding_id: str, *, material: str) -> NumericFact:
    verified = _verified(binding_id)
    config = _FACT_CONFIG[binding_id]
    effective_at = config["effective_at"]
    assert isinstance(effective_at, datetime)
    source_published_at = effective_at + timedelta(seconds=12)
    known_at = source_published_at + timedelta(minutes=2)
    proxy = str(verified["proxy_address"]).lower()
    proxy_round_id = str(verified["proxy_round_id"])
    group_key = f"evm:1:{proxy}:{proxy_round_id}"
    return NumericFact(
        fact_type=str(config["fact_type"]),
        contract_version=str(config["contract_version"]),
        value=Decimal(str(verified["normalized_answer"])),
        raw_value=str(verified["raw_answer"]),
        unit=str(config["unit"]),
        dimensions=dict(config["dimensions"]),
        effective_at=effective_at,
        effective_at_method="chainlink_round_updated_at",
        source_published_at=source_published_at,
        received_at=None,
        accepted_at=known_at + timedelta(hours=1),
        known_at=known_at,
        known_at_method="evm_confirmation_block",
        source_event_key=f"{group_key}:answer",
        source_event_group_key=group_key,
        source_event_component_key="answer",
        source_event_material_hash=_material_hash(material),
    )


def _verified(binding_id: str) -> dict[str, Any]:
    payload = json.loads(_VERIFIED_FIXTURE.read_text(encoding="utf-8"))
    return next(
        item for item in payload["observations"] if item["binding_id"] == binding_id
    )


def _material_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _candle() -> CandleFact:
    open_time = _DAY + timedelta(hours=12)
    close_time = open_time + timedelta(hours=1)
    return CandleFact(
        open_time=open_time,
        close_time=close_time,
        open=1900.0,
        high=1920.0,
        low=1890.0,
        close=1914.0,
        volume=100.0,
        trade_count=50,
        source_published_at=None,
        received_at=None,
        accepted_at=close_time + timedelta(minutes=1),
        known_at=close_time,
        known_at_method="interval_close_inferred",
    )
