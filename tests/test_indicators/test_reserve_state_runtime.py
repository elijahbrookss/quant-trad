from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from indicators.reserve_state.runtime import TypedReserveStateIndicator
from market_data.canonical import CanonicalFact, CanonicalFactRecord
from market_data.contracts import SourceIdentity, record_effective_time
from market_data.requirements import latest_known_record


_OBSERVED_AT = datetime(2026, 8, 7, 19, 0, tzinfo=UTC)
_KNOWN_AT = _OBSERVED_AT + timedelta(minutes=2)
_DECISION_TIME = _KNOWN_AT + timedelta(hours=1)


def _record(*, provider: str = "CHAINLINK", known_at: datetime = _KNOWN_AT) -> CanonicalFactRecord:
    source = SourceIdentity(
        provider=provider,
        venue="ARBITRUM_MAINNET",
        source_kind="mvr_proxy",
        adapter_version="chainlink.mvr.reserve_state.v1",
    )
    return CanonicalFactRecord(
        series_id=71,
        source_id=17,
        revision=1,
        market_commit_seq=101,
        fact=CanonicalFact(
            fact_type="asset.reserve_state",
            payload_schema_id="asset.reserve_state.v1",
            observation_key="arbitrum:42161:report:2026-08-07T19:00:00Z",
            observation_time=_OBSERVED_AT,
            observation_time_method="chainlink_latest_bundle_timestamp",
            source_published_at=_OBSERVED_AT,
            received_at=known_at,
            accepted_at=known_at,
            known_at=known_at,
            known_at_method="platform_acceptance",
            source=source,
            transformation_id="chainlink_mvr_reserve_state.v1",
            payload={
                "report_id": "DE000NXTA018",
                "reserve_asset": "BTC",
                "reserve_quantity": Decimal("514.32323119"),
                "unit": "BTC",
            },
            external_event_key="arbitrum:42161:bundle:2026-08-07T19:00:00Z",
            provenance={
                "chain_id": 42161,
                "proxy_address": "0xf5eA763bbFc7968A27b28bc612a8B89fCF9E0069",
                "bundle_hash": "a" * 64,
            },
        ),
    )


def _candle() -> Candle:
    return Candle(
        time=_DECISION_TIME,
        end=_DECISION_TIME + timedelta(hours=1),
        known_at=_DECISION_TIME,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1.0,
    )


def _run(record: CanonicalFactRecord) -> dict[str, object]:
    indicator = TypedReserveStateIndicator(
        indicator_id="reserve-state-proof",
        version="v1",
    )
    frame = IndicatorExecutionEngine([indicator]).step(
        bar=_candle(),
        bar_time=_DECISION_TIME,
        market_data_inputs={
            "reserve-state-proof": {"reserve_state": record},
        },
    )
    return frame.outputs["reserve-state-proof.reserve_state"].value


def test_structured_reserve_fact_runs_through_indicator_engine() -> None:
    record = _record()

    output = _run(record)

    assert record_effective_time(record) == _OBSERVED_AT
    assert latest_known_record(
        [record],
        evaluation_time=_DECISION_TIME,
        max_staleness_seconds=259200,
    ) is record
    assert output == {
        "state_key": "observed",
        "fields": {
            "report_id": "DE000NXTA018",
            "reserve_asset": "BTC",
            "reserve_quantity": 514.32323119,
            "reserve_quantity_exact": "514.32323119",
            "unit": "BTC",
            "observation_time": _OBSERVED_AT.isoformat(),
            "known_at": _KNOWN_AT.isoformat(),
            "age_seconds": 3720,
        },
    }


def test_indicator_output_is_provider_neutral_after_canonicalization() -> None:
    chainlink = _record()
    alternate = _record(provider="FIXTURE_PROVIDER")

    assert chainlink.fact.provenance_hash != alternate.fact.provenance_hash
    assert _run(chainlink) == _run(alternate)


def test_indicator_rejects_fact_not_known_at_decision_time() -> None:
    future = _record(known_at=_DECISION_TIME + timedelta(seconds=1))

    with pytest.raises(RuntimeError, match="not causally visible"):
        _run(future)
