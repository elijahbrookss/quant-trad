from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Mapping

import pytest

from engines.indicator_engine.contracts import (
    Indicator,
    IndicatorRuntimeSpec,
    OutputDefinition,
    OutputRef,
    RuntimeOutput,
)
from engines.indicator_engine.runtime_engine import IndicatorExecutionEngine
from market_data.contracts import (
    OPEN_INTEREST_FACT_TYPE,
    OPEN_INTEREST_FACT_VERSION,
    TypedFeatureRecord,
    OpenInterestFact,
    OpenInterestRecord,
    SourceIdentity,
)
from market_data.normalization import NormalizedFeatureFact, NormalizedStatus
from market_data.requirements import UnavailableMarketData
from portal.backend.service.market.runtime_market_data import RuntimeMarketDataResolver


class _Store:
    def __init__(self, records: list[OpenInterestRecord]) -> None:
        self.records = records
        self.current_reads = 0
        self.dataset_reads = 0

    def resolve_series_id(self, **_kwargs: Any) -> int:
        return 9

    def read_open_interest(self, **kwargs: Any) -> list[OpenInterestRecord]:
        self.current_reads += 1
        return [
            record
            for record in self.records
            if kwargs["start"] <= record.fact.sample_time < kwargs["end"]
            and (
                kwargs.get("known_at_lte") is None
                or record.fact.known_at <= kwargs["known_at_lte"]
            )
        ]

    def read_dataset_series(self, **_kwargs: Any) -> list[OpenInterestRecord]:
        self.dataset_reads += 1
        return list(self.records)


def _record(*, sample_time: datetime, known_at: datetime, value: float) -> OpenInterestRecord:
    return OpenInterestRecord(
        series_id=9,
        revision=1,
        market_commit_seq=int(value),
        ingestion_run_id=f"poll-{int(value)}",
        source_identity_key="coinbase-open-interest",
        source=SourceIdentity(
            provider="COINBASE",
            venue="COINBASE_DIRECT",
            source_kind="poll_api",
            adapter_version="coinbase.oi.v1",
        ),
        provenance={"provider_event_time_available": False},
        fact=OpenInterestFact(
            sample_time=sample_time,
            value=value,
            received_at=known_at,
            accepted_at=known_at,
            known_at=known_at,
            known_at_method="platform_acceptance",
        ),
    )


class _OpenInterestIndicator(Indicator):
    def __init__(self) -> None:
        self.runtime_spec = IndicatorRuntimeSpec(
            instance_id="oi-indicator",
            manifest_type="test_oi",
            version="v1",
            dependencies=(),
            outputs=(OutputDefinition(name="value", type="metric"),),
        )
        self._output: RuntimeOutput | None = None

    def apply_bar(
        self, bar: Any, inputs: Mapping[OutputRef, RuntimeOutput]
    ) -> None:
        record = self.market_data_input("open_interest")
        self._output = RuntimeOutput(
            bar_time=bar,
            ready=True,
            value={"open_interest": record.fact.value},
        )

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        assert self._output is not None
        return {"value": self._output}


def _declarations(*, required: bool = True) -> dict[str, tuple[dict[str, Any], ...]]:
    return {
        "oi-indicator": (
            {
                "key": "open_interest",
                "fact_type": OPEN_INTEREST_FACT_TYPE,
                "contract_version": OPEN_INTEREST_FACT_VERSION,
                "instrument_role": "primary",
                "alignment": "latest_known",
                "max_staleness_seconds": 60,
                "required": required,
                "required_fields": ["value", "known_at"],
            },
        )
    }


def test_runtime_resolver_and_indicator_engine_deliver_only_latest_known_fact() -> None:
    decision = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    known = _record(
        sample_time=decision - timedelta(seconds=30),
        known_at=decision - timedelta(seconds=25),
        value=10,
    )
    future = _record(
        sample_time=decision - timedelta(seconds=10),
        known_at=decision + timedelta(seconds=1),
        value=99,
    )
    resolver = RuntimeMarketDataResolver(store=_Store([known, future]))
    market_inputs = resolver.resolve(
        requirements_by_consumer=_declarations(),
        primary_instrument_id="coinbase-btc-future",
        evaluation_time=decision,
    )

    frame = IndicatorExecutionEngine([_OpenInterestIndicator()]).step(
        bar=decision,
        bar_time=decision,
        market_data_inputs=market_inputs,
    )

    assert frame.outputs["oi-indicator.value"].value["open_interest"] == 10


def test_runtime_resolver_fails_loudly_when_required_fact_is_stale() -> None:
    decision = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    stale = _record(
        sample_time=decision - timedelta(minutes=5),
        known_at=decision - timedelta(minutes=4),
        value=10,
    )

    with pytest.raises(RuntimeError, match="market_data_required_unavailable"):
        RuntimeMarketDataResolver(store=_Store([stale])).resolve(
            requirements_by_consumer=_declarations(),
            primary_instrument_id="coinbase-btc-future",
            evaluation_time=decision,
        )


def test_indicator_engine_clears_market_inputs_between_bars() -> None:
    decision = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    record = _record(
        sample_time=decision - timedelta(seconds=5),
        known_at=decision,
        value=10,
    )
    engine = IndicatorExecutionEngine([_OpenInterestIndicator()])
    engine.step(
        bar=decision,
        bar_time=decision,
        market_data_inputs={"oi-indicator": {"open_interest": record}},
    )

    with pytest.raises(KeyError, match="indicator_market_data_input_missing"):
        engine.step(bar=decision + timedelta(minutes=1), bar_time=decision + timedelta(minutes=1))


def test_frozen_runtime_reads_dataset_once_and_never_uses_mutable_latest(
    monkeypatch,
) -> None:
    import portal.backend.service.market.runtime_market_data as runtime_market_data

    decision = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    store = _Store(
        [
            _record(
                sample_time=decision - timedelta(seconds=30),
                known_at=decision - timedelta(seconds=25),
                value=10,
            )
        ]
    )
    binding = {
        "dataset_id": "mds_frozen",
        "series": [
            {
                "series_id": 9,
                "fact_type": OPEN_INTEREST_FACT_TYPE,
                "contract_version": OPEN_INTEREST_FACT_VERSION,
                "bindings": [
                    {
                        "consumer_id": "oi-indicator",
                        "primary_instrument_id": "coinbase-btc-future",
                        "input": _declarations()["oi-indicator"][0],
                    }
                ],
            }
        ],
    }
    monkeypatch.setattr(
        runtime_market_data,
        "normalize_backtest_dataset_binding",
        lambda payload: dict(payload),
    )
    resolver = RuntimeMarketDataResolver(store=store, dataset_binding=binding)

    first = resolver.resolve(
        requirements_by_consumer=_declarations(),
        primary_instrument_id="coinbase-btc-future",
        evaluation_time=decision,
    )
    second = resolver.resolve(
        requirements_by_consumer=_declarations(),
        primary_instrument_id="coinbase-btc-future",
        evaluation_time=decision,
    )

    assert first["oi-indicator"]["open_interest"].fact.value == 10
    assert second["oi-indicator"]["open_interest"].fact.value == 10
    assert store.dataset_reads == 1
    assert store.current_reads == 0


_NORMALIZED_SPEC_ID = f"nsp_{'a' * 31}"
_NORMALIZED_FACT_TYPE = "market.normalized.aggressive_buy_share"
_NORMALIZED_CONTRACT = f"market.normalized_feature.v1/{_NORMALIZED_SPEC_ID}"


def _normalized_record(
    *, effective_at: datetime, known_at: datetime, value: str | None, commit_seq: int
) -> TypedFeatureRecord:
    status = NormalizedStatus.VALID if value is not None else NormalizedStatus.INVALID_INPUT
    fingerprint = f"{commit_seq:064x}"
    fact = NormalizedFeatureFact(
        series_id=19,
        spec_id=_NORMALIZED_SPEC_ID,
        spec_hash="b" * 64,
        effective_at=effective_at,
        known_at=known_at,
        value=value,
        status=status,
        reason=None if value is not None else "source_window_gap",
        input_start=effective_at,
        input_end=effective_at,
        input_count=1,
        input_watermark=commit_seq,
        source_series_ids=(11,),
        source_material_hashes=("c" * 64,),
        input_fingerprint=fingerprint,
    )
    return TypedFeatureRecord(
        version_id=f"nfv-{commit_seq}",
        series_id=19,
        revision=1,
        market_commit_seq=commit_seq,
        provenance_hash="d" * 64,
        quality={
            "classification": status.value,
            "valid": status is NormalizedStatus.VALID,
            "reason": fact.reason,
        },
        fact=fact,
    )


def _normalized_declarations(*, required: bool = True):
    return {
        "normalized-indicator": (
            {
                "key": "aggressive_buy_share",
                "fact_type": _NORMALIZED_FACT_TYPE,
                "contract_version": _NORMALIZED_CONTRACT,
                "timeframe_seconds": 60,
                "instrument_role": "primary",
                "alignment": "exact_interval",
                "required": required,
            },
        )
    }


def _normalized_binding():
    declaration = _normalized_declarations()["normalized-indicator"][0]
    return {
        "dataset_id": "mds_normalized",
        "series": [
            {
                "series_id": 19,
                "fact_type": _NORMALIZED_FACT_TYPE,
                "contract_version": _NORMALIZED_CONTRACT,
                "bindings": [
                    {
                        "consumer_id": "normalized-indicator",
                        "primary_instrument_id": "coinbase-btc-future",
                        "input": declaration,
                    }
                ],
            }
        ],
    }


def test_frozen_normalized_exact_interval_is_provider_free_and_never_looks_ahead(
    monkeypatch,
) -> None:
    import portal.backend.service.market.runtime_market_data as runtime_market_data

    decision = datetime(2025, 1, 1, 12, 1, tzinfo=UTC)
    selected = _normalized_record(
        effective_at=decision - timedelta(minutes=1),
        known_at=decision - timedelta(seconds=1),
        value="0.6",
        commit_seq=21,
    )
    future = _normalized_record(
        effective_at=decision,
        known_at=decision,
        value="0.9",
        commit_seq=22,
    )
    store = _Store([selected, future])
    monkeypatch.setattr(
        runtime_market_data,
        "normalize_backtest_dataset_binding",
        lambda payload: dict(payload),
    )

    result = RuntimeMarketDataResolver(
        store=store,
        dataset_binding=_normalized_binding(),
    ).resolve(
        requirements_by_consumer=_normalized_declarations(),
        primary_instrument_id="coinbase-btc-future",
        evaluation_time=decision,
    )

    assert result["normalized-indicator"]["aggressive_buy_share"] is selected
    assert store.dataset_reads == 1
    assert store.current_reads == 0


def test_frozen_invalid_normalized_fact_remains_visible(monkeypatch) -> None:
    import portal.backend.service.market.runtime_market_data as runtime_market_data

    decision = datetime(2025, 1, 1, 12, 1, tzinfo=UTC)
    invalid = _normalized_record(
        effective_at=decision - timedelta(minutes=1),
        known_at=decision - timedelta(seconds=1),
        value=None,
        commit_seq=21,
    )
    monkeypatch.setattr(
        runtime_market_data,
        "normalize_backtest_dataset_binding",
        lambda payload: dict(payload),
    )
    resolver = RuntimeMarketDataResolver(
        store=_Store([invalid]),
        dataset_binding=_normalized_binding(),
    )

    with pytest.raises(RuntimeError, match="source_window_gap"):
        resolver.resolve(
            requirements_by_consumer=_normalized_declarations(required=True),
            primary_instrument_id="coinbase-btc-future",
            evaluation_time=decision,
        )
    optional = resolver.resolve(
        requirements_by_consumer=_normalized_declarations(required=False),
        primary_instrument_id="coinbase-btc-future",
        evaluation_time=decision,
    )
    assert isinstance(
        optional["normalized-indicator"]["aggressive_buy_share"],
        UnavailableMarketData,
    )


def test_mutable_runtime_does_not_implicitly_read_normalized_facts() -> None:
    with pytest.raises(RuntimeError, match="mutable runtime reads are unavailable"):
        RuntimeMarketDataResolver(store=_Store([])).resolve(
            requirements_by_consumer=_normalized_declarations(),
            primary_instrument_id="coinbase-btc-future",
            evaluation_time=datetime(2025, 1, 1, 12, 1, tzinfo=UTC),
        )
