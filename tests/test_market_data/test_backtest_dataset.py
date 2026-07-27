from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from market_data.backtest import bound_series_for_request
from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    CandleFact,
    CandleRecord,
    SourceIdentity,
    build_candle_material_hash,
    build_dataset_identity_hash,
    build_provenance_hash,
    build_quality_hash,
)
from market_data.store import FrozenDataset
from portal.backend.service.market.backtest_dataset_service import (
    derive_backtest_dataset_plan,
    prepare_backtest_dataset,
    resolve_backtest_strategy_identity,
    validate_backtest_dataset,
)


UTC = timezone.utc
EVALUATION_START = datetime(2024, 1, 1, tzinfo=UTC)
EVALUATION_END = datetime(2024, 1, 1, 2, tzinfo=UTC)
MATERIALIZATION_START = EVALUATION_START - timedelta(hours=14)


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(
        id="strategy-1",
        name="Reference strategy",
        timeframe="1h",
        indicator_ids=[],
        instrument_links=[SimpleNamespace(instrument_id="instrument-1")],
        rules={},
        resolved_params={},
        run_strategy_snapshot={"effective_strategy_config_hash": "config-hash-1"},
        effective_strategy_config={"effective_strategy_config_hash": "config-hash-1"},
        variant_id=None,
        variant_name=None,
    )


def _bot() -> dict[str, Any]:
    return {
        "id": "bot-1",
        "run_type": "backtest",
        "backtest_start": EVALUATION_START.isoformat(),
        "backtest_end": EVALUATION_END.isoformat(),
        "wallet_config": {"balances": {"USD": 10_000.0}},
        "execution_mode": "fast",
        "execution_semantics": "spot",
    }


def _instrument(_instrument_id: str) -> dict[str, Any]:
    return {
        "id": "instrument-1",
        "symbol": "BTC/USD",
        "datasource": "CCXT",
        "exchange": "COINBASE",
        "instrument_type": "spot",
    }


def _records(*, skip: int | None = None) -> list[CandleRecord]:
    source = SourceIdentity(
        provider="CCXT",
        venue="COINBASE",
        source_kind="historical_api",
        adapter_version="test.candle.ohlcv.v1",
    )
    records: list[CandleRecord] = []
    cursor = MATERIALIZATION_START
    index = 0
    while cursor < EVALUATION_END:
        if index != skip:
            fact = CandleFact(
                open_time=cursor,
                close_time=cursor + timedelta(hours=1),
                open=100.0 + index,
                high=102.0 + index,
                low=99.0 + index,
                close=101.0 + index,
                volume=10.0 + index,
                trade_count=index + 1,
                known_at=cursor + timedelta(hours=1),
                known_at_method="interval_close_inferred",
                accepted_at=EVALUATION_END + timedelta(days=1),
            )
            records.append(
                CandleRecord(
                    series_id=7,
                    revision=1,
                    market_commit_seq=index + 1,
                    ingestion_run_id="ingestion-1",
                    source_identity_key=source.identity_key,
                    source=source,
                    provenance={"operation": "historical_candle_ingest"},
                    fact=fact,
                )
            )
        cursor += timedelta(hours=1)
        index += 1
    return records


def _dataset(
    records: list[CandleRecord],
    *,
    quality: list[dict[str, Any]] | None = None,
) -> FrozenDataset:
    quality = list(quality or [])
    range_start = MATERIALIZATION_START.isoformat(
        timespec="microseconds"
    ).replace("+00:00", "Z")
    range_end = EVALUATION_END.isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )
    identity = {
        "identity_key": "series-identity-1",
        "instrument_id": "instrument-1",
        "fact_type": CANDLE_FACT_TYPE,
        "timeframe_seconds": 3600,
        "contract_version": CANDLE_FACT_VERSION,
    }
    source_key = records[0].source_identity_key
    entry = {
        "series_id": 7,
        "range_start": range_start,
        "range_end": range_end,
        "max_commit_seq": max(record.market_commit_seq for record in records),
        "row_count": len(records),
        "material_hash": build_candle_material_hash(
            series_identity=identity,
            records=records,
        ),
        "provenance_hash": build_provenance_hash(records),
        "source_summary": {
            "counts": {source_key: len(records)},
            "sources": {
                source_key: {
                    "provider": "CCXT",
                    "venue": "COINBASE",
                    "source_kind": "historical_api",
                    "adapter_version": "test.candle.ohlcv.v1",
                }
            },
        },
        "quality_hash": build_quality_hash(quality),
        "quality_summary": {
            "evidence_count": len(quality),
            "classifications": {
                name: sum(1 for row in quality if row["classification"] == name)
                for name in sorted({row["classification"] for row in quality})
            },
        },
        **identity,
    }
    hashed_entry = {
        key: entry[key]
        for key in (
            "series_id",
            "range_start",
            "range_end",
            "max_commit_seq",
            "row_count",
            "material_hash",
            "provenance_hash",
            "source_summary",
            "quality_hash",
            "quality_summary",
        )
    }
    dataset_hash = build_dataset_identity_hash([hashed_entry])
    return FrozenDataset(
        dataset_id=f"mds_{dataset_hash[:32]}",
        dataset_hash=dataset_hash,
        max_commit_seq=int(entry["max_commit_seq"]),
        series=(entry,),
        purpose="backtest",
    )


class _Store:
    def __init__(
        self,
        records: list[CandleRecord],
        *,
        quality: list[dict[str, Any]] | None = None,
    ) -> None:
        self.records = list(records)
        self.quality = list(quality or [])
        self.dataset = _dataset(self.records, quality=self.quality)
        self.dataset_reads: list[dict[str, Any]] = []
        self.freeze_requests: list[Any] = []

    def get_dataset(self, dataset_id: str) -> FrozenDataset:
        if dataset_id != self.dataset.dataset_id:
            raise ValueError(f"market_dataset_unknown: dataset_id={dataset_id}")
        return self.dataset

    def read_dataset_series(self, **kwargs: Any) -> list[CandleRecord]:
        self.dataset_reads.append(dict(kwargs))
        start = kwargs.get("start") or MATERIALIZATION_START
        end = kwargs.get("end") or EVALUATION_END
        return [
            record
            for record in self.records
            if start <= record.fact.open_time < end
        ]

    def list_gap_evidence(self, **_kwargs: Any) -> list[dict[str, Any]]:
        return list(self.quality)

    def resolve_series_id(self, **_kwargs: Any) -> int:
        return 7

    def freeze_dataset(self, requests: Any, **_kwargs: Any) -> FrozenDataset:
        self.freeze_requests = list(requests)
        return self.dataset


def _validate(store: _Store) -> dict[str, Any]:
    return validate_backtest_dataset(
        dataset_id=store.dataset.dataset_id,
        bot=_bot(),
        strategy=_strategy(),
        store=store,
        instrument_loader=_instrument,
    )


def test_plan_separates_warmup_materialization_evaluation_and_decision_ranges() -> None:
    plan = derive_backtest_dataset_plan(
        bot=_bot(),
        strategy=_strategy(),
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        instrument_loader=_instrument,
    )

    assert plan["warmup_range"]["required_bars"] == 14
    assert plan["warmup_range"]["start"] == "2023-12-31T10:00:00.000000Z"
    assert plan["materialization_range"]["start"] == plan["warmup_range"]["start"]
    assert plan["evaluation_range"] == plan["decision_range"] == {
        "start": "2024-01-01T00:00:00.000000Z",
        "end_exclusive": "2024-01-01T02:00:00.000000Z",
    }


def test_admission_revalidates_exact_snapshot_and_rejects_tampering() -> None:
    store = _Store(_records())
    binding = _validate(store)

    assert binding["dataset_id"] == store.dataset.dataset_id
    assert binding["strategy_hash"] == resolve_backtest_strategy_identity(
        _strategy()
    )["strategy_hash"]
    assert binding["effective_strategy_config_hash"] == "config-hash-1"
    assert len(binding["indicator_config_hash"]) == 64
    assert binding["quality"]["status"] == "ready"
    assert binding["series"][0]["loaded_range"] == {
        "start": "2023-12-31T10:00:00.000000Z",
        "end_exclusive": "2024-01-01T02:00:00.000000Z",
    }

    tampered = {**dict(store.dataset.series[0]), "material_hash": "tampered"}
    store.dataset = FrozenDataset(
        dataset_id=store.dataset.dataset_id,
        dataset_hash=store.dataset.dataset_hash,
        max_commit_seq=store.dataset.max_commit_seq,
        series=(tampered,),
        purpose="backtest",
    )
    with pytest.raises(RuntimeError, match="hash_disagreement"):
        _validate(store)


def test_admission_rejects_undisclosed_gap_but_preserves_disclosed_closure() -> None:
    gap_start = MATERIALIZATION_START + timedelta(hours=3)
    records = _records(skip=3)
    with pytest.raises(RuntimeError, match="unacceptable_gap"):
        _validate(_Store(records))

    evidence = [
        {
            "start": gap_start,
            "end": gap_start + timedelta(hours=1),
            "classification": "provider_closure",
            "expected_count": 1,
            "observed_count": 0,
            "evidence_hash": "gap-1",
        }
    ]
    binding = _validate(_Store(records, quality=evidence))
    assert binding["quality"]["status"] == "ready_with_caveats"
    assert binding["quality"]["classifications"] == {"provider_closure": 1}


def test_binding_forbids_range_expansion_and_series_substitution() -> None:
    binding = _validate(_Store(_records()))
    with pytest.raises(ValueError, match="range_expansion_forbidden"):
        bound_series_for_request(
            binding,
            instrument_id="instrument-1",
            timeframe_seconds=3600,
            start=MATERIALIZATION_START - timedelta(hours=1),
            end=EVALUATION_END,
        )
    with pytest.raises(ValueError, match="series_missing"):
        bound_series_for_request(
            binding,
            instrument_id="different-instrument",
            timeframe_seconds=3600,
            start=EVALUATION_START,
            end=EVALUATION_END,
        )


def test_plan_freezes_exact_execution_instrument_configuration() -> None:
    def instrument_with(**changes: Any) -> dict[str, Any]:
        return {
            **_instrument("instrument-1"),
            "maker_fee_rate": 0.004,
            "taker_fee_rate": 0.006,
            "price_tick_size": 0.01,
            "quantity_step_size": 0.00000001,
            "created_at": "2025-01-01T00:00:00Z",
            "updated_at": "2025-01-02T00:00:00Z",
            **changes,
        }

    first = derive_backtest_dataset_plan(
        bot=_bot(),
        strategy=_strategy(),
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        instrument_loader=lambda _instrument_id: instrument_with(),
    )
    operational_timestamp_change = derive_backtest_dataset_plan(
        bot=_bot(),
        strategy=_strategy(),
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        instrument_loader=lambda _instrument_id: instrument_with(
            created_at="2026-01-01T00:00:00Z",
            updated_at="2026-01-02T00:00:00Z",
        ),
    )
    profiling_change = derive_backtest_dataset_plan(
        bot={**_bot(), "profile": True},
        strategy=_strategy(),
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        instrument_loader=lambda _instrument_id: instrument_with(),
    )
    execution_change = derive_backtest_dataset_plan(
        bot=_bot(),
        strategy=_strategy(),
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        instrument_loader=lambda _instrument_id: instrument_with(
            maker_fee_rate=0.003,
        ),
    )

    snapshot = first["instruments"][0]["snapshot"]
    assert "created_at" not in snapshot
    assert "updated_at" not in snapshot
    assert snapshot["maker_fee_rate"] == 0.004
    assert first["instrument_config_hash"] == operational_timestamp_change[
        "instrument_config_hash"
    ]
    assert first["execution_config_hash"] == operational_timestamp_change[
        "execution_config_hash"
    ]
    assert first["execution_config_hash"] == profiling_change[
        "execution_config_hash"
    ]
    assert first["instrument_config_hash"] != execution_change[
        "instrument_config_hash"
    ]
    assert first["execution_config_hash"] != execution_change[
        "execution_config_hash"
    ]


def test_preparation_is_explicit_and_provider_free_when_coverage_is_complete() -> None:
    store = _Store(_records())

    class DenyProvider:
        def ingest_by_instrument(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("provider must not be invoked when coverage is complete")

    result = prepare_backtest_dataset(
        bot=_bot(),
        strategy=_strategy(),
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        acquire_missing=False,
        store=store,
        ingestor=DenyProvider(),
        coverage_loader=lambda *_args: {"row_count": 16, "missing_ranges": []},
        instrument_loader=_instrument,
    )

    assert result["status"] == "ready"
    assert result["acquisitions"] == []
    assert len(store.freeze_requests) == 1
    assert store.freeze_requests[0].start == MATERIALIZATION_START
    assert set(result["performance"]["phases"]) == {
        "requirement_resolution",
        "coverage_inspection",
        "provider_acquisition",
        "ingestion_validation",
        "dataset_hashing_and_freezing",
        "dataset_admission",
    }


def test_preparation_requires_explicit_provider_authority() -> None:
    with pytest.raises(RuntimeError, match="explicit acquisition enabled"):
        prepare_backtest_dataset(
            bot=_bot(),
            strategy=_strategy(),
            evaluation_start=EVALUATION_START,
            evaluation_end=EVALUATION_END,
            acquire_missing=False,
            store=_Store(_records()),
            coverage_loader=lambda *_args: {
                "row_count": 15,
                "missing_ranges": [
                    {
                        "start": "2023-12-31T10:00:00Z",
                        "end": "2023-12-31T11:00:00Z",
                    }
                ],
            },
            instrument_loader=_instrument,
        )


def test_admission_requires_dataset_identity_and_exact_evaluation_range() -> None:
    store = _Store(_records())
    with pytest.raises(ValueError, match="dataset_id is required"):
        validate_backtest_dataset(
            dataset_id="",
            bot=_bot(),
            strategy=_strategy(),
            store=store,
            instrument_loader=_instrument,
        )
    drifted = {**_bot(), "backtest_end": "2024-01-01T03:00:00Z"}
    with pytest.raises(ValueError, match="range_mismatch"):
        validate_backtest_dataset(
            dataset_id=store.dataset.dataset_id,
            bot=drifted,
            strategy=_strategy(),
            store=store,
            instrument_loader=_instrument,
        )


def test_strategy_identity_rejects_declared_hash_disagreement() -> None:
    strategy = _strategy()
    strategy.run_strategy_snapshot = {
        "strategy_hash": "not-the-compiled-strategy-hash",
        "effective_strategy_config_hash": "config-hash-1",
    }

    with pytest.raises(RuntimeError, match="strategy_identity_disagreement"):
        resolve_backtest_strategy_identity(strategy)


def test_indicator_configuration_is_part_of_execution_identity() -> None:
    strategy = _strategy()
    strategy.indicator_ids = ["indicator-1"]

    first = resolve_backtest_strategy_identity(
        strategy,
        indicator_meta_loader=lambda _indicator_id: {
            "id": "indicator-1",
            "type": "candle_stats",
            "params": {"warmup_bars": 20},
            "enabled": True,
        },
    )
    second = resolve_backtest_strategy_identity(
        strategy,
        indicator_meta_loader=lambda _indicator_id: {
            "id": "indicator-1",
            "type": "candle_stats",
            "params": {"warmup_bars": 21},
            "enabled": True,
        },
    )

    assert first["strategy_hash"] == second["strategy_hash"]
    assert first["indicator_config_hash"] != second["indicator_config_hash"]


def test_transitive_indicator_configuration_is_part_of_execution_identity() -> None:
    strategy = _strategy()
    strategy.indicator_ids = ["regime-1"]

    def loader(dependency_window: int):
        metas = {
            "regime-1": {
                "id": "regime-1",
                "type": "regime",
                "params": {},
                "enabled": True,
                "dependencies": [
                    {
                        "indicator_id": "candle-stats-1",
                        "indicator_type": "candle_stats",
                        "output_name": "candle_stats",
                    }
                ],
            },
            "candle-stats-1": {
                "id": "candle-stats-1",
                "type": "candle_stats",
                "params": {"atr_short_window": dependency_window},
                "enabled": True,
                "dependencies": [],
            },
        }
        return lambda indicator_id: metas[indicator_id]

    first = resolve_backtest_strategy_identity(
        strategy,
        indicator_meta_loader=loader(14),
    )
    changed_dependency = resolve_backtest_strategy_identity(
        strategy,
        indicator_meta_loader=loader(21),
    )

    assert first["strategy_hash"] == changed_dependency["strategy_hash"]
    assert first["indicator_count"] == 2
    assert first["direct_indicator_count"] == 1
    assert first["indicator_config_hash"] != changed_dependency["indicator_config_hash"]


def test_transitive_indicator_inputs_participate_in_dataset_planning() -> None:
    strategy = _strategy()
    strategy.indicator_ids = ["regime-1"]

    identity_loader = {
        "regime-1": {
            "id": "regime-1",
            "type": "regime",
            "params": {},
            "dependencies": [{"indicator_id": "candle-stats-1", "output_name": "candle_stats"}],
        },
        "candle-stats-1": {"id": "candle-stats-1", "type": "candle_stats", "params": {}},
    }
    plan = derive_backtest_dataset_plan(
        bot=_bot(),
        strategy=strategy,
        evaluation_start=EVALUATION_START,
        evaluation_end=EVALUATION_END,
        indicator_meta_loader=lambda indicator_id: identity_loader[indicator_id],
        indicator_input_plan_loader=lambda indicator_id, **_kwargs: {
            "source_timeframe": "1h",
            "start": EVALUATION_START - timedelta(hours=50 if indicator_id == "candle-stats-1" else 20),
        },
        instrument_loader=_instrument,
    )

    assert {row["indicator_id"] for row in plan["indicator_inputs"]} == {
        "regime-1",
        "candle-stats-1",
    }
    assert plan["materialization_range"]["start"] == "2023-12-29T22:00:00.000000Z"
