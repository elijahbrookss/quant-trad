"""Causally safe runtime delivery for declared non-candle market facts."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Optional, Sequence

from market_data.backtest import normalize_backtest_dataset_binding
from market_data.contracts import (
    CANDLE_FACT_TYPE,
    FUNDING_RATE_FACT_TYPE,
    OPEN_INTEREST_FACT_TYPE,
    InstrumentRole,
    MarketDataAlignment,
    MarketDataRequirement,
    TypedFeatureRecord,
    record_effective_time,
)
from market_data.requirements import UnavailableMarketData, latest_known_record
from market_data.store import MarketDataStore

from ..storage.repos.market_data import market_data_repo
from .collector_service import MarketDataCollectorService


class RuntimeMarketDataResolver:
    """Resolve declared facts from a frozen dataset or canonical stored state."""

    def __init__(
        self,
        *,
        store: MarketDataStore = market_data_repo,
        dataset_binding: Optional[Mapping[str, Any]] = None,
        instrument_bindings: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.store = store
        self.dataset_binding = (
            normalize_backtest_dataset_binding(dataset_binding)
            if dataset_binding is not None
            else None
        )
        bindings = dict(instrument_bindings or {})
        self.underlying_by_primary = dict(
            bindings.get("underlying_by_primary")
            if isinstance(bindings.get("underlying_by_primary"), Mapping)
            else {}
        )
        self.benchmarks = dict(
            bindings.get("benchmarks")
            if isinstance(bindings.get("benchmarks"), Mapping)
            else {}
        )
        self._frozen_records: dict[int, tuple[Any, ...]] = {}
        self._latest_service = MarketDataCollectorService(store=store)

    @staticmethod
    def _requirement(raw: Mapping[str, Any]) -> MarketDataRequirement:
        return MarketDataRequirement(
            key=str(raw.get("key") or ""),
            fact_type=str(raw.get("fact_type") or ""),
            contract_version=str(raw.get("contract_version") or ""),
            timeframe_seconds=raw.get("timeframe_seconds"),
            instrument_role=str(raw.get("instrument_role") or "primary"),
            instrument_ref=raw.get("instrument_ref"),
            alignment=raw.get("alignment"),
            max_staleness_seconds=raw.get("max_staleness_seconds"),
            required=bool(raw.get("required", True)),
            allow_gaps=bool(raw.get("allow_gaps", False)),
            known_at_required=bool(raw.get("known_at_required", True)),
            required_fields=tuple(raw.get("required_fields") or ()),
            lookback_bars=raw.get("lookback_bars"),
            lookback_seconds=raw.get("lookback_seconds"),
        )

    @staticmethod
    def _unavailable(
        requirement: MarketDataRequirement,
        *,
        evaluation_time: datetime,
        reason: str,
        details: Mapping[str, Any],
        consumer_id: str,
    ) -> UnavailableMarketData:
        result = UnavailableMarketData(
            key=requirement.key,
            reason=reason,
            evaluation_time=evaluation_time,
            details=dict(details),
        )
        if requirement.required:
            raise RuntimeError(
                "market_data_required_unavailable: "
                f"consumer_id={consumer_id} key={requirement.key} reason={reason}"
            )
        return result

    def _frozen_series(
        self,
        *,
        consumer_id: str,
        requirement: MarketDataRequirement,
        primary_instrument_id: str,
        evaluation_time: datetime,
    ) -> Any:
        assert self.dataset_binding is not None
        matches: list[Mapping[str, Any]] = []
        for series in self.dataset_binding["series"]:
            for binding in series.get("bindings") or []:
                raw_input = binding.get("input")
                if not isinstance(raw_input, Mapping):
                    continue
                bound_primary = str(binding.get("primary_instrument_id") or "").strip()
                if (
                    str(binding.get("consumer_id") or "") == consumer_id
                    and str(raw_input.get("key") or "") == requirement.key
                    and (not bound_primary or bound_primary == primary_instrument_id)
                ):
                    matches.append(series)
        if len(matches) != 1:
            return self._unavailable(
                requirement,
                evaluation_time=evaluation_time,
                reason="frozen_binding_missing" if not matches else "frozen_binding_ambiguous",
                details={"match_count": len(matches)},
                consumer_id=consumer_id,
            )
        series = matches[0]
        if (
            str(series.get("fact_type") or "") != requirement.fact_type
            or str(series.get("contract_version") or "")
            != requirement.contract_version
        ):
            raise RuntimeError(
                "market_data_frozen_contract_disagreement: "
                f"consumer_id={consumer_id} key={requirement.key}"
            )
        series_id = int(series["series_id"])
        if series_id not in self._frozen_records:
            self._frozen_records[series_id] = tuple(
                self.store.read_dataset_series(
                    dataset_id=str(self.dataset_binding["dataset_id"]),
                    series_id=series_id,
                )
            )
        records = self._frozen_records[series_id]
        if requirement.alignment is MarketDataAlignment.EXACT_INTERVAL:
            timeframe = int(requirement.timeframe_seconds or 0)
            interval_start = evaluation_time - timedelta(seconds=timeframe)
            visible = [
                record
                for record in records
                if record_effective_time(record) == interval_start
                and record.fact.known_at <= evaluation_time
            ]
            if visible:
                selected: Any = max(
                    visible,
                    key=lambda record: (
                        record.fact.known_at,
                        int(record.market_commit_seq),
                    ),
                )
            else:
                selected = UnavailableMarketData(
                    key=requirement.key,
                    reason="exact_interval_unavailable",
                    evaluation_time=evaluation_time,
                    details={
                        "interval_start": interval_start.isoformat(),
                        "timeframe_seconds": timeframe,
                    },
                )
        else:
            assert requirement.max_staleness_seconds is not None
            selected = latest_known_record(
                records,
                evaluation_time=evaluation_time,
                max_staleness_seconds=requirement.max_staleness_seconds,
            )
        if isinstance(selected, UnavailableMarketData):
            return self._unavailable(
                requirement,
                evaluation_time=evaluation_time,
                reason=selected.reason,
                details={**dict(selected.details), "series_id": series_id},
                consumer_id=consumer_id,
            )
        if isinstance(selected, TypedFeatureRecord):
            status = getattr(selected.fact, "status", None)
            status_value = str(getattr(status, "value", status)) if status else None
            quality_valid = selected.quality.get("valid", True)
            if quality_valid is False or (
                status_value is not None and status_value != "valid"
            ):
                return self._unavailable(
                    requirement,
                    evaluation_time=evaluation_time,
                    reason=str(
                        getattr(selected.fact, "reason", None)
                        or selected.quality.get("reason")
                        or status_value
                        or "invalid_source_fact"
                    ),
                    details={
                        "series_id": series_id,
                        "material_hash": selected.fact.material_hash,
                    },
                    consumer_id=consumer_id,
                )
        return selected

    def _instrument_id(
        self, requirement: MarketDataRequirement, *, primary_instrument_id: str
    ) -> str:
        if requirement.instrument_role is InstrumentRole.PRIMARY:
            return primary_instrument_id
        if requirement.instrument_role is InstrumentRole.EXPLICIT:
            return str(requirement.instrument_ref or "").strip()
        if requirement.instrument_role is InstrumentRole.UNDERLYING:
            return str(
                self.underlying_by_primary.get(primary_instrument_id) or ""
            ).strip()
        if requirement.instrument_role is InstrumentRole.BENCHMARK:
            return str(
                self.benchmarks.get(str(requirement.instrument_ref or "")) or ""
            ).strip()
        raise AssertionError(f"unsupported instrument role {requirement.instrument_role}")

    def resolve(
        self,
        *,
        requirements_by_consumer: Mapping[str, Sequence[Mapping[str, Any]]],
        primary_instrument_id: str,
        evaluation_time: datetime,
    ) -> dict[str, dict[str, Any]]:
        decision_time = evaluation_time.astimezone(UTC)
        primary_id = str(primary_instrument_id or "").strip()
        if not primary_id:
            raise ValueError("runtime_market_data_invalid: primary instrument is required")
        resolved: dict[str, dict[str, Any]] = {}
        for raw_consumer_id, declarations in requirements_by_consumer.items():
            consumer_id = str(raw_consumer_id or "").strip()
            values: dict[str, Any] = {}
            for raw in declarations:
                requirement = self._requirement(raw)
                if requirement.fact_type == CANDLE_FACT_TYPE:
                    continue
                if self.dataset_binding is not None:
                    values[requirement.key] = self._frozen_series(
                        consumer_id=consumer_id,
                        requirement=requirement,
                        primary_instrument_id=primary_id,
                        evaluation_time=decision_time,
                    )
                    continue
                if requirement.fact_type not in {
                    OPEN_INTEREST_FACT_TYPE,
                    FUNDING_RATE_FACT_TYPE,
                }:
                    raise RuntimeError(
                        "runtime_market_data_unsupported_fact: mutable runtime reads "
                        f"are unavailable consumer_id={consumer_id} "
                        f"fact_type={requirement.fact_type}"
                    )
                instrument_id = self._instrument_id(
                    requirement, primary_instrument_id=primary_id
                )
                if not instrument_id:
                    values[requirement.key] = self._unavailable(
                        requirement,
                        evaluation_time=decision_time,
                        reason="instrument_role_unresolved",
                        details={
                            "instrument_role": requirement.instrument_role.value,
                            "instrument_ref": requirement.instrument_ref,
                        },
                        consumer_id=consumer_id,
                    )
                    continue
                latest_reader = (
                    self._latest_service.latest_open_interest
                    if requirement.fact_type == OPEN_INTEREST_FACT_TYPE
                    else self._latest_service.latest_funding_rate
                )
                values[requirement.key] = latest_reader(
                    instrument_id=instrument_id,
                    decision_time=decision_time,
                    max_staleness_seconds=int(
                        requirement.max_staleness_seconds or 0
                    ),
                    required=requirement.required,
                )
            if values:
                resolved[consumer_id] = values
        return resolved


__all__ = ["RuntimeMarketDataResolver"]
