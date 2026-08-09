"""Explicit acquisition, canonical reads, replay, and paper candle intake."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence

import pandas as pd

from core.settings import get_settings
from data_providers.providers.factory import get_provider
from data_providers.utils.ohlcv import interval_to_timedelta, split_history_range
from engines.bot_runtime.live_market import ClosedLiveCandle
from market_data.contracts import (
    CANDLE_FACT_TYPE,
    CANDLE_FACT_VERSION,
    CandleFact,
    CandleRecord,
    SourceIdentity,
)
from market_data.store import IngestionOutcome, MarketDataStore

from ..storage.repos.market_data import market_data_repo


@dataclass(frozen=True)
class HistoricalIngestionResult:
    source_id: int
    series_id: int
    outcome: IngestionOutcome
    gap_evidence_count: int


def _as_utc(value: Any, *, field: str) -> datetime:
    try:
        parsed = pd.Timestamp(value)
    except Exception as exc:  # noqa: BLE001 - fail with contract context.
        raise ValueError(f"market_data_time_invalid: {field}={value!r}") from exc
    if pd.isna(parsed):
        raise ValueError(f"market_data_time_invalid: {field} is missing")
    if parsed.tzinfo is None:
        parsed = parsed.tz_localize("UTC")
    else:
        parsed = parsed.tz_convert("UTC")
    return parsed.to_pydatetime()


def _optional_utc(value: Any, *, field: str) -> Optional[datetime]:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return _as_utc(value, field=field)


def _optional_number(value: Any, *, field: str) -> Optional[float]:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"market_data_value_invalid: {field}={value!r}") from exc


def _optional_int(value: Any, *, field: str) -> Optional[int]:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        raise ValueError(f"market_data_value_invalid: {field}={value!r}")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"market_data_value_invalid: {field}={value!r}") from exc
    if float(value) != float(result):
        raise ValueError(f"market_data_value_invalid: {field} must be integral")
    return result


def _timeframe_seconds(interval: str) -> int:
    seconds = int(interval_to_timedelta(str(interval or "").strip()).total_seconds())
    if seconds <= 0:
        raise ValueError("market_data_interval_invalid: interval must be positive")
    return seconds


def _normalized_provider_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or frame.empty:
        raise RuntimeError("market_data_acquisition_empty: provider returned no candles")
    normalized = frame.copy()
    if "timestamp" not in normalized.columns:
        if normalized.index.name == "timestamp":
            normalized = normalized.reset_index()
        else:
            raise ValueError("market_data_acquisition_invalid: timestamp column is required")
    required = {"timestamp", "open", "high", "low", "close"}
    missing = sorted(required - set(normalized.columns))
    if missing:
        raise ValueError(
            "market_data_acquisition_invalid: missing columns " + ", ".join(missing)
        )
    normalized["timestamp"] = pd.to_datetime(
        normalized["timestamp"], utc=True, errors="raise"
    )
    normalized.sort_values("timestamp", inplace=True)
    duplicates = normalized["timestamp"].duplicated(keep=False)
    if bool(duplicates.any()):
        duplicate = normalized.loc[duplicates, "timestamp"].iloc[0]
        raise ValueError(
            "market_data_acquisition_invalid: duplicate provider candle "
            f"timestamp={duplicate.isoformat()}"
        )
    return normalized.reset_index(drop=True)


class HistoricalCandleIngestor:
    """Fetch provider data only when explicitly invoked, then persist canonically."""

    def __init__(
        self,
        store: MarketDataStore = market_data_repo,
        *,
        max_segment_points: Optional[int] = None,
    ) -> None:
        self.store = store
        configured = get_settings().providers.runtime.history_segment_points
        self.max_segment_points = int(max_segment_points or configured)
        if self.max_segment_points <= 0:
            raise ValueError(
                "market_data_acquisition_invalid: max_segment_points must be positive"
            )

    def ingest_by_instrument(
        self,
        instrument: Mapping[str, Any],
        *,
        start: Any,
        end: Any,
        interval: str,
        source_revision: Optional[str] = None,
    ) -> HistoricalIngestionResult:
        instrument_id = str(instrument.get("id") or instrument.get("instrument_id") or "").strip()
        provider_name = str(instrument.get("datasource") or "").strip()
        venue = str(instrument.get("exchange") or "").strip()
        symbol = str(instrument.get("symbol") or "").strip()
        if not instrument_id or not provider_name or not symbol:
            raise ValueError(
                "market_data_acquisition_invalid: instrument id, datasource, and symbol are required"
            )
        start_at = _as_utc(start, field="start")
        end_at = _as_utc(end, field="end")
        if end_at <= start_at:
            raise ValueError("market_data_acquisition_invalid: end must be after start")
        timeframe_seconds = _timeframe_seconds(interval)
        provider = get_provider(provider_name, exchange=venue or None)
        adapter_version = (
            f"{type(provider).__module__}.{type(provider).__name__}:candle.ohlcv.v1"
        )
        source_identity = SourceIdentity(
            provider=provider_name,
            venue=venue,
            source_kind="historical_api",
            adapter_version=adapter_version,
        )
        source_id = self.store.register_source(
            source_identity,
            lineage={
                "schema_version": "market_source_lineage.v1",
                "acquisition": "explicit_historical_request",
                "provider_class": f"{type(provider).__module__}.{type(provider).__name__}",
            },
        )
        series_id = self.store.register_series(
            instrument_id=instrument_id,
            fact_type=CANDLE_FACT_TYPE,
            timeframe_seconds=timeframe_seconds,
            contract_version=CANDLE_FACT_VERSION,
        )
        segments = split_history_range(
            pd.Timestamp(start_at),
            pd.Timestamp(end_at),
            interval,
            max_points=self.max_segment_points,
        )
        frames: list[pd.DataFrame] = []
        for segment_start, segment_end in segments:
            try:
                frame = provider.fetch_from_api(
                    symbol,
                    segment_start.to_pydatetime(),
                    segment_end.to_pydatetime(),
                    interval,
                )
            except Exception as exc:
                self.store.record_gap_evidence(
                    series_id=series_id,
                    source_id=source_id,
                    start=segment_start.to_pydatetime(),
                    end=segment_end.to_pydatetime(),
                    classification="ingestion_failure",
                    expected_count=max(
                        int(
                            (segment_end - segment_start).total_seconds()
                            // timeframe_seconds
                        ),
                        1,
                    ),
                    observed_count=0,
                    evidence={
                        "schema_version": "market_gap_evidence.v1",
                        "reason_code": "provider_fetch_exception",
                        "source_id": source_id,
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc)[:1000],
                    },
                )
                raise RuntimeError(
                    "market_data_acquisition_failed: provider request failed; "
                    "ingestion failure evidence was recorded"
                ) from exc
            if frame is None or frame.empty:
                self.store.record_gap_evidence(
                    series_id=series_id,
                    source_id=source_id,
                    start=segment_start.to_pydatetime(),
                    end=segment_end.to_pydatetime(),
                    classification="provider_missing_data",
                    expected_count=max(
                        int(
                            (segment_end - segment_start).total_seconds()
                            // timeframe_seconds
                        ),
                        1,
                    ),
                    observed_count=0,
                    evidence={
                        "schema_version": "market_gap_evidence.v1",
                        "reason_code": "provider_response_empty",
                        "source_id": source_id,
                    },
                )
                continue
            frames.append(frame.copy())
        if not frames:
            raise RuntimeError(
                "market_data_acquisition_empty: provider returned no candles; "
                "missing-data evidence was recorded"
            )
        normalized = _normalized_provider_frame(pd.concat(frames, ignore_index=True))
        accepted_at = datetime.now(timezone.utc)
        facts: list[CandleFact] = []
        for row in normalized.to_dict(orient="records"):
            open_time = _as_utc(row["timestamp"], field="timestamp")
            close_time = _optional_utc(row.get("close_time"), field="close_time")
            if close_time is None:
                close_time = open_time + timedelta(seconds=timeframe_seconds)
            if open_time < start_at or open_time >= end_at:
                raise ValueError(
                    "market_data_acquisition_invalid: provider returned out-of-window candle "
                    f"open_time={open_time.isoformat()}"
                )
            if close_time > accepted_at:
                raise ValueError(
                    "market_data_acquisition_invalid: provider returned provisional candle "
                    f"open_time={open_time.isoformat()} close_time={close_time.isoformat()}"
                )
            source_published_at = _optional_utc(
                row.get("source_time"), field="source_time"
            )
            known_at = source_published_at or close_time
            known_at_method = (
                "provider_publication"
                if source_published_at is not None
                else "interval_close_inferred"
            )
            facts.append(
                CandleFact(
                    open_time=open_time,
                    close_time=close_time,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=_optional_number(row.get("volume"), field="volume"),
                    trade_count=_optional_int(
                        row.get("trade_count"), field="trade_count"
                    ),
                    source_published_at=source_published_at,
                    received_at=None,
                    accepted_at=accepted_at,
                    known_at=known_at,
                    known_at_method=known_at_method,
                )
            )
        outcome = self.store.ingest_candles(
            series_id=series_id,
            source_id=source_id,
            facts=facts,
            request={
                "schema_version": "market_ingestion_request.v1",
                "operation": "historical_candle_ingest",
                "instrument_id": instrument_id,
                "symbol": symbol,
                "provider": provider_name,
                "venue": venue,
                "interval": interval,
                "start": start_at.isoformat(),
                "end": end_at.isoformat(),
            },
            source_revision=source_revision or adapter_version,
        )
        gaps = self._record_gaps(
            series_id=series_id,
            source_id=source_id,
            outcome=outcome,
            facts=facts,
            start=start_at,
            end=end_at,
            timeframe_seconds=timeframe_seconds,
        )
        return HistoricalIngestionResult(
            source_id=source_id,
            series_id=series_id,
            outcome=outcome,
            gap_evidence_count=gaps,
        )

    def _record_gaps(
        self,
        *,
        series_id: int,
        source_id: int,
        outcome: IngestionOutcome,
        facts: list[CandleFact],
        start: datetime,
        end: datetime,
        timeframe_seconds: int,
    ) -> int:
        ranges: list[tuple[datetime, datetime]] = []
        if facts[0].open_time > start:
            ranges.append((start, facts[0].open_time))
        for previous, current in zip(facts, facts[1:]):
            if current.open_time > previous.close_time:
                ranges.append((previous.close_time, current.open_time))
        if facts[-1].close_time < end:
            ranges.append((facts[-1].close_time, end))
        for gap_start, gap_end in ranges:
            expected = max(
                int((gap_end - gap_start).total_seconds() // timeframe_seconds), 1
            )
            self.store.record_gap_evidence(
                series_id=series_id,
                source_id=source_id,
                start=gap_start,
                end=gap_end,
                classification="provider_missing_data",
                expected_count=expected,
                observed_count=0,
                evidence={
                    "schema_version": "market_gap_evidence.v1",
                    "reason_code": "provider_coverage_gap",
                    "source_id": source_id,
                },
                ingestion_run_id=outcome.ingestion_run_id,
                detected_as_of_commit_seq=outcome.max_commit_seq,
            )
        return len(ranges)


class CanonicalCandleFeed:
    """Read-only feed over stored source facts; it never calls a provider."""

    def __init__(self, store: MarketDataStore = market_data_repo) -> None:
        self.store = store

    def read_by_instrument(
        self,
        instrument: Mapping[str, Any],
        *,
        start: Any,
        end: Any,
        interval: str,
        as_of_commit_seq: Optional[int] = None,
        known_at_lte: Optional[Any] = None,
    ) -> pd.DataFrame:
        instrument_id = str(instrument.get("id") or instrument.get("instrument_id") or "").strip()
        if not instrument_id:
            raise ValueError("market_data_read_invalid: instrument id is required")
        timeframe_seconds = _timeframe_seconds(interval)
        series_id = self.store.resolve_series_id(
            instrument_id=instrument_id,
            fact_type=CANDLE_FACT_TYPE,
            timeframe_seconds=timeframe_seconds,
            contract_version=CANDLE_FACT_VERSION,
        )
        start_at = _as_utc(start, field="start")
        end_at = _as_utc(end, field="end")
        records = self.store.read_candles(
            series_id=series_id,
            start=start_at,
            end=end_at,
            as_of_commit_seq=as_of_commit_seq,
            known_at_lte=(
                _as_utc(known_at_lte, field="known_at_lte")
                if known_at_lte is not None
                else None
            ),
        )
        quality = self.store.list_gap_evidence(
            series_id=series_id,
            start=start_at,
            end=end_at,
            as_of_commit_seq=as_of_commit_seq,
        )
        return self._records_to_frame(
            records,
            instrument=instrument,
            interval=interval,
            series_id=series_id,
            quality=quality,
        )

    def read_dataset_series(
        self,
        *,
        dataset_id: str,
        series_id: int,
        instrument: Mapping[str, Any],
        interval: str,
        known_at_lte: Optional[Any] = None,
        start: Optional[Any] = None,
        end: Optional[Any] = None,
        quality: Sequence[Mapping[str, Any]] = (),
        source_identity_keys: Sequence[str] = (),
    ) -> pd.DataFrame:
        records = self.store.read_dataset_series(
            dataset_id=dataset_id,
            series_id=series_id,
            start=(
                _as_utc(start, field="start") if start is not None else None
            ),
            end=(
                _as_utc(end, field="end") if end is not None else None
            ),
            known_at_lte=(
                _as_utc(known_at_lte, field="known_at_lte")
                if known_at_lte is not None
                else None
            ),
            source_identity_keys=tuple(
                sorted(
                    {
                        str(value).strip()
                        for value in source_identity_keys
                        if str(value).strip()
                    }
                )
            ),
            causal_at_interval_close=True,
        )
        return self._records_to_frame(
            records,
            instrument=instrument,
            interval=interval,
            series_id=series_id,
            quality=[dict(row) for row in quality],
            dataset_id=dataset_id,
        )

    @staticmethod
    def _records_to_frame(
        records: list[CandleRecord],
        *,
        instrument: Mapping[str, Any],
        interval: str,
        series_id: int,
        quality: list[Mapping[str, Any]],
        dataset_id: Optional[str] = None,
    ) -> pd.DataFrame:
        columns = [
            "timestamp", "open", "high", "low", "close", "volume",
            "trade_count", "close_time", "known_at", "known_at_method",
            "revision", "market_commit_seq", "source_identity_key", "provenance",
            "source_provider", "source_venue", "source_kind", "source_adapter_version",
        ]
        rows = [
            {
                "timestamp": record.fact.open_time,
                "open": record.fact.open,
                "high": record.fact.high,
                "low": record.fact.low,
                "close": record.fact.close,
                "volume": record.fact.volume,
                "trade_count": record.fact.trade_count,
                "close_time": record.fact.close_time,
                "known_at": record.fact.known_at,
                "known_at_method": record.fact.known_at_method,
                "revision": record.revision,
                "market_commit_seq": record.market_commit_seq,
                "source_identity_key": record.source_identity_key,
                "provenance": dict(record.provenance),
                "source_provider": record.source.provider,
                "source_venue": record.source.venue,
                "source_kind": record.source.source_kind,
                "source_adapter_version": record.source.adapter_version,
            }
            for record in records
        ]
        frame = pd.DataFrame(rows, columns=columns)
        if not frame.empty:
            frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
            frame.set_index("timestamp", inplace=True, drop=False)
        frame["instrument_id"] = str(
            instrument.get("id") or instrument.get("instrument_id") or ""
        )
        frame["symbol"] = str(instrument.get("symbol") or "")
        frame["datasource"] = instrument.get("datasource")
        frame["exchange"] = instrument.get("exchange")
        frame["interval"] = interval
        frame.attrs["market_data_contract"] = CANDLE_FACT_VERSION
        frame.attrs["market_data_series_id"] = series_id
        frame.attrs["market_data_dataset_id"] = dataset_id
        frame.attrs["gap_classification"] = [dict(item) for item in quality]
        frame.attrs["gap_classification_source"] = "canonical_market_gap_evidence"
        frame.attrs["provider_call_performed"] = False
        source_counts: dict[str, int] = {}
        for record in records:
            source_counts[record.source_identity_key] = (
                source_counts.get(record.source_identity_key, 0) + 1
            )
        frame.attrs["market_data_provenance"] = {
            "schema_version": "market_data_provenance_summary.v1",
            "source_counts": dict(sorted(source_counts.items())),
            "row_count": len(records),
        }
        if records:
            frame.attrs["market_data_max_commit_seq"] = max(
                record.market_commit_seq for record in records
            )
        return frame


class PaperCandlePersistenceSink:
    """Persist a closed live candle before making it visible to paper runtime."""

    def __init__(self, store: MarketDataStore = market_data_repo) -> None:
        self.store = store
        self._source_ids: dict[tuple[str, str], int] = {}
        self._series_ids: dict[tuple[str, int], int] = {}

    def persist(
        self,
        candle: ClosedLiveCandle,
        *,
        instrument_id: str,
        bot_id: str,
        run_id: str,
    ) -> CandleRecord:
        instrument_id = str(instrument_id or "").strip()
        if not instrument_id:
            raise ValueError("paper_candle_persistence_invalid: instrument_id is required")
        timeframe_seconds = _timeframe_seconds(candle.timeframe)
        source_key = (str(candle.provider), str(candle.venue))
        source_id = self._source_ids.get(source_key)
        if source_id is None:
            source_id = self.store.register_source(
                SourceIdentity(
                    provider=candle.provider,
                    venue=candle.venue,
                    source_kind="live_stream",
                    adapter_version="paper_stream.candle_aggregator.v1",
                ),
                lineage={
                    "schema_version": "market_source_lineage.v1",
                    "acquisition": "paper_public_stream",
                },
            )
            self._source_ids[source_key] = source_id
        logical_key = (instrument_id, timeframe_seconds)
        series_id = self._series_ids.get(logical_key)
        if series_id is None:
            series_id = self.store.register_series(
                instrument_id=instrument_id,
                fact_type=CANDLE_FACT_TYPE,
                timeframe_seconds=timeframe_seconds,
                contract_version=CANDLE_FACT_VERSION,
            )
            self._series_ids[logical_key] = series_id

        received_at = _optional_utc(
            candle.last_known_at or candle.first_known_at,
            field="received_at",
        )
        accepted_at = datetime.now(timezone.utc)
        if received_at is not None and received_at > accepted_at:
            accepted_at = received_at
        fact = CandleFact(
            open_time=candle.time,
            close_time=candle.end,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
            trade_count=None,
            source_published_at=None,
            received_at=received_at,
            accepted_at=accepted_at,
            known_at=accepted_at,
            known_at_method="platform_acceptance",
        )
        self.store.ingest_candles(
            series_id=series_id,
            source_id=source_id,
            facts=[fact],
            request={
                "schema_version": "market_ingestion_request.v1",
                "operation": "paper_closed_candle_accept",
                "bot_id": str(bot_id),
                "run_id": str(run_id),
                "symbol": candle.symbol,
                "timeframe": candle.timeframe,
                "source_event_count": candle.source_event_count,
            },
            source_revision="paper_stream.candle_aggregator.v1",
            allow_corrections=False,
        )
        persisted = self.store.read_candles(
            series_id=series_id,
            start=candle.time,
            end=candle.end,
        )
        if len(persisted) != 1 or persisted[0].fact.row_hash != fact.row_hash:
            raise RuntimeError(
                "paper_candle_persistence_failed: persisted fact does not match accepted candle"
            )
        return persisted[0]


canonical_candle_feed = CanonicalCandleFeed()
historical_candle_ingestor = HistoricalCandleIngestor()
paper_candle_persistence_sink = PaperCandlePersistenceSink()


__all__ = [
    "CanonicalCandleFeed",
    "HistoricalCandleIngestor",
    "HistoricalIngestionResult",
    "PaperCandlePersistenceSink",
    "canonical_candle_feed",
    "historical_candle_ingestor",
    "paper_candle_persistence_sink",
]
