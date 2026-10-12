"""Explicit provider-free materialization from a verified frozen candle Dataset."""
from dataclasses import asdict
from datetime import datetime, timezone
import logging

from market_data.candle_derivation import MAX_INPUT_ROWS, TRANSFORMATION, derive_frozen_candles
from market_data.contracts import CANDLE_FACT_TYPE, CANDLE_FACT_VERSION, DatasetSeriesRequest
from market_data.store import MarketDataStore
from .backtest_dataset_service import validate_frozen_dataset_series

logger = logging.getLogger(__name__)


def derive_candles(*, store: MarketDataStore, dataset_id: str, source_series_id: int,
                   start: str, end: str, target_seconds: int) -> dict:
    window = DatasetSeriesRequest(series_id=source_series_id, start=start, end=end)
    dataset = store.get_dataset(dataset_id)
    entries = [dict(row) for row in dataset.series if int(row["series_id"]) == source_series_id]
    if len(entries) != 1:
        raise ValueError(f"candle_derivation_invalid: source series not unique in Dataset {dataset_id}")
    entry = entries[0]
    if entry["fact_type"] != CANDLE_FACT_TYPE or entry["contract_version"] != CANDLE_FACT_VERSION:
        raise ValueError("candle_derivation_invalid: frozen candle source required")
    if int(entry["row_count"]) > MAX_INPUT_ROWS:
        raise ValueError("candle_derivation_invalid: frozen input exceeds bounded row budget")
    frozen_window = DatasetSeriesRequest(series_id=source_series_id,
        start=entry["range_start"], end=entry["range_end"])
    if window.start < frozen_window.start or window.end > frozen_window.end:
        raise ValueError("candle_derivation_invalid: request exceeds frozen source range")
    _, _, all_records = validate_frozen_dataset_series(store=store,
        entry={**entry, "dataset_id": dataset.dataset_id})
    records = [row for row in all_records if window.start <= row.fact.open_time < window.end]
    source, facts = derive_frozen_candles(records, dataset_id=dataset.dataset_id,
        dataset_hash=dataset.dataset_hash, source_series_id=source_series_id,
        source_seconds=int(entry["timeframe_seconds"]), target_seconds=target_seconds,
        start=window.start, end=window.end, accepted_at=datetime.now(timezone.utc))
    request = {"transformation_id": TRANSFORMATION, "dataset_id": dataset.dataset_id,
               "dataset_hash": dataset.dataset_hash, "source_series_id": source_series_id,
               "start": window.start.isoformat(), "end": window.end.isoformat(),
               "target_seconds": target_seconds}
    logger.info("candle_derivation_started | dataset_id=%s series_id=%s target_seconds=%s rows=%s",
                dataset_id, source_series_id, target_seconds, len(facts))
    source_id = store.register_source(source, lineage=request)
    series_id = store.register_series(instrument_id=str(entry["instrument_id"]),
        fact_type=CANDLE_FACT_TYPE, timeframe_seconds=target_seconds, contract_version=CANDLE_FACT_VERSION)
    outcome = store.ingest_facts(series_id=series_id, source_id=source_id, facts=facts,
        request=request, allow_corrections=False, require_same_source=True)
    logger.info("candle_derivation_completed | dataset_id=%s series_id=%s ingestion_run_id=%s inserted=%s noop=%s",
                dataset_id, series_id, outcome.ingestion_run_id, outcome.inserted_count, outcome.noop_count)
    return {"schema_version": "market_candle_derivation_result.v1", **request,
            "series_id": series_id, "source_id": source_id, "source_identity_key": source.identity_key,
            "provider_call_performed": False, "outcome": asdict(outcome)}
