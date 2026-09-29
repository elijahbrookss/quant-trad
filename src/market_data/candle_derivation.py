"""Canonical, snapshot-bound coarsening of accepted candle Facts."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from math import fsum
from typing import Sequence

from .canonical import CanonicalFact
from .contracts import CANDLE_FACT_TYPE, CandleRecord, SourceIdentity

TRANSFORMATION = "frozen_candle_coarsening.v1"
MAX_INPUT_ROWS = 100_000
MAX_OUTPUT_ROWS = 10_000


def derive_frozen_candles(
    records: Sequence[CandleRecord], *, dataset_id: str, dataset_hash: str,
    source_series_id: int, source_seconds: int, target_seconds: int,
    start: datetime, end: datetime, accepted_at: datetime,
) -> tuple[SourceIdentity, list[CanonicalFact]]:
    """Require exact contiguous buckets; preserve later-known source revisions.

    This consumes the selected rows of one verified frozen snapshot, not a
    reconstruction of its revision history. The caller owns manifest verification.
    """
    if any(value.tzinfo is None for value in (start, end, accepted_at)):
        raise ValueError("candle_derivation_invalid: timezone-aware clocks required")
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    if (source_seconds <= 0 or target_seconds <= source_seconds
            or target_seconds % source_seconds or target_seconds > 86400):
        raise ValueError("candle_derivation_invalid: target must be a larger integral cadence up to one day")
    if end <= start or any(value.timestamp() % target_seconds for value in (start, end)):
        raise ValueError("candle_derivation_invalid: window must align to UTC target buckets")
    expected_count = int((end - start).total_seconds()) // source_seconds
    if expected_count > MAX_INPUT_ROWS or expected_count // (target_seconds // source_seconds) > MAX_OUTPUT_ROWS:
        raise ValueError("candle_derivation_invalid: bounded row budget exceeded")
    if len(records) != expected_count or not records:
        raise ValueError("candle_derivation_incomplete: exact source slots required")
    source = records[0].source
    for index, record in enumerate(records):
        opened = start + timedelta(seconds=index * source_seconds)
        if (record.series_id != source_series_id
                or record.source_identity_key != source.identity_key
                or record.source.identity_key != source.identity_key):
            raise ValueError("candle_derivation_invalid: mixed source or series")
        if record.fact.open_time != opened or record.fact.close_time != opened + timedelta(seconds=source_seconds):
            raise ValueError(f"candle_derivation_incomplete: missing/duplicate/off-grid slot at {opened.isoformat()}")
    lineage = {"transformation_id": TRANSFORMATION, "dataset_id": dataset_id,
               "dataset_hash": dataset_hash, "source_series_id": source_series_id,
               "source_identity_key": source.identity_key, "source_seconds": source_seconds}
    identity_hash = sha256(json.dumps(lineage, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    output_source = SourceIdentity(provider=source.provider, venue=source.venue,
        source_kind="canonical_candle_derivation", adapter_version=f"{TRANSFORMATION}.{identity_hash}")
    facts = []
    width = target_seconds // source_seconds
    for offset in range(0, len(records), width):
        inputs = records[offset:offset + width]
        opened, closed = inputs[0].fact.open_time, inputs[-1].fact.close_time
        known_at = max(closed, *(row.fact.known_at for row in inputs))
        payload = {"close_time": closed, "open": inputs[0].fact.open,
                   "high": max(row.fact.high for row in inputs),
                   "low": min(row.fact.low for row in inputs), "close": inputs[-1].fact.close,
                   "volume": (fsum(row.fact.volume for row in inputs)
                              if all(row.fact.volume is not None for row in inputs) else None),
                   "trade_count": (sum(row.fact.trade_count for row in inputs)
                                   if all(row.fact.trade_count is not None for row in inputs) else None)}
        provenance = {**lineage, "target_seconds": target_seconds, "inputs": [
            {"open_time": row.fact.open_time.isoformat(), "revision": row.revision,
             "market_commit_seq": row.market_commit_seq, "row_hash": row.fact.row_hash,
             "canonical_material_hash": row.canonical_material_hash,
             "ingestion_run_id": row.ingestion_run_id} for row in inputs]}
        facts.append(CanonicalFact(fact_type=CANDLE_FACT_TYPE, payload_schema_id="candle.ohlcv.v1",
            observation_key=opened.isoformat(), observation_time=opened,
            observation_time_method="interval_open", accepted_at=accepted_at,
            known_at=known_at, known_at_method="max_pinned_input_known_at_and_interval_close",
            source=output_source, transformation_id=TRANSFORMATION, payload=payload, provenance=provenance))
    return output_source, facts
