"""Call-scoped candle preparation and public event selection."""
from __future__ import annotations
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from types import MappingProxyType
import pandas as pd
from .event_time import _utc
from .event_features import prepare_atr
from .event_configuration import _INDICATOR_EVENT_DETECTOR, _FACT_SNAPSHOT_DETECTOR, _REQUIRED_FACTS_AVAILABLE, _STRUCTURED_FACT_OPERATORS
from .event_facts import _snapshot_revision_order_key, _required_fact_decisions, _fact_snapshot_outputs


def select_public_events(
    *, detector, detector_type, indicator_evidence, candles, baseline_specs,
    enriched_specs, fact_records, fact_requirements, fact_gap_evidence,
    evaluation_start, evaluation_end,
):
    structured_aliases = {
        str(row["input_alias"])
        for row in enriched_specs
        if str(row.get("operator") or "") in _STRUCTURED_FACT_OPERATORS
    }
    if detector_type == _FACT_SNAPSHOT_DETECTOR:
        structured_aliases.add(str(detector["input_alias"]))
    ordered_revisions_by_alias = {
        alias: tuple(
            sorted(
                tuple(fact_records.get(alias) or ()),
                key=_snapshot_revision_order_key,
            )
        )
        for alias in sorted(structured_aliases)
    }
    direction_by_key: dict[str, str] = {}
    output_name: str | None = None
    detector_snapshots: dict[
        tuple[datetime, datetime | None], Any | None
    ] = {}
    trigger_readiness: dict[datetime, dict[str, Any]] = {}
    if detector_type == _INDICATOR_EVENT_DETECTOR:
        direction_by_key = {
            str(row["key"]): str(row["direction"])
            for row in detector["event_keys"]
        }
        output_name = str(detector["output_name"])
        event_rows = [
            dict(row)
            for row in indicator_evidence.get("outputs") or []
            if str(row.get("output_name") or "") == output_name
            and str(row.get("event_key") or "") in direction_by_key
        ]
        if any(
            str(row.get("output_type") or "") != "signal"
            for row in event_rows
        ):
            raise RuntimeError(
                "event_fact_event_ownership_invalid: configured Indicator event is not a signal output"
            )
    elif detector_type == _FACT_SNAPSHOT_DETECTOR:
        decision_times = None
        trigger_references = None
        if detector.get("evaluation_trigger") == _REQUIRED_FACTS_AVAILABLE:
            decision_times, trigger_references, trigger_readiness = _required_fact_decisions(
                detector=detector, specs=enriched_specs, baseline_specs=baseline_specs, candles=candles,
                records_by_alias=fact_records, requirements=fact_requirements,
                gap_evidence=fact_gap_evidence,
                evaluation_start=evaluation_start, evaluation_end=evaluation_end,
            )
        event_rows, detector_snapshots = _fact_snapshot_outputs(
            detector=detector,
            candles=candles,
            records_by_alias=fact_records,
            requirements=fact_requirements,
            gap_evidence=fact_gap_evidence,
            evaluation_start=evaluation_start,
            evaluation_end=evaluation_end,
            ordered_revisions_by_alias=ordered_revisions_by_alias,
            decision_times=decision_times,
            trigger_references=trigger_references,
        )
    else:
        raise ValueError(
            f"event_fact_check_invalid: unsupported detector type {detector_type or '<empty>'}"
        )

    return (ordered_revisions_by_alias, direction_by_key, output_name,
            event_rows, detector_snapshots, trigger_readiness)

def _candle_rows(value: Any, *, field: str) -> list[dict[str, Any]]:
    """Normalize supported execution candle containers into evaluator rows."""

    if value is None:
        return []
    if isinstance(value, pd.DataFrame):
        raw_rows: Sequence[Any] = value.to_dict(orient="records")
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        raw_rows = value
    else:
        raise ValueError(
            f"event_fact_check_invalid: {field} must be a pandas DataFrame "
            "or a sequence of objects; "
            f"received_type={type(value).__name__}"
        )

    normalized: list[dict[str, Any]] = []
    for index, raw_row in enumerate(raw_rows):
        if not isinstance(raw_row, Mapping):
            raise ValueError(
                f"event_fact_check_invalid: {field}[{index}] must be an object; "
                f"received_type={type(raw_row).__name__}"
            )
        row = dict(raw_row)
        if (
            "open_time" not in row
            and "time" not in row
            and "timestamp" in row
        ):
            row["open_time"] = row["timestamp"]
        normalized.append(row)
    return normalized


class EventCandlePreparation:
    """Reuse readonly candle/ATR preparation within one owned procedure call.

    Public evaluation always creates a fresh instance. Only procedure passes
    consuming the same unchanged candle object/field share it. Detector decisions,
    causal Facts, event membership and outcomes are never retained here.
    """

    def __init__(self):
        self._source = None
        self._field = None
        self._candles = None
        self._by_open = None
        self._atr = {}

    def rows(self, source, *, field):
        if self._candles is None or source is not self._source or field != self._field:
            rows = _candle_rows(source, field=field)
            self._candles = tuple(MappingProxyType(row) for row in rows)
            self._by_open = None
            self._source, self._field = source, field
            self._atr = {}
        return self._candles

    def candles(self, source, *, field):
        rows = self.rows(source, field=field)
        if self._by_open is None:
            self._by_open = MappingProxyType({
                _utc(row.get("open_time") or row.get("time"), field="candle.open_time"): row
                for row in rows
            })
        return rows, self._by_open

    def atr(self, candles, baseline_specs):
        periods = (
            tuple(sorted({int(row["period"]) for row in baseline_specs
                          if row["operator"] == "atr_fraction"}))
            if any(str(row.get("operator")) == "atr_fraction" for row in baseline_specs)
            else ()
        )
        if periods not in self._atr:
            values = prepare_atr(candles, baseline_specs)
            self._atr[periods] = MappingProxyType({key: MappingProxyType(row) for key, row in values.items()})
        return self._atr[periods]
