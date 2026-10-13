"""Causal Fact selection and feature calculations from owned frozen inputs."""

from __future__ import annotations

from core.execution_control import execution_checkpoint

import heapq
import math
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any


from market_data.frozen import semantic_hash
from market_data.requirements import causal_numeric_fact_records


from .event_time import _utc, _iso, _known_at
from .event_features import _path
from .event_configuration import (
    _INDICATOR_EVENT_DETECTOR,
    _FACT_SNAPSHOT_DETECTOR,
    _REQUIRED_FACTS_AVAILABLE,
    _STRUCTURED_FACT_OPERATORS,
    _FACT_OPERATORS,
    _mapping,
    _finite,
)

def _record_known_at(record: Any) -> datetime:
    return _utc(getattr(record.fact, "known_at", None), field="fact.known_at")


def _record_effective_at(record: Any) -> datetime:
    fact = record.fact
    value = getattr(fact, "observation_time", None) or getattr(
        fact, "effective_at", None
    )
    return _utc(value, field="fact.observation_time")


def _record_freshness_time(record: Any) -> datetime:
    payload = getattr(record.fact, "payload", None)
    if isinstance(payload, Mapping) and payload.get("bucket_end") is not None:
        return _utc(payload["bucket_end"], field="fact.payload.bucket_end")
    return _record_effective_at(record)


def _record_event_key(record: Any) -> str:
    fact = record.fact
    return str(
        getattr(fact, "observation_key", None)
        or getattr(fact, "source_event_key", None)
        or ""
    )


def _record_payload_document(record: Any) -> dict[str, Any]:
    payload = getattr(record.fact, "payload", None)
    if not isinstance(payload, Mapping):
        raise RuntimeError(
            "event_fact_structured_input_invalid: canonical payload is unavailable "
            f"series_id={getattr(record, 'series_id', '<missing>')}"
        )
    return {"payload": dict(payload)}


def _record_matches_where(record: Any, where: Mapping[str, Any]) -> bool:
    if not where:
        return True
    document = _record_payload_document(record)
    return all(_path(document, path) == expected for path, expected in where.items())


def _record_is_active(record: Any) -> bool:
    state = getattr(record.fact, "state", None)
    if state is None:
        return True
    return str(getattr(state, "value", state)).strip().lower() == "active"


def _causal_fact_records(
    records: Sequence[Any], *, evaluation_time: datetime
) -> tuple[Any, ...]:
    if not records:
        return ()
    if all(hasattr(record.fact, "source_event_key") for record in records):
        return tuple(
            causal_numeric_fact_records(records, evaluation_time=evaluation_time)
        )
    latest_by_observation: dict[tuple[int, str], Any] = {}
    for record in records:
        if _record_known_at(record) > evaluation_time:
            continue
        key = (int(record.series_id), _record_event_key(record))
        current = latest_by_observation.get(key)
        if current is None or (
            int(record.revision), int(record.market_commit_seq)
        ) > (
            int(current.revision), int(current.market_commit_seq)
        ):
            latest_by_observation[key] = record
    return tuple(
        sorted(
            (
                record
                for record in latest_by_observation.values()
                if _record_is_active(record)
            ),
            key=lambda record: (
                _record_effective_at(record),
                _record_event_key(record),
                _alignment_key(record),
            ),
        )
    )


def _record_revision_key(record: Any) -> tuple[int, int]:
    return int(record.revision), int(record.market_commit_seq)


def _snapshot_revision_order_key(record: Any) -> tuple[Any, ...]:
    return (
        _record_known_at(record),
        int(record.market_commit_seq),
        int(record.revision),
        _record_event_key(record),
        int(record.series_id),
    )


def _causal_snapshot_series(
    records: Sequence[Any],
    *,
    snapshot_requests: Sequence[tuple[datetime, datetime | None]],
    where: Mapping[str, Any],
    alias: str,
    ordered_revisions: Sequence[Any] | None = None,
    numeric_selection: bool = False,
    defer_ambiguity: bool = False,
) -> dict[tuple[datetime, datetime | None], Any | None]:
    """Select one latest market-time observation in one known-at sweep."""

    revisions = tuple(ordered_revisions) if ordered_revisions is not None else sorted(
        records,
        key=_snapshot_revision_order_key,
    )
    def selection_time(record: Any) -> datetime:
        return _record_known_at(record) if numeric_selection else _record_freshness_time(record)

    active: dict[tuple[int, str], Any] = {}
    eligible_by_time: dict[int, dict[tuple[int, str], Any]] = defaultdict(dict)
    time_heap: list[int] = []
    times_in_heap: set[int] = set()
    cursor = 0
    selected: dict[tuple[datetime, datetime | None], Any | None] = {}
    requests = sorted(
        set(snapshot_requests),
        key=lambda row: (row[0], row[1] is None, row[1] or row[0]),
    )
    for decision_time, expected_sample in requests:
        execution_checkpoint()
        request_key = (decision_time, expected_sample)
        while cursor < len(revisions) and _record_known_at(
            revisions[cursor]
        ) <= decision_time:
            record = revisions[cursor]
            cursor += 1
            key = (int(record.series_id), _record_event_key(record))
            current = active.get(key)
            if current is not None and _record_revision_key(record) <= _record_revision_key(
                current
            ):
                continue
            if current is not None and _record_is_active(
                current
            ) and _record_matches_where(current, where):
                current_time = int(
                    selection_time(current).timestamp() * 1_000_000
                )
                eligible_by_time[current_time].pop(key, None)
            active[key] = record
            if _record_is_active(record) and _record_matches_where(record, where):
                effective_time = int(
                    selection_time(record).timestamp() * 1_000_000
                )
                eligible_by_time[effective_time][key] = record
                if effective_time not in times_in_heap:
                    heapq.heappush(time_heap, -effective_time)
                    times_in_heap.add(effective_time)
        if expected_sample is not None:
            latest_time = int(expected_sample.timestamp() * 1_000_000)
            candidates = tuple(eligible_by_time[latest_time].values())
        else:
            while time_heap and not eligible_by_time[-time_heap[0]]:
                exhausted = -heapq.heappop(time_heap)
                times_in_heap.discard(exhausted)
            if not time_heap:
                selected[request_key] = None
                continue
            latest_time = -time_heap[0]
            candidates = tuple(eligible_by_time[latest_time].values())
        if not candidates:
            selected[request_key] = None
            continue
        if numeric_selection:
            selected[request_key] = max(candidates, key=_latest_record_key)
            continue
        if len(candidates) != 1:
            keys = sorted(_record_event_key(record) for record in candidates)
            error = RuntimeError(
                "event_fact_snapshot_ambiguous: "
                f"alias={alias} decision_time={_iso(decision_time)} "
                f"observation_time={_iso(datetime.fromtimestamp(latest_time / 1_000_000, tz=UTC))} "
                f"candidate_count={len(candidates)} observation_keys={keys[:10]}"
            )
            if defer_ambiguity:
                selected[request_key] = error
                continue
            raise error
        selected[request_key] = candidates[0]
    return selected


def _gap_bounds(row: Mapping[str, Any]) -> tuple[datetime, datetime] | None:
    raw_start = row.get("start") or row.get("range_start") or row.get("gap_start")
    raw_end = row.get("end") or row.get("range_end") or row.get("gap_end")
    if raw_start in (None, "") or raw_end in (None, ""):
        return None
    start = _utc(raw_start, field="gap.start")
    end = _utc(raw_end, field="gap.end")
    return (start, end) if end > start else None


def _fact_gap_intersects(
    gaps: Sequence[Mapping[str, Any]],
    *,
    alias: str,
    start: datetime,
    end: datetime,
) -> bool:
    for row in gaps:
        gap_alias = str(row.get("alias") or "").strip()
        if gap_alias and gap_alias != alias:
            continue
        bounds = _gap_bounds(row)
        if bounds is not None and bounds[0] < end and bounds[1] > start:
            return True
    return False


def _fact_material(record: Any) -> dict[str, Any]:
    fact = record.fact
    if hasattr(fact, "observation_key"):
        return {
            "series_id": int(record.series_id),
            "revision": int(record.revision),
            "market_commit_seq": int(record.market_commit_seq),
            "source_identity_key": str(record.source_identity_key),
            "fact_version_id": str(record.fact_version_id),
            "fact_type": str(fact.fact_type),
            "payload_schema_id": str(fact.payload_schema_id),
            "payload_contract_hash": str(fact.payload_contract_hash),
            "observation_key": str(fact.observation_key),
            "observation_time": _iso(fact.observation_time),
            "known_at": _iso(fact.known_at),
            "row_hash": str(record.row_hash),
        }
    return {
        "series_id": int(record.series_id),
        "revision": int(record.revision),
        "market_commit_seq": int(record.market_commit_seq),
        "source_identity_key": str(record.source_identity_key),
        "effective_at": _iso(fact.effective_at),
        "known_at": _iso(fact.known_at),
        "source_event_key": str(fact.source_event_key),
        "value": str(fact.value),
        "unit": str(fact.unit),
        "row_hash": str(fact.row_hash),
    }


def _alignment_key(record: Any) -> tuple[Any, ...]:
    """Match the frozen binding's deterministic latest-known selection rule."""

    return (
        _record_known_at(record),
        int(record.market_commit_seq),
        str(record.source_identity_key),
        _record_effective_at(record),
        _record_event_key(record),
        int(record.revision),
        int(record.series_id),
    )


def _latest_record_key(record: Any) -> tuple[Any, ...]:
    if hasattr(record.fact, "observation_key"):
        return (
            _record_effective_at(record),
            _record_known_at(record),
            int(record.market_commit_seq),
            _record_event_key(record),
            int(record.revision),
            int(record.series_id),
        )
    return _alignment_key(record)


def _selected_record_key(alias: str, record: Any) -> tuple[Any, ...]:
    return (
        str(alias),
        int(record.series_id),
        str(record.source_identity_key),
        _record_event_key(record),
        int(record.revision),
        int(record.market_commit_seq),
    )


def _structured_snapshot_key(
    alias: str, where: Mapping[str, Any]
) -> tuple[str, str]:
    return str(alias), semantic_hash({"where": dict(where)})


def _fact_features_for_event(
    *,
    specs: Sequence[Mapping[str, Any]],
    requirements: Mapping[str, Mapping[str, Any]],
    records_by_alias: Mapping[str, Sequence[Any]],
    structured_snapshots: Mapping[
        tuple[str, str],
        Mapping[tuple[datetime, datetime | None], Any | None],
    ],
    gap_evidence: Sequence[Mapping[str, Any]],
    decision_time: datetime,
    sample_time: datetime,
    venue_close: float,
    direction: float,
) -> tuple[
    dict[str, float | None],
    dict[str, list[dict[str, Any]]],
    list[str],
    dict[tuple[Any, ...], dict[str, Any]],
]:
    features: dict[str, float | None] = {}
    references: dict[str, list[dict[str, Any]]] = defaultdict(list)
    exclusions: list[str] = []
    selected: dict[tuple[Any, ...], dict[str, Any]] = {}
    for spec in specs:
        execution_checkpoint()
        name = str(spec["name"])
        alias = str(spec["input_alias"])
        requirement = requirements.get(alias)
        records = tuple(records_by_alias.get(alias) or ())
        if requirement is None:
            raise RuntimeError(f"event_fact_input_missing: alias={alias}")
        where = dict(spec.get("where") or {})
        operator = str(spec["operator"])
        exact_alignment = (
            operator in _STRUCTURED_FACT_OPERATORS
            and str(requirement.get("alignment") or "") == "exact_interval"
        )
        if operator in _STRUCTURED_FACT_OPERATORS:
            latest = structured_snapshots.get(
                _structured_snapshot_key(alias, where), {}
            ).get(
                (
                    decision_time,
                    sample_time if exact_alignment else None,
                )
            )
            visible = [latest] if latest is not None else []
        else:
            visible = list(
                _causal_fact_records(records, evaluation_time=decision_time)
            )
            if where:
                visible = [
                    record
                    for record in visible
                    if _record_matches_where(record, where)
                ]
            visible.sort(key=_latest_record_key)
            latest = visible[-1] if visible else None
        max_staleness = int(requirement.get("max_staleness_seconds") or 0)
        source_step = timedelta(
            seconds=int(requirement.get("timeframe_seconds") or 0)
        )
        exact_gap = exact_alignment and _fact_gap_intersects(
            gap_evidence,
            alias=alias,
            start=sample_time - source_step,
            end=sample_time,
        )
        reason = None
        if latest is None and exact_gap:
            reason = f"fact_gap:{alias}"
        elif latest is None:
            reason = f"fact_missing:{alias}"
        elif max_staleness and (
            decision_time
            - (
                _record_freshness_time(latest)
                if operator in _STRUCTURED_FACT_OPERATORS
                else _record_known_at(latest)
            )
        ) > timedelta(seconds=max_staleness):
            reason = f"fact_stale:{alias}"
        elif (
            operator in _STRUCTURED_FACT_OPERATORS
            and not exact_alignment
            and _fact_gap_intersects(
            gap_evidence,
            alias=alias,
            start=_record_freshness_time(latest),
            end=decision_time,
            )
        ):
            reason = f"fact_gap:{alias}"
        if reason:
            features[name] = None
            if reason not in exclusions:
                exclusions.append(reason)
            continue
        assert latest is not None
        latest_index = visible.index(latest)
        previous = visible[latest_index - 1] if latest_index > 0 else None

        def retain(record: Any, *, role: str) -> None:
            material = {"alias": alias, "role": role, **_fact_material(record)}
            selected[_selected_record_key(alias, record)] = material
            if material not in references[alias]:
                references[alias].append(material)

        retain(latest, role="latest")
        latest_value = (
            float(latest.fact.value)
            if hasattr(latest.fact, "value")
            else None
        )
        if operator in _FACT_OPERATORS and operator != "age_seconds" and latest_value is None:
            raise RuntimeError(
                "event_fact_input_kind_invalid: numeric operator requires a numeric Fact "
                f"alias={alias} operator={operator}"
            )
        if operator == "latest_value":
            assert latest_value is not None
            value = latest_value
        elif operator == "age_seconds":
            value = (decision_time - _record_known_at(latest)).total_seconds()
        elif operator == "venue_basis_bps":
            assert latest_value is not None
            value = ((venue_close / latest_value) - 1.0) * 10000.0
        elif operator in _STRUCTURED_FACT_OPERATORS:
            document = _record_payload_document(latest)

            def payload_number(path: str) -> float | None:
                raw_value = _path(document, path)
                if raw_value is None:
                    return None
                try:
                    parsed = float(raw_value)
                except (TypeError, ValueError) as exc:
                    raise RuntimeError(
                        "event_fact_payload_invalid: selected payload field is not numeric "
                        f"alias={alias} path={path} fact_version_id="
                        f"{getattr(latest, 'fact_version_id', '<missing>')}"
                    ) from exc
                if not math.isfinite(parsed):
                    raise RuntimeError(
                        "event_fact_payload_invalid: selected payload field is not finite "
                        f"alias={alias} path={path} fact_version_id="
                        f"{getattr(latest, 'fact_version_id', '<missing>')}"
                    )
                return parsed

            value = payload_number(str(spec["path"]))
            if value is None:
                missing_reason = f"fact_payload_value_missing:{alias}:{name}"
                if missing_reason not in exclusions:
                    exclusions.append(missing_reason)
        elif operator in {
            "latest_update_direction",
            "latest_update_magnitude_bps",
            "update_agreement",
        }:
            if previous is None:
                features[name] = None
                missing_reason = f"fact_predecessor_missing:{alias}"
                if missing_reason not in exclusions:
                    exclusions.append(missing_reason)
                continue
            retain(previous, role="previous")
            assert latest_value is not None
            change = (latest_value / float(previous.fact.value)) - 1.0
            if operator == "latest_update_direction":
                value = 1.0 if change > 0 else -1.0 if change < 0 else 0.0
            elif operator == "latest_update_magnitude_bps":
                value = abs(change) * 10000.0
            else:
                update_direction = (
                    1.0 if change > 0 else -1.0 if change < 0 else 0.0
                )
                value = 1.0 if update_direction == direction else 0.0
        else:
            window_start = decision_time - timedelta(
                seconds=int(spec["window_seconds"])
            )
            window = [
                record
                for record in visible
                if window_start < _record_known_at(record) <= decision_time
            ]
            for record in window:
                retain(record, role="window")
            if operator == "update_count_window":
                value = float(len(window))
            elif operator == "aggregate_abs_change_bps_window":
                total = 0.0
                predecessor_missing = False
                for record in window:
                    index = visible.index(record)
                    if index <= 0:
                        predecessor_missing = True
                        break
                    prior = visible[index - 1]
                    retain(prior, role="window_previous")
                    total += abs(
                        (float(record.fact.value) / float(prior.fact.value)) - 1.0
                    ) * 10000.0
                if predecessor_missing:
                    value = None
                    missing_reason = f"fact_predecessor_missing:{alias}"
                    if missing_reason not in exclusions:
                        exclusions.append(missing_reason)
                else:
                    value = total
            else:  # pragma: no cover - normalization rejects this.
                raise AssertionError(operator)
        features[name] = value
    return features, dict(references), exclusions, selected


def _required_fact_decisions(
    *,
    detector: Mapping[str, Any],
    specs: Sequence[Mapping[str, Any]],
    baseline_specs: Sequence[Mapping[str, Any]],
    candles: Sequence[Mapping[str, Any]],
    records_by_alias: Mapping[str, Sequence[Any]],
    requirements: Mapping[str, Mapping[str, Any]],
    gap_evidence: Sequence[Mapping[str, Any]],
    evaluation_start: datetime,
    evaluation_end: datetime,
) -> tuple[
    dict[datetime, datetime],
    dict[datetime, list[dict[str, Any]]],
    dict[datetime, dict[str, Any]],
]:
    """Find first simultaneous availability, without changing any source clock.

    Market samples remain fixed. Only source availability transitions can wake
    a pending sample. Existing alignment, staleness and revision rules decide
    whether it is ready; evaluation_end bounds unresolved samples.
    """
    selectors: dict[tuple[str, str], dict[str, Any]] = {}
    used_aliases: set[str] = set()
    for spec in (detector, *specs):
        alias = str(spec["input_alias"])
        where = dict(spec.get("where") or {})
        selectors[_structured_snapshot_key(alias, where)] = {
            "alias": alias, "where": where,
        }
        used_aliases.add(alias)
    for alias in sorted(set(requirements) - used_aliases):
        selectors[_structured_snapshot_key(alias, {})] = {"alias": alias, "where": {}}

    timelines: dict[str, tuple[list[datetime], dict[datetime, list[datetime]]]] = {}
    for alias in requirements:
        times: set[datetime] = set()
        by_sample: dict[datetime, set[datetime]] = defaultdict(set)
        for record in records_by_alias.get(alias, ()):
            known_at = _record_known_at(record)
            if known_at <= evaluation_end:
                times.add(known_at)
                by_sample[_record_freshness_time(record)].add(known_at)
        timelines[alias] = (
            sorted(times), {key: sorted(values) for key, values in by_sample.items()}
        )
    candle_by_open = {
        _utc(row.get("open_time") or row.get("time"), field="candle.open_time"): row
        for row in candles
    }
    prefix_known: dict[datetime, datetime] = {}
    watermark = datetime.min.replace(tzinfo=UTC)
    for opened, candle in sorted(candle_by_open.items()):
        watermark = max(watermark, _utc(candle.get("known_at") or candle["close_time"], field="candle.known_at"))
        prefix_known[opened] = watermark
    requests_by_open: dict[datetime, list[tuple[datetime, datetime]]] = {}
    total_requests = 0
    for candle in candles:
        execution_checkpoint()
        opened = _utc(candle.get("open_time") or candle.get("time"), field="candle.open_time")
        sample = _utc(candle.get("close_time"), field="candle.close_time")
        start = max(sample, _utc(candle.get("known_at") or sample, field="candle.known_at"))
        for spec in baseline_specs:
            if spec["operator"] == "atr_fraction":
                start = max(start, prefix_known[opened])
            else:
                for offset in range(1, int(spec.get("lookback_bars") or 0) + 1):
                    previous = candle_by_open.get(opened - (sample - opened) * offset)
                    if previous is not None:
                        start = max(start, _utc(previous.get("known_at") or previous["close_time"], field="candle.known_at"))
        if not evaluation_start <= opened < evaluation_end or start >= evaluation_end:
            continue
        end = evaluation_end
        for alias, requirement in requirements.items():
            staleness = int(requirement.get("max_staleness_seconds") or 0)
            if staleness <= 0:
                raise ValueError(
                    f"event_fact_trigger_invalid: explicit max_staleness_seconds required alias={alias}"
                )
            if requirement.get("alignment") == "exact_interval":
                end = min(end, sample + timedelta(seconds=staleness))
        candidates = {start} if start <= end else set()
        for alias, requirement in requirements.items():
            times, by_sample = timelines[alias]
            if requirement.get("alignment") == "exact_interval":
                times = by_sample.get(sample, [])
            last = (
                bisect_left(times, end)
                if end == evaluation_end else bisect_right(times, end)
            )
            candidates.update(times[bisect_right(times, start):last])
        total_requests += len(candidates)
        if total_requests > 100_000:
            raise ValueError(
                "event_fact_trigger_budget_exceeded: narrow the evaluation range (100000 candidate times maximum)"
            )
        requests_by_open[opened] = [(time, sample) for time in sorted(candidates)]

    snapshots: dict[tuple[str, str], dict[tuple[datetime, datetime | None], Any | None]] = {}
    for key, selector in selectors.items():
        alias = selector["alias"]
        if alias not in requirements:
            raise ValueError(f"event_fact_input_missing: alias={alias}")
        exact = requirements[alias].get("alignment") == "exact_interval"
        snapshots[key] = _causal_snapshot_series(
            tuple(records_by_alias.get(alias, ())),
            snapshot_requests=[
                (decision, sample if exact else None)
                for requests in requests_by_open.values() for decision, sample in requests
            ],
            where=selector["where"], alias=alias, defer_ambiguity=True,
            numeric_selection=all(
                hasattr(record.fact, "source_event_key")
                and not hasattr(record.fact, "observation_key")
                for record in records_by_alias.get(alias, ())
            ),
        )
    decisions: dict[datetime, datetime] = {}
    references: dict[datetime, list[dict[str, Any]]] = {}
    readiness: dict[datetime, dict[str, Any]] = {}
    for opened, requests in requests_by_open.items():
        execution_checkpoint()
        for decision, sample in requests:
            details = {
                "checked_at": _iso(decision),
                "required_inputs": [
                    {**selector, "known_at": None, "status": "not_evaluated", "reason": None}
                    for selector in selectors.values()
                ],
            }
            readiness[opened] = details
            selected_refs: list[dict[str, Any]] = []
            for index, (key, selector) in enumerate(selectors.items()):
                detail = details["required_inputs"][index]
                detail["status"] = "unavailable"
                alias = selector["alias"]
                requirement = requirements[alias]
                exact = requirement.get("alignment") == "exact_interval"
                record = snapshots[key].get((decision, sample if exact else None))
                if record is None:
                    detail["reason"] = f"fact_missing:{alias}"
                    break
                if isinstance(record, RuntimeError):
                    raise record
                detail["known_at"] = _iso(_record_known_at(record))
                freshness = _record_freshness_time(record)
                if freshness > decision or _record_effective_at(record) > decision:
                    detail["reason"] = "required_facts_unavailable"
                    detail["failed_gate"] = "source_time_after_evaluation"
                    break
                if (decision - freshness).total_seconds() > int(requirement["max_staleness_seconds"]):
                    detail["reason"] = f"fact_stale:{alias}"
                    break
                gap_start = (
                    sample - timedelta(seconds=int(requirement.get("timeframe_seconds") or 0))
                    if exact else freshness
                )
                if _fact_gap_intersects(
                    gap_evidence, alias=alias, start=gap_start,
                    end=sample if exact else decision,
                ):
                    detail["reason"] = f"fact_gap:{alias}"
                    break
                detail["status"] = "ready"
                selected_refs.append({"alias": alias, **_fact_material(record)})
            else:
                decisions[opened] = decision
                references[opened] = selected_refs
                break
    return decisions, references, readiness


def _fact_snapshot_outputs(
    *,
    detector: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    records_by_alias: Mapping[str, Sequence[Any]],
    requirements: Mapping[str, Mapping[str, Any]],
    gap_evidence: Sequence[Mapping[str, Any]],
    evaluation_start: datetime,
    evaluation_end: datetime,
    ordered_revisions_by_alias: Mapping[str, Sequence[Any]],
    decision_times: Mapping[datetime, datetime] | None = None,
    trigger_references: Mapping[datetime, list[dict[str, Any]]] | None = None,
) -> tuple[
    list[dict[str, Any]],
    dict[tuple[datetime, datetime | None], Any | None],
]:
    alias = str(detector["input_alias"])
    if alias not in requirements:
        raise RuntimeError(
            f"event_fact_detector_input_missing: alias={alias}"
        )
    requirement = requirements[alias]
    max_staleness = int(requirement.get("max_staleness_seconds") or 0)
    if max_staleness <= 0:
        raise RuntimeError(
            "event_fact_detector_input_invalid: fact_snapshot requires explicit "
            f"max_staleness_seconds alias={alias}"
        )
    records = tuple(records_by_alias.get(alias) or ())
    where = dict(detector.get("where") or {})
    ordered_candles = sorted(
        (dict(row) for row in candles),
        key=lambda row: _utc(
            row.get("open_time") or row.get("time"), field="candle.open_time"
        ),
    )
    candidates: list[tuple[dict[str, Any], datetime, datetime, datetime]] = []
    for candle in ordered_candles:
        execution_checkpoint()
        event_time = _utc(
            candle.get("open_time") or candle.get("time"),
            field="candle.open_time",
        )
        decision_time = _utc(
            candle.get("known_at") or candle.get("close_time"),
            field="candle.known_at",
        )
        sample_time = _utc(candle.get("close_time"), field="candle.close_time")
        if decision_times is not None:
            decision_time = decision_times.get(event_time, decision_time)
        if not (
            evaluation_start <= event_time < evaluation_end
            and decision_time <= evaluation_end
        ):
            continue
        candidates.append((candle, event_time, decision_time, sample_time))
    exact_alignment = str(requirement.get("alignment") or "") == "exact_interval"
    snapshots = _causal_snapshot_series(
        records,
        snapshot_requests=[
            (
                row[2],
                row[3] if exact_alignment else None,
            )
            for row in candidates
        ],
        where=where,
        alias=alias,
        ordered_revisions=ordered_revisions_by_alias.get(alias),
    )
    outputs: list[dict[str, Any]] = []
    source_step = timedelta(seconds=int(requirement.get("timeframe_seconds") or 0))
    for _candle, event_time, decision_time, sample_time in candidates:
        execution_checkpoint()
        selected = snapshots.get(
            (
                decision_time,
                sample_time if exact_alignment else None,
            )
        )
        reason = None
        exact_gap = exact_alignment and _fact_gap_intersects(
            gap_evidence,
            alias=alias,
            start=sample_time - source_step,
            end=sample_time,
        )
        if selected is None and exact_gap:
            reason = f"detector_fact_gap:{alias}"
        elif selected is None:
            reason = f"detector_fact_missing:{alias}"
        elif decision_time - _record_freshness_time(selected) > timedelta(
            seconds=max_staleness
        ):
            reason = f"detector_fact_stale:{alias}"
        elif not exact_alignment and _fact_gap_intersects(
            gap_evidence,
            alias=alias,
            start=_record_freshness_time(selected),
            end=decision_time,
        ):
            reason = f"detector_fact_gap:{alias}"
        if decision_times is not None and event_time not in decision_times:
            selected = None
            reason = "required_facts_unavailable"
        material = _fact_material(selected) if selected is not None else None
        outputs.append(
            {
                "time": _iso(event_time),
                "known_at": _iso(decision_time),
                "output_name": "fact_snapshot",
                "output_type": "check_occurrence",
                "event_key": f"fact_snapshot:{alias}",
                "event": {
                    "key": f"fact_snapshot:{alias}",
                    "known_at": _iso(decision_time),
                    "direction": "neutral",
                    "metadata": {
                        **({
                            "evaluation_trigger": _REQUIRED_FACTS_AVAILABLE,
                            "trigger_status": "ready" if event_time in decision_times else "unavailable",
                            "required_fact_references": (trigger_references or {}).get(event_time, []),
                        } if decision_times is not None else {}),
                        "input_alias": alias,
                        "sampling": "primary_bar_close",
                        "sample_time": _iso(sample_time),
                        "selection_rule": (
                            "exact_source_interval_known_at_decision.v1"
                            if exact_alignment
                            else "latest_observation_known_at_decision.v1"
                        ),
                        "where": where,
                        "selected_fact": material,
                    },
                },
                "detector_record": selected,
                "detector_exclusion_reason": reason,
            }
        )
    return outputs, snapshots


