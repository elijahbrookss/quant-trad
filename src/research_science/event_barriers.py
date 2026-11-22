"""Indicator-neutral barrier observations and original-clock weighted descriptions.

No recognition, matching, provider access, or execution authority lives here.
"""
from bisect import bisect_left, bisect_right
from collections import Counter
from datetime import timedelta
import json
import math

from .completed_event_context import locked_reference_geometry
from .hashing import stable_hash
from .recognition import plain, utc
from .reversal_outcomes import CLASSES, first_passage_path
from .reversal_selection import _components, moment_weights
from .weighted_summaries import first_crossing_quantiles, sequential_prefixes

QUANTILES = (0.0, 0.25, 0.5, 0.75, 0.9, 0.95, 1.0)
SCHEMA = "indicator_event_barriers_result.v1"
POPULATION_SCHEMA = "indicator_event_barriers_population.v1"
PRELABEL_FIELDS = (
    "event_id",
    "decision_at",
    "event",
    "boundary_geometry",
    "reference_window_start",
    "reference_window_bars",
    "support_start",
    "support_end",
    "source_reference_ids",
    "features",
    "feature_missing_reasons",
    "original_weight",
)


def path_value(document, path):
    value = document
    for key in path.split("."):
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return value


def finite(value):
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def geometry_for(event, *, config, candles, by_close, by_open, source_key):
    at = utc(event["known_at"])
    if at not in by_close:
        raise ValueError("event_barriers_invalid: completion close unavailable")
    index = by_close[at]
    bar = candles[index]
    if bar.known_at != at:
        raise ValueError("event_barriers_invalid: completion availability differs")
    for field, expected in (
        ("identity_path", event["pattern_id"]),
        ("source_path", source_key),
        ("known_at_path", plain(at)),
    ):
        if config.get(field):
            actual = path_value(event, config[field])
            if field == "known_at_path" and actual is not None:
                actual = plain(utc(actual))
            if actual != expected:
                raise ValueError(
                    "event_barriers_invalid: public " + field + " differs"
                )

    start = at - timedelta(seconds=900)
    if config.get("window_start_path"):
        value = path_value(event, config["window_start_path"])
        if value is None:
            raise ValueError(
                "event_barriers_invalid: reference window start missing"
            )
        start = utc(value)
    if start not in by_open or by_open[start] > index:
        raise ValueError(
            "event_barriers_invalid: reference window outside frozen input"
        )
    first = by_open[start]
    reference = (
        bar.close
        if config["reference"]["operator"] == "event_close"
        else path_value(event, config["reference"]["path"])
    )
    failure = (
        min(candle.low for candle in candles[first : index + 1])
        if config["failure"]["operator"] == "window_low"
        else path_value(event, config["failure"]["path"])
    )
    if not finite(reference) or not finite(failure):
        raise ValueError(
            "event_barriers_invalid: reference/failure field missing or nonfinite"
        )
    geometry = locked_reference_geometry(reference, failure, at)
    if config["target"]["operator"] == "event_metadata_number":
        geometry["upper"] = path_value(event, config["target"]["path"])
    else:
        geometry["upper"] = reference + (
            reference - failure
        ) * config["target"]["multiple"]
    if not finite(geometry["upper"]):
        geometry["upper"] = None
    return geometry, first, index


def summarize_field(rows, weights, name, *, checkpoint=lambda: None):
    checkpoint()
    observed = sorted(
        (row for row in rows if finite(row["features"].get(name))),
        key=lambda row: (
            row["features"][name],
            row["decision_at"],
            row["event_id"],
        ),
    )
    pairs = [
        (row["features"][name], weights[row["event_id"]])
        for row in observed
    ]
    mass = math.fsum(weight for _, weight in pairs)
    total = math.fsum(weights[row["event_id"]] for row in rows)
    prefixes = sequential_prefixes([weight for _, weight in pairs])
    missing = [row for row in rows if not finite(row["features"].get(name))]
    reasons = Counter(
        row["feature_missing_reasons"].get(name, "unavailable")
        for row in missing
    )
    checkpoint()
    return {
        "observed_count": len(observed),
        "missing_count": len(missing),
        "original_label_mass": total,
        "observed_mass": mass,
        "missing_mass": math.fsum(
            weights[row["event_id"]] for row in missing
        ),
        "missing_reasons": dict(sorted(reasons.items())),
        "mean": math.fsum(value * weight for value, weight in pairs) / mass
        if mass
        else None,
        "quantiles": first_crossing_quantiles(
            pairs,
            QUANTILES,
            total_weight=mass,
            prefixes=sequential_prefixes,
        ),
        "ecdf": [
            {
                "value": row["features"][name],
                "event_id": row["event_id"],
                "decision_at": row["decision_at"],
                "original_weight": weights[row["event_id"]],
                "cdf": prefix / mass,
            }
            for row, prefix in zip(observed, prefixes)
        ]
        if mass
        else [],
        "empty_reason": None if mass else "no_observed_mass",
    }


def rank_dominance(left, right, weights, name, *, checkpoint=lambda: None):
    checkpoint()
    a = [
        (row["features"][name], weights[row["event_id"]])
        for row in left
        if finite(row["features"].get(name))
    ]
    b = sorted(
        (row["features"][name], weights[row["event_id"]])
        for row in right
        if finite(row["features"].get(name))
    )
    a_mass = math.fsum(weight for _, weight in a)
    b_mass = math.fsum(weight for _, weight in b)
    if not a_mass or not b_mass:
        return {"value": None, "reason": "empty_observed_class_mass"}
    values = [value for value, _ in b]
    prefixes = [0.0] + sequential_prefixes([weight for _, weight in b])
    score = math.fsum(
        weight
        * (
            prefixes[bisect_left(values, value)]
            + 0.5
            * (
                prefixes[bisect_right(values, value)]
                - prefixes[bisect_left(values, value)]
            )
        )
        for value, weight in a
    ) / (a_mass * b_mass)
    checkpoint()
    return {
        "value": 2 * score - 1,
        "reason": None,
        "meaning": "2*(P(target feature>other)+0.5*P(tie))-1;descriptive",
    }


def population_summary(
    records,
    features,
    *,
    start,
    end,
    illustrations,
    checkpoint=lambda: None,
):
    # Construct roots once over ALL completions, before class/field filtering.
    weights = moment_weights(records)
    groups = {
        label: [
            row
            for row in records
            if row["outcome"]["classification"] == label
        ]
        for label in CLASSES
    }
    labels = {}
    for label, rows in groups.items():
        checkpoint()
        labels[label] = {
            "chain_count": len(rows),
            "completion_clock_count": len(
                {row["decision_at"] for row in rows}
            ),
            "original_mass": math.fsum(
                weights[row["event_id"]] for row in rows
            ),
            "features": {
                feature["name"]: summarize_field(
                    rows,
                    weights,
                    feature["name"],
                    checkpoint=checkpoint,
                )
                for feature in features
            },
        }
    contrasts = {}
    for other in ("failure", "neither"):
        contrasts["continuation_minus_" + other] = {}
        for feature in features:
            checkpoint()
            name = feature["name"]
            left = labels["continuation"]["features"][name]
            right = labels[other]["features"][name]
            contrasts["continuation_minus_" + other][name] = {
                "mean_difference": left["mean"] - right["mean"]
                if left["mean"] is not None and right["mean"] is not None
                else None,
                "units": feature["units"],
                "rank_dominance": rank_dominance(
                    groups["continuation"],
                    groups[other],
                    weights,
                    name,
                    checkpoint=checkpoint,
                ),
            }
    picks = []
    for spec in illustrations:
        checkpoint()
        rows = [
            row
            for row in groups[spec["classification"]]
            if finite(row["features"].get(spec["feature"]))
        ]
        rows.sort(
            key=lambda row: (
                (-1 if spec["order"] == "max" else 1)
                * row["features"][spec["feature"]],
                row["decision_at"],
                row["event_id"],
            )
        )
        picks.append(
            {
                **spec,
                "event_id": rows[0]["event_id"] if rows else None,
                "empty_reason": None if rows else "no_finite_feature_event",
            }
        )
    day = utc(start).replace(hour=0, minute=0, second=0, microsecond=0)
    days = []
    while day < utc(end):
        checkpoint()
        rows = [
            row
            for row in records
            if day <= utc(row["decision_at"]) < day + timedelta(days=1)
        ]
        days.append(
            {
                "utc_day": day.date().isoformat(),
                "count": len(rows),
                "original_mass": math.fsum(
                    weights[row["event_id"]] for row in rows
                ),
            }
        )
        day += timedelta(days=1)
    return {
        "chain_count": len(records),
        "completion_clock_count": len(
            {row["decision_at"] for row in records}
        ),
        "all_case_weights": weights,
        "labels": labels,
        "contrasts": contrasts,
        "completion_days": days,
        "illustrations": picks,
        "inference": "descriptive P(feature|label),not prediction,causality,independent samples or trading permission",
    }


def population_projection(records, dependence):
    return {
        "schema_version": POPULATION_SCHEMA,
        "cases": [
            {field: row[field] for field in PRELABEL_FIELDS}
            for row in records
        ],
        "dependence": dependence,
    }


def measure_events(
    event_rows,
    candles,
    *,
    config,
    features,
    start,
    end,
    source_key,
    checkpoint=lambda: None,
):
    by_close = {candle.close_time: index for index, candle in enumerate(candles)}
    by_open = {candle.open_time: index for index, candle in enumerate(candles)}
    if not candles or len(by_close) != len(candles) or len(by_open) != len(candles):
        raise ValueError("event_barriers_invalid: unique nonempty candle path required")
    path = {candle.open_time: candle for candle in candles}
    records = []
    seen = set()
    for output in event_rows:
        checkpoint()
        event = output["event"]
        try:
            json.dumps(event, allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "event_barriers_invalid: public event must be canonical JSON"
            ) from exc
        at = utc(event["known_at"])
        if output.get("known_at") is not None and utc(output["known_at"]) != at:
            raise ValueError(
                "event_barriers_invalid: public output/event known_at differs"
            )
        if not utc(start) <= at < utc(end):
            continue
        identity = event.get("pattern_id")
        if (
            not isinstance(identity, str)
            or not identity
            or identity in seen
        ):
            raise ValueError(
                "event_barriers_invalid: unique public pattern_id required"
            )
        if (
            event.get("direction") != "long"
            or output.get("output_type") != "signal"
        ):
            raise ValueError(
                "event_barriers_invalid: long typed signal required"
            )
        seen.add(identity)
        geometry, first, index = geometry_for(
            event,
            config=config,
            candles=candles,
            by_close=by_close,
            by_open=by_open,
            source_key=source_key,
        )
        row = {
            "event_id": identity,
            "decision_at": plain(at),
            "event": event,
            "boundary_geometry": geometry,
            "reference_window_start": plain(candles[first].open_time),
            "reference_window_bars": index - first + 1,
            "support_start": plain(
                candles[
                    max(
                        0,
                        min(first, index - config["support_lookback_bars"]),
                    )
                ].open_time
            ),
            "support_end": plain(
                at + timedelta(seconds=900 * config["horizon_bars"])
            ),
            "source_reference_ids": [],
            "features": {},
            "feature_missing_reasons": {},
        }
        for feature in features:
            value = (
                (index - first + 1) / 4
                if feature["operator"] == "window_duration_hours"
                else path_value(event, feature["path"])
            )
            row["features"][feature["name"]] = value if finite(value) else None
            if not finite(value):
                row["feature_missing_reasons"][feature["name"]] = (
                    path_value(event, feature["missing_reason_path"])
                    if feature.get("missing_reason_path")
                    else None
                ) or "missing_or_nonfinite_public_feature"
        records.append(row)
        if len(records) > 10000:
            raise ValueError(
                "event_barriers_limit: more than10000events;no truncation"
            )
    records.sort(key=lambda row: (row["decision_at"], row["event_id"]))
    roots = moment_weights(records)
    for row in records:
        row["original_weight"] = roots[row["event_id"]]
    dependence = _components(
        records,
        [],
        recognition_origin=plain(candles[0].open_time),
    )
    projection = population_projection(records, dependence)
    population_hash = stable_hash(projection)

    for row in records:
        checkpoint()
        geometry = row["boundary_geometry"]
        at = utc(row["decision_at"])
        row["outcome"] = first_passage_path(
            anchor_at=at,
            known_at=at,
            entry=geometry["E"],
            low=geometry["L"],
            target=geometry["upper"]
            if geometry["upper"] is not None
            else math.inf,
            horizon_bars=config["horizon_bars"],
            source_identity_key=source_key,
            timeframe_seconds=900,
            by_open=path,
            evaluation_end=utc(end),
            checkpoint=checkpoint,
        )
        row["first_touch"] = (
            {
                "bar_index": row["outcome"]["observed_bars"],
                "known_at": row["outcome"]["outcome_known_at"],
            }
            if row["outcome"]["reason"]
            in ("first_touch", "intrabar_order_unknown")
            else None
        )
        available = 0
        for offset in range(config["horizon_bars"]):
            opened = at + timedelta(seconds=900 * offset)
            if opened >= utc(end) or opened not in path:
                break
            available += 1
        row["followup_support"] = {
            "available_bars": available,
            "complete_horizon": available == config["horizon_bars"],
            "nominal_end": row["outcome"]["nominal_end"],
            "last_available_close": plain(
                at + timedelta(seconds=900 * available)
            ),
            "censor_reason": None
            if available == config["horizon_bars"]
            else "evaluation_boundary",
        }
        row["later_target"] = later_target(
            row,
            candles,
            source_key=source_key,
            end=end,
            horizon_bars=config["horizon_bars"],
            checkpoint=checkpoint,
        )
    summary = population_summary(
        records,
        features,
        start=start,
        end=end,
        illustrations=config["illustrations"],
        checkpoint=checkpoint,
    )
    return {
        "schema_version": SCHEMA,
        "status": "completed",
        "stage": "first_passage_and_strictly_later_target",
        "cases": records,
        "population_hash": population_hash,
        "population_hash_projection": {
            "schema_version": POPULATION_SCHEMA,
            "case_fields": list(PRELABEL_FIELDS),
            "includes_dependence": True,
            "excludes": [
                "outcome",
                "first_touch",
                "followup_support",
                "later_target",
            ],
        },
        "dependence": dependence,
        "summary": summary,
        "later_target_summary": followup_summary(
            records, horizon_bars=config["horizon_bars"]
        ),
        "sample_count": summary["completion_clock_count"],
        "independent_sample_size": None,
        "outcomes_calculated": True,
    }


def later_target(
    saved,
    candles,
    *,
    source_key,
    end,
    horizon_bars,
    checkpoint=lambda: None,
):
    """Additional observation; original failure and deadline stay fixed."""
    outcome = saved["outcome"]
    failure_bar = outcome["observed_bars"]
    at = utc(saved["decision_at"])
    by_open = {candle.open_time: candle for candle in candles}
    if outcome["classification"] != "failure":
        return {
            "status": "not_applicable",
            "reason": "original_label_is_not_failure",
        }
    geometry = saved["boundary_geometry"]
    verified = first_passage_path(
        anchor_at=at,
        known_at=at,
        entry=geometry["E"],
        low=geometry["L"],
        target=geometry["upper"],
        horizon_bars=horizon_bars,
        source_identity_key=source_key,
        timeframe_seconds=900,
        by_open=by_open,
        evaluation_end=utc(end),
        checkpoint=checkpoint,
    )
    if verified != outcome:
        raise ValueError(
            "event_barriers_invalid: original first passage differs"
        )
    # Reuse the historical complete-window verifier when its exact contract fits.
    if (
        horizon_bars == 12
        and geometry["upper"]
        == geometry["E"] + (geometry["E"] - geometry["L"])
    ):
        from .reversal_matched_progress import progress_path

        progress_path(
            saved=outcome,
            source_identity_key=source_key,
            by_open=by_open,
            evaluation_end=utc(end),
            checkpoint=checkpoint,
        )

    found = None
    observed = 0
    reason = None
    for bar_index in range(failure_bar + 1, horizon_bars + 1):
        checkpoint()
        opened = at + timedelta(seconds=900 * (bar_index - 1))
        if opened >= utc(end):
            reason = "evaluation_boundary"
            break
        bar = by_open.get(opened)
        if bar is None:
            reason = "candle_missing"
            break
        if (
            bar.source_identity_key != source_key
            or bar.known_at != bar.close_time
        ):
            raise ValueError(
                "event_barriers_invalid: followup source/availability differs"
            )
        observed += 1
        if found is None and bar.high >= geometry["upper"]:
            found = {
                "bar_index": bar_index,
                "close_time": plain(bar.close_time),
                "known_at": plain(bar.known_at),
            }
    return {
        "status": "positive" if found else "unknown" if reason else "negative",
        "reason": reason,
        "first_failure_bar": failure_bar,
        "nominal_remaining_bars": horizon_bars - failure_bar,
        "observed_postfailure_bars": observed,
        "structural_no_remaining_opportunity": failure_bar == horizon_bars,
        "first_later_target": found,
        "original_nominal_end": outcome["nominal_end"],
        "original_label": "failure",
        "complete_horizon": reason is None,
        "observation_known_at": found["known_at"]
        if found
        else plain(at + timedelta(seconds=900 * horizon_bars))
        if reason is None
        else None,
    }


def followup_summary(records, *, horizon_bars):
    failures = [
        row for row in records if row["outcome"]["classification"] == "failure"
    ]
    denominator = math.fsum(row["original_weight"] for row in failures)

    def summarize(rows):
        mass = math.fsum(row["original_weight"] for row in rows)
        return {
            "count": len(rows),
            "original_failure_mass": mass,
            "statuses": {
                status: {
                    "count": sum(
                        row["later_target"]["status"] == status
                        for row in rows
                    ),
                    "original_mass": math.fsum(
                        row["original_weight"]
                        for row in rows
                        if row["later_target"]["status"] == status
                    ),
                    "share_of_failure_mass": math.fsum(
                        row["original_weight"]
                        for row in rows
                        if row["later_target"]["status"] == status
                    )
                    / denominator
                    if denominator
                    else None,
                }
                for status in ("positive", "negative", "unknown")
            },
        }

    return {
        **summarize(failures),
        "opportunity_by_first_failure_bar": [
            {
                "first_failure_bar": bar_index,
                **summarize(
                    [
                        row
                        for row in failures
                        if row["later_target"]["first_failure_bar"]
                        == bar_index
                    ]
                ),
            }
            for bar_index in range(1, horizon_bars + 1)
        ],
    }
