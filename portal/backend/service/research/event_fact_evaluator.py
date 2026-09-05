"""Registered, provider-neutral event-and-fact analytical Check evaluator."""

from __future__ import annotations

import heapq
import math
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from statistics import fmean, median
from typing import Any

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import pearsonr, pointbiserialr

from data_providers.utils.ohlcv import compute_tr_atr
from market_data.frozen import semantic_hash
from market_data.requirements import causal_numeric_fact_records
from research_science import adjusted_p_values
from research_science.check import (
    GAP_POLICY_CONTINUE_DEGRADED,
    GAP_POLICY_REJECT,
    CheckDefinition,
    CheckRequest,
    ResolvedCheckPlan,
)


EVENT_FACT_ANALYSIS = "event_fact_analysis"
LEGACY_EVENT_FACT_EVALUATOR_VERSION = "2"
LEGACY_EVENT_FACT_RESULT_VERSION = "event_fact_analysis_result.v2"
EVENT_FACT_EVALUATOR_VERSION = "3"
EVENT_FACT_RESULT_VERSION = "event_fact_analysis_result.v3"

_INDICATOR_EVENT_DETECTOR = "indicator_event"
_FACT_SNAPSHOT_DETECTOR = "fact_snapshot"

_BASELINE_OPERATORS = frozenset(
    {
        "direction_signed_return",
        "lagged_return",
        "atr_fraction",
        "event_metadata_number",
        "event_direction",
        "volume_ratio",
    }
)
_FACT_OPERATORS = frozenset(
    {
        "latest_value",
        "age_seconds",
        "venue_basis_bps",
        "latest_update_direction",
        "latest_update_magnitude_bps",
        "update_agreement",
        "update_count_window",
        "aggregate_abs_change_bps_window",
    }
)
_STRUCTURED_FACT_OPERATORS = frozenset(
    {
        "latest_payload_number",
    }
)


def _utc(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip()
        if not raw:
            raise ValueError(f"event_fact_check_invalid: {field} is required")
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(
                f"event_fact_check_invalid: {field} must be ISO-8601"
            ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _known_at(value: Any, *, field: str) -> datetime:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return datetime.fromtimestamp(float(value), tz=UTC)
    raw = str(value or "").strip()
    if raw and raw.replace(".", "", 1).isdigit():
        return datetime.fromtimestamp(float(raw), tz=UTC)
    return _utc(value, field=field)


def _mapping(value: Any, *, field: str) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"event_fact_check_invalid: {field} must be an object")
    return dict(value)


def _list(value: Any, *, field: str) -> list[dict[str, Any]]:
    if value in (None, ""):
        return []
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise ValueError(
            f"event_fact_check_invalid: {field} must be a list of objects"
        )
    return [dict(row) for row in value]


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


def _positive_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"event_fact_check_invalid: {field} must be positive")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"event_fact_check_invalid: {field} must be positive"
        ) from exc
    if result <= 0:
        raise ValueError(f"event_fact_check_invalid: {field} must be positive")
    return result


def _finite(value: Any, *, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"event_fact_check_invalid: {field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"event_fact_check_invalid: {field} must be numeric"
        ) from exc
    if not math.isfinite(result):
        raise ValueError(f"event_fact_check_invalid: {field} must be finite")
    return result


def _payload_path(value: Any, *, field: str) -> str:
    path = str(value or "").strip()
    if not path.startswith("payload.") or len(path.split(".")) != 2:
        raise ValueError(
            f"event_fact_check_invalid: {field} must name one top-level payload field"
        )
    return path


def _normalize_payload_where(value: Any, *, field: str) -> dict[str, Any]:
    where = _mapping(value, field=field)
    if len(where) > 8:
        raise ValueError(
            f"event_fact_check_invalid: {field} cannot contain more than eight fields"
        )
    normalized: dict[str, Any] = {}
    for raw_path, expected in sorted(where.items(), key=lambda row: str(row[0])):
        path = _payload_path(raw_path, field=field)
        if isinstance(expected, (Mapping, list, tuple, set)) or expected is None:
            raise ValueError(
                f"event_fact_check_invalid: {field}.{path} must be a non-null scalar"
            )
        normalized[path] = expected
    return normalized


def normalize_event_fact_configuration(
    *,
    detector: Mapping[str, Any],
    outcomes: Mapping[str, Any],
    statistics: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Validate the deliberately small registered operator vocabulary."""

    normalized_detector = dict(detector or {})
    detector_type = str(normalized_detector.get("type") or "").strip().lower()
    if detector_type == _INDICATOR_EVENT_DETECTOR:
        output_name = str(normalized_detector.get("output_name") or "").strip()
        if not output_name:
            raise ValueError(
                "event_fact_check_invalid: detector.output_name is required"
            )
        event_keys = _list(
            normalized_detector.get("event_keys"), field="detector.event_keys"
        )
        if not event_keys:
            raise ValueError(
                "event_fact_check_invalid: detector.event_keys is required"
            )
        seen_keys: set[str] = set()
        normalized_keys: list[dict[str, Any]] = []
        for row in event_keys:
            key = str(row.get("key") or "").strip()
            direction = str(row.get("direction") or "").strip().lower()
            if not key or key in seen_keys or direction not in {"long", "short"}:
                raise ValueError(
                    "event_fact_check_invalid: event keys must be unique and map to long or short"
                )
            seen_keys.add(key)
            normalized_keys.append(
                {
                    "key": key,
                    "direction": direction,
                    "alias": str(row.get("alias") or key),
                }
            )
        normalized_detector = {
            "type": _INDICATOR_EVENT_DETECTOR,
            "output_name": output_name,
            "event_keys": normalized_keys,
        }
    elif detector_type == _FACT_SNAPSHOT_DETECTOR:
        input_alias = str(normalized_detector.get("input_alias") or "").strip()
        if not input_alias:
            raise ValueError(
                "event_fact_check_invalid: detector.input_alias is required"
            )
        sampling = str(
            normalized_detector.get("sampling") or "primary_bar_close"
        ).strip().lower()
        if sampling != "primary_bar_close":
            raise ValueError(
                "event_fact_check_invalid: fact_snapshot sampling must be primary_bar_close"
            )
        normalized_detector = {
            "type": _FACT_SNAPSHOT_DETECTOR,
            "input_alias": input_alias,
            "sampling": sampling,
            "where": _normalize_payload_where(
                normalized_detector.get("where"), field="detector.where"
            ),
        }
    else:
        raise ValueError(
            "event_fact_check_invalid: detector.type must be indicator_event or fact_snapshot"
        )

    normalized_outcomes = dict(outcomes or {})
    raw_horizons = normalized_outcomes.get("horizons") or normalized_outcomes.get(
        "forward_bars"
    )
    if not isinstance(raw_horizons, list) or not raw_horizons:
        raise ValueError("event_fact_check_invalid: outcomes.horizons is required")
    horizons = sorted(
        {_positive_int(value, field="outcomes.horizons") for value in raw_horizons}
    )
    horizon_kind = str(
        normalized_outcomes.get("horizon_kind") or "bars"
    ).strip().lower()
    if horizon_kind not in {"bars", "elapsed_time"}:
        raise ValueError(
            "event_fact_check_invalid: horizon_kind must be bars or elapsed_time"
        )
    primary_horizon = int(
        normalized_outcomes.get("primary_horizon")
        or normalized_outcomes.get("primary_horizon_bars")
        or horizons[0]
    )
    if primary_horizon not in horizons:
        raise ValueError(
            "event_fact_check_invalid: primary horizon must be declared"
        )
    raw_required_horizons = normalized_outcomes.get("required_horizons") or horizons
    if not isinstance(raw_required_horizons, list):
        raise ValueError(
            "event_fact_check_invalid: outcomes.required_horizons must be a list"
        )
    required_horizons = sorted(
        {
            _positive_int(value, field="outcomes.required_horizons")
            for value in raw_required_horizons
        }
    )
    if not required_horizons or not set(required_horizons).issubset(horizons):
        raise ValueError(
            "event_fact_check_invalid: required horizons must be declared horizons"
        )
    normalized_outcomes = {
        "horizons": horizons,
        "required_horizons": required_horizons,
        "horizon_kind": horizon_kind,
        "primary_horizon": primary_horizon,
        "entry_lag_bars": int(normalized_outcomes.get("entry_lag_bars") or 0),
        "positive_rule": (
            "direction_signed_forward_return_gt_zero.v1"
            if detector_type == _INDICATOR_EVENT_DETECTOR
            else "raw_forward_return_gt_zero.v1"
        ),
    }
    if normalized_outcomes["entry_lag_bars"] < 0:
        raise ValueError(
            "event_fact_check_invalid: entry_lag_bars must be nonnegative"
        )
    invalidation = _mapping(
        outcomes.get("invalidation"), field="outcomes.invalidation"
    )
    if invalidation:
        if detector_type != _INDICATOR_EVENT_DETECTOR:
            raise ValueError(
                "event_fact_check_invalid: fact_snapshot does not support directional invalidation"
            )
        if str(invalidation.get("type") or "").strip() != "close_crosses_event_reference":
            raise ValueError(
                "event_fact_check_invalid: only close_crosses_event_reference invalidation is registered"
            )
        reference_path = str(invalidation.get("reference_path") or "").strip()
        if not reference_path.startswith("metadata."):
            raise ValueError(
                "event_fact_check_invalid: invalidation reference_path must start metadata."
            )
        normalized_outcomes["invalidation"] = {
            "type": "close_crosses_event_reference",
            "version": "directional_close_cross.v1",
            "reference_path": reference_path,
            "max_bars": _positive_int(
                invalidation.get("max_bars"),
                field="outcomes.invalidation.max_bars",
            ),
        }

    normalized_statistics = dict(statistics or {})
    supported_statistics_fields = {
        "features",
        "folds",
        "purge_bars",
        "embargo_bars",
        "model",
        "bootstrap",
        "direct_tests",
        "feature_bins",
        "eligibility",
    }
    unknown_statistics_fields = sorted(
        set(normalized_statistics) - supported_statistics_fields
    )
    if unknown_statistics_fields:
        raise ValueError(
            "event_fact_check_invalid: unsupported statistics fields "
            f"{unknown_statistics_fields}"
        )
    features = _mapping(
        normalized_statistics.get("features"), field="statistics.features"
    )
    unknown_feature_groups = sorted(set(features) - {"baseline", "enriched"})
    if unknown_feature_groups:
        raise ValueError(
            "event_fact_check_invalid: unsupported statistics.features fields "
            f"{unknown_feature_groups}"
        )
    baseline = _list(features.get("baseline"), field="features.baseline")
    enriched = _list(features.get("enriched"), field="features.enriched")
    names: set[str] = set()
    for collection, allowed, requires_alias in (
        (baseline, _BASELINE_OPERATORS, False),
        (enriched, _FACT_OPERATORS | _STRUCTURED_FACT_OPERATORS, True),
    ):
        for raw in collection:
            name = str(raw.get("name") or "").strip()
            operator = str(raw.get("operator") or "").strip().lower()
            if not name or name in names or operator not in allowed:
                raise ValueError(
                    "event_fact_check_invalid: feature names must be unique and operators registered"
                )
            names.add(name)
            raw["name"] = name
            raw["operator"] = operator
            if requires_alias and not str(raw.get("input_alias") or "").strip():
                raise ValueError(
                    f"event_fact_check_invalid: feature {name} requires input_alias"
                )
            if operator in {"direction_signed_return", "lagged_return", "volume_ratio"}:
                raw["lookback_bars"] = _positive_int(
                    raw.get("lookback_bars"), field=f"feature.{name}.lookback_bars"
                )
            if operator == "atr_fraction":
                raw["period"] = _positive_int(
                    raw.get("period") or 14, field=f"feature.{name}.period"
                )
            if operator == "event_metadata_number":
                path = str(raw.get("path") or "").strip()
                if not path.startswith("metadata."):
                    raise ValueError(
                        f"event_fact_check_invalid: feature {name} path must start metadata."
                    )
                raw["scale"] = _finite(
                    raw.get("scale", 1.0), field=f"feature.{name}.scale"
                )
            if operator == "latest_payload_number":
                raw["path"] = _payload_path(
                    raw.get("path"), field=f"feature.{name}.path"
                )
                raw["where"] = _normalize_payload_where(
                    raw.get("where"), field=f"feature.{name}.where"
                )
            if operator in {
                "update_count_window",
                "aggregate_abs_change_bps_window",
            }:
                raw["window_seconds"] = _positive_int(
                    raw.get("window_seconds"), field=f"feature.{name}.window_seconds"
                )
    if detector_type == _FACT_SNAPSHOT_DETECTOR:
        unsupported = sorted(
            str(row["operator"])
            for row in baseline
            if str(row["operator"])
            in {"direction_signed_return", "event_metadata_number", "event_direction"}
        )
        if unsupported:
            raise ValueError(
                "event_fact_check_invalid: fact_snapshot baseline features cannot use "
                f"directional event operators {unsupported}"
            )
        if any(str(row["operator"]) == "update_agreement" for row in enriched):
            raise ValueError(
                "event_fact_check_invalid: fact_snapshot cannot use directional update_agreement"
            )

    model = _mapping(normalized_statistics.get("model"), field="statistics.model")
    folds = _list(normalized_statistics.get("folds"), field="statistics.folds")
    normalized_folds: list[dict[str, Any]] = []
    for index, raw in enumerate(folds):
        train = _mapping(raw.get("train"), field=f"folds[{index}].train")
        validation = _mapping(
            raw.get("validation"), field=f"folds[{index}].validation"
        )
        train_start = _utc(train.get("start"), field=f"folds[{index}].train.start")
        train_end = _utc(train.get("end"), field=f"folds[{index}].train.end")
        validation_start = _utc(
            validation.get("start"), field=f"folds[{index}].validation.start"
        )
        validation_end = _utc(
            validation.get("end"), field=f"folds[{index}].validation.end"
        )
        if not (train_start < train_end <= validation_start < validation_end):
            raise ValueError(
                "event_fact_check_invalid: folds must be ordered walk-forward windows"
            )
        normalized_folds.append(
            {
                "id": str(raw.get("id") or f"fold-{index + 1}"),
                "train": {"start": _iso(train_start), "end": _iso(train_end)},
                "validation": {
                    "start": _iso(validation_start),
                    "end": _iso(validation_end),
                },
            }
        )
    if model and not normalized_folds:
        raise ValueError(
            "event_fact_check_invalid: a model requires walk-forward folds"
        )
    normalized_model = {}
    if model:
        if str(model.get("type") or "standardized_l2_logistic").strip() != "standardized_l2_logistic":
            raise ValueError(
                "event_fact_check_invalid: only standardized_l2_logistic is registered"
            )
        normalized_model = {
            "type": "standardized_l2_logistic",
            "implementation": "qt_scipy_lbfgsb.v1",
            "c": _finite(model.get("c", 1.0), field="statistics.model.c"),
            "fit_intercept": bool(model.get("fit_intercept", True)),
            "tolerance": _finite(
                model.get("tolerance", 1e-9), field="statistics.model.tolerance"
            ),
            "max_iterations": _positive_int(
                model.get("max_iterations", 1000),
                field="statistics.model.max_iterations",
            ),
            "seed": int(model.get("seed", 0)),
            "standardization": "train_fold_mean_population_std.v1",
        }
        if normalized_model["c"] <= 0 or normalized_model["tolerance"] <= 0:
            raise ValueError(
                "event_fact_check_invalid: model c and tolerance must be positive"
            )
    bootstrap = _mapping(
        normalized_statistics.get("bootstrap"), field="statistics.bootstrap"
    )
    normalized_bootstrap = {}
    if bootstrap:
        if str(bootstrap.get("method") or "utc_day_cluster").strip() != "utc_day_cluster":
            raise ValueError(
                "event_fact_check_invalid: only utc_day_cluster bootstrap is registered"
            )
        confidence = _finite(
            bootstrap.get("confidence", 0.95), field="statistics.bootstrap.confidence"
        )
        if not 0 < confidence < 1:
            raise ValueError(
                "event_fact_check_invalid: bootstrap confidence must be between zero and one"
            )
        normalized_bootstrap = {
            "method": "utc_day_cluster",
            "version": "utc_day_cluster_mean.v1",
            "replicates": _positive_int(
                bootstrap.get("replicates", 2000),
                field="statistics.bootstrap.replicates",
            ),
            "confidence": confidence,
            "seed": int(bootstrap.get("seed", 0)),
        }
    eligibility = _mapping(
        normalized_statistics.get("eligibility"), field="statistics.eligibility"
    )
    normalized_eligibility = {
        "min_samples": int(eligibility.get("min_samples") or 0),
        "min_class_count": int(eligibility.get("min_class_count") or 0),
        "min_distinct_utc_days": int(
            eligibility.get("min_distinct_utc_days") or 0
        ),
        "min_validation_samples_per_fold": int(
            eligibility.get("min_validation_samples_per_fold") or 0
        ),
        "min_valid_folds": int(eligibility.get("min_valid_folds") or 0),
    }
    if any(value < 0 for value in normalized_eligibility.values()):
        raise ValueError(
            "event_fact_check_invalid: eligibility thresholds must be nonnegative"
        )
    direct_tests = _mapping(
        normalized_statistics.get("direct_tests"),
        field="statistics.direct_tests",
    )
    normalized_direct_tests = {}
    if direct_tests:
        direct_method = str(
            direct_tests.get("method") or "point_biserial"
        ).strip().lower()
        if direct_method not in {"point_biserial", "pearson"}:
            raise ValueError(
                "event_fact_check_invalid: direct test method is not registered"
            )
        if str(direct_tests.get("multiplicity") or "holm").strip() != "holm":
            raise ValueError(
                "event_fact_check_invalid: only Holm multiplicity is registered"
            )
        default_target = (
            "primary_binary"
            if direct_method == "point_biserial"
            else (
                "primary_forward_return"
                if detector_type == _FACT_SNAPSHOT_DETECTOR
                else "primary_signed_return"
            )
        )
        normalized_direct_tests = {
            "method": direct_method,
            "version": (
                "scipy_pointbiserialr.v1"
                if direct_method == "point_biserial"
                else "scipy_pearsonr.v1"
            ),
            "multiplicity": "holm",
            "target": str(
                direct_tests.get("target")
                or default_target
            ).strip().lower(),
        }
        if normalized_direct_tests["target"] not in {
            "primary_binary",
            "primary_signed_return",
            "primary_forward_return",
        }:
            raise ValueError(
                "event_fact_check_invalid: direct test target is not registered"
            )
        expected_target = default_target
        if normalized_direct_tests["target"] != expected_target:
            raise ValueError(
                "event_fact_check_invalid: direct test method and target disagree"
            )
    feature_bins = _mapping(
        normalized_statistics.get("feature_bins"),
        field="statistics.feature_bins",
    )
    normalized_feature_bins = {}
    if feature_bins:
        if str(feature_bins.get("method") or "pooled_quantiles").strip() != "pooled_quantiles":
            raise ValueError(
                "event_fact_check_invalid: only pooled_quantiles feature bins are registered"
            )
        raw_quantiles = feature_bins.get("quantiles") or [0.25, 0.5, 0.75]
        if not isinstance(raw_quantiles, list):
            raise ValueError(
                "event_fact_check_invalid: feature bin quantiles must be a list"
            )
        quantiles = sorted(
            {_finite(value, field="statistics.feature_bins.quantiles") for value in raw_quantiles}
        )
        if not quantiles or any(value <= 0.0 or value >= 1.0 for value in quantiles):
            raise ValueError(
                "event_fact_check_invalid: feature bin quantiles must be between zero and one"
            )
        normalized_feature_bins = {
            "method": "pooled_quantiles",
            "version": "pooled_quantiles_ties_collapsed.v1",
            "quantiles": quantiles,
            "target": "primary_binary",
            "feature_scope": "enriched",
        }
    purge_bars = int(normalized_statistics.get("purge_bars") or 0)
    embargo_bars = int(normalized_statistics.get("embargo_bars") or 0)
    if purge_bars < 0 or embargo_bars < 0:
        raise ValueError(
            "event_fact_check_invalid: purge_bars and embargo_bars must be nonnegative"
        )
    return (
        normalized_detector,
        normalized_outcomes,
        {
            "features": {"baseline": baseline, "enriched": enriched},
            "folds": normalized_folds,
            "purge_bars": purge_bars,
            "embargo_bars": embargo_bars,
            "model": normalized_model,
            "bootstrap": normalized_bootstrap,
            "direct_tests": normalized_direct_tests,
            "feature_bins": normalized_feature_bins,
            "eligibility": normalized_eligibility,
        },
    )


def _path(payload: Mapping[str, Any], path: str) -> Any:
    current: Any = payload
    for part in str(path).split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


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
) -> dict[tuple[datetime, datetime | None], Any | None]:
    """Select one latest market-time observation in one known-at sweep."""

    revisions = tuple(ordered_revisions) if ordered_revisions is not None else sorted(
        records,
        key=_snapshot_revision_order_key,
    )
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
                    _record_freshness_time(current).timestamp() * 1_000_000
                )
                eligible_by_time[current_time].pop(key, None)
            active[key] = record
            if _record_is_active(record) and _record_matches_where(record, where):
                effective_time = int(
                    _record_freshness_time(record).timestamp() * 1_000_000
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
        if len(candidates) != 1:
            keys = sorted(_record_event_key(record) for record in candidates)
            raise RuntimeError(
                "event_fact_snapshot_ambiguous: "
                f"alias={alias} decision_time={_iso(decision_time)} "
                f"observation_time={_iso(datetime.fromtimestamp(latest_time / 1_000_000, tz=UTC))} "
                f"candidate_count={len(candidates)} observation_keys={keys[:10]}"
            )
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


def _distribution(values: Sequence[float]) -> dict[str, Any]:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    if not finite:
        return {"count": 0, "mean": None, "median": None, "std": None, "min": None, "max": None}
    return {
        "count": len(finite),
        "mean": fmean(finite),
        "median": median(finite),
        "std": float(np.std(np.asarray(finite, dtype=float), ddof=0)),
        "min": min(finite),
        "max": max(finite),
    }


def _auc(y: np.ndarray, probability: np.ndarray) -> float | None:
    positives = int(np.sum(y == 1.0))
    negatives = int(np.sum(y == 0.0))
    if not positives or not negatives:
        return None
    order = np.argsort(probability, kind="mergesort")
    ranks = np.empty(len(probability), dtype=float)
    cursor = 0
    while cursor < len(order):
        end = cursor + 1
        while end < len(order) and probability[order[end]] == probability[order[cursor]]:
            end += 1
        rank = (cursor + 1 + end) / 2.0
        ranks[order[cursor:end]] = rank
        cursor = end
    rank_sum = float(np.sum(ranks[y == 1.0]))
    return (rank_sum - positives * (positives + 1) / 2.0) / (
        positives * negatives
    )


def _fit_logistic(
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_validation: np.ndarray,
    model: Mapping[str, Any],
) -> tuple[np.ndarray, dict[str, Any]]:
    means = np.mean(x_train, axis=0)
    scales = np.std(x_train, axis=0, ddof=0)
    scales = np.where(scales == 0.0, 1.0, scales)
    train = (x_train - means) / scales
    validation = (x_validation - means) / scales
    fit_intercept = bool(model["fit_intercept"])
    design = (
        np.column_stack([np.ones(len(train)), train])
        if fit_intercept
        else train
    )
    validation_design = (
        np.column_stack([np.ones(len(validation)), validation])
        if fit_intercept
        else validation
    )
    c_value = float(model["c"])

    def objective(beta: np.ndarray) -> tuple[float, np.ndarray]:
        logits = np.clip(design @ beta, -40.0, 40.0)
        probabilities = 1.0 / (1.0 + np.exp(-logits))
        epsilon = 1e-15
        loss = -float(
            np.sum(
                y_train * np.log(np.clip(probabilities, epsilon, 1.0))
                + (1.0 - y_train)
                * np.log(np.clip(1.0 - probabilities, epsilon, 1.0))
            )
        )
        penalty_start = 1 if fit_intercept else 0
        penalty = beta.copy()
        penalty[:penalty_start] = 0.0
        loss += 0.5 * float(np.dot(penalty, penalty)) / c_value
        gradient = design.T @ (probabilities - y_train) + penalty / c_value
        return loss, gradient

    optimized = minimize(
        objective,
        np.zeros(design.shape[1], dtype=float),
        method="L-BFGS-B",
        jac=True,
        options={
            "ftol": float(model["tolerance"]),
            "gtol": float(model["tolerance"]),
            "maxiter": int(model["max_iterations"]),
        },
    )
    if not optimized.success:
        raise RuntimeError(
            f"event_fact_logistic_failed: {optimized.message}"
        )
    logits = np.clip(validation_design @ optimized.x, -40.0, 40.0)
    probabilities = 1.0 / (1.0 + np.exp(-logits))
    return probabilities, {
        "iterations": int(optimized.nit),
        "objective": float(optimized.fun),
        "converged": True,
    }


def _log_loss(y: np.ndarray, probability: np.ndarray) -> float:
    p = np.clip(probability, 1e-15, 1.0 - 1e-15)
    return -float(np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def _utc_day_cluster_ci(
    rows: Sequence[Mapping[str, Any]],
    *,
    replicates: int,
    confidence: float,
    seed: int,
) -> dict[str, Any]:
    by_day: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        by_day[str(row["decision_time"])[:10]].append(
            float(row["paired_log_loss_delta"])
        )
    days = sorted(by_day)
    if len(days) < 2:
        return {
            "status": "indeterminate",
            "reason": "fewer_than_two_utc_day_clusters",
            "cluster_count": len(days),
        }
    rng = np.random.default_rng(int(seed))
    estimates: list[float] = []
    for _ in range(int(replicates)):
        sampled_days = rng.choice(days, size=len(days), replace=True)
        values = [value for day in sampled_days for value in by_day[str(day)]]
        estimates.append(fmean(values))
    estimates.sort()
    alpha = (1.0 - float(confidence)) / 2.0
    return {
        "status": "completed",
        "cluster_count": len(days),
        "replicates": int(replicates),
        "confidence": float(confidence),
        "lower": float(np.quantile(estimates, alpha)),
        "upper": float(np.quantile(estimates, 1.0 - alpha)),
    }


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
        event_time = _utc(
            candle.get("open_time") or candle.get("time"),
            field="candle.open_time",
        )
        decision_time = _utc(
            candle.get("known_at") or candle.get("close_time"),
            field="candle.known_at",
        )
        sample_time = _utc(candle.get("close_time"), field="candle.close_time")
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


@dataclass(frozen=True)
class EventFactEvaluator:
    evaluator_id: str = EVENT_FACT_ANALYSIS
    version: str = EVENT_FACT_EVALUATOR_VERSION
    result_schema_version: str = EVENT_FACT_RESULT_VERSION
    fact_snapshot_enabled: bool = True

    def declare_requirements(
        self,
        *,
        definition: CheckDefinition,
        request: CheckRequest,
    ) -> Mapping[str, Any]:
        del definition
        statistics = dict(request.parameters.get("statistics") or {})
        detector = dict(request.parameters.get("detector") or {})
        detector_type = str(detector.get("type") or "").strip()
        if detector_type == _FACT_SNAPSHOT_DETECTOR and not self.fact_snapshot_enabled:
            raise ValueError(
                "event_fact_check_invalid: fact_snapshot requires evaluator version 3"
            )
        features = dict(statistics.get("features") or {})
        baseline = [dict(row) for row in features.get("baseline") or []]
        enriched = [dict(row) for row in features.get("enriched") or []]
        feature_lookback = max(
            (
                int(row.get("lookback_bars") or row.get("period") or 0)
                for row in baseline
            ),
            default=0,
        )
        windows: dict[str, int] = {}
        predecessor_aliases: set[str] = set()
        for row in enriched:
            alias = str(row.get("input_alias") or "")
            windows[alias] = max(
                int(windows.get(alias) or 0),
                int(row.get("window_seconds") or 0),
            )
            if str(row.get("operator") or "") in {
                "latest_update_direction",
                "latest_update_magnitude_bps",
                "update_agreement",
                "aggregate_abs_change_bps_window",
            }:
                predecessor_aliases.add(alias)
        indicator_ids: list[str] = []
        if detector_type == _INDICATOR_EVENT_DETECTOR:
            indicator_id = str(request.scope.get("indicator_id") or "").strip()
            if not indicator_id:
                raise ValueError(
                    "check_requirement_plan_invalid: indicator_id is required"
                )
            indicator_ids.append(indicator_id)
        outcomes = dict(request.parameters.get("outcomes") or {})
        invalidation = dict(outcomes.get("invalidation") or {})
        return {
            "input_kind": "market_data",
            "indicator_ids": indicator_ids,
            "warmup_floor_bars": 0,
            "feature_lookback_bars": feature_lookback,
            "feature_windows_seconds_by_alias": windows,
            "feature_predecessor_aliases": sorted(predecessor_aliases),
            "outcome_horizons": list(outcomes.get("horizons") or []),
            "required_outcome_horizons": list(
                outcomes.get("required_horizons") or outcomes.get("horizons") or []
            ),
            "horizon_kind": str(outcomes.get("horizon_kind") or "bars"),
            "entry_lag_bars": int(outcomes.get("entry_lag_bars") or 0),
            "invalidation_max_bars": int(invalidation.get("max_bars") or 0),
            "event_source": (
                "indicator"
                if detector_type == _INDICATOR_EVENT_DETECTOR
                else "check_fact_snapshot"
            ),
            "fact_history_required": bool(enriched)
            or detector_type == _FACT_SNAPSHOT_DETECTOR,
        }

    def evaluate(
        self,
        *,
        plan: ResolvedCheckPlan,
        inputs: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        detector = dict(inputs.get("detector") or {})
        detector_type = str(detector.get("type") or "").strip()
        if detector_type == _FACT_SNAPSHOT_DETECTOR and not self.fact_snapshot_enabled:
            raise ValueError(
                "event_fact_check_invalid: fact_snapshot requires evaluator version 3"
            )
        outcomes = dict(inputs.get("outcomes") or {})
        statistics = dict(inputs.get("statistics") or {})
        indicator_evidence = dict(inputs.get("indicator_evidence") or {})
        fact_records = {
            str(alias): tuple(rows)
            for alias, rows in dict(inputs.get("fact_records_by_alias") or {}).items()
        }
        fact_requirements = {
            str(alias): dict(raw)
            for alias, raw in dict(inputs.get("fact_requirements_by_alias") or {}).items()
        }
        gap_transitions = [
            dict(row) for row in indicator_evidence.get("gap_transitions") or []
        ]
        data_quality = dict(inputs.get("data_quality") or {})
        relevant_aliases = {
            "primary_bars",
            str(detector.get("input_alias") or ""),
            *(
                str(row.get("input_alias") or "")
                for row in (statistics.get("features") or {}).get("enriched") or []
            ),
        }
        fact_gap_evidence = [
            dict(row)
            for row in data_quality.get("recorded_gaps") or []
            if detector_type == _FACT_SNAPSHOT_DETECTOR
            and (
                not str(row.get("alias") or "").strip()
                or str(row.get("alias") or "") in relevant_aliases
            )
        ]
        gap_rejection = inputs.get("indicator_gap_rejection")
        if (
            detector_type == _FACT_SNAPSHOT_DETECTOR
            and plan.gap_policy == GAP_POLICY_REJECT
            and fact_gap_evidence
        ):
            gap_rejection = {
                "policy": GAP_POLICY_REJECT,
                "reason": "recorded_fact_gap",
                "recorded_gap_count": len(fact_gap_evidence),
                "gaps": fact_gap_evidence[:10],
            }
        gap_count = (
            len(fact_gap_evidence)
            if detector_type == _FACT_SNAPSHOT_DETECTOR
            else len(gap_transitions) + (1 if gap_rejection else 0)
        )
        if gap_rejection:
            return {
                "schema_version": self.result_schema_version,
                "check_family": EVENT_FACT_ANALYSIS,
                "status": "blocked",
                "analysis_status": "gap_policy_rejected",
                **(
                    {"event_ownership": "check"}
                    if detector_type == _FACT_SNAPSHOT_DETECTOR
                    else {}
                ),
                "gap_decision": {
                    "policy": GAP_POLICY_REJECT,
                    "recorded_gap_count": gap_count,
                    **(
                        {"check_action": "rejected_before_sample_emission"}
                        if detector_type == _FACT_SNAPSHOT_DETECTOR
                        else {"indicator_action": "rejected_before_event_emission"}
                    ),
                    "rejection": dict(gap_rejection),
                },
                "sample_count": 0,
                "events": [],
                "outcome_resolution": {},
                "statistics": {},
                "caveats": ["Recorded Dataset gaps intersect the bound evidence."],
            }

        candle_field = (
            "indicator_evidence.candles"
            if detector_type == _INDICATOR_EVENT_DETECTOR
            else "candles"
        )
        candles = _candle_rows(
            indicator_evidence.get("candles")
            if detector_type == _INDICATOR_EVENT_DETECTOR
            else inputs.get("candles"),
            field=candle_field,
        )
        candle_by_open = {
            _utc(row.get("open_time") or row.get("time"), field="candle.open_time"): row
            for row in candles
        }
        interval_seconds = int(plan.warmup.get("timeframe_seconds") or 0)
        if interval_seconds <= 0:
            raise ValueError(
                "event_fact_check_invalid: planned timeframe is missing"
            )
        evaluation_start = _utc(
            plan.evaluation_range["start"], field="evaluation_start"
        )
        evaluation_end = _utc(
            plan.evaluation_range["end_exclusive"], field="evaluation_end"
        )
        baseline_specs = list(
            (statistics.get("features") or {}).get("baseline") or []
        )
        enriched_specs = list(
            (statistics.get("features") or {}).get("enriched") or []
        )
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
            event_rows, detector_snapshots = _fact_snapshot_outputs(
                detector=detector,
                candles=candles,
                records_by_alias=fact_records,
                requirements=fact_requirements,
                gap_evidence=fact_gap_evidence,
                evaluation_start=evaluation_start,
                evaluation_end=evaluation_end,
                ordered_revisions_by_alias=ordered_revisions_by_alias,
            )
        else:
            raise ValueError(
                f"event_fact_check_invalid: unsupported detector type {detector_type or '<empty>'}"
            )

        atr_by_period: dict[int, dict[datetime, float]] = {}
        if any(str(row.get("operator")) == "atr_fraction" for row in baseline_specs):
            frame = pd.DataFrame(candles)
            frame.index = pd.to_datetime(
                [row.get("open_time") or row.get("time") for row in candles], utc=True
            )
            for period in sorted(
                {int(row["period"]) for row in baseline_specs if row["operator"] == "atr_fraction"}
            ):
                enriched_frame = compute_tr_atr(frame.copy(), period=period)
                atr_by_period[period] = {
                    timestamp.to_pydatetime(): float(value)
                    for timestamp, value in enriched_frame["atr_wilder"].items()
                    if pd.notna(value)
                }

        selected_fact_material: dict[tuple[Any, ...], dict[str, Any]] = {}
        exclusion_counts: Counter[str] = Counter()
        event_results: list[dict[str, Any]] = []
        horizon_resolution: dict[int, Counter[str]] = {
            int(horizon): Counter() for horizon in outcomes["horizons"]
        }
        invalidation_resolution: Counter[str] = Counter()
        entry_lag = int(outcomes.get("entry_lag_bars") or 0)
        step = timedelta(seconds=interval_seconds)
        for output in event_rows:
            event_time = _utc(output.get("time"), field="event.time")
            event_payload = dict(output.get("event") or {})
            event_decision_time = _known_at(
                event_payload.get("known_at")
                or output.get("known_at")
                or candle_by_open.get(event_time, {}).get("known_at")
                or candle_by_open.get(event_time, {}).get("close_time"),
                field="event.known_at",
            )
            if not (
                evaluation_start <= event_time < evaluation_end
                and event_decision_time <= evaluation_end
            ):
                continue
            direction_text = str(event_payload.get("direction") or "").strip().lower()
            if detector_type == _INDICATOR_EVENT_DETECTOR:
                configured_direction = direction_by_key[str(output.get("event_key"))]
                if direction_text not in {"long", "short"}:
                    raise RuntimeError(
                        "event_fact_event_direction_missing: Indicator event must own long/short direction"
                    )
                if direction_text != configured_direction:
                    raise RuntimeError(
                        "event_fact_event_direction_mismatch: configured direction cannot relabel Indicator evidence"
                    )
                direction = 1.0 if direction_text == "long" else -1.0
            else:
                if direction_text != "neutral":
                    raise RuntimeError(
                        "event_fact_event_ownership_invalid: Check-owned fact snapshots must be neutral"
                    )
                direction = 1.0
            entry_time = event_time + step * entry_lag
            event_candle = candle_by_open.get(event_time)
            entry = candle_by_open.get(entry_time)
            row: dict[str, Any] = {
                "event_time": _iso(event_time),
                "decision_time": _iso(event_decision_time),
                "entry_time": None,
                "entry_known_at": None,
                "event_key": output.get("event_key"),
                "direction": direction_text,
                "indicator_id": (
                    output.get("indicator_id")
                    if detector_type == _INDICATOR_EVENT_DETECTOR
                    else None
                ),
                "indicator_output": output_name,
                "event": dict(output.get("event") or {}),
                "features": {},
                "outcomes": {},
                "eligible": True,
                "exclusion_reasons": [],
            }
            if detector_type == _FACT_SNAPSHOT_DETECTOR:
                row["detector_input_alias"] = detector.get("input_alias")
                detector_exclusion = str(
                    output.get("detector_exclusion_reason") or ""
                ).strip()
                detector_record = output.get("detector_record")
                if detector_exclusion:
                    row["eligible"] = False
                    row["exclusion_reasons"].append(detector_exclusion)
                    exclusion_counts[detector_exclusion] += 1
                if detector_record is not None:
                    detector_alias = str(detector["input_alias"])
                    detector_material = {
                        "alias": detector_alias,
                        "role": "detector",
                        **_fact_material(detector_record),
                    }
                    row["detector_fact_reference"] = detector_material
                    selected_fact_material[
                        _selected_record_key(detector_alias, detector_record)
                    ] = detector_material
                else:
                    row["detector_fact_reference"] = None
            entry_path_missing = [
                event_time + step * offset
                for offset in range(0, entry_lag + 1)
                if event_time + step * offset not in candle_by_open
            ]
            if event_candle is None or entry is None or entry_path_missing:
                entry_reason = (
                    "entry_bar_missing"
                    if event_candle is None or entry is None
                    else "gap_before_delayed_entry"
                )
                row["eligible"] = False
                row["exclusion_reasons"].append(entry_reason)
                exclusion_counts[entry_reason] += 1
                for horizon in outcomes["horizons"]:
                    row["outcomes"][str(horizon)] = {
                        "status": "unresolved",
                        "reason": entry_reason,
                        "horizon_kind": outcomes["horizon_kind"],
                    }
                    horizon_resolution[int(horizon)][f"unresolved:{entry_reason}"] += 1
                row["population_eligible"] = False
                row["analysis_eligible"] = False
                row["fact_references"] = {}
                event_results.append(row)
                continue
            entry_known_at = _utc(
                entry.get("known_at") or entry.get("close_time"),
                field="entry.known_at",
            )
            entry_close_time = _utc(
                entry.get("close_time") or entry.get("known_at"),
                field="entry.close_time",
            )
            row["entry_time"] = _iso(entry_time)
            row["entry_known_at"] = _iso(entry_known_at)
            entry_close = float(entry["close"])
            event_close = float(event_candle["close"])
            row["entry_price"] = entry_close
            row["event_price"] = event_close

            for horizon in outcomes["horizons"]:
                horizon_value = int(horizon)
                if outcomes["horizon_kind"] == "bars":
                    target_open = entry_time + step * horizon_value
                    required_times = [
                        entry_time + step * offset
                        for offset in range(1, horizon_value + 1)
                    ]
                    missing_times = [
                        value for value in required_times if value not in candle_by_open
                    ]
                    target = candle_by_open.get(target_open)
                    if target is None:
                        reason = (
                            "outside_materialized_range"
                            if target_open >= evaluation_end + step * max(outcomes["horizons"])
                            else "target_bar_missing"
                        )
                    elif missing_times:
                        reason = "gap_in_horizon"
                    else:
                        reason = None
                else:
                    target_close = entry_close_time + timedelta(seconds=horizon_value)
                    matches = [
                        candidate
                        for candidate in candles
                        if _utc(
                            candidate.get("close_time") or candidate.get("known_at"),
                            field="candle.close_time",
                        )
                        == target_close
                    ]
                    target = matches[0] if len(matches) == 1 else None
                    if target is None:
                        reason = "target_time_unavailable"
                        required_times = []
                    else:
                        target_open = _utc(
                            target.get("open_time") or target.get("time"),
                            field="target.open_time",
                        )
                        required_times = []
                        cursor = entry_time + step
                        while cursor <= target_open:
                            required_times.append(cursor)
                            cursor += step
                        missing_times = [
                            value
                            for value in required_times
                            if value not in candle_by_open
                        ]
                        reason = "gap_in_horizon" if missing_times else None
                if reason is not None or target is None:
                    row["outcomes"][str(horizon_value)] = {
                        "status": "unresolved",
                        "reason": reason,
                        "horizon_kind": outcomes["horizon_kind"],
                    }
                    horizon_resolution[horizon_value][f"unresolved:{reason}"] += 1
                    continue
                forward_return = (float(target["close"]) / entry_close) - 1.0
                signed_return = (
                    forward_return * direction
                    if detector_type == _INDICATOR_EVENT_DETECTOR
                    else forward_return
                )
                path_rows = (
                    [candle_by_open[value] for value in required_times]
                    if required_times
                    else [target]
                )
                if detector_type == _FACT_SNAPSHOT_DETECTOR:
                    favorable = (
                        max(float(value["high"]) for value in path_rows)
                        / entry_close
                    ) - 1.0
                    adverse = (
                        min(float(value["low"]) for value in path_rows)
                        / entry_close
                    ) - 1.0
                elif direction > 0:
                    favorable = (
                        max(float(value["high"]) for value in path_rows)
                        / entry_close
                    ) - 1.0
                    adverse = (
                        min(float(value["low"]) for value in path_rows)
                        / entry_close
                    ) - 1.0
                else:
                    favorable = 1.0 - (
                        min(float(value["low"]) for value in path_rows)
                        / entry_close
                    )
                    adverse = 1.0 - (
                        max(float(value["high"]) for value in path_rows)
                        / entry_close
                    )
                row["outcomes"][str(horizon_value)] = {
                    "status": "resolved",
                    "reason": None,
                    "horizon_kind": outcomes["horizon_kind"],
                    "target_time": str(target.get("open_time") or target.get("time")),
                    "forward_return": forward_return,
                    "direction_signed_forward_return": signed_return,
                    "maximum_favorable_excursion": favorable,
                    "maximum_adverse_excursion": adverse,
                    "positive": (
                        signed_return > 0.0
                        if detector_type == _INDICATOR_EVENT_DETECTOR
                        else forward_return > 0.0
                    ),
                }
                horizon_resolution[horizon_value]["resolved"] += 1

            invalidation = dict(outcomes.get("invalidation") or {})
            if invalidation:
                reference = _path(event_payload, str(invalidation["reference_path"]))
                if reference is None:
                    row["invalidation"] = {
                        "status": "unresolved",
                        "reason": "event_reference_missing",
                        "invalidated": None,
                        "time_to_invalidation_bars": None,
                    }
                    invalidation_resolution["unresolved:event_reference_missing"] += 1
                else:
                    reference_value = float(reference)
                    max_bars = int(invalidation["max_bars"])
                    path = [
                        candle_by_open.get(entry_time + step * offset)
                        for offset in range(1, max_bars + 1)
                    ]
                    if any(candidate is None for candidate in path):
                        row["invalidation"] = {
                            "status": "unresolved",
                            "reason": "gap_in_invalidation_window",
                            "invalidated": None,
                            "time_to_invalidation_bars": None,
                            "reference_price": reference_value,
                        }
                        invalidation_resolution[
                            "unresolved:gap_in_invalidation_window"
                        ] += 1
                    else:
                        first = next(
                            (
                                offset
                                for offset, candidate in enumerate(path, start=1)
                                if (
                                    float(candidate["close"]) <= reference_value
                                    if direction > 0
                                    else float(candidate["close"]) >= reference_value
                                )
                            ),
                            None,
                        )
                        row["invalidation"] = {
                            "status": "resolved",
                            "reason": None,
                            "invalidated": first is not None,
                            "time_to_invalidation_bars": first,
                            "reference_price": reference_value,
                            "max_bars": max_bars,
                        }
                        invalidation_resolution[
                            "resolved:invalidated" if first is not None else "resolved:not_invalidated"
                        ] += 1

            for spec in baseline_specs:
                name = str(spec["name"])
                operator = str(spec["operator"])
                value: float | None
                if operator == "event_direction":
                    value = direction
                elif operator == "event_metadata_number":
                    raw_value = _path(dict(output.get("event") or {}), str(spec["path"]))
                    value = (
                        float(raw_value) * float(spec.get("scale", 1.0))
                        if raw_value is not None
                        else None
                    )
                elif operator in {"direction_signed_return", "lagged_return"}:
                    lookback = int(spec["lookback_bars"])
                    previous_time = event_time - step * lookback
                    previous = candle_by_open.get(previous_time)
                    local_times = [
                        event_time - step * offset
                        for offset in range(1, lookback + 1)
                    ]
                    raw_return = (
                        (event_close / float(previous["close"])) - 1.0
                        if previous is not None
                        and all(item in candle_by_open for item in local_times)
                        else None
                    )
                    value = (
                        raw_return * direction
                        if raw_return is not None
                        and operator == "direction_signed_return"
                        else raw_return
                    )
                elif operator == "atr_fraction":
                    atr = atr_by_period[int(spec["period"])].get(event_time)
                    value = atr / event_close if atr is not None and event_close else None
                elif operator == "volume_ratio":
                    lookback = int(spec["lookback_bars"])
                    history = [
                        candle_by_open.get(event_time - step * offset)
                        for offset in range(1, lookback + 1)
                    ]
                    volumes = [
                        float(item.get("volume") or 0.0)
                        for item in history
                        if item is not None
                    ]
                    denominator = fmean(volumes) if len(volumes) == lookback else 0.0
                    value = (
                        float(event_candle.get("volume") or 0.0) / denominator
                        if denominator > 0.0
                        else None
                    )
                else:  # pragma: no cover - normalization makes this unreachable.
                    raise AssertionError(operator)
                row["features"][name] = value

            unresolved_required = [
                int(horizon)
                for horizon in outcomes["required_horizons"]
                if (row["outcomes"].get(str(horizon)) or {}).get("status")
                != "resolved"
            ]
            for horizon in unresolved_required:
                outcome = row["outcomes"].get(str(horizon)) or {}
                reason = (
                    f"required_outcome_unresolved:{horizon}:{outcome.get('reason')}"
                )
                row["eligible"] = False
                row["exclusion_reasons"].append(reason)
                exclusion_counts[reason] += 1
            if any(
                row["features"].get(str(spec["name"])) is None
                for spec in baseline_specs
            ):
                row["eligible"] = False
                if "baseline_feature_incomplete" not in row["exclusion_reasons"]:
                    row["exclusion_reasons"].append("baseline_feature_incomplete")
                    exclusion_counts["baseline_feature_incomplete"] += 1
            row["population_eligible"] = bool(row["eligible"])
            row["analysis_eligible"] = False
            row["fact_references"] = {}
            event_results.append(row)

        population_rows = [
            row for row in event_results if row["population_eligible"]
        ]
        primary_key = str(outcomes["primary_horizon"])
        positives = sum(
            1
            for row in population_rows
            if bool(row["outcomes"][primary_key]["positive"])
        )
        negatives = len(population_rows) - positives
        distinct_days = len(
            {str(row["decision_time"])[:10] for row in population_rows}
        )

        eligibility = dict(statistics.get("eligibility") or {})
        folds = list(statistics.get("folds") or [])
        eligibility_reasons: list[str] = []
        if len(population_rows) < int(eligibility.get("min_samples") or 0):
            eligibility_reasons.append("minimum_sample_count_not_met")
        if min(positives, negatives) < int(
            eligibility.get("min_class_count") or 0
        ):
            eligibility_reasons.append("minimum_class_count_not_met")
        if distinct_days < int(eligibility.get("min_distinct_utc_days") or 0):
            eligibility_reasons.append("minimum_distinct_utc_days_not_met")

        minimum_validation = int(
            eligibility.get("min_validation_samples_per_fold") or 0
        )
        population_fold_counts = [
            {
                "id": fold["id"],
                "validation_count": sum(
                    1
                    for row in population_rows
                    if _utc(
                        fold["validation"]["start"],
                        field="fold.validation.start",
                    )
                    <= _utc(row["decision_time"], field="decision_time")
                    < _utc(
                        fold["validation"]["end"],
                        field="fold.validation.end",
                    )
                ),
            }
            for fold in folds
        ]
        population_valid_fold_count = sum(
            1
            for row in population_fold_counts
            if int(row["validation_count"]) >= minimum_validation
        )
        if population_valid_fold_count < int(
            eligibility.get("min_valid_folds") or 0
        ):
            eligibility_reasons.append("minimum_population_folds_not_met")

        structured_snapshots: dict[
            tuple[str, str],
            dict[tuple[datetime, datetime | None], Any | None],
        ] = (
            {
                _structured_snapshot_key(
                    str(detector["input_alias"]),
                    dict(detector.get("where") or {}),
                ): detector_snapshots
            }
            if detector_type == _FACT_SNAPSHOT_DETECTOR
            else {}
        )
        for spec in enriched_specs:
            if str(spec.get("operator") or "") not in _STRUCTURED_FACT_OPERATORS:
                continue
            alias = str(spec["input_alias"])
            where = dict(spec.get("where") or {})
            key = _structured_snapshot_key(alias, where)
            if key not in structured_snapshots:
                requirement = fact_requirements.get(alias) or {}
                exact_alignment = (
                    str(requirement.get("alignment") or "") == "exact_interval"
                )
                structured_snapshots[key] = _causal_snapshot_series(
                    tuple(fact_records.get(alias) or ()),
                    snapshot_requests=[
                        (
                            _utc(row["decision_time"], field="decision_time"),
                            (
                                _utc(row["event_time"], field="event_time")
                                + step
                                if exact_alignment
                                else None
                            ),
                        )
                        for row in population_rows
                    ],
                    where=where,
                    alias=alias,
                    ordered_revisions=ordered_revisions_by_alias.get(alias),
                )

        if not eligibility_reasons:
            for row in population_rows:
                features, references, reasons, selected = _fact_features_for_event(
                    specs=enriched_specs,
                    requirements=fact_requirements,
                    records_by_alias=fact_records,
                    structured_snapshots=structured_snapshots,
                    gap_evidence=fact_gap_evidence,
                    decision_time=_utc(
                        row["decision_time"], field="decision_time"
                    ),
                    sample_time=(
                        _utc(row["event_time"], field="event_time") + step
                    ),
                    venue_close=float(row["event_price"]),
                    direction=1.0 if row["direction"] == "long" else -1.0,
                )
                row["features"].update(features)
                row["fact_references"] = references
                selected_fact_material.update(selected)
                for reason in reasons:
                    if reason not in row["exclusion_reasons"]:
                        row["exclusion_reasons"].append(reason)
                        exclusion_counts[reason] += 1
                if reasons or any(
                    row["features"].get(str(spec["name"])) is None
                    for spec in enriched_specs
                ):
                    row["eligible"] = False
                    row["analysis_eligible"] = False
                    if "fact_feature_incomplete" not in row["exclusion_reasons"]:
                        row["exclusion_reasons"].append("fact_feature_incomplete")
                        exclusion_counts["fact_feature_incomplete"] += 1
                else:
                    row["analysis_eligible"] = True
        else:
            for row in event_results:
                row["eligible"] = False
                row["analysis_eligible"] = False

        complete_rows = [
            row for row in event_results if row["analysis_eligible"]
        ]
        if enriched_specs and not eligibility_reasons:
            complete_positives = sum(
                1
                for row in complete_rows
                if bool(row["outcomes"][primary_key]["positive"])
            )
            complete_negatives = len(complete_rows) - complete_positives
            complete_days = len(
                {str(row["decision_time"])[:10] for row in complete_rows}
            )
            if len(complete_rows) < int(eligibility.get("min_samples") or 0):
                eligibility_reasons.append("minimum_complete_sample_count_not_met")
            if min(complete_positives, complete_negatives) < int(
                eligibility.get("min_class_count") or 0
            ):
                eligibility_reasons.append("minimum_complete_class_count_not_met")
            if complete_days < int(
                eligibility.get("min_distinct_utc_days") or 0
            ):
                eligibility_reasons.append("minimum_complete_distinct_days_not_met")
        baseline_names = [str(row["name"]) for row in baseline_specs]
        enriched_feature_names = [str(row["name"]) for row in enriched_specs]
        distributions = {
            name: _distribution(
                [float(row["features"][name]) for row in population_rows]
            )
            for name in baseline_names
        }
        if not eligibility_reasons:
            distributions.update(
                {
                    name: _distribution(
                        [float(row["features"][name]) for row in complete_rows]
                    )
                    for name in enriched_feature_names
                }
            )

        statistical_result: dict[str, Any] = {
            "feature_distributions": distributions,
            "feature_bins": [],
            "model": None,
            "direct_tests": [],
        }
        model = dict(statistics.get("model") or {})
        fold_results: list[dict[str, Any]] = []
        paired_rows: list[dict[str, Any]] = []
        if model and not eligibility_reasons:
            enriched_names = [
                *baseline_names,
                *enriched_feature_names,
            ]
            primary_horizon_seconds = (
                int(outcomes["primary_horizon"]) * interval_seconds
                if outcomes["horizon_kind"] == "bars"
                else int(outcomes["primary_horizon"])
            )
            for fold in folds:
                train_start = _utc(fold["train"]["start"], field="fold.train.start")
                train_end = _utc(fold["train"]["end"], field="fold.train.end")
                validation_start = _utc(
                    fold["validation"]["start"], field="fold.validation.start"
                )
                validation_end = _utc(
                    fold["validation"]["end"], field="fold.validation.end"
                )
                purge_bars = int(statistics.get("purge_bars") or 0)
                embargo_bars = int(statistics.get("embargo_bars") or 0)
                effective_train_end = min(
                    train_end,
                    validation_start - timedelta(seconds=purge_bars * interval_seconds),
                )
                effective_validation_start = max(
                    validation_start,
                    train_end + timedelta(seconds=embargo_bars * interval_seconds),
                )
                train_rows = [
                    row
                    for row in complete_rows
                    if train_start
                    <= _utc(row["decision_time"], field="decision_time")
                    < effective_train_end
                    and _utc(
                        row.get("entry_time") or row["event_time"],
                        field="entry_time",
                    )
                    + timedelta(seconds=primary_horizon_seconds)
                    <= effective_validation_start
                ]
                validation_rows = [
                    row
                    for row in complete_rows
                    if effective_validation_start
                    <= _utc(row["decision_time"], field="decision_time")
                    < validation_end
                ]
                fold_result: dict[str, Any] = {
                    "id": fold["id"],
                    "train_count": len(train_rows),
                    "validation_count": len(validation_rows),
                    "status": "invalid",
                    "reason": None,
                    "purge_bars": purge_bars,
                    "embargo_bars": embargo_bars,
                    "effective_train_end": _iso(effective_train_end),
                    "effective_validation_start": _iso(
                        effective_validation_start
                    ),
                }
                train_labels = {
                    bool(row["outcomes"][primary_key]["positive"])
                    for row in train_rows
                }
                if len(validation_rows) < minimum_validation:
                    fold_result["reason"] = "minimum_validation_samples_not_met"
                elif len(train_labels) < 2:
                    fold_result["reason"] = "training_class_missing"
                elif not baseline_names or not enriched_names:
                    fold_result["reason"] = "feature_set_empty"
                else:
                    y_train = np.asarray(
                        [
                            1.0 if row["outcomes"][primary_key]["positive"] else 0.0
                            for row in train_rows
                        ],
                        dtype=float,
                    )
                    y_validation = np.asarray(
                        [
                            1.0 if row["outcomes"][primary_key]["positive"] else 0.0
                            for row in validation_rows
                        ],
                        dtype=float,
                    )
                    baseline_train = np.asarray(
                        [[row["features"][name] for name in baseline_names] for row in train_rows],
                        dtype=float,
                    )
                    baseline_validation = np.asarray(
                        [[row["features"][name] for name in baseline_names] for row in validation_rows],
                        dtype=float,
                    )
                    enriched_train = np.asarray(
                        [[row["features"][name] for name in enriched_names] for row in train_rows],
                        dtype=float,
                    )
                    enriched_validation = np.asarray(
                        [[row["features"][name] for name in enriched_names] for row in validation_rows],
                        dtype=float,
                    )
                    baseline_probability, baseline_fit = _fit_logistic(
                        baseline_train, y_train, baseline_validation, model
                    )
                    enriched_probability, enriched_fit = _fit_logistic(
                        enriched_train, y_train, enriched_validation, model
                    )
                    baseline_loss = _log_loss(y_validation, baseline_probability)
                    enriched_loss = _log_loss(y_validation, enriched_probability)
                    baseline_brier = float(
                        np.mean((baseline_probability - y_validation) ** 2)
                    )
                    enriched_brier = float(
                        np.mean((enriched_probability - y_validation) ** 2)
                    )
                    baseline_auc = _auc(y_validation, baseline_probability)
                    enriched_auc = _auc(y_validation, enriched_probability)
                    fold_result.update(
                        {
                            "status": "valid",
                            "baseline": {
                                "log_loss": baseline_loss,
                                "brier": baseline_brier,
                                "roc_auc": baseline_auc,
                                "fit": baseline_fit,
                            },
                            "enriched": {
                                "log_loss": enriched_loss,
                                "brier": enriched_brier,
                                "roc_auc": enriched_auc,
                                "fit": enriched_fit,
                            },
                            "delta_log_loss": baseline_loss - enriched_loss,
                            "delta_brier": baseline_brier - enriched_brier,
                            "delta_roc_auc": (
                                enriched_auc - baseline_auc
                                if baseline_auc is not None
                                and enriched_auc is not None
                                else None
                            ),
                        }
                    )
                    for index, source_row in enumerate(validation_rows):
                        actual = float(y_validation[index])
                        baseline_p = float(baseline_probability[index])
                        enriched_p = float(enriched_probability[index])
                        paired_rows.append(
                            {
                                "event_time": source_row["event_time"],
                                "decision_time": source_row["decision_time"],
                                "actual": actual,
                                "baseline_probability": baseline_p,
                                "enriched_probability": enriched_p,
                                "paired_log_loss_delta": (
                                    -actual * math.log(max(baseline_p, 1e-15))
                                    - (1.0 - actual)
                                    * math.log(max(1.0 - baseline_p, 1e-15))
                                    + actual * math.log(max(enriched_p, 1e-15))
                                    + (1.0 - actual)
                                    * math.log(max(1.0 - enriched_p, 1e-15))
                                ),
                            }
                        )
                fold_results.append(fold_result)
            valid_folds = [row for row in fold_results if row["status"] == "valid"]
            if len(valid_folds) < int(eligibility.get("min_valid_folds") or 0):
                eligibility_reasons.append("minimum_valid_folds_not_met")
            if paired_rows and not eligibility_reasons:
                y_all = np.asarray([row["actual"] for row in paired_rows], dtype=float)
                baseline_all = np.asarray(
                    [row["baseline_probability"] for row in paired_rows], dtype=float
                )
                enriched_all = np.asarray(
                    [row["enriched_probability"] for row in paired_rows], dtype=float
                )
                bootstrap = dict(statistics.get("bootstrap") or {})
                aggregate_baseline_brier = float(
                    np.mean((baseline_all - y_all) ** 2)
                )
                aggregate_enriched_brier = float(
                    np.mean((enriched_all - y_all) ** 2)
                )
                aggregate_baseline_auc = _auc(y_all, baseline_all)
                aggregate_enriched_auc = _auc(y_all, enriched_all)
                statistical_result["model"] = {
                    "implementation": dict(model),
                    "baseline_features": baseline_names,
                    "enriched_features": enriched_names,
                    "folds": fold_results,
                    "valid_fold_count": len(valid_folds),
                    "positive_delta_fold_count": sum(
                        1
                        for row in valid_folds
                        if float(row.get("delta_log_loss") or 0.0) > 0.0
                    ),
                    "oos_count": len(paired_rows),
                    "baseline": {
                        "log_loss": _log_loss(y_all, baseline_all),
                        "brier": aggregate_baseline_brier,
                        "roc_auc": aggregate_baseline_auc,
                    },
                    "enriched": {
                        "log_loss": _log_loss(y_all, enriched_all),
                        "brier": aggregate_enriched_brier,
                        "roc_auc": aggregate_enriched_auc,
                    },
                    "delta_log_loss": fmean(
                        [row["paired_log_loss_delta"] for row in paired_rows]
                    ),
                    "delta_brier": (
                        aggregate_baseline_brier - aggregate_enriched_brier
                    ),
                    "delta_roc_auc": (
                        aggregate_enriched_auc - aggregate_baseline_auc
                        if aggregate_baseline_auc is not None
                        and aggregate_enriched_auc is not None
                        else None
                    ),
                    "bootstrap": (
                        _utc_day_cluster_ci(
                            paired_rows,
                            replicates=int(bootstrap["replicates"]),
                            confidence=float(bootstrap["confidence"]),
                            seed=int(bootstrap["seed"]),
                        )
                        if bootstrap
                        else None
                    ),
                    "prediction_hash": semantic_hash(
                        {"paired_predictions": paired_rows}
                    ),
                }

        bin_config = dict(statistics.get("feature_bins") or {})
        if bin_config and complete_rows and not eligibility_reasons:
            for spec in enriched_specs:
                name = str(spec["name"])
                values = np.asarray(
                    [float(row["features"][name]) for row in complete_rows],
                    dtype=float,
                )
                edges = sorted(
                    {
                        float(value)
                        for value in np.quantile(
                            values,
                            list(bin_config["quantiles"]),
                            method="linear",
                        ).tolist()
                    }
                )
                bins: list[dict[str, Any]] = []
                for index in range(len(edges) + 1):
                    members = [
                        row
                        for row in complete_rows
                        if int(
                            np.searchsorted(
                                np.asarray(edges, dtype=float),
                                float(row["features"][name]),
                                side="right",
                            )
                        )
                        == index
                    ]
                    positives_in_bin = sum(
                        1
                        for row in members
                        if bool(row["outcomes"][primary_key]["positive"])
                    )
                    bins.append(
                        {
                            "index": index,
                            "lower_inclusive": edges[index - 1] if index else None,
                            "upper_exclusive": edges[index] if index < len(edges) else None,
                            "count": len(members),
                            "positive_count": positives_in_bin,
                            "success_rate": (
                                positives_in_bin / len(members) if members else None
                            ),
                        }
                    )
                statistical_result["feature_bins"].append(
                    {
                        "feature": name,
                        "method": dict(bin_config),
                        "collapsed_edges": edges,
                        "bins": bins,
                    }
                )

        direct_config = dict(statistics.get("direct_tests") or {})
        if direct_config and complete_rows and not eligibility_reasons:
            y = np.asarray(
                [
                    (
                        1.0 if row["outcomes"][primary_key]["positive"] else 0.0
                    )
                    if direct_config["target"] == "primary_binary"
                    else float(
                        row["outcomes"][primary_key][
                            (
                                "forward_return"
                                if direct_config["target"]
                                == "primary_forward_return"
                                else "direction_signed_forward_return"
                            )
                        ]
                    )
                    for row in complete_rows
                ],
                dtype=float,
            )
            raw_tests: list[dict[str, Any]] = []
            for spec in enriched_specs:
                name = str(spec["name"])
                values = np.asarray(
                    [float(row["features"][name]) for row in complete_rows],
                    dtype=float,
                )
                if len(set(values.tolist())) < 2 or len(set(y.tolist())) < 2:
                    effect, p_value = 0.0, 1.0
                else:
                    result = (
                        pointbiserialr(y, values)
                        if direct_config["method"] == "point_biserial"
                        else pearsonr(y, values)
                    )
                    effect = float(result.statistic)
                    p_value = float(result.pvalue)
                raw_tests.append(
                    {"feature": name, "effect": effect, "p_value": p_value}
                )
            adjusted = adjusted_p_values(
                [row["p_value"] for row in raw_tests], method="holm"
            )
            statistical_result["direct_tests"] = [
                    {**row, "adjusted_p_value": adjusted[index]}
                for index, row in enumerate(raw_tests)
            ]

        outcome_resolution = {
            str(horizon): {
                "horizon": horizon,
                "horizon_kind": outcomes["horizon_kind"],
                "resolved_count": counts.get("resolved", 0),
                "unresolved_count": sum(
                    count
                    for reason, count in counts.items()
                    if reason.startswith("unresolved:")
                ),
                "unresolved_reasons": {
                    reason.split(":", 1)[1]: count
                    for reason, count in sorted(counts.items())
                    if reason.startswith("unresolved:")
                },
            }
            for horizon, counts in sorted(horizon_resolution.items())
        }
        invalidation_summary = {
            "configured": bool(outcomes.get("invalidation")),
            "resolved_count": sum(
                count
                for reason, count in invalidation_resolution.items()
                if reason.startswith("resolved:")
            ),
            "unresolved_count": sum(
                count
                for reason, count in invalidation_resolution.items()
                if reason.startswith("unresolved:")
            ),
            "invalidated_count": invalidation_resolution.get(
                "resolved:invalidated", 0
            ),
            "not_invalidated_count": invalidation_resolution.get(
                "resolved:not_invalidated", 0
            ),
            "unresolved_reasons": {
                reason.split(":", 1)[1]: count
                for reason, count in sorted(invalidation_resolution.items())
                if reason.startswith("unresolved:")
            },
        }
        analysis_status = (
            "insufficient_evidence" if eligibility_reasons else "completed"
        )
        transition_actions = [
            str(action.get("action") or "")
            for transition in gap_transitions
            for action in transition.get("actions") or []
            if isinstance(action, Mapping)
        ]
        return {
            "schema_version": self.result_schema_version,
            "check_family": EVENT_FACT_ANALYSIS,
            "status": "completed",
            "analysis_status": analysis_status,
            "event_ownership": (
                "indicator"
                if detector_type == _INDICATOR_EVENT_DETECTOR
                else "check"
            ),
            **(
                {"sampling": "primary_bar_close"}
                if detector_type == _FACT_SNAPSHOT_DETECTOR
                else {}
            ),
            "sample_count": len(population_rows),
            "analysis_sample_count": len(complete_rows),
            "candidate_count": len(event_results),
            "direction_counts": dict(
                sorted(Counter(row["direction"] for row in event_results).items())
            ),
            "primary_outcome_counts": {
                "positive": positives,
                "nonpositive": negatives,
            },
            "distinct_utc_days": distinct_days,
            "eligibility": {
                "eligible": not eligibility_reasons,
                "criteria": eligibility,
                "reasons": eligibility_reasons,
                "exclusions": dict(sorted(exclusion_counts.items())),
                "population_count": len(population_rows),
                "analysis_complete_case_count": len(complete_rows),
                "population_folds": population_fold_counts,
                "population_valid_fold_count": population_valid_fold_count,
                "enriched_features_evaluated": not bool(eligibility_reasons),
            },
            "gap_decision": {
                "policy": plan.gap_policy,
                "recorded_gap_count": gap_count,
                **(
                    {
                        "check_action": (
                            "fact_samples_excluded"
                            if gap_count
                            else "no_gap_action_required"
                        )
                    }
                    if detector_type == _FACT_SNAPSHOT_DETECTOR
                    else {
                        "indicator_action": (
                            "reset_and_rewarm"
                            if "reset_and_rewarm" in transition_actions
                            else "continued_with_degraded_status"
                            if "continued_degraded" in transition_actions
                            else "no_gap_action_required"
                        )
                    }
                ),
                "degraded": bool(
                    gap_count and plan.gap_policy == GAP_POLICY_CONTINUE_DEGRADED
                ),
                "transitions": gap_transitions,
            },
            "outcome_resolution": outcome_resolution,
            "invalidation_resolution": invalidation_summary,
            "statistics": statistical_result,
            "events": event_results,
            "hashes": {
                "event_population_hash": semantic_hash(
                    {"events": event_results}
                ),
                "selected_facts_hash": semantic_hash(
                    {
                        "facts": [
                            selected_fact_material[key]
                            for key in sorted(selected_fact_material)
                        ]
                    }
                ),
                "feature_matrix_hash": semantic_hash(
                    {
                        "rows": [
                            {
                                "event_time": row["event_time"],
                                "decision_time": row["decision_time"],
                                "features": row["features"],
                                "primary_outcome": row["outcomes"].get(primary_key),
                                "eligible": row["eligible"],
                            }
                            for row in event_results
                        ]
                    }
                ),
                "fold_assignment_hash": semantic_hash(
                    {
                        "folds": folds,
                        "fold_results": fold_results,
                    }
                ),
                "statistics_hash": semantic_hash(statistical_result),
                "indicator_gap_semantics_hash": semantic_hash(
                    {
                        "schema_version": "check_gap_transition_material.v1",
                        "policy": plan.gap_policy,
                        "gap_transitions": gap_transitions,
                        "rejection": gap_rejection,
                    }
                ),
            },
            "data_quality": data_quality,
            "caveats": (
                [
                    (
                        "Dataset gaps were retained and affected research samples were excluded."
                        if detector_type == _FACT_SNAPSHOT_DETECTOR
                        else "Dataset gaps were retained and execution continued explicitly."
                    )
                ]
                if gap_count and plan.gap_policy == GAP_POLICY_CONTINUE_DEGRADED
                else []
            ),
        }


__all__ = [
    "EVENT_FACT_ANALYSIS",
    "EVENT_FACT_EVALUATOR_VERSION",
    "EVENT_FACT_RESULT_VERSION",
    "LEGACY_EVENT_FACT_EVALUATOR_VERSION",
    "LEGACY_EVENT_FACT_RESULT_VERSION",
    "EventFactEvaluator",
    "normalize_event_fact_configuration",
]
