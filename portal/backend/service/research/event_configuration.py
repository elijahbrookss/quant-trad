"""Accepted event-and-fact settings and fixed historical normalization rules."""
from __future__ import annotations
import math
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any
from .event_time import _utc, _iso

EVENT_FACT_ANALYSIS = "event_fact_analysis"
LEGACY_EVENT_FACT_EVALUATOR_VERSION = "2"
LEGACY_EVENT_FACT_RESULT_VERSION = "event_fact_analysis_result.v2"
EVENT_FACT_EVALUATOR_VERSION = "3"
EVENT_FACT_RESULT_VERSION = "event_fact_analysis_result.v3"
_INDICATOR_EVENT_DETECTOR = "indicator_event"
_FACT_SNAPSHOT_DETECTOR = "fact_snapshot"
_REQUIRED_FACTS_AVAILABLE = "required_facts_available"
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

    trigger = str(detector.get("evaluation_trigger") or "primary_available").strip()
    if trigger not in {"primary_available", _REQUIRED_FACTS_AVAILABLE}:
        raise ValueError("event_fact_check_invalid: unsupported evaluation_trigger")
    if trigger == _REQUIRED_FACTS_AVAILABLE:
        if detector_type != _FACT_SNAPSHOT_DETECTOR:
            raise ValueError(
                "event_fact_check_invalid: required_facts_available requires fact_snapshot"
            )
        normalized_detector["evaluation_trigger"] = trigger

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


