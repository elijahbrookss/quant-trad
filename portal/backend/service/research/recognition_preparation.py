"""Canonical outcome-blind preparation shared by active recognition consumers."""
from datetime import timedelta
from typing import Mapping

from core.execution_control import execution_checkpoint
from research_science.recognition import RecognitionCandle, RecognitionContext, utc
from research_science.reversal_recognition import ReversalSettings, recognize_reversals
from .raw_candle_evaluator import _prepare

SCHEMA = "reversal_recognition.v1"

def choices(detector):
    if not isinstance(detector, Mapping) or set(detector) != {
        "type", "computation_origin", "reference_basis", "ownership", "max_bars", "max_candidates"
    } or detector["type"] != "completed_reversal_candidates":
        raise ValueError("reversal_recognition_invalid: explicit detector and all supported settings required")
    return ReversalSettings.from_dict({k: v for k, v in detector.items()
                                      if k not in {"type", "computation_origin"}})

def recognition_requirements(warmup_bars):
    """Common outcome-blind candle requirements for recognition and selection."""
    return {"input_kind": "market_data", "indicator_ids": [], "warmup_floor_bars": warmup_bars,
        "feature_lookback_bars": warmup_bars, "feature_windows_seconds_by_alias": {},
        "outcome_horizons": [], "required_outcome_horizons": [], "horizon_kind": "bars",
        "entry_lag_bars": 0, "invalidation_max_bars": 0,
        "outcome_boundary": "evaluation_end_exclusive",
        "primary_candle_required_fields": ["open", "high", "low", "close", "known_at"]}

def prepare_recognition(*, plan, inputs, schema=SCHEMA, checkpoint=None, expected_outcomes=None):
    """Shared canonical preparation and one sequential recognizer for both Checks."""
    settings = choices(inputs["detector"])
    expected = {"schema_version": schema} if expected_outcomes is None else expected_outcomes
    if inputs["outcomes"] != expected or plan.gap_policy != "reject":
        raise ValueError("reversal_recognition_invalid: pinned inputs required")
    primary = next(x for x in plan.market_data_requirements if x["alias"] == "primary_bars")
    seconds = int(primary["timeframe_seconds"])
    origin = utc(inputs["detector"]["computation_origin"])
    start, end = utc(plan.evaluation_range["start"]), utc(plan.evaluation_range["end_exclusive"])
    if utc(plan.materialization_range["start"]) != origin:
        raise ValueError("reversal_recognition_invalid: planned origin differs from pinned detector")
    _, rows = _prepare(inputs["candles"], step=timedelta(seconds=seconds), volume_required=False)
    if not rows or utc(rows[0]["open_time"]) != origin or utc(rows[-1]["close_time"]) != end:
        raise ValueError("reversal_recognition_invalid: incomplete origin-to-end candle path")
    candles = tuple(RecognitionCandle.from_mapping(r) for r in rows)
    context = RecognitionContext(str(primary["instrument_id"]), seconds, origin, candles[0].source_identity_key)
    snapshot = recognize_reversals(candles, context=context, settings=settings, checkpoint=checkpoint or execution_checkpoint)
    return settings, snapshot, rows, start, end, candles
