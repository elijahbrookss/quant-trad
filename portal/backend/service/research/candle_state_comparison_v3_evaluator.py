"""Versioned candle-state comparison with causal historical transforms."""

from __future__ import annotations

from dataclasses import dataclass

from . import candle_state_comparison_v2_evaluator as v2


SCHEMA = "candle_state_paired_comparison.v3"
RESULT_SCHEMA = "candle_state_paired_comparison_result.v3"
SELECTION_AUDIT_SCHEMA = v2.SELECTION_AUDIT_SCHEMA
RULES = {
    **v2.RULES,
    "feature_binding": "typed_candle_state_historical_transform_or_public_indicator.v2",
}


def normalize_configuration(*, detector, outcomes, statistics, inputs, scope, gap_policy):
    """Admit existing typed features with identity or causal past-return sign."""
    return v2._normalize_configuration(
        detector=detector,
        outcomes=outcomes,
        statistics=statistics,
        inputs=inputs,
        scope=scope,
        gap_policy=gap_policy,
        schema_version=SCHEMA,
        allow_ordinary_transforms=True,
    )


@dataclass(frozen=True)
class CandleStateComparisonV3Evaluator(v2.CandleStateComparisonV2Evaluator):
    """Preserve v22 pairing while adding bounded historical feature transforms."""

    version: str = "23"
    result_schema_version: str = RESULT_SCHEMA
