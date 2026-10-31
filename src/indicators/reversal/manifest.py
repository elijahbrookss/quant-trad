"""Public causal outputs; no forward path or independent-event claim."""
from indicators.manifest import IndicatorManifest, IndicatorOutput, IndicatorParam, IndicatorRuntimeInput

MANIFEST = IndicatorManifest(
    type="reversal", version="v1", label="Provisional Reversal Chains",
    description="Causal retreat-inclusive reversal chains; retain all overlapping references without trading or independence claims.",
    params=(
        IndicatorParam(key="computation_origin", type="string", label="Computation Origin",
                       description="Explicit UTC origin; input cannot be cropped or implicitly reset.", required=True),
        IndicatorParam(key="reference_basis", type="string", label="Reference Basis",
                       default="rising_leg_plus_retreat", editable=False),
        IndicatorParam(key="ownership", type="string", label="Chain Ownership",
                       default="retain_all_provisional", editable=False),
        IndicatorParam(key="max_bars", type="int", label="Maximum Bars", default=19104),
        IndicatorParam(key="max_candidates", type="int", label="Maximum References", default=10000),
    ),
    outputs=(
        IndicatorOutput(name="provisional_reversals", type="signal", label="New Provisional Completions",
                        event_keys=("provisional_reversal_completed",)),
        IndicatorOutput(name="candidate_lifecycle", type="lifecycle", label="Current Chain Changes"),
        IndicatorOutput(name="reversal_context", type="context", label="Causal Recognition Context",
                        state_keys=("provisional",), fields=("instrument_id", "source_identity_key",
                            "computation_origin", "processed_bars", "reference_count", "completion_count",
                            "ownership", "reference_basis")),
    ),
    runtime_inputs=(IndicatorRuntimeInput(),),
)
