"""Initial structured reserve-state indicator manifest."""

from __future__ import annotations

from indicators.manifest import (
    IndicatorManifest,
    IndicatorMarketInput,
    IndicatorOutput,
)


MANIFEST = IndicatorManifest(
    type="reserve_state",
    version="v1",
    label="Reserve State",
    description=(
        "Causally exposes exact reserve quantity and report age from a canonical "
        "structured asset.reserve_state Fact."
    ),
    outputs=(
        IndicatorOutput(
            name="reserve_state",
            type="context",
            label="Reserve State",
            state_keys=("observed",),
            fields=(
                "report_id",
                "reserve_asset",
                "reserve_quantity",
                "reserve_quantity_exact",
                "unit",
                "observation_time",
                "known_at",
                "age_seconds",
            ),
        ),
    ),
    market_inputs=(
        IndicatorMarketInput(
            key="reserve_state",
            fact_type="asset.reserve_state",
            contract_version="asset.reserve_state.v1",
            instrument_role="explicit",
            instrument_ref="nxtassets-de000nxta018",
            dimensions={"reserve_asset": "BTC"},
            alignment="latest_known",
            max_staleness_seconds=259200,
            required=True,
            allow_gaps=False,
            required_fields=(
                "report_id",
                "reserve_asset",
                "reserve_quantity",
                "unit",
                "observation_time",
                "known_at",
            ),
            known_at_required=True,
        ),
    ),
)


__all__ = ["MANIFEST"]
