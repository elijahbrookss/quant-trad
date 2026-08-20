"""Walk-forward projection of canonical structured reserve-state facts."""

from __future__ import annotations

import math
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import Indicator, RuntimeOutput
from indicators.manifest import build_runtime_spec
from market_data.canonical import CanonicalFactRecord

from .manifest import MANIFEST


class TypedReserveStateIndicator(Indicator):
    """Expose provider-free reserve metrics at each decision time."""

    def __init__(self, *, indicator_id: str, version: str) -> None:
        self.runtime_spec = build_runtime_spec(
            MANIFEST,
            instance_id=indicator_id,
            version=version,
        )
        self._output = RuntimeOutput(
            bar_time=datetime.min,
            ready=False,
            value={},
        )

    def apply_bar(
        self,
        bar: Any,
        inputs: Mapping[Any, RuntimeOutput],
    ) -> None:
        if not isinstance(bar, Candle):
            raise RuntimeError("reserve_state_apply_failed: Candle input required")
        if inputs:
            raise RuntimeError(
                "reserve_state_apply_failed: reserve_state has no indicator dependencies"
            )
        record = self.market_data_input("reserve_state")
        if not isinstance(record, CanonicalFactRecord):
            raise RuntimeError(
                "reserve_state_apply_failed: canonical Fact record required"
            )
        fact = record.fact
        if (
            fact.fact_type != "asset.reserve_state"
            or fact.payload_schema_id != "asset.reserve_state.v1"
        ):
            raise RuntimeError(
                "reserve_state_apply_failed: canonical reserve contract disagreement"
            )
        if fact.known_at > bar.time:
            raise RuntimeError(
                "reserve_state_apply_failed: Fact is not causally visible at bar time"
            )
        try:
            exact_quantity = Decimal(str(fact.payload["reserve_quantity"]))
        except (InvalidOperation, KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "reserve_state_apply_failed: reserve quantity is invalid"
            ) from exc
        quantity = float(exact_quantity)
        if not math.isfinite(quantity) or quantity < 0:
            raise RuntimeError(
                "reserve_state_apply_failed: reserve quantity is non-finite"
            )
        age_seconds = int((bar.time - fact.observation_time).total_seconds())
        if age_seconds < 0:
            raise RuntimeError(
                "reserve_state_apply_failed: observation follows bar time"
            )
        self._output = RuntimeOutput(
            bar_time=bar.time,
            ready=True,
            value={
                "state_key": "observed",
                "fields": {
                    "report_id": str(fact.payload["report_id"]),
                    "reserve_asset": str(fact.payload["reserve_asset"]),
                    "reserve_quantity": quantity,
                    "reserve_quantity_exact": str(
                        fact.payload["reserve_quantity"]
                    ),
                    "unit": str(fact.payload["unit"]),
                    "observation_time": fact.observation_time.isoformat(),
                    "known_at": fact.known_at.isoformat(),
                    "age_seconds": age_seconds,
                },
            },
        )

    def snapshot(self) -> Mapping[str, RuntimeOutput]:
        return {"reserve_state": self._output}


__all__ = ["TypedReserveStateIndicator"]
