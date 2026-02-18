"""Runtime settlement helpers for bot trades."""

from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping, Optional

from engines.bot_runtime.core.exit_settlement import ExitSettlement, ExitSettlementContext
from utils.log_context import build_log_context
from utils.perf_log import get_obs_enabled, get_obs_slow_ms, perf_log

logger = logging.getLogger(__name__)


class SettlementApplier:
    """Apply exit settlement side effects based on emitted events."""

    def __init__(self, *, obs_enabled: Optional[bool] = None, obs_slow_ms: Optional[float] = None) -> None:
        self._obs_enabled = obs_enabled if obs_enabled is not None else get_obs_enabled()
        self._obs_slow_ms = obs_slow_ms if obs_slow_ms is not None else get_obs_slow_ms()

    def apply(self, events: Iterable[Mapping[str, Any]], exit_settlement: Optional[ExitSettlement]) -> None:
        if not exit_settlement:
            return
        for event in events:
            settlement = event.get("settlement")
            if not isinstance(settlement, Mapping):
                continue
            base_context = build_log_context(trade_id=settlement.get("trade_id"))
            context = ExitSettlementContext(
                event_type=str(settlement.get("event_type") or "EXIT_FILL"),
                side=str(settlement.get("side") or ""),
                base_currency=str(settlement.get("base_currency") or ""),
                quote_currency=str(settlement.get("quote_currency") or ""),
                qty=float(settlement.get("qty") or 0.0),
                price=float(settlement.get("price") or 0.0),
                fee=float(settlement.get("fee") or 0.0),
                notional=float(settlement.get("notional") or 0.0),
                trade_id=str(settlement.get("trade_id") or ""),
                leg_id=str(settlement.get("leg_id") or ""),
                position_direction=str(settlement.get("position_direction") or ""),
                accounting_mode=settlement.get("accounting_mode"),
                realized_pnl=float(settlement.get("realized_pnl") or 0.0),
                allow_short_borrow=bool(settlement.get("allow_short_borrow")),
                instrument=dict(settlement.get("instrument") or {}),
            )
            with perf_log(
                "bot_runtime_settlement_apply",
                logger=logger,
                base_context=base_context,
                enabled=self._obs_enabled,
                slow_ms=self._obs_slow_ms,
                event_type=context.event_type,
                qty=context.qty,
                price=context.price,
                fee=context.fee,
            ):
                exit_settlement.apply_exit_fill(context, force=True)
