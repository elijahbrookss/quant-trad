"""Runtime settlement helpers for bot trades."""

from __future__ import annotations

from typing import Any, Iterable, Mapping, Optional

from engines.bot_runtime.core.exit_settlement import ExitSettlement, ExitSettlementContext


class SettlementApplier:
    """Apply exit settlement side effects based on emitted events."""

    def apply(self, events: Iterable[Mapping[str, Any]], exit_settlement: Optional[ExitSettlement]) -> None:
        if not exit_settlement:
            return
        for event in events:
            settlement = event.get("settlement")
            if not isinstance(settlement, Mapping):
                continue
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
            exit_settlement.apply_exit_fill(context, force=True)

