"""Ladder risk engine orchestration and sizing logic."""

from __future__ import annotations

import logging
import math
import uuid
from dataclasses import asdict
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, TYPE_CHECKING

import risk as risk_math
from atm import merge_templates
from risk import normalise_risk_config

from utils.log_context import build_log_context, merge_log_context, with_log_context
from ..amount_constraints import normalize_qty_with_constraints
from ..entry_execution import EntryExecutionCoordinator, PendingEntry
from ..entry_settlement import EntrySettlement, EntrySettlementContext, EntrySettlementService
from ..execution_adapter import ExecutionAdapter
from ..execution_intent import ExecutionIntent, LimitParams
from ..execution_model import ExecutionModel
from ..execution_profile import SeriesExecutionProfile, compile_series_execution_profile
from ..execution_runtime import DeterministicExecutionModel
from ..exit_settlement import ExitSettlement, ExitSettlementService
from ..fees import FeeResolver, FeeSchedule
from ..margin import calculate_max_qty_by_margin
from ..wallet import WalletLedger, trace_wallet_balance
from ..wallet_gateway import WalletGateway
from .models import (
    Candle,
    CandleSnapshot,
    EntryFill,
    EntryFillResult,
    EntryRequest,
    EntryValidation,
    Leg,
)
from .position import LadderPosition
from .time_utils import coalesce_numeric, coerce_float

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from ..execution_intent import ExecutionOutcome

class LadderRiskEngine:
    """Create and manage laddered trades for simulated bots."""

    def __init__(
        self,
        config: Optional[Dict[str, object]] = None,
        instrument: Optional[Dict[str, Any]] = None,
        execution_profile: Optional[SeriesExecutionProfile] = None,
        risk_config: Optional[Mapping[str, Any]] = None,
    ):
        provided_template = config or {}
        self.template = merge_templates(provided_template)
        self.instrument = instrument or {}
        self.risk_config = normalise_risk_config(risk_config)
        self.execution_profile = execution_profile or compile_series_execution_profile(
            self.instrument,
            template=self.template,
            risk_config=self.risk_config,
            runtime_requires_derivatives=False,
        )
        self._runtime_log_context = build_log_context(
            strategy_id=self.template.get("strategy_id"),
            strategy_name=self.template.get("strategy_name"),
            timeframe=self.template.get("timeframe"),
            symbol=self.instrument.get("symbol"),
            datasource=self.instrument.get("datasource"),
            exchange=self.instrument.get("exchange"),
            instrument_id=self.instrument.get("id"),
            instrument_type=(
                self.execution_profile.instrument.instrument_type if self.execution_profile is not None else None
            ),
        )

        # Always validate - same for all modes (backtest, sim_trade, paper, live)
        self._validate_template(self.template)
        self._validate_instrument(self.instrument)

        # Resolve tick_size (required)
        config_tick = coerce_float(provided_template.get("tick_size"))
        instrument_tick = coerce_float(self.instrument.get("tick_size"))
        profile_tick = (
            self.execution_profile.constraints.tick_size
            if self.execution_profile is not None
            else None
        )
        tick_size = coalesce_numeric(config_tick, instrument_tick, profile_tick, default=0.0)
        if tick_size == 0:
            raise ValueError("tick_size required from either template or instrument configuration")
        self.tick_size = tick_size

        self.stop_ticks = max(int(self.template.get("stop_ticks") or 0), 0)

        initial_stop_config = self.template.get("initial_stop")
        if not isinstance(initial_stop_config, dict):
            initial_stop_config = {}
        self.r_multiple = float(initial_stop_config.get("atr_multiplier") or 1.0)

        self.base_risk_per_trade = coerce_float(self.execution_profile.risk.base_risk_per_trade)
        self.stop_r_multiple = coerce_float(self.template.get("stop_r_multiple"))

        self.stop_adjustments_config: List[Dict[str, Any]] = list(self.template.get("stop_adjustments") or [])
        self.execution_mode = str(self.template.get("execution_mode") or "market").lower()
        self.limit_maker_config: Dict[str, Any] = dict(self.template.get("limit_maker") or {})

        # Resolve contract_size (config > instrument > 1.0)
        config_contract = coerce_float(self.template.get("contract_size"))
        instrument_contract = coerce_float(self.instrument.get("contract_size"))
        profile_contract = (
            self.execution_profile.constraints.contract_size
            if self.execution_profile is not None
            else None
        )
        self.contract_size = coalesce_numeric(config_contract, instrument_contract, profile_contract, default=0.0)
        if self.contract_size in (None, 0):
            raise ValueError("contract_size required from either template or instrument configuration")
        # Resolve tick_value (config > instrument > calculated from tick_size * contract_size)
        config_tick_value = coerce_float(self.template.get("tick_value"))
        instrument_tick_value = coerce_float(self.instrument.get("tick_value"))
        calculated_tick_value = self.tick_size * self.contract_size
        profile_tick_value = (
            self.execution_profile.constraints.tick_value
            if self.execution_profile is not None
            else None
        )
        self.tick_value = coalesce_numeric(
            config_tick_value,
            instrument_tick_value,
            profile_tick_value,
            calculated_tick_value,
            default=0.0,
        )
        if self.tick_value in (None, 0):
            raise ValueError("tick_value required from either template or instrument configuration")

        risk_mode = str(initial_stop_config.get("mode") or "atr").lower()
        self.risk_unit_mode = risk_mode if risk_mode in {"atr", "ticks"} else "atr"
        self.ticks_stop = int(
            self.template.get("ticks_stop")
            or self.template.get("stop_ticks")
            or self.stop_ticks
        )
        self.global_risk_multiplier = float(self.execution_profile.risk.global_risk_multiplier)
        self.instrument_risk_multiplier = float(self.execution_profile.risk.instrument_risk_multiplier)
        self.amount_constraints = self.execution_profile.constraints.amount_constraints
        self.min_qty = self.amount_constraints.min_qty
        self.max_qty = self.amount_constraints.max_qty
        self.qty_step = self.amount_constraints.qty_step
        self.min_notional = self.amount_constraints.min_notional
        self.amount_precision = self.amount_constraints.precision
        constraints_context = self.runtime_log_context(
            min_qty=self.min_qty,
            max_qty=self.max_qty,
            qty_step=self.qty_step,
            min_notional=self.min_notional,
            amount_precision=self.amount_precision,
            qty_step_source=self.amount_constraints.step_source,
        )
        logger.debug(with_log_context("ladder_risk_constraints", constraints_context))
        self.execution_model = None
        self.execution_adapter: Optional[ExecutionAdapter] = None
        self.last_rejection_reason: Optional[str] = None
        self.last_rejection_detail: Optional[Dict[str, Any]] = None
        self._wallet_ledger: Optional[WalletLedger] = None
        self._wallet_gateway: Optional[WalletGateway] = None
        self._wallet_fill_metadata_by_trade: Dict[str, Dict[str, Any]] = {}
        self.can_short = bool(self.execution_profile.capabilities.supports_short)
        self.short_requires_borrow = bool(self.execution_profile.capabilities.short_requires_borrow)

        self.orders = self._orders_from_template()
        self.targets = [int(order.get("ticks") or 0) for order in self.orders]
        # Resolve quote currency
        quote_value = (
            self.template.get("quote_currency")
            or self.instrument.get("quote_currency")
            or self.execution_profile.instrument.quote_currency
            or "USD"
        )
        self.quote_currency = str(quote_value).upper()

        # Resolve fee rates (config > instrument > 0.0, allow_zero since 0% fees are valid)
        config_maker = coerce_float(self.template.get("maker_fee_rate"))
        instrument_maker = coerce_float(self.instrument.get("maker_fee_rate"))
        self.maker_fee = coalesce_numeric(config_maker, instrument_maker, default=0.0, allow_zero=True)

        config_taker = coerce_float(self.template.get("taker_fee_rate"))
        instrument_taker = coerce_float(self.instrument.get("taker_fee_rate"))
        self.taker_fee = coalesce_numeric(config_taker, instrument_taker, default=0.0, allow_zero=True)
        self.execution_intent_model: ExecutionModel = DeterministicExecutionModel(
            FeeResolver(
                FeeSchedule(
                    maker_rate=float(self.maker_fee or 0.0),
                    taker_rate=float(self.taker_fee or 0.0),
                    source="template_or_instrument",
                )
            )
        )
        self.entry_settlement: EntrySettlement = EntrySettlementService(self)
        self.exit_settlement: ExitSettlement = ExitSettlementService(None)
        self.entry_execution = EntryExecutionCoordinator(self)
        self.active_trade: Optional[LadderPosition] = None
        self.trades: List[LadderPosition] = []
        self.trade_revision: int = 0
        configured_context = self.runtime_log_context(
            targets=",".join(str(order.get("ticks") or order.get("r_multiple") or "?") for order in self.orders),
            stop_ticks=self.stop_ticks,
            tick_size=self.tick_size,
            execution_mode=self.execution_mode,
            instrument_type=self.execution_profile.instrument.instrument_type if self.execution_profile is not None else None,
            accounting_mode=self.execution_profile.accounting_mode if self.execution_profile is not None else None,
            supports_margin=self.execution_profile.capabilities.supports_margin if self.execution_profile is not None else None,
            supports_short=self.execution_profile.capabilities.supports_short if self.execution_profile is not None else None,
        )
        logger.info(with_log_context("ladder_risk_configured", configured_context))

    def set_runtime_context(self, **fields: Optional[object]) -> None:
        """Merge additional context fields into engine log context."""
        self._runtime_log_context = merge_log_context(self._runtime_log_context, build_log_context(**fields))

    def runtime_log_context(self, **fields: Optional[object]) -> Dict[str, object]:
        """Build context for runtime-domain logs with stable engine fields."""
        base = build_log_context(
            symbol=self.instrument.get("symbol"),
            datasource=self.instrument.get("datasource"),
            exchange=self.instrument.get("exchange"),
            instrument_id=self.instrument.get("id"),
            instrument_type=(
                self.execution_profile.instrument.instrument_type if self.execution_profile is not None else None
            ),
        )
        return merge_log_context(self._runtime_log_context, base, build_log_context(**fields))

    def attach_wallet(self, ledger: WalletLedger) -> None:
        raise RuntimeError(
            "attach_wallet is not supported. Use attach_wallet_gateway with SharedWalletGateway runtime wiring."
        )

    def attach_wallet_gateway(self, gateway: WalletGateway) -> None:
        self._wallet_gateway = gateway
        self._wallet_ledger = getattr(gateway, "ledger", None)

    def attach_execution_adapter(self, adapter: ExecutionAdapter) -> None:
        """Inject a run-type specific execution adapter (backtest/paper/live)."""
        self.execution_adapter = adapter

    def attach_entry_settlement(self, settlement: EntrySettlement) -> None:
        """Inject a custom entry settlement adapter (paper/live)."""
        self.entry_settlement = settlement

    def attach_exit_settlement(self, settlement: ExitSettlement) -> None:
        """Inject a custom exit settlement adapter (paper/live)."""
        self.exit_settlement = settlement

    def remember_wallet_fill_metadata(self, trade_id: str, payload: Mapping[str, Any]) -> None:
        key = str(trade_id or "").strip()
        if not key:
            return
        self._wallet_fill_metadata_by_trade[key] = dict(payload or {})

    def pop_wallet_fill_metadata(self, trade_id: str) -> Dict[str, Any]:
        key = str(trade_id or "").strip()
        if not key:
            return {}
        value = self._wallet_fill_metadata_by_trade.pop(key, None)
        if isinstance(value, Mapping):
            return dict(value)
        return {}

    def _validate_template(self, template: Dict[str, Any]) -> None:
        """Validate that required fields are present in template - same for all modes."""
        missing_fields = []

        # Validate stop configuration exists
        if not template.get("initial_stop"):
            missing_fields.append("initial_stop")

        # Validate take profit orders exist
        if not template.get("take_profit_orders"):
            missing_fields.append("take_profit_orders")

        if missing_fields:
            raise ValueError(
                f"Incomplete ATM template. Missing required fields: {', '.join(missing_fields)}. "
                f"All modes (backtest/sim_trade/paper/live) require complete templates."
            )

    def _validate_instrument(self, instrument: Dict[str, Any]) -> None:
        """Validate that instrument configuration is complete."""
        if not instrument:
            raise ValueError("Instrument configuration is required. Cannot proceed without instrument metadata.")

        if not instrument.get("tick_size"):
            raise ValueError(
                "Instrument configuration must include tick_size. "
                "This is required for accurate position sizing and PnL calculation."
            )

    def _orders_from_template(self) -> List[Dict[str, Any]]:
        orders: List[Dict[str, Any]] = []
        entries = self.template.get("take_profit_orders") or []
        base_contracts = int(self.template.get("contracts") or len(entries) or 0)
        for idx, entry in enumerate(entries):
            ticks = coerce_float(entry.get("ticks"))
            r_multiple = coerce_float(entry.get("r_multiple"))
            price = coerce_float(entry.get("price"))
            if ticks is None and r_multiple is None and price is None:
                continue
            label = entry.get("label") or f"Target {idx + 1}"
            size_fraction = coerce_float(entry.get("size_fraction"))
            size_percent = None
            if size_fraction is not None and 0 <= size_fraction <= 1:
                size_percent = size_fraction * 100

            contracts = int(entry.get("contracts") or 0)
            if contracts <= 0 and size_percent is not None and base_contracts > 0:
                contracts = int(round((size_percent / 100) * base_contracts))
            if contracts <= 0:
                continue
            orders.append(
                {
                    "label": label,
                    "ticks": int(ticks) if ticks is not None else None,
                    "r_multiple": r_multiple,
                    "price": price,
                    "contracts": contracts,
                    "size_fraction": size_fraction,
                    "id": entry.get("id"),
                }
            )
        return orders

    @staticmethod
    def _new_order_intent_id() -> str:
        return str(uuid.uuid4())

    @staticmethod
    def _new_trade_id() -> str:
        return str(uuid.uuid4())

    def _entry_request_id(self, candle: Candle, direction: str) -> str:
        bar_time = candle.time.isoformat() if getattr(candle, "time", None) is not None else ""
        run_id = getattr(self, "run_id", None) or self.template.get("run_id") or ""
        strategy_id = getattr(self, "strategy_id", None) or self.template.get("strategy_id") or ""
        timeframe = self.template.get("timeframe") or self.instrument.get("timeframe") or ""
        instrument_id = self.instrument.get("id") or ""
        symbol = self.instrument.get("symbol") or ""
        decision_id = getattr(self, "last_decision_id", None) or ""
        signal_id = getattr(self, "last_signal_id", None) or ""
        stable_key = "|".join(
            str(part or "")
            for part in (
                "entry_request",
                run_id,
                strategy_id,
                instrument_id,
                symbol,
                timeframe,
                bar_time,
                direction,
                decision_id,
                signal_id,
            )
        )
        return f"entry_request:{uuid.uuid5(uuid.NAMESPACE_URL, stable_key)}"

    @staticmethod
    def _entry_request_rejection_detail(
        *,
        entry_request_id: str,
        detail: Optional[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        payload = dict(detail or {})
        payload.setdefault("entry_request_id", entry_request_id)
        payload.setdefault("attempt_id", entry_request_id)
        return payload

    def _normalize_qty(self, requested_qty: float):
        return normalize_qty_with_constraints(self.amount_constraints, requested_qty)

    def _resolve_execution_model(self) -> ExecutionModel:
        return self.execution_intent_model

    def attach_execution_model(self, model: ExecutionModel) -> None:
        """Inject a run-type specific execution model for intent evaluation."""
        self.execution_intent_model = model

    def _resolve_anchor_price(
        self,
        candle: Candle,
        *,
        anchor: str,
    ) -> float:
        anchor = str(anchor or "signal_price").lower()
        if anchor == "next_bar_open":
            return float(candle.open)
        if anchor == "prior_close":
            return float(candle.close)
        return float(candle.close)

    def _resolve_limit_offset(
        self,
        candle: Candle,
        *,
        offset_type: str,
        offset_value: float,
        r_value: Optional[float],
    ) -> float:
        offset_type = str(offset_type or "ticks").lower()
        if offset_type == "atr_pct":
            if not self._has_valid_atr(candle.atr):
                return 0.0
            return float(candle.atr) * float(offset_value or 0.0)
        if offset_type == "r_fraction":
            if r_value in (None, 0):
                return 0.0
            return float(r_value) * float(offset_value or 0.0)
        return float(offset_value or 0.0) * float(self.tick_size)

    def _build_limit_params(
        self,
        candle: Candle,
        *,
        direction: str,
        r_value: Optional[float],
    ) -> LimitParams:
        anchor_price = str(self.limit_maker_config.get("anchor_price") or "signal_price").lower()
        offset_type = str(self.limit_maker_config.get("offset_type") or "ticks").lower()
        offset_value = float(self.limit_maker_config.get("offset_value") or 0.0)
        validity_window = int(self.limit_maker_config.get("validity_window") or 1)
        fallback = str(self.limit_maker_config.get("fallback") or "cancel").lower()
        anchor_value = self._resolve_anchor_price(candle, anchor=anchor_price)
        offset = self._resolve_limit_offset(
            candle,
            offset_type=offset_type,
            offset_value=offset_value,
            r_value=r_value,
        )
        if direction == "long":
            limit_price = anchor_value - offset
        else:
            limit_price = anchor_value + offset
        return LimitParams(
            anchor_price=anchor_price,
            offset_type=offset_type,
            offset_value=float(offset_value),
            validity_window=max(validity_window, 1),
            fallback=fallback,
            limit_price=float(limit_price),
        )

    def build_entry_request(self, candle: Candle, direction: str) -> EntryRequest:
        entry_request_id = self._entry_request_id(candle, direction)
        atr_at_entry = candle.atr if self._has_valid_atr(candle.atr) else None
        r_ticks = self._compute_r_ticks(candle)

        r_value = self._r_value(candle)
        if self.stop_r_multiple not in (None, 0) and r_value not in (None, 0):
            r_value = float(self.stop_r_multiple) * float(r_value)

        risk_based_qty = self._calculate_total_contracts(r_ticks)
        capped_qty, was_margin_capped, margin_info = self._cap_qty_by_margin(
            risk_qty=risk_based_qty,
            price=candle.close,
            direction=direction,
        )

        if margin_info and margin_info.get("reason") == "margin_calculation_failed":
            context = self.runtime_log_context(
                reason="MARGIN_CALCULATION_FAILED",
                error=margin_info.get("error"),
            )
            logger.warning(with_log_context("entry_rejected", context))
            return EntryRequest(
                trade_id=None,
                order_intent_id=None,
                entry_request_id=entry_request_id,
                direction=direction,
                requested_qty=float(capped_qty),
                qty_raw=float(capped_qty),
                r_ticks=float(r_ticks),
                r_value=r_value,
                atr_at_entry=atr_at_entry,
                r_multiple_at_entry=self.r_multiple,
                order_type="market" if self.execution_mode != "limit_maker" else "limit_maker",
                limit_params=None,
                side="buy" if direction == "long" else "sell",
                requested_price=float(candle.close),
                intent=None,
                validation=EntryValidation(
                    ok=False,
                    rejection_reason="MARGIN_CALCULATION_FAILED",
                    rejection_detail=self._entry_request_rejection_detail(
                        entry_request_id=entry_request_id,
                        detail=margin_info,
                    ),
                ),
                margin_info=margin_info,
                was_margin_capped=was_margin_capped,
            )

        if capped_qty <= 0:
            rejection_detail = self._entry_request_rejection_detail(
                entry_request_id=entry_request_id,
                detail=margin_info or {"risk_qty": risk_based_qty},
            )
            rejection_reason = "QTY_CAPPED_TO_ZERO"
            if margin_info and (
                margin_info.get("reason") == "no_available_collateral"
                or float(margin_info.get("max_qty_by_margin") or 0.0) <= 0.0
            ):
                rejection_reason = "WALLET_INSUFFICIENT_MARGIN"
            context = self.runtime_log_context(
                reason=rejection_reason,
                risk_qty=risk_based_qty,
                capped_qty=capped_qty,
                was_margin_capped=was_margin_capped,
                price=candle.close,
                direction=direction,
                margin_reason=margin_info.get("reason") if margin_info else None,
                margin_error=margin_info.get("error") if margin_info else None,
                available_collateral=margin_info.get("available_collateral") if margin_info else None,
                max_qty_by_margin=margin_info.get("max_qty_by_margin") if margin_info else None,
                cost_per_contract=margin_info.get("cost_per_contract") if margin_info else None,
                margin_per_contract=margin_info.get("margin_per_contract") if margin_info else None,
                fee_per_contract=margin_info.get("fee_per_contract") if margin_info else None,
                margin_rate=margin_info.get("margin_rate") if margin_info else None,
                margin_method=margin_info.get("calculation_method") if margin_info else None,
                balance_trace=margin_info.get("balance_trace") if margin_info else None,
                qty_step=self.qty_step,
                min_qty=self.min_qty,
                max_qty=self.max_qty,
                min_notional=self.min_notional,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return EntryRequest(
                trade_id=None,
                order_intent_id=None,
                entry_request_id=entry_request_id,
                direction=direction,
                requested_qty=float(capped_qty),
                qty_raw=float(capped_qty),
                r_ticks=float(r_ticks),
                r_value=r_value,
                atr_at_entry=atr_at_entry,
                r_multiple_at_entry=self.r_multiple,
                order_type="market" if self.execution_mode != "limit_maker" else "limit_maker",
                limit_params=None,
                side="buy" if direction == "long" else "sell",
                requested_price=float(candle.close),
                intent=None,
                validation=EntryValidation(
                    ok=False,
                    rejection_reason=rejection_reason,
                    rejection_detail=rejection_detail,
                ),
                margin_info=margin_info,
                was_margin_capped=was_margin_capped,
            )

        qty_raw = float(capped_qty)
        requested_qty = float(capped_qty)
        normalization = self._normalize_qty(requested_qty)
        if not normalization.ok:
            rejection_reason = normalization.rejected_reason or "QTY_CONSTRAINT_FAILED"
            rejection_detail = normalization.to_log_dict()
            context = merge_log_context(
                self.runtime_log_context(reason=rejection_reason),
                build_log_context(**rejection_detail),
            )
            logger.warning(with_log_context("entry_rejected", context))
            return EntryRequest(
                trade_id=None,
                order_intent_id=None,
                entry_request_id=entry_request_id,
                direction=direction,
                requested_qty=requested_qty,
                qty_raw=qty_raw,
                r_ticks=float(r_ticks),
                r_value=r_value,
                atr_at_entry=atr_at_entry,
                r_multiple_at_entry=self.r_multiple,
                order_type="market" if self.execution_mode != "limit_maker" else "limit_maker",
                limit_params=None,
                side="buy" if direction == "long" else "sell",
                requested_price=float(candle.close),
                intent=None,
                validation=EntryValidation(
                    ok=False,
                    rejection_reason=rejection_reason,
                    rejection_detail=self._entry_request_rejection_detail(
                        entry_request_id=entry_request_id,
                        detail=rejection_detail,
                    ),
                ),
                margin_info=margin_info,
                was_margin_capped=was_margin_capped,
            )

        requested_qty = float(normalization.qty_final)

        order_type = "market" if self.execution_mode != "limit_maker" else "limit_maker"
        limit_params: Optional[LimitParams] = None
        if order_type == "limit_maker":
            limit_params = self._build_limit_params(candle, direction=direction, r_value=r_value)

        order_intent_id = self._new_order_intent_id()
        trade_id = self._new_trade_id()
        side = "buy" if direction == "long" else "sell"
        intent = ExecutionIntent(
            order_id=order_intent_id,
            side=side,
            qty=requested_qty,
            symbol=str(self.instrument.get("symbol") or ""),
            order_type=order_type,
            requested_price=float(candle.close),
            limit_params=limit_params,
            metadata={
                "direction": direction,
                "symbol": self.instrument.get("symbol"),
                "entry_request_id": entry_request_id,
            },
        )

        return EntryRequest(
            trade_id=trade_id,
            order_intent_id=order_intent_id,
            entry_request_id=entry_request_id,
            direction=direction,
            requested_qty=requested_qty,
            qty_raw=qty_raw,
            r_ticks=float(r_ticks),
            r_value=r_value,
            atr_at_entry=atr_at_entry,
            r_multiple_at_entry=self.r_multiple,
            order_type=order_type,
            limit_params=limit_params,
            side=side,
            requested_price=float(candle.close),
            intent=intent,
            validation=EntryValidation(ok=True),
            margin_info=margin_info,
            was_margin_capped=was_margin_capped,
        )

    def build_entry_fill(
        self,
        *,
        pending: PendingEntry,
        outcome: "ExecutionOutcome",
        candle: Candle,
    ) -> EntryFill:
        fill_price = float(outcome.avg_fill_price or candle.close)
        return EntryFill(
            order_intent_id=str(pending.order_intent_id),
            trade_id=str(pending.trade_id),
            candle=CandleSnapshot(
                time=candle.time,
                open=float(candle.open),
                high=float(candle.high),
                low=float(candle.low),
                close=float(candle.close),
                atr=candle.atr,
                lookback_15=candle.lookback_15,
            ),
            filled_qty=float(outcome.filled_qty or 0.0),
            fill_price=fill_price,
            fee_paid=float(outcome.fee_paid or 0.0),
            liquidity_role=str(outcome.fee_role or "unknown"),
            fill_time=outcome.filled_at or outcome.updated_at,
            raw={"outcome": asdict(outcome)},
        )

    def apply_entry_fill(
        self,
        *,
        request: EntryRequest,
        pending: Optional[PendingEntry],
        fill: EntryFill,
    ) -> EntryFillResult:
        events: List[Dict[str, Any]] = []
        settlement_payloads: List[Dict[str, Any]] = []

        if fill.filled_qty <= 0:
            return EntryFillResult(
                status="rejected",
                pending=None,
                position=None,
                events=events,
                settlement_payloads=settlement_payloads,
                rejection_reason="ENTRY_FILL_EMPTY",
                rejection_detail={"filled_qty": fill.filled_qty},
            )

        if pending is None:
            pending = PendingEntry(
                request=request,
                intent=request.intent or ExecutionIntent(
                    order_id=str(request.order_intent_id or ""),
                    side=request.side,
                    qty=request.requested_qty,
                    symbol=str(self.instrument.get("symbol") or ""),
                    order_type=request.order_type,
                    requested_price=request.requested_price,
                    limit_params=request.limit_params,
                    metadata={"direction": request.direction, "symbol": self.instrument.get("symbol")},
                ),
                direction=request.direction,
                qty_raw=request.qty_raw,
                requested_qty=request.requested_qty,
                r_ticks=float(request.r_ticks),
                r_value=request.r_value,
                atr_at_entry=request.atr_at_entry,
                r_multiple_at_entry=request.r_multiple_at_entry,
                order_intent_id=str(request.order_intent_id),
                trade_id=str(request.trade_id),
                validity_remaining=0,
                fallback=request.limit_params.fallback if request.limit_params else "cancel",
                remaining_qty=float(request.requested_qty),
            )

        filled_qty_total = float(pending.filled_qty) + float(fill.filled_qty)
        filled_notional_total = float(pending.filled_notional) + (float(fill.filled_qty) * float(fill.fill_price))
        fees_paid_total = float(pending.fees_paid) + float(fill.fee_paid or 0.0)
        remaining_qty = max(float(request.requested_qty) - filled_qty_total, 0.0)
        avg_fill_price = filled_notional_total / filled_qty_total if filled_qty_total else 0.0

        outcome_payload = {}
        if isinstance(fill.raw, dict):
            outcome_payload = dict(fill.raw.get("outcome") or {})
        if fill.candle is None or not fill.candle.is_complete():
            return EntryFillResult(
                status="rejected",
                pending=None,
                position=None,
                events=events,
                settlement_payloads=settlement_payloads,
                rejection_reason="ENTRY_CANDLE_MISSING",
                rejection_detail={"order_intent_id": pending.order_intent_id},
            )
        candle = Candle(
            time=fill.candle.time,
            open=float(fill.candle.open),
            high=float(fill.candle.high),
            low=float(fill.candle.low),
            close=float(fill.candle.close),
            atr=fill.candle.atr,
            lookback_15=fill.candle.lookback_15,
        )

        pending.filled_qty = filled_qty_total
        pending.filled_notional = filled_notional_total
        pending.fees_paid = fees_paid_total
        pending.remaining_qty = remaining_qty

        if filled_qty_total + 1e-12 < float(request.requested_qty):
            return EntryFillResult(
                status="pending",
                pending=pending,
                position=None,
                events=events,
                settlement_payloads=settlement_payloads,
            )

        notional = abs(avg_fill_price * self.contract_size * filled_qty_total)
        if self.min_notional not in (None, 0) and notional < float(self.min_notional):
            self.pop_wallet_fill_metadata(str(pending.trade_id))
            context = self.runtime_log_context(
                reason="MIN_NOTIONAL_NOT_MET",
                notional=round(notional, 4),
                min_notional=self.min_notional,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return EntryFillResult(
                status="rejected",
                pending=None,
                position=None,
                events=events,
                settlement_payloads=settlement_payloads,
                rejection_reason="MIN_NOTIONAL_NOT_MET",
                rejection_detail={
                    "notional": notional,
                    "min_notional": self.min_notional,
                    "entry_request_id": pending.request.entry_request_id,
                    "attempt_id": pending.request.entry_request_id,
                    "order_request_id": pending.order_intent_id,
                },
            )

        base_currency, quote_currency = self._resolve_base_quote()
        use_wallet_execution = bool(self.execution_adapter and self._wallet_gateway)
        if use_wallet_execution:
            settled = self.entry_settlement.apply_entry_fill(
                EntrySettlementContext(
                    side=pending.intent.side,
                    filled_qty=filled_qty_total,
                    entry_price=avg_fill_price,
                    notional=notional,
                    fee_paid=fees_paid_total,
                    trade_id=pending.trade_id,
                    direction=pending.direction,
                    qty_raw=pending.qty_raw,
                    base_currency=base_currency,
                    quote_currency=quote_currency,
                )
            )
            if not settled:
                settlement_detail = dict(self.last_rejection_detail or {})
                settlement_detail.pop("trade_id", None)
                settlement_detail.setdefault("entry_request_id", pending.request.entry_request_id)
                settlement_detail.setdefault("attempt_id", pending.request.entry_request_id)
                settlement_detail.setdefault("settlement_attempt_id", pending.trade_id)
                settlement_detail.setdefault("order_request_id", pending.order_intent_id)
                return EntryFillResult(
                    status="rejected",
                    pending=None,
                    position=None,
                    events=events,
                    settlement_payloads=settlement_payloads,
                    rejection_reason=self.last_rejection_reason or "ENTRY_SETTLEMENT_FAILED",
                    rejection_detail=settlement_detail,
                )

        stop_price = self._calculate_stop_price(avg_fill_price, pending.direction, pending.r_ticks)
        legs = self._build_legs(
            candle,
            pending.direction,
            pending.r_ticks,
            filled_qty_total,
            entry_price=avg_fill_price,
            qty_raw=pending.qty_raw,
            qty_final=filled_qty_total,
            order_intent_id=pending.order_intent_id,
            side=pending.intent.side,
        )
        if not legs:
            self.pop_wallet_fill_metadata(str(pending.trade_id))
            rounded_qty = (
                self._floor_to_step(pending.requested_qty, self.qty_step)
                if self.qty_step not in (None, 0)
                else pending.requested_qty
            )
            rejection_reason = "QTY_ROUNDS_TO_ZERO" if rounded_qty <= 0 else "TP_LEGS_EMPTY"
            context = self.runtime_log_context(
                reason=rejection_reason,
                requested_qty=pending.requested_qty,
                rounded_qty=rounded_qty,
                qty_step=self.qty_step,
                min_qty=self.min_qty,
                min_notional=self.min_notional,
                tp_leg_count=len(self.orders),
                tp_allocation=self._last_tp_allocation,
            )
            logger.warning(with_log_context("entry_rejected", context))
            return EntryFillResult(
                status="rejected",
                pending=None,
                position=None,
                events=events,
                settlement_payloads=settlement_payloads,
                rejection_reason=rejection_reason,
                rejection_detail={
                    "requested_qty": pending.requested_qty,
                    "rounded_qty": rounded_qty,
                    "symbol": self.instrument.get("symbol"),
                    "qty_step": self.qty_step,
                    "min_qty": self.min_qty,
                    "min_notional": self.min_notional,
                    "tp_leg_count": len(self.orders),
                    "tp_allocation": self._last_tp_allocation,
                },
            )

        runtime_stop_adjustments = self._build_stop_adjustments(legs, pending.r_ticks)
        breakeven_ticks = 0.0 if runtime_stop_adjustments else self._breakeven_threshold(legs, pending.r_ticks)
        trailing_activation_ticks = self._trailing_activation_ticks(legs, pending.r_ticks)
        trailing_distance_ticks = self._trailing_distance_ticks(pending.atr_at_entry)
        position = self._build_position(
            candle=candle,
            entry_price=avg_fill_price,
            stop_price=stop_price,
            direction=pending.direction,
            entry_order=asdict(pending.intent),
            entry_outcome=outcome_payload
            or {
                "avg_fill_price": avg_fill_price,
                "filled_qty": filled_qty_total,
                "fee_paid": fees_paid_total,
                "fee_role": fill.liquidity_role,
                "filled_at": fill.fill_time,
            },
            legs=legs,
            breakeven_ticks=breakeven_ticks,
            trailing_activation_ticks=trailing_activation_ticks,
            trailing_distance_ticks=trailing_distance_ticks,
            runtime_stop_adjustments=runtime_stop_adjustments,
            base_currency=base_currency,
            quote_currency=quote_currency,
            atr_at_entry=pending.atr_at_entry,
            r_multiple_at_entry=pending.r_multiple_at_entry,
            r_value=pending.r_value,
            r_ticks=pending.r_ticks,
            trade_id=pending.trade_id,
            pre_entry_context=getattr(candle, "lookback_15", None),
            use_wallet_execution=use_wallet_execution,
            execution_profile=self.execution_profile,
        )
        position.apply_entry_fee(fees_paid_total)
        position.wallet_fill_metadata = self.pop_wallet_fill_metadata(str(pending.trade_id))

        events.append(
            {
                "type": "entry_opened",
                "trade_id": pending.trade_id,
                "order_intent_id": pending.order_intent_id,
                "avg_fill_price": avg_fill_price,
                "filled_qty": filled_qty_total,
                "fee_paid": fees_paid_total,
                "direction": pending.direction,
            }
        )

        return EntryFillResult(
            status="opened",
            pending=None,
            position=position,
            events=events,
            settlement_payloads=settlement_payloads,
        )

    @staticmethod
    def _has_valid_atr(value: Optional[float]) -> bool:
        if value is None:
            return False
        if isinstance(value, float) and math.isnan(value):
            return False
        if value == 0:
            return False
        return True

    def _breakeven_threshold(self, legs: Sequence[Leg], r_ticks: Optional[float]) -> float:
        if self.stop_adjustments_config:
            return 0.0
        breakeven = self.template.get("breakeven_trigger_ticks")
        if breakeven not in (None, "", 0):
            try:
                return float(breakeven)
            except (TypeError, ValueError):
                return 0.0
        leg_ticks = min((leg.ticks for leg in legs if leg.ticks), default=None)
        if leg_ticks is None:
            return 0.0
        return max(leg_ticks / 2, 0.0)

    def _trailing_activation_ticks(self, legs: Sequence[Leg], r_ticks: Optional[float]) -> Optional[float]:
        trailing_activation_config = self.template.get("trailing_activation")
        trailing_activation_ticks = None
        if trailing_activation_config:
            try:
                trailing_activation_ticks = float(trailing_activation_config)
            except (TypeError, ValueError):
                trailing_activation_ticks = None
        if trailing_activation_ticks in (None, 0):
            return None
        return float(trailing_activation_ticks)

    def _trailing_distance_ticks(self, atr_at_entry: Optional[float]) -> Optional[float]:
        trailing = self.template.get("trailing_stop")
        if not isinstance(trailing, Mapping):
            trailing = {}
        trailing_ticks = coerce_float(trailing.get("ticks"))
        trailing_atr_multiple = coerce_float(trailing.get("atr_multiplier"))
        trailing_distance_ticks = None
        if trailing_atr_multiple not in (None, 0) and self._has_valid_atr(atr_at_entry):
            trailing_distance_ticks = float(trailing_atr_multiple) * float(atr_at_entry) / float(self.tick_size or 1)
        elif trailing_ticks not in (None, 0):
            trailing_distance_ticks = float(trailing_ticks)
        return trailing_distance_ticks

    def _compute_r_ticks(self, candle: Candle) -> float:
        """Compute stop distance in ticks from initial_stop config.

        Stops are ALWAYS derived from ATR * atr_multiplier from initial_stop config.
        Raises ValueError if ATR is invalid or configuration is missing.
        """
        if not self._has_valid_atr(candle.atr):
            raise ValueError(
                f"Cannot compute stop: ATR is required but got {candle.atr}. "
                f"Ensure strategy includes ATR indicator and candles have valid ATR data."
            )

        if self.tick_size in (None, 0):
            raise ValueError("tick_size is required to compute ATR-based stops")

        if self.r_multiple in (None, 0):
            raise ValueError(
                f"Cannot compute stop: initial_stop.atr_multiplier is required but got {self.r_multiple}. "
                f"Configure atr_multiplier in strategy template."
            )

        tick_stop = int(round((candle.atr * self.r_multiple) / self.tick_size))
        if tick_stop <= 0:
            raise ValueError(
                f"Computed stop is {tick_stop} ticks (ATR={candle.atr}, multiplier={self.r_multiple}, "
                f"tick_size={self.tick_size}). Stop must be > 0."
            )

        return float(tick_stop)

    def _build_stop_adjustments(self, legs: Sequence[Leg], r_ticks: Optional[float]) -> List[Dict[str, Any]]:
        adjustments: List[Dict[str, Any]] = []
        for entry in self.stop_adjustments_config:
            if not isinstance(entry, Mapping):
                continue
            trigger_type = str(entry.get("trigger_type") or "target_id")
            trigger_target_id = entry.get("trigger_target_id")
            trigger_ticks = coerce_float(entry.get("trigger_ticks"))
            action_type = str(entry.get("action_type") or "move_to_breakeven")
            action_r = coerce_float(entry.get("action_r"))
            if trigger_type == "r_multiple" and trigger_ticks in (None, 0):
                continue
            if trigger_type != "r_multiple" and not trigger_target_id:
                continue
            adjustments.append(
                {
                    "trigger_type": trigger_type,
                    "trigger_target_id": trigger_target_id,
                    "trigger_ticks": trigger_ticks,
                    "action_type": action_type,
                    "action_r": action_r,
                }
            )
        return adjustments

    def _build_position(
        self,
        *,
        candle: Candle,
        entry_price: float,
        stop_price: float,
        direction: str,
        entry_order: Dict[str, Any],
        entry_outcome: Dict[str, Any],
        legs: List[Leg],
        breakeven_ticks: float,
        trailing_activation_ticks: Optional[float],
        trailing_distance_ticks: Optional[float],
        runtime_stop_adjustments: List[Dict[str, Any]],
        base_currency: Optional[str],
        quote_currency: Optional[str],
        atr_at_entry: Optional[float],
        r_multiple_at_entry: Optional[float],
        r_value: Optional[float],
        r_ticks: Optional[float],
        trade_id: str,
        pre_entry_context: Optional[Dict[str, Optional[float]]],
        use_wallet_execution: bool,
        execution_profile: Optional[SeriesExecutionProfile],
    ) -> LadderPosition:
        self.trailing_config = (
            self.template.get("trailing_stop") if isinstance(self.template.get("trailing_stop"), dict) else {}
        )
        self._last_tp_allocation = None
        trailing_atr_multiple = float(self.trailing_config.get("atr_multiplier") or 0.0)

        return LadderPosition(
            entry_time=candle.time,
            entry_price=entry_price,
            entry_order=entry_order,
            entry_outcome=entry_outcome,
            direction=direction,
            stop_price=stop_price,
            tick_size=self.tick_size,
            execution_model=self.execution_model if use_wallet_execution else None,
            execution_adapter=self.execution_adapter if use_wallet_execution else None,
            wallet_gateway=self._wallet_gateway if use_wallet_execution else None,
            exit_settlement=self.exit_settlement,
            base_currency=base_currency,
            quote_currency_code=quote_currency,
            legs=legs,
            breakeven_trigger_ticks=breakeven_ticks,
            tick_value=self.tick_value,
            contract_size=self.contract_size,
            maker_fee_rate=self.maker_fee,
            taker_fee_rate=self.taker_fee,
            quote_currency=self.quote_currency,
            short_requires_borrow=bool(self.short_requires_borrow),
            instrument=self.instrument if use_wallet_execution else None,
            execution_profile=execution_profile if use_wallet_execution else None,
            signal_id=getattr(self, "last_signal_id", None),
            decision_id=getattr(self, "last_decision_id", None),
            strategy_id=str(self.strategy_id) if getattr(self, "strategy_id", None) else None,
            bar_time=candle.time,
            atr_at_entry=atr_at_entry,
            r_multiple_at_entry=r_multiple_at_entry,
            r_value=r_value,
            r_ticks=r_ticks,
            trailing_activation_ticks=trailing_activation_ticks,
            trailing_distance_ticks=trailing_distance_ticks,
            trailing_atr_multiple=trailing_atr_multiple,
            pre_entry_context=pre_entry_context,
            stop_adjustments=runtime_stop_adjustments,
            trade_id=trade_id,
        )

    def _r_value(self, candle: Candle) -> Optional[float]:
        """Calculate the monetary value of 1R (ATR * multiplier * tick_value)."""
        if not self._has_valid_atr(candle.atr):
            return None
        return self.tick_value * candle.atr * self.r_multiple

    def _r_ticks(self, candle: Candle) -> Optional[float]:
        """Calculate R in ticks (ATR * multiplier / tick_size)."""
        if not self._has_valid_atr(candle.atr) or self.tick_size in (None, 0):
            return None
        return float((candle.atr * self.r_multiple) / self.tick_size)

    def _calculate_stop_price(self, entry_price: float, direction: str, r_ticks: float) -> float:
        """Calculate initial stop loss price for position.

        Args:
            candle: Current candle
            direction: Trade direction ("long" or "short")
            r_ticks: Stop distance in ticks (must be > 0)

        Returns:
            Stop price
        """
        if r_ticks <= 0:
            raise ValueError(f"r_ticks must be > 0, got {r_ticks}")

        stop_distance = r_ticks * self.tick_size
        if direction == "long":
            return entry_price - stop_distance
        return entry_price + stop_distance

    def _floor_to_step(self, qty: float, step: float) -> float:
        if step in (None, 0):
            return qty
        return math.floor((qty + 1e-12) / step) * step

    def _ceil_to_step(self, qty: float, step: float) -> float:
        if step in (None, 0):
            return qty
        return math.ceil((qty - 1e-12) / step) * step

    def _calculate_total_contracts(self, r_ticks: float) -> float:
        """Calculate total contracts based on base_risk_per_trade and R value.

        Formula: contracts = base_risk_per_trade / (r_ticks * tick_value)

        Returns:
            Total number of contracts to trade, or None if sizing cannot honor risk

        Raises:
            ValueError: If base_risk_per_trade is not configured
        """
        if self.base_risk_per_trade is None or self.base_risk_per_trade <= 0:
            raise ValueError(
                f"base_risk_per_trade is required but got {self.base_risk_per_trade}. "
                f"Configure risk_config.base_risk_per_trade in your strategy or bot sizing config. "
                f"This is required for dynamic position sizing."
            )

        if r_ticks <= 0:
            raise ValueError(
                f"Cannot calculate position size: r_ticks must be > 0, got {r_ticks}"
            )

        if self.tick_value <= 0:
            raise ValueError(
                f"Cannot calculate position size: tick_value must be > 0, got {self.tick_value}"
            )

        # Calculate dollar value of 1R per contract
        r_value_per_contract = r_ticks * self.tick_value

        # Calculate how many contracts fit within base_risk_per_trade
        contracts = self.base_risk_per_trade / r_value_per_contract

        # Apply global and instrument risk multipliers
        contracts = contracts * self.global_risk_multiplier * self.instrument_risk_multiplier

        sizing_context = self.runtime_log_context(
            base_risk=self.base_risk_per_trade,
            r_value_per_contract=r_value_per_contract,
            raw_qty=contracts,
        )
        logger.info(with_log_context("position_sizing", sizing_context))
        return float(contracts)

    def _cap_qty_by_margin(
        self,
        risk_qty: float,
        price: float,
        direction: str,
    ) -> Tuple[float, bool, Optional[Dict[str, Any]]]:
        """Cap risk-based qty by available margin for futures/derivatives.

        For spot instruments, returns risk_qty unchanged.
        For futures/swaps, calculates max qty allowed by wallet margin and caps.

        Args:
            risk_qty: Qty calculated from risk-based sizing
            price: Current price
            direction: "long" or "short"

        Returns:
            Tuple of (final_qty, was_capped, margin_info)
        """
        if not self._wallet_gateway:
            return risk_qty, False, None

        # Margin cap applies only to margin-accounting profiles.
        if self.execution_profile is None or not self.execution_profile.is_margin_accounting():
            return risk_qty, False, None

        # Get available collateral from wallet (for backtest, same as cash balance)
        wallet_state = self._wallet_gateway.project()
        quote = self.quote_currency.upper()
        available_collateral = wallet_state.free_collateral.get(quote, wallet_state.balances.get(quote, 0.0))

        if available_collateral <= 0:
            balance_trace = None
            ledger = getattr(self._wallet_gateway, "ledger", None)
            if ledger and hasattr(ledger, "events"):
                balance_trace = trace_wallet_balance(ledger.events(), quote, limit=8)
            return (
                0.0,
                True,
                {
                    "reason": "no_available_collateral",
                    "available_collateral": float(available_collateral),
                    "max_qty_by_margin": 0.0,
                    "cost_per_contract": None,
                    "margin_per_contract": None,
                    "fee_per_contract": None,
                    "margin_rate": None,
                    "calculation_method": None,
                    "balance_trace": balance_trace,
                },
            )

        try:
            margin_result = calculate_max_qty_by_margin(
                available_collateral=available_collateral,
                price=price,
                contract_size=self.contract_size,
                direction=direction,
                instrument=self.instrument,
                execution_profile=self.execution_profile,
                fee_rate=self.taker_fee or 0.0,  # Use taker (worst case) for conservative sizing
                safety_multiplier=1.05,
                qty_step=self.qty_step,
                min_order_size=self.min_qty,
            )
        except ValueError as exc:
            # Instrument misconfigured - fail loud
            return (
                0.0,
                True,
                {
                    "reason": "margin_calculation_failed",
                    "error": str(exc),
                    "available_collateral": float(available_collateral),
                    "max_qty_by_margin": 0.0,
                    "cost_per_contract": None,
                    "margin_per_contract": None,
                    "fee_per_contract": None,
                    "margin_rate": None,
                    "calculation_method": None,
                },
            )

        max_qty = margin_result.max_qty
        was_capped = risk_qty > max_qty

        margin_info = {
            "risk_qty": risk_qty,
            "max_qty_by_margin": max_qty,
            "was_capped": was_capped,
            "available_collateral": available_collateral,
            "cost_per_contract": margin_result.cost_per_contract,
            "margin_per_contract": margin_result.margin_per_contract,
            "fee_per_contract": margin_result.fee_per_contract,
            "margin_rate": margin_result.margin_rate,
            "calculation_method": margin_result.calculation_method,
        }

        final_qty = min(risk_qty, max_qty) if was_capped else risk_qty

        if was_capped:
            context = self.runtime_log_context(
                risk_qty=round(risk_qty, 6),
                max_qty_by_margin=round(max_qty, 6),
                final_qty=round(final_qty, 6),
                available_collateral=round(available_collateral, 2),
                cost_per_contract=round(margin_result.cost_per_contract, 4),
                margin_rate=round(margin_result.margin_rate, 6),
            )
            logger.info(with_log_context("qty_capped_by_margin", context))

        return final_qty, was_capped, margin_info

    def _resolve_base_quote(self) -> Tuple[str, str]:
        base = self.instrument.get("base_currency")
        quote = self.instrument.get("quote_currency")
        if self.execution_profile is not None:
            base = base or self.execution_profile.instrument.base_currency
            quote = quote or self.execution_profile.instrument.quote_currency
        symbol = str(self.instrument.get("symbol") or "")
        if not base or not quote:
            context = self.runtime_log_context(
                base_currency=base,
                quote_currency=quote,
                instrument=self.instrument,
            )
            logger.error(with_log_context("instrument_base_quote_missing", context))
            raise ValueError(f"Cannot resolve base/quote currencies for instrument {symbol}")
        return str(base).upper(), str(quote).upper()

    def _resolve_tp_step(self) -> Optional[float]:
        step = self.qty_step
        if self.execution_profile is not None and self.execution_profile.is_derivatives():
            if step not in (None, 0):
                if step >= 1 and abs(step - round(step)) <= 1e-9:
                    return float(step)
                return None
            symbol = self.instrument.get("symbol")
            raise ValueError(f"Missing instrument metadata qty step for TP allocation: {symbol}")

        if step not in (None, 0) and step >= 1 and abs(step - round(step)) <= 1e-9:
            return float(step)
        return None

    def _allocate_tp_contracts(
        self,
        *,
        qty_final: float,
        tp_leg_count: int,
        step: float,
    ) -> Tuple[List[float], List[int]]:
        if tp_leg_count <= 0:
            return [], []
        total_units = int(math.floor((qty_final + 1e-12) / step))
        if total_units <= 0:
            return [0.0 for _ in range(tp_leg_count)], list(range(1, tp_leg_count + 1))
        if total_units < tp_leg_count:
            units = [1] * total_units + [0] * (tp_leg_count - total_units)
        else:
            base = total_units // tp_leg_count
            remainder = total_units % tp_leg_count
            units = [base + (1 if idx < remainder else 0) for idx in range(tp_leg_count)]
        contracts = [float(unit) * float(step) for unit in units]
        dropped = [idx + 1 for idx, qty in enumerate(contracts) if qty <= 0]
        return contracts, dropped

    def _build_legs(
        self,
        candle: Candle,
        direction: str,
        r_ticks: Optional[float],
        total_contracts: float,
        *,
        entry_price: float,
        qty_raw: Optional[float] = None,
        qty_final: Optional[float] = None,
        order_intent_id: Optional[str] = None,
        side: Optional[str] = None,
    ) -> List[Leg]:
        """Build take-profit legs from template configuration.

        Args:
            candle: Current candle data
            direction: Trade direction ('long' or 'short')
            r_ticks: Stop distance in ticks
            total_contracts: Total number of contracts to distribute across legs
            qty_raw: Raw qty before normalization (for logging)
            qty_final: Normalized qty used for allocation (for logging)
            order_intent_id: Correlation id for log tracing
            side: Order side for log context
        """
        leg_specs: List[Dict[str, Any]] = []

        for idx, order in enumerate(self.orders):
            ticks = order.get("ticks")
            r_multiple = order.get("r_multiple")
            price = order.get("price")
            target_ticks = ticks
            target_price = None

            # Calculate target price based on configuration type
            if r_multiple not in (None, 0) and r_ticks not in (None, 0):
                computed_ticks = float(r_multiple) * float(r_ticks)
                distance = computed_ticks * self.tick_size
                target_price = entry_price + distance if direction == "long" else entry_price - distance
                target_ticks = int(round(computed_ticks))
            elif ticks is not None:
                distance = ticks * self.tick_size
                target_price = entry_price + distance if direction == "long" else entry_price - distance
            elif price is not None:
                target_price = float(price)
                computed_ticks = risk_math.ticks_from_entry(entry_price, target_price, direction, self.tick_size)
                target_ticks = int(round(computed_ticks))

            if target_price is None:
                continue

            leg_specs.append(
                {
                    "name": order.get("label") or f"TP{target_ticks or ticks or idx + 1}",
                    "ticks": target_ticks or 0,
                    "target_price": target_price,
                    "leg_id": order.get("id") or order.get("label") or f"tp-{idx + 1}",
                    "order": order,
                }
            )

        qty_raw_value = float(qty_raw) if qty_raw is not None else float(total_contracts)
        qty_final_value = float(qty_final) if qty_final is not None else float(total_contracts)
        tp_leg_count = len(leg_specs)

        contracts_by_leg: List[float] = []
        dropped_legs: List[int] = []

        tp_step = self._resolve_tp_step()
        if tp_step is not None:
            contracts_by_leg, dropped_legs = self._allocate_tp_contracts(
                qty_final=qty_final_value,
                tp_leg_count=tp_leg_count,
                step=tp_step,
            )
        else:
            for spec in leg_specs:
                size_fraction = coerce_float(spec["order"].get("size_fraction"))
                if size_fraction is not None and 0 < size_fraction <= 1:
                    leg_contracts = float(qty_final_value) * float(size_fraction)
                else:
                    leg_contracts = float(qty_final_value) / float(tp_leg_count or 1)
                contracts_by_leg.append(leg_contracts)

            step = self.qty_step
            if step not in (None, 0):
                rounded: List[float] = []
                for qty in contracts_by_leg:
                    rounded_qty = float(int((qty + 1e-12) / step)) * float(step)
                    rounded.append(rounded_qty)
                total_allocated = sum(rounded)
                remainder = qty_final_value - total_allocated
                if remainder > 0:
                    extra = float(int((remainder + 1e-12) / step)) * float(step)
                    if extra > 0:
                        rounded[-1] += extra
                contracts_by_leg = rounded

            dropped_legs = [idx + 1 for idx, qty in enumerate(contracts_by_leg) if qty <= 0]

        if dropped_legs:
            if qty_final_value < tp_leg_count:
                drop_reason = "INSUFFICIENT_CONTRACTS_FOR_LEGS"
                drop_explain = f"qty_final {qty_final_value} < tp_leg_count {tp_leg_count}; dropped legs {dropped_legs}"
            elif qty_final_value < qty_raw_value:
                drop_reason = "QTY_NORMALIZED_DOWN"
                drop_explain = f"qty normalized down from {qty_raw_value} to {qty_final_value}; dropped legs {dropped_legs}"
            else:
                drop_reason = "INSUFFICIENT_CONTRACTS_FOR_LEGS"
                drop_explain = f"tp allocation dropped legs {dropped_legs}"
        else:
            drop_reason = "NONE"
            drop_explain = "no legs dropped"

        context = self.runtime_log_context(
            order_intent_id=order_intent_id,
            side=side,
            qty_raw=qty_raw_value,
            qty_final=qty_final_value,
            qty_step=self.qty_step,
            tp_step=tp_step,
            min_order_size=self.min_qty,
            tp_leg_count=tp_leg_count,
            tp_contracts=contracts_by_leg,
            dropped_legs=dropped_legs,
            drop_reason=drop_reason,
            drop_explain=drop_explain,
        )
        logger.info(with_log_context("tp_leg_allocation_finalized", context))
        self._last_tp_allocation = dict(context)

        legs: List[Leg] = []
        for spec, contracts in zip(leg_specs, contracts_by_leg):
            if contracts <= 0:
                continue
            legs.append(
                Leg(
                    name=spec["name"],
                    ticks=spec["ticks"],
                    target_price=spec["target_price"],
                    contracts=contracts,
                    leg_id=spec["leg_id"],
                )
            )

        if not legs:
            return []

        min_qty = self.min_qty
        if min_qty not in (None, 0):
            for leg in legs:
                if leg.contracts < float(min_qty):
                    return []

        return legs

    def maybe_enter(self, candle: Candle, direction: Optional[str]) -> Optional[LadderPosition]:
        if direction is None or self.active_trade is not None or self.entry_execution.has_pending:
            return None
        if not self.execution_adapter:
            raise ValueError("Execution adapter is required for trade execution")
        if not self._wallet_gateway:
            raise ValueError("Wallet gateway is required for trade execution")
        if direction == "short" and not self.can_short:
            self.last_rejection_reason = "CAN_SHORT_DISABLED"
            self.last_rejection_detail = {"symbol": self.instrument.get("symbol"), "direction": direction}
            context = self.runtime_log_context(
                reason="CAN_SHORT_DISABLED",
                direction=direction,
            )
            logger.warning(with_log_context("short_entry_rejected", context))
            return None
        self.active_trade = self.entry_execution.submit_entry(candle, direction)
        if self.active_trade is None:
            return None
        self.trades.append(self.active_trade)
        self._bump_trade_revision()
        return self.active_trade

    def step(self, candle: Candle) -> List[Dict[str, Any]]:
        if self.active_trade is None:
            new_trade = self.entry_execution.process_pending(candle)
            if new_trade:
                self.active_trade = new_trade
                self.trades.append(self.active_trade)
                self._bump_trade_revision()
            if self.active_trade is None:
                return []
        events = self.active_trade.apply_bar(candle)
        self._bump_trade_revision()
        if not self.active_trade.is_active():
            self.active_trade = None
            self._bump_trade_revision()
        return events

    def _bump_trade_revision(self) -> None:
        self.trade_revision += 1

    def serialise_trades(self) -> List[Dict[str, object]]:
        return [trade.serialize() for trade in self.trades]

    def stats(self) -> Dict[str, float]:
        legs = [leg for trade in self.trades for leg in trade.legs]
        leg_wins = sum(1 for leg in legs if leg.status == "target")
        leg_losses = sum(1 for leg in legs if leg.status == "stop")
        completed = [trade for trade in self.trades if not trade.is_active()]
        tolerance = 1e-8
        trade_wins = sum(1 for trade in completed if trade.net_pnl > tolerance)
        trade_losses = sum(1 for trade in completed if trade.net_pnl < -tolerance)
        breakeven = max(len(completed) - trade_wins - trade_losses, 0)
        completed_total = len(completed)
        denominator = completed_total or 1
        long_trades = sum(1 for trade in self.trades if trade.direction == "long")
        short_trades = sum(1 for trade in self.trades if trade.direction == "short")
        gross = sum(trade.gross_pnl for trade in self.trades)
        fees = sum(trade.fees_paid for trade in self.trades)
        net = gross - fees
        return {
            "total_trades": len(self.trades),
            "completed_trades": completed_total,
            "legs_closed": leg_wins + leg_losses,
            "wins": trade_wins,
            "losses": trade_losses,
            "breakeven_trades": breakeven,
            "win_rate": round(trade_wins / denominator, 4),
            "long_trades": long_trades,
            "short_trades": short_trades,
            "gross_pnl": round(gross, 4),
            "fees_paid": round(fees, 4),
            "net_pnl": round(net, 4),
            "quote_currency": self.quote_currency,
        }
