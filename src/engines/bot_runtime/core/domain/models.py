"""Domain datamodels for bot runtime core."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, TYPE_CHECKING

from ..execution_intent import ExecutionIntent, LimitParams
from .time_utils import isoformat

if TYPE_CHECKING:
    from ..entry_execution import PendingEntry
    from .position import LadderPosition


@dataclass
class Candle:
    """Single OHLC datapoint used by the simulated bot."""

    time: datetime
    open: float
    high: float
    low: float
    close: float
    end: Optional[datetime] = None
    atr: Optional[float] = None
    volume: Optional[float] = None
    range: Optional[float] = None
    lookback_15: Optional[Dict[str, Optional[float]]] = None

    def serialize(self) -> Dict[str, Optional[float]]:
        payload = {
            "time": isoformat(self.time),
            "open": round(self.open, 4),
            "high": round(self.high, 4),
            "low": round(self.low, 4),
            "close": round(self.close, 4),
            "end": isoformat(self.end),
        }
        if self.range is not None:
            payload["range"] = round(self.range, 6)
        if self.atr is not None:
            payload["atr"] = round(self.atr, 6)
        if self.volume is not None:
            payload["volume"] = round(self.volume, 6)
        return payload

    def to_dict(self) -> Dict[str, Optional[float]]:
        return self.serialize()

    @property
    def start_time(self) -> datetime:
        return self.time

    @property
    def end_time(self) -> datetime:
        return self.end or self.time


@dataclass(frozen=True)
class StrategySignal:
    """Queued strategy action derived from rule markers."""

    epoch: int
    direction: str
    signal_id: Optional[str] = None
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    strategy_hash: Optional[str] = None
    decision_id: Optional[str] = None
    rule_id: Optional[str] = None
    intent: Optional[str] = None
    event_key: Optional[str] = None

    def __post_init__(self) -> None:
        signal_id = self._optional_text(self.signal_id)
        decision_id = self._optional_text(self.decision_id)
        object.__setattr__(self, "signal_id", signal_id)
        object.__setattr__(self, "source_type", self._optional_text(self.source_type))
        object.__setattr__(self, "source_id", self._optional_text(self.source_id))
        object.__setattr__(self, "strategy_hash", self._optional_text(self.strategy_hash))
        object.__setattr__(self, "decision_id", decision_id)
        object.__setattr__(self, "rule_id", self._optional_text(self.rule_id))
        object.__setattr__(self, "intent", self._optional_text(self.intent))
        object.__setattr__(self, "event_key", self._optional_text(self.event_key))
        if signal_id is not None and decision_id is not None and signal_id == decision_id:
            raise RuntimeError(
                "strategy_signal_invalid: signal_id must not equal decision_id "
                f"decision_id={decision_id}"
            )

    @classmethod
    def from_decision_artifact(
        cls,
        artifact: Mapping[str, Any],
        *,
        source_type: Optional[str] = None,
        source_id: Optional[str] = None,
    ) -> "StrategySignal":
        decision_id = str(artifact.get("decision_id") or "").strip()
        rule_id = str(artifact.get("rule_id") or "").strip()
        if not decision_id:
            raise RuntimeError(
                "strategy_signal_invalid: decision artifact missing decision_id "
                f"rule_id={rule_id or '<missing>'}"
            )
        raw_epoch = artifact.get("bar_epoch")
        try:
            epoch = int(raw_epoch)
        except (TypeError, ValueError):
            raise RuntimeError(
                "strategy_signal_invalid: decision artifact missing bar_epoch "
                f"decision_id={decision_id or '<missing>'} rule_id={rule_id or '<missing>'}"
            ) from None
        intent = cls._normalize_intent(artifact.get("emitted_intent") or artifact.get("intent"))
        trigger = artifact.get("trigger") if isinstance(artifact.get("trigger"), Mapping) else {}
        return cls(
            epoch=epoch,
            direction="long" if intent == "enter_long" else "short",
            signal_id=cls._resolve_signal_id(
                artifact=artifact,
                decision_id=decision_id,
                source_type=source_type,
                source_id=source_id,
            ),
            source_type=cls._optional_text(source_type),
            source_id=cls._optional_text(source_id),
            strategy_hash=cls._optional_text(artifact.get("strategy_hash")),
            decision_id=decision_id or None,
            rule_id=rule_id or None,
            intent=intent,
            event_key=cls._optional_text(trigger.get("event_key")),
        )

    @classmethod
    def from_runtime_event_context(
        cls,
        context: Mapping[str, Any],
        *,
        default_source_id: Optional[str] = None,
    ) -> "StrategySignal":
        decision_artifact = context.get("decision_artifact")
        artifact = dict(decision_artifact) if isinstance(decision_artifact, Mapping) else {}
        if artifact.get("bar_epoch") in (None, ""):
            bar = context.get("bar")
            bar_time = bar.get("time") if isinstance(bar, Mapping) else None
            if isinstance(bar_time, str):
                artifact["bar_epoch"] = cls._epoch_from_iso(bar_time)
        artifact.setdefault("decision_id", context.get("decision_id"))
        artifact.setdefault("rule_id", context.get("rule_id"))
        artifact.setdefault("strategy_hash", context.get("strategy_hash"))
        artifact.setdefault("signal_id", context.get("signal_id"))
        artifact.setdefault("emitted_intent", context.get("intent"))
        artifact.setdefault(
            "trigger",
            {"event_key": context.get("event_key")},
        )
        return cls.from_decision_artifact(
            artifact,
            source_type=cls._optional_text(context.get("source_type")) or "runtime",
            source_id=cls._optional_text(context.get("source_id")) or cls._optional_text(default_source_id),
        )

    @staticmethod
    def _normalize_intent(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in {"enter_long", "buy", "long"}:
            return "enter_long"
        if text in {"enter_short", "sell", "short"}:
            return "enter_short"
        raise RuntimeError(f"strategy_signal_invalid: unsupported intent value={value!r}")

    @staticmethod
    def _optional_text(value: Any) -> Optional[str]:
        text = str(value or "").strip()
        return text or None

    @classmethod
    def build_signal_id(
        cls,
        *,
        decision_id: str,
        source_type: Optional[str] = None,
        source_id: Optional[str] = None,
    ) -> str:
        resolved_decision_id = cls._optional_text(decision_id)
        if resolved_decision_id is None:
            raise RuntimeError("strategy_signal_invalid: decision_id is required to build signal_id")
        return cls._resolve_signal_id(
            artifact={},
            decision_id=resolved_decision_id,
            source_type=source_type,
            source_id=source_id,
        )

    @classmethod
    def _resolve_signal_id(
        cls,
        *,
        artifact: Mapping[str, Any],
        decision_id: str,
        source_type: Optional[str],
        source_id: Optional[str],
    ) -> str:
        signal_id = cls._optional_text(artifact.get("signal_id"))
        if signal_id is not None:
            if signal_id == decision_id:
                raise RuntimeError(
                    "strategy_signal_invalid: signal_id must not equal decision_id "
                    f"decision_id={decision_id}"
                )
            return signal_id
        digest = hashlib.sha1(
            json.dumps(
                {
                    "decision_id": decision_id,
                    "source_type": cls._optional_text(source_type),
                    "source_id": cls._optional_text(source_id),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:24]
        return f"signal:{digest}"

    @staticmethod
    def _epoch_from_iso(value: str) -> int:
        text = str(value or "").strip()
        if not text:
            raise RuntimeError("strategy_signal_invalid: runtime event missing bar.time")
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise RuntimeError(
                f"strategy_signal_invalid: runtime event bar.time is invalid value={value!r}"
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return int(parsed.timestamp())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "epoch": self.epoch,
            "direction": self.direction,
            "signal_id": self.signal_id,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "strategy_hash": self.strategy_hash,
            "decision_id": self.decision_id,
            "rule_id": self.rule_id,
            "intent": self.intent,
            "event_key": self.event_key,
        }


@dataclass
class EntryValidation:
    """Validation result for entry sizing and intent construction."""

    ok: bool
    rejection_reason: Optional[str] = None
    rejection_detail: Optional[Dict[str, Any]] = None


@dataclass
class EntryRequest:
    """Prepared entry sizing and intent information for execution."""

    trade_id: Optional[str]
    order_intent_id: Optional[str]
    entry_request_id: str
    direction: str
    requested_qty: float
    qty_raw: float
    r_ticks: float
    r_value: Optional[float]
    atr_at_entry: Optional[float]
    r_multiple_at_entry: Optional[float]
    order_type: str
    limit_params: Optional[LimitParams]
    side: str
    requested_price: float
    intent: Optional[ExecutionIntent]
    validation: EntryValidation
    margin_info: Optional[Dict[str, Any]]
    was_margin_capped: bool


@dataclass
class CandleSnapshot:
    """Minimal candle context for execution fills."""

    time: datetime
    open: float
    high: float
    low: float
    close: float
    atr: Optional[float] = None
    lookback_15: Optional[Dict[str, Optional[float]]] = None

    def is_complete(self) -> bool:
        return all(
            value is not None
            for value in (
                self.time,
                self.open,
                self.high,
                self.low,
                self.close,
            )
        )


@dataclass
class EntryFill:
    """Normalized entry fill event for execution adapters."""

    order_intent_id: str
    trade_id: str
    candle: Optional[CandleSnapshot]
    filled_qty: float
    fill_price: float
    fee_paid: float
    liquidity_role: Optional[str]
    fill_time: Optional[str]
    raw: Optional[Dict[str, Any]] = None


@dataclass
class EntryFillResult:
    """Result of applying an entry fill against domain state."""

    status: str
    pending: Optional[PendingEntry]
    position: Optional[LadderPosition]
    events: List[Dict[str, Any]]
    settlement_payloads: List[Dict[str, Any]]
    rejection_reason: Optional[str] = None
    rejection_detail: Optional[Dict[str, Any]] = None


@dataclass
class Leg:
    """Take-profit leg metadata."""

    name: str
    ticks: int
    target_price: float
    status: str = "open"
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    exit_created_at: Optional[str] = None
    contracts: float = 1.0
    pnl: float = 0.0
    leg_id: Optional[str] = None

    def serialize(self) -> Dict[str, Optional[float]]:
        return {
            "name": self.name,
            "ticks": self.ticks,
            "target_price": round(self.target_price, 4),
            "status": self.status,
            "exit_price": None if self.exit_price is None else round(self.exit_price, 4),
            "exit_time": self.exit_time,
            "exit_created_at": self.exit_created_at,
            "contracts": self.contracts,
            "pnl": round(self.pnl, 4),
            "id": self.leg_id,
        }


__all__ = [
    "Candle",
    "CandleSnapshot",
    "EntryFill",
    "EntryFillResult",
    "EntryRequest",
    "EntryValidation",
    "Leg",
    "StrategySignal",
]
