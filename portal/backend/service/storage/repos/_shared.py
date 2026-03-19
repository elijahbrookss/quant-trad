"""Shared storage repository primitives and helpers."""

from __future__ import annotations

import logging
import uuid
import math
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from sqlalchemy import delete, func, select, text
from sqlalchemy.exc import SQLAlchemyError

from ....db import (
    ATMTemplateRecord,
    BotRecord,
    BotRunEventRecord,
    BotRunRecord,
    BotRunViewStateRecord,
    BotRunStepRecord,
    BotTradeEventRecord,
    BotTradeRecord,
    IndicatorRecord,
    InstrumentRecord,
    StrategyIndicatorLink,
    StrategyInstrumentLink,
    StrategyRecord,
    StrategyRuleRecord,
    SymbolPresetRecord,
    db,
)
from ...risk.atm import normalise_template

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    """Return a naive UTC timestamp."""

    return datetime.utcnow()


def _parse_optional_timestamp(value: Any) -> Optional[datetime]:
    """Best-effort parsing of ISO8601 strings into naive UTC datetimes."""

    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1]
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _json_safe(value: Any) -> Any:
    """Recursively convert values to JSON-safe primitives."""

    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.isoformat() + "Z"
        return value.astimezone(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


__all__ = [
    "ATMTemplateRecord",
    "BotRecord",
    "BotRunEventRecord",
    "BotRunRecord",
    "BotRunViewStateRecord",
    "BotRunStepRecord",
    "BotTradeEventRecord",
    "BotTradeRecord",
    "IndicatorRecord",
    "InstrumentRecord",
    "StrategyIndicatorLink",
    "StrategyInstrumentLink",
    "StrategyRecord",
    "StrategyRuleRecord",
    "SymbolPresetRecord",
    "db",
    "delete",
    "func",
    "logger",
    "normalise_template",
    "select",
    "SQLAlchemyError",
    "text",
    "timedelta",
    "uuid",
    "_coerce_float",
    "_coerce_int",
    "_json_safe",
    "_parse_optional_timestamp",
    "_utcnow",
]
