"""Lightweight performance logging helpers."""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from contextlib import AbstractContextManager
from typing import Any, Dict, Mapping, Optional

from .log_context import build_log_context, merge_log_context, with_log_context

DEFAULT_OBS_ENABLED = True
DEFAULT_OBS_STEP_SAMPLE_RATE = 0.01
DEFAULT_OBS_SLOW_MS = 250.0


def _coerce_bool(value: Optional[object], default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return default


def _coerce_float(value: Optional[object], default: float) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return default
    return default


def _config_value(config: Optional[Mapping[str, Any]], key: str) -> Optional[object]:
    if not config:
        return None
    if key in config:
        return config.get(key)
    return config.get(key.lower())


def get_obs_enabled(config: Optional[Mapping[str, Any]] = None) -> bool:
    value = _config_value(config, "OBS_ENABLED")
    if value is None:
        value = os.getenv("OBS_ENABLED")
    return _coerce_bool(value, DEFAULT_OBS_ENABLED)


def get_obs_step_sample_rate(config: Optional[Mapping[str, Any]] = None) -> float:
    value = _config_value(config, "OBS_STEP_SAMPLE_RATE")
    if value is None:
        value = os.getenv("OBS_STEP_SAMPLE_RATE")
    rate = _coerce_float(value, DEFAULT_OBS_STEP_SAMPLE_RATE)
    if rate <= 0:
        return 0.0
    if rate >= 1:
        return 1.0
    return rate


def get_obs_slow_ms(config: Optional[Mapping[str, Any]] = None) -> float:
    value = _config_value(config, "OBS_SLOW_MS")
    if value is None:
        value = os.getenv("OBS_SLOW_MS")
    slow_ms = _coerce_float(value, DEFAULT_OBS_SLOW_MS)
    return slow_ms if slow_ms > 0 else DEFAULT_OBS_SLOW_MS


def should_sample(rate: float, *, random_fn: Optional[Any] = None) -> bool:
    if rate <= 0:
        return False
    if rate >= 1:
        return True
    rng = random_fn or random.random
    return rng() < rate


class PerfLog(AbstractContextManager["PerfLog"]):
    """Context manager that logs timing metrics on exit."""

    def __init__(
        self,
        event: str,
        *,
        logger: logging.Logger,
        base_context: Optional[Mapping[str, object]] = None,
        enabled: bool = True,
        slow_ms: Optional[float] = None,
        level: int = logging.DEBUG,
        **fields: object,
    ) -> None:
        self._event = event
        self._logger = logger
        self._base_context = dict(base_context or {})
        self._enabled = enabled
        self._slow_ms = slow_ms
        self._level = level
        self._fields: Dict[str, object] = build_log_context(**fields)
        self._start: Optional[float] = None

    def __enter__(self) -> "PerfLog":
        if self._enabled:
            self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if not self._enabled or self._start is None:
            return False
        elapsed_ms = (time.perf_counter() - self._start) * 1000.0
        ok = exc_type is None
        error_type = exc_type.__name__ if exc_type else None
        error_message = str(exc) if exc else None
        context = merge_log_context(
            self._base_context,
            build_log_context(
                event=self._event,
                time_taken_ms=elapsed_ms,
                ok=ok,
                error_type=error_type,
                error=error_message,
                pid=os.getpid(),
                thread_name=threading.current_thread().name,
                **self._fields,
            ),
        )
        if ok:
            level = self._level
            if self._slow_ms is not None and elapsed_ms >= self._slow_ms:
                level = logging.WARNING
            self._logger.log(level, with_log_context(self._event, context))
        else:
            self._logger.error(with_log_context(self._event, context), exc_info=exc)
        return False

    def add_fields(self, **fields: object) -> None:
        self._fields.update(build_log_context(**fields))


def perf_log(
    event: str,
    *,
    logger: logging.Logger,
    base_context: Optional[Mapping[str, object]] = None,
    enabled: bool = True,
    slow_ms: Optional[float] = None,
    level: int = logging.DEBUG,
    **fields: object,
) -> PerfLog:
    return PerfLog(
        event,
        logger=logger,
        base_context=base_context,
        enabled=enabled,
        slow_ms=slow_ms,
        level=level,
        **fields,
    )


__all__ = [
    "DEFAULT_OBS_ENABLED",
    "DEFAULT_OBS_STEP_SAMPLE_RATE",
    "DEFAULT_OBS_SLOW_MS",
    "get_obs_enabled",
    "get_obs_step_sample_rate",
    "get_obs_slow_ms",
    "should_sample",
    "perf_log",
    "PerfLog",
]
