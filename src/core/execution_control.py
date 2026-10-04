"""Cooperative stop boundary shared by research computation and its I/O.

This is execution-local control, not a scheduler or persisted job authority.
The async-job repository owns durable requests and publication fencing.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from threading import Event, RLock, Thread
from time import monotonic
import logging
import math
import json
from functools import wraps
from typing import Callable, Iterator


class ExecutionCancelledError(RuntimeError):
    """Execution must unwind before its owner acknowledges cancellation."""


class ExecutionBudgetExceededError(RuntimeError):
    """An admitted operation exhausted a declared resource budget."""


logger = logging.getLogger(__name__)


class ExecutionControl:
    def __init__(self) -> None:
        self._lock = RLock()
        self._error: Exception | None = None
        self._interrupts: dict[object, Callable[[], None]] = {}
        self._started = monotonic()
        self._deadline: float | None = None
        self._limits: dict[str, int] = {}
        self._used: dict[str, int] = {}
        self._stage_seconds: dict[str, float] = {}

    def record_time(self, stage: str, seconds: float) -> None:
        with self._lock:
            self._stage_seconds[stage] = self._stage_seconds.get(stage, 0.0) + seconds

    def snapshot(self) -> dict:
        with self._lock:
            return {"schema_version": "research_execution_metrics.v1",
                    "elapsed_seconds": round(monotonic() - self._started, 6),
                    "limits": dict(self._limits), "consumed": dict(self._used),
                    "stage_seconds": {name: round(value, 6) for name, value in self._stage_seconds.items()}}

    def limit(self, *, seconds: float, **resources: int) -> None:
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError("execution_budget_seconds_invalid")
        with self._lock:
            deadline = self._started + seconds
            self._deadline = min(self._deadline, deadline) if self._deadline else deadline
            for name, value in resources.items():
                if type(value) is not int or value <= 0:
                    raise ValueError(f"execution_budget_invalid: resource={name}")
                self._limits[name] = min(self._limits.get(name, value), value)
            self.check()

    def consume(self, resource: str, amount: int) -> None:
        if amount < 0:
            raise ValueError("execution_budget_negative_consumption")
        with self._lock:
            self._used[resource] = self._used.get(resource, 0) + amount
            limit = self._limits.get(resource)
            if limit is not None and self._used[resource] > limit and self._error is None:
                self._error = ExecutionBudgetExceededError(
                    f"research_execution_budget_exceeded: resource={resource} "
                    f"used={self._used[resource]} limit={limit}"
                )
            self.check()

    def tracks(self, resource: str) -> bool:
        with self._lock:
            return resource in self._limits

    def remaining_seconds(self) -> float | None:
        self.check()
        with self._lock:
            return max(0.001, self._deadline - monotonic()) if self._deadline else None

    def check(self) -> None:
        with self._lock:
            if self._error is None and self._deadline is not None and monotonic() >= self._deadline:
                self._error = ExecutionBudgetExceededError("research_execution_budget_exceeded: resource=elapsed_seconds")
            error = self._error
        if error is not None:
            raise error

    def stop(self, error: Exception) -> None:
        # Hold the lock through interruption so the connection cannot be returned
        # to its pool and handed to another job while cancel() addresses it.
        with self._lock:
            if self._error is None:
                self._error = error
            for interrupt in tuple(self._interrupts.values()):
                interrupt()

    def register(self, key: object, interrupt: Callable[[], None]) -> None:
        with self._lock:
            self.check()
            self._interrupts[key] = interrupt

    def unregister(self, key: object) -> None:
        with self._lock:
            self._interrupts.pop(key, None)


_CURRENT: ContextVar[ExecutionControl | None] = ContextVar("execution_control", default=None)


def current_execution_control() -> ExecutionControl | None:
    return _CURRENT.get()


def execution_checkpoint() -> None:
    control = _CURRENT.get()
    if control is not None:
        control.check()


def consume_execution_resource(resource: str, amount: int) -> None:
    control = _CURRENT.get()
    if control is not None:
        control.consume(resource, amount)


def consume_execution_json(resource: str, value: object) -> None:
    """Account logical UTF-8 bytes without creating a second encoded payload."""
    control = _CURRENT.get()
    if control is None or not control.tracks(resource):
        return
    size = 0
    for chunk in json.JSONEncoder(default=str, separators=(",", ":"), ensure_ascii=True).iterencode(value):
        size += len(chunk.encode("utf-8"))
        if size >= 65536:
            control.consume(resource, size)
            size = 0
    control.consume(resource, size)


@contextmanager
def measure_execution_stage(stage: str) -> Iterator[None]:
    control = _CURRENT.get()
    if control is None:
        yield
        return
    control.check()
    started = monotonic()
    try:
        yield
    finally:
        control.record_time(stage, monotonic() - started)


def measured_execution(stage: str):
    def decorate(function):
        @wraps(function)
        def invoke(*args, **kwargs):
            with measure_execution_stage(stage):
                return function(*args, **kwargs)
        return invoke
    return decorate


@contextmanager
def controlled_execution(control: ExecutionControl, *, check_on_exit: bool = True) -> Iterator[None]:
    token = _CURRENT.set(control)
    finished = Event()

    def watch():
        while not finished.wait(0.25):
            try:
                control.check()
            except Exception as error:
                try:
                    # Repeat while unwinding: cancellation may first arrive in
                    # the tiny gap between query registration and SQL execution.
                    control.stop(error)
                except Exception:
                    logger.exception("execution_statement_interrupt_failed")

    watcher = Thread(target=watch, name="research-execution-stop", daemon=True)
    try:
        control.check()
        watcher.start()
        yield
        if check_on_exit:
            control.check()
    finally:
        finished.set()
        if watcher.ident is not None:
            watcher.join(timeout=5)
        _CURRENT.reset(token)
        if watcher.is_alive():
            raise RuntimeError("execution_interrupt_shutdown_uncertain")
