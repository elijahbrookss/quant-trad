"""Cooperative stop boundary shared by research computation and its I/O.

This is execution-local control, not a scheduler or persisted job authority.
The async-job repository owns durable requests and publication fencing.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from threading import RLock
from typing import Callable, Iterator


class ExecutionCancelledError(RuntimeError):
    """Execution must unwind before its owner acknowledges cancellation."""


class ExecutionControl:
    def __init__(self) -> None:
        self._lock = RLock()
        self._error: Exception | None = None
        self._interrupts: dict[object, Callable[[], None]] = {}

    def check(self) -> None:
        with self._lock:
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


@contextmanager
def controlled_execution(control: ExecutionControl) -> Iterator[None]:
    token = _CURRENT.set(control)
    try:
        control.check()
        yield
        control.check()
    finally:
        _CURRENT.reset(token)
