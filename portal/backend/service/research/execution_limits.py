"""Declared limits on the existing research service execution boundary."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from functools import wraps
from threading import BoundedSemaphore

from core.execution_control import (
    ExecutionControl, controlled_execution, current_execution_control, consume_execution_json,
)
from core.settings import get_settings


class ResearchAdmissionError(RuntimeError):
    """The bounded synchronous research capacity is already occupied."""


# Worker concurrency already belongs to the supervisor. This separately caps
# synchronous requests in each API process instead of letting HTTP threads add
# unlimited expensive work beside that pool. It grants no storage-operation slot.
_SYNC_SLOTS = BoundedSemaphore(get_settings().workers.research.processes)


@contextmanager
def _admit(current):
    acquired = current is None
    if acquired and not _SYNC_SLOTS.acquire(blocking=False):
        raise ResearchAdmissionError("research_execution_busy: synchronous capacity occupied; dispatch or retry later")
    try:
        yield
    finally:
        if acquired:
            _SYNC_SLOTS.release()


def research_execution_limits() -> dict[str, int | float]:
    settings = get_settings().async_jobs
    return {
        "seconds": settings.research_execution_seconds,
        "input_rows": settings.research_input_rows,
        "input_bytes": settings.research_input_bytes,
        "evidence_bytes": settings.research_evidence_bytes,
        "result_bytes": settings.research_result_bytes,
    }


@contextmanager
def research_execution_scope(*, check_on_exit: bool = True):
    current = current_execution_control()
    control = current or ExecutionControl()
    control.limit(**research_execution_limits())
    scope = nullcontext() if current is not None else controlled_execution(control, check_on_exit=check_on_exit)
    with _admit(current), scope:
        yield control


def bounded_research_execution(function):
    """Nested computation calls share one budget; they cannot reset the clock."""
    @wraps(function)
    def invoke(*args, **kwargs):
        with research_execution_scope() as control:
            result = function(*args, **kwargs)
            # Before publication. Nested operations count repeated work too.
            consume_execution_json("result_bytes", result)
            control.check()
            return result
    return invoke
