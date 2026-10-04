"""Declared limits on the existing research service execution boundary."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from functools import wraps
from contextvars import ContextVar
from threading import BoundedSemaphore, Event, Thread
from time import monotonic
import json
import logging

from core.execution_control import (
    ExecutionControl, ExecutionStopUncertainError, controlled_execution, current_execution_control, consume_execution_json,
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


_GLOBAL_ADMITTED = ContextVar("research_global_admitted", default=False)
_GLOBAL_LOCK = "qt.research.execution.v1"


@contextmanager
def _global_admission(control, *, session_factory=None, check_on_exit=True):
    """One cooperating heavy operation across API processes and workers.

    The helper owns its own connection from acquisition through release. It
    never publishes research data. Uncertain helper termination stays explicit;
    the caller cannot return its connection to the pool from another thread.
    """
    if _GLOBAL_ADMITTED.get() or not get_settings().async_jobs.research_global_serialization:
        yield
        return
    from sqlalchemy import text
    if session_factory is None:
        from portal.backend.db.session import db
        session_factory = db.session
    ready, finished = Event(), Event()
    errors = []

    def own():
        admitted = False
        try:
            with session_factory() as owner:
                owner.execute(text("SET LOCAL statement_timeout='1000ms'"))
                if not owner.scalar(text("SELECT pg_try_advisory_xact_lock(hashtextextended(:name,0))"),
                                    {"name": _GLOBAL_LOCK}):
                    raise ResearchAdmissionError("research_execution_busy: global capacity occupied")
                admitted = True
                ready.set()
                while not finished.wait(0.25):
                    if not owner.scalar(text("""
                        WITH key AS (SELECT hashtextextended(:name,0) AS value)
                        SELECT EXISTS(SELECT 1 FROM pg_locks,key WHERE locktype='advisory'
                          AND pid=pg_backend_pid() AND mode='ExclusiveLock' AND granted AND objsubid=1
                          AND classid::bigint=((key.value>>32)&4294967295)
                          AND objid::bigint=(key.value&4294967295))
                    """), {"name": _GLOBAL_LOCK}):
                        raise ResearchAdmissionError("research_execution_owner_lost")
        except Exception as error:
            errors.append(error)
            if admitted:
                try:
                    control.stop(ResearchAdmissionError("research_execution_owner_lost: admission connection failed"))
                except Exception as interrupt_error:
                    errors.append(interrupt_error)
                    logging.getLogger(__name__).exception("research_admission_interrupt_failed")
        finally:
            ready.set()

    helper = Thread(target=own, name="research-global-admission", daemon=True)
    helper.start()
    token = None
    deadline = monotonic() + 5
    try:
        while not ready.wait(0.05):
            control.check()
            if monotonic() >= deadline:
                raise ResearchAdmissionError("research_execution_admission_timeout")
        if errors:
            raise errors[0]
        control.check()
        token = _GLOBAL_ADMITTED.set(True)
        yield
        if check_on_exit:
            control.check()
    finally:
        if token is not None:
            _GLOBAL_ADMITTED.reset(token)
        finished.set()
        helper.join(timeout=3)
        if helper.is_alive():
            raise ExecutionStopUncertainError("research_execution_admission_shutdown_uncertain")
        if errors and token is not None:
            control.check()
            raise ResearchAdmissionError("research_execution_admission_failed") from errors[0]


def research_execution_limits() -> dict[str, int | float]:
    settings = get_settings().async_jobs
    return {
        "seconds": settings.research_execution_seconds,
        "input_rows": settings.research_input_rows,
        "input_bytes": settings.research_input_bytes,
        # Bound repeated whole-object reads independently of selected JSON size.
        "archive_read_bytes": settings.research_input_bytes,
        "archive_cache_write_bytes": settings.research_input_bytes,
        "evidence_bytes": settings.research_evidence_bytes,
        "result_bytes": settings.research_result_bytes,
    }


@contextmanager
def research_execution_scope(*, check_on_exit: bool = True):
    current = current_execution_control()
    control = current or ExecutionControl()
    control.limit(**research_execution_limits())
    scope = nullcontext() if current is not None else controlled_execution(control, check_on_exit=check_on_exit)
    with _admit(current), scope, _global_admission(control):
        yield control


def bounded_research_execution(function):
    """Nested computation calls share one budget; they cannot reset the clock."""
    @wraps(function)
    def invoke(*args, **kwargs):
        outer = current_execution_control() is None
        with research_execution_scope() as control:
            try:
                result = function(*args, **kwargs)
                # Before publication. Nested operations count repeated work too.
                consume_execution_json("result_bytes", result)
                control.check()
                return result
            finally:
                if outer:
                    logging.getLogger(__name__).info(
                        "research_execution_metrics | operation=%s metrics=%s",
                        function.__name__, json.dumps(control.snapshot(), sort_keys=True))
    return invoke
