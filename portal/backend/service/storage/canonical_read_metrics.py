"""Bounded canonical-read measurements through the existing observer.

Iterator latency measures time executing the producer, including its cleanup;
time a consumer spends between requests is excluded. Metrics carry no identity,
request, source, query, or exception text and never change a read's outcome.
"""
from functools import wraps
from collections.abc import Mapping
import logging
from time import perf_counter

from portal.backend.service.observability import BackendObserver

logger = logging.getLogger(__name__)
_observer = BackendObserver(component="canonical_storage_read")


def _record(tier, operation, outcome, seconds, rows):
    labels = {"storage_target": tier, "pipeline_stage": operation, "outcome": outcome}
    try:
        _observer.increment("canonical_read_calls_total", **labels)
        _observer.observe("canonical_read_ms", max(0.0, seconds * 1000), unit="ms", **labels)
        _observer.increment("canonical_read_rows_total", rows, **labels)
        if outcome == "error":
            _observer.increment("canonical_read_errors_total", **labels)
    except Exception as exc:
        # Telemetry failure is visible, but cannot replace a storage exception
        # or turn a successfully selected canonical input into a failed read.
        logger.warning("canonical_read_metrics_unavailable | error_type=%s", type(exc).__name__)


def observe_read(*, tier, operation):
    assert tier in {"hot", "cold", "canonical"} and operation in {"range", "identity", "hydration"}
    def decorate(function):
        @wraps(function)
        def read(*args, **kwargs):
            started, outcome, rows = perf_counter(), "error", 0
            actual_tier = tier
            if operation == "hydration":
                selected = kwargs.get("rows", args[2] if len(args) > 2 else ())
                # Inspect only the normal, already-materialized input. Never
                # consume an iterator or validate/replace the read's arguments.
                hot = isinstance(selected, (list, tuple)) and all(
                    isinstance(row, Mapping) and all(row.get(key) is not None
                        for key in ("payload", "provenance", "quality")) for row in selected)
                actual_tier = "hot" if hot else "canonical"
            try:
                result = function(*args, **kwargs)
                rows = len(result) if isinstance(result, (list, tuple, Mapping)) else 0
                outcome = "completed"
                return result
            finally:
                _record(actual_tier, operation, outcome, perf_counter() - started, rows)
        return read
    return decorate


class _TimedIterator:
    def __init__(self, source):
        self.source, self.seconds, self.rows, self.closed = iter(source), 0.0, 0, False

    def __iter__(self):
        return self

    def _call(self, method, *args):
        started = perf_counter()
        try:
            return method(*args)
        finally:
            self.seconds += perf_counter() - started

    def __next__(self):
        value = self._call(next, self.source)
        self.rows += 1
        return value

    def send(self, value):
        result = self._call(self.source.send, value)
        self.rows += 1
        return result

    def throw(self, *args):
        result = self._call(self.source.throw, *args)
        self.rows += 1
        return result

    def close(self):
        if not self.closed:
            self.closed = True
            close = getattr(self.source, "close", None)
            if close is not None:
                return self._call(close)


def observe_read_iterator(*, tier, operation):
    assert tier in {"hot", "cold", "canonical"} and operation == "range"
    def decorate(function):
        @wraps(function)
        def read(*args, **kwargs):
            source, outcome = None, "closed"
            try:
                source = _TimedIterator(function(*args, **kwargs))
                result = yield from source
                outcome = "completed"
                return result
            except GeneratorExit:
                raise
            except BaseException:
                outcome = "error"
                raise
            finally:
                try:
                    if source is not None:
                        source.close()
                except BaseException:
                    outcome = "error"
                    raise
                finally:
                    _record(tier, operation, outcome,
                            source.seconds if source is not None else 0,
                            source.rows if source is not None else 0)
        return read
    return decorate
