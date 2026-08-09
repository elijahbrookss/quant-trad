"""Bounded background sampler for database and relation capacity."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any, Callable

from core.settings import get_settings

from .storage.repos.capacity import record_database_capacity_snapshot

logger = logging.getLogger(__name__)
_SETTINGS = get_settings().observability


def capacity_sample_bucket(
    value: datetime,
    *,
    interval_seconds: int,
) -> datetime:
    """Return an aligned naive-UTC identity for one capacity sample."""

    normalized = (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )
    seconds = max(int(interval_seconds), 1)
    epoch = int(normalized.timestamp())
    bucket_epoch = epoch - (epoch % seconds)
    return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc).replace(tzinfo=None)


class DatabaseCapacitySampler:
    """Sample PostgreSQL capacity at a bounded cadence with bounded retention."""

    def __init__(
        self,
        *,
        sample_fn: Callable[..., dict[str, Any]] = record_database_capacity_snapshot,
        enabled: bool | None = None,
        interval_seconds: int | None = None,
        retention_days: int | None = None,
    ) -> None:
        configured_enabled = bool(
            _SETTINGS.persist_enabled and _SETTINGS.capacity_sample_enabled
        )
        self._enabled = configured_enabled if enabled is None else bool(enabled)
        self._interval_seconds = max(
            int(
                interval_seconds
                if interval_seconds is not None
                else _SETTINGS.capacity_sample_interval_seconds
            ),
            1,
        )
        self._retention_days = max(
            int(
                retention_days
                if retention_days is not None
                else _SETTINGS.capacity_sample_retention_days
            ),
            1,
        )
        self._sample_fn = sample_fn
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def sample_once(self, *, now: datetime | None = None) -> dict[str, Any]:
        sampled_at = capacity_sample_bucket(
            now or datetime.now(timezone.utc),
            interval_seconds=self._interval_seconds,
        )
        result = self._sample_fn(
            sampled_at=sampled_at,
            retention_days=self._retention_days,
        )
        if result.get("sampled"):
            logger.info(
                "database_capacity_sampled | sampled_at=%s database_size_bytes=%s "
                "relations=%s relation_rows_inserted=%s retention_rows_deleted=%s "
                "sample_query_ms=%.3f",
                sampled_at.isoformat() + "Z",
                result.get("database_size_bytes"),
                result.get("relation_count"),
                result.get("relation_rows_inserted"),
                result.get("retention_rows_deleted"),
                float(result.get("sample_query_ms") or 0.0),
            )
        else:
            logger.debug(
                "database_capacity_sample_skipped | sampled_at=%s reason=%s",
                sampled_at.isoformat() + "Z",
                result.get("reason") or "duplicate_bucket",
            )
        return result

    def start(self) -> None:
        if not self._enabled:
            logger.info("database_capacity_sampler_disabled")
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._worker_loop,
                name="database-capacity-sampler",
                daemon=True,
            )
            self._thread.start()

    def stop(self, *, timeout_s: float = 10.0) -> None:
        with self._lock:
            thread = self._thread
        if thread is None:
            return
        self._stop.set()
        thread.join(timeout=max(float(timeout_s), 0.1))
        if thread.is_alive():
            raise RuntimeError("database capacity sampler did not stop cleanly")

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.sample_once()
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "database_capacity_sample_failed | error=%s",
                    exc,
                    exc_info=True,
                )
            self._stop.wait(self._interval_seconds)


_DEFAULT_CAPACITY_SAMPLER = DatabaseCapacitySampler()


def start_database_capacity_sampler() -> None:
    _DEFAULT_CAPACITY_SAMPLER.start()


def stop_database_capacity_sampler(*, timeout_s: float = 10.0) -> None:
    _DEFAULT_CAPACITY_SAMPLER.stop(timeout_s=timeout_s)


__all__ = [
    "DatabaseCapacitySampler",
    "capacity_sample_bucket",
    "start_database_capacity_sampler",
    "stop_database_capacity_sampler",
]
