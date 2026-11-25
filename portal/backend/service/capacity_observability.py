"""Bounded background sampler for database and relation capacity."""

from __future__ import annotations

import logging
import json
import os
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Callable

from core.settings import get_settings
from core.storage_mounts import (StorageMountError, require_configured_archive_mount,
                                 require_configured_working_mount)
from .observability import BackendObserver

from .storage.repos.capacity import record_database_capacity_snapshot

logger = logging.getLogger(__name__)
_SETTINGS = get_settings().observability
_STORAGE_OBSERVER = BackendObserver(component="storage_operating")
_MOUNT_OBSERVER = BackendObserver(component="storage_mount")
_NODE_OBSERVER = BackendObserver(component="storage_node")
_STORAGE_GAUGE_FIELDS = (
    "eligible_cohorts", "known_backlog_rows", "backlog_rows", "oldest_eligible_age_seconds",
    "published_rows_window", "published_bytes_window", "verified_rows_window", "verified_bytes_window",
    "retired_rows_window", "retired_relation_bytes_window", "retirement_pending_cohorts",
    "last_publication_unixtime", "last_verification_unixtime", "last_retirement_unixtime", "wal_resident_bytes",
    "worker_phase_age_seconds",
)


def _observe_storage_snapshot(snapshot):
    """Persist a fixed, small gauge set through the existing rollup exporter."""
    labels = {"storage_target": "canonical", "pipeline_stage": "lifecycle"}
    try:
        for name in _STORAGE_GAUGE_FIELDS:
            value = snapshot.get(name)
            if value is not None:
                _STORAGE_OBSERVER.gauge("storage_"+name, value, **labels)
        for name, value in {
            "storage_snapshot_available": snapshot.get("state") in {"ready", "partial"},
            "storage_snapshot_complete": snapshot.get("state") == "ready" and not snapshot.get("partial"),
            "storage_backlog_rows_available": snapshot.get("backlog_rows") is not None,
            "storage_admitted_revisions_available": False,
            "storage_worker_phase_known": snapshot.get("worker_phase") in {"canonical_retention", "local_recovery", "history_movement"},
        }.items():
            _STORAGE_OBSERVER.gauge(name, int(value), **labels)
    except Exception as exc:
        logger.warning("storage_snapshot_metrics_unavailable | error_type=%s", type(exc).__name__)


def _observe_mount_capacity():
    # Directory-backed development/guest modes cannot assert physical capacity.
    # Reuse the native UUID/udev admission; never mkdir, traverse data, or probe PGDATA.
    native_verified = False
    for tier, uuid_key, root_key, inspect in (
        ("hot", "QT_MARKET_DATA_WORKING_EXPECTED_UUID", "MARKET_STRUCTURE_WORKING_ROOT", require_configured_working_mount),
        ("cold", "QT_MARKET_DATA_EXPECTED_UUID", "MARKET_STRUCTURE_STORAGE_ROOT", require_configured_archive_mount),
    ):
        labels = {"storage_target": tier, "pipeline_stage": "mount"}
        try:
            expected = os.environ.get(uuid_key, "").strip()
            evidence = inspect(require_writable=False) if expected and os.environ.get(root_key, "").strip() else None
            if evidence is None or evidence.filesystem_uuid != expected:
                _MOUNT_OBSERVER.gauge("storage_mount_sample_available", 0, **labels)
                continue
            allocatable = evidence.used_bytes + evidence.available_bytes
            if evidence.total_bytes <= 0 or allocatable <= 0:
                raise ValueError("mount_capacity_invalid")
            native_verified = True
            values = {"used_bytes": evidence.used_bytes, "available_bytes": evidence.available_bytes,
                      "total_bytes": evidence.total_bytes,
                      "used_percent": 100.0*evidence.used_bytes/allocatable}
            for name, value in values.items():
                _MOUNT_OBSERVER.gauge("storage_mount_"+name, value, **labels)
            _MOUNT_OBSERVER.gauge("storage_mount_sample_available", 1, **labels)
            logger.info("storage_mount_snapshot | %s", json.dumps({
                "schema_version": "qt.storage_mount_snapshot.v1", "sampled_at": datetime.now(timezone.utc).isoformat(),
                "storage_target": tier, "capacity_authority": "configured_uuid_verified_mount",
                "used_percent_basis": "used_divided_by_used_plus_available", **values}, sort_keys=True))
        except (StorageMountError, OSError, ValueError) as exc:
            _MOUNT_OBSERVER.gauge("storage_mount_sample_available", 0, **labels)
            logger.warning("storage_mount_sample_unavailable | storage_target=%s error_type=%s", tier, type(exc).__name__)
    if native_verified:
        _observe_node_pressure()
    else:
        _NODE_OBSERVER.gauge("storage_node_pressure_available", 0,
                             storage_target="node", pipeline_stage="pressure")


def _observe_node_pressure():
    """Coarse host PSI, not a per-HDD measurement; no timer or device scan."""
    labels = {"storage_target": "node", "pipeline_stage": "pressure"}
    try:
        values = {}
        for kind in ("io", "memory"):
            raw = Path("/proc/pressure", kind).read_text()
            if len(raw) > 2048:
                raise ValueError("pressure_sample_limit")
            rows = {parts[0]: dict(token.split("=", 1) for token in parts[1:])
                    for parts in (line.split() for line in raw.splitlines())}
            for field, suffix in (("some", "stall_percent"), ("full", "full_stall_percent")):
                value = float(rows[field]["avg300"])
                if not 0 <= value <= 100:
                    raise ValueError("pressure_sample_invalid")
                values["storage_node_"+kind+"_"+suffix] = value
        for name, value in values.items():
            _NODE_OBSERVER.gauge(name, value, **labels)
        _NODE_OBSERVER.gauge("storage_node_pressure_available", 1, **labels)
    except (OSError, ValueError, KeyError):
        _NODE_OBSERVER.gauge("storage_node_pressure_available", 0, **labels)


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
            metrics = result.get("storage_metrics")
            if isinstance(metrics, dict):
                logger.info("storage_operating_snapshot | %s",
                            json.dumps(metrics["operating"], sort_keys=True, allow_nan=False))
                _observe_storage_snapshot(metrics["operating"])
                for allocation in metrics["allocation"]:
                    logger.info("storage_relation_allocation_snapshot | %s",
                                json.dumps(allocation, sort_keys=True, allow_nan=False))
            _observe_mount_capacity()
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
