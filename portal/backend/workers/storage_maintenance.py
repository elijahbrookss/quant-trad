"""Database-owned host for the existing storage lifecycle supervisor.

This worker does not create policy, repositories, migrations or another schedule.
The collector must select the same explicit dedicated ownership composition.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import threading

from core.settings import get_settings
from core.storage_mounts import require_configured_archive_mount, require_configured_working_mount
from market_data.archive import FilesystemRawArchiveObjectStore
from portal.backend.db import db
from portal.backend.service.async_jobs import wait_for_database_ready
from portal.backend.service.market.market_storage_lifecycle import MarketStorageLifecycleSupervisor
from portal.backend.service.market.market_structure_service import DEFAULT_STORAGE_ROOT
from portal.backend.service.storage.maintenance_runtime import storage_maintenance_runners
from portal.backend.service.storage.repos.market_collection import market_collection_repo

logger = logging.getLogger(__name__)
_HEARTBEAT_SECONDS = 10.0
_TTL_SECONDS = 30.0


def run(*, settings, stop: threading.Event) -> int:
    if settings.storage.maintenance_owner != "dedicated":
        raise ValueError("storage_maintenance_dedicated_owner_required")
    if os.geteuid() != 70:
        raise PermissionError("storage_maintenance_database_user_required")
    if settings.storage.archive_shared_group_id is None:
        raise ValueError("storage_maintenance_shared_archive_required")
    require_configured_archive_mount()
    require_configured_working_mount()
    FilesystemRawArchiveObjectStore(DEFAULT_STORAGE_ROOT / "objects")
    runners = storage_maintenance_runners(db, storage_root=DEFAULT_STORAGE_ROOT,
        limits_path=settings.storage.maintenance_limits_path, require_incremental=True)
    if not wait_for_database_ready(
            timeout_seconds=settings.workers.collectors.db_wait_timeout_seconds,
            poll_interval_seconds=0.5):
        raise RuntimeError("storage_maintenance_database_timeout")
    worker_id = f"storage-maintenance:{socket.gethostname()}:{os.getpid()}"
    supervisor = MarketStorageLifecycleSupervisor(policy=settings.market_data_lifecycle,
        owner_id=worker_id, storage_root=DEFAULT_STORAGE_ROOT, **runners)
    market_collection_repo.register_worker(worker_id=worker_id,
        worker_role="market_storage_maintenance", worker_version="storage_maintenance.v1",
        ttl_seconds=_TTL_SECONDS, state="starting",
        capabilities={"storage_maintenance": True, "collector_modes": [], "concurrency": 1},
        context={"hostname": socket.gethostname(), "pid": os.getpid(),
                 "storage_lifecycle": supervisor.snapshot()})
    started = False
    failed = False
    try:
        started = True
        supervisor.start()
        logger.info("storage_maintenance_started | worker_id=%s", worker_id)
        while not stop.is_set():
            snapshot = supervisor.snapshot()
            market_collection_repo.heartbeat_worker(worker_id=worker_id,
                ttl_seconds=_TTL_SECONDS,
                state="degraded" if snapshot.get("state") == "degraded" else "idle",
                last_error=snapshot.get("last_error"),
                context={"storage_lifecycle": snapshot})
            stop.wait(_HEARTBEAT_SECONDS)
    except Exception:
        failed = True
        logger.exception("storage_maintenance_failed | worker_id=%s", worker_id)
    finally:
        if started:
            try:
                supervisor.stop()
            except Exception:
                failed = True
                logger.exception("storage_maintenance_stop_failed | worker_id=%s", worker_id)
        # Do not publish healthy completion if a background operation did not
        # drain. Container supervision must reap that unresolved process.
        try:
            market_collection_repo.stop_worker(worker_id=worker_id,
                state="degraded" if failed else "stopped")
        except Exception:
            failed = True
            logger.exception("storage_maintenance_status_stop_failed | worker_id=%s", worker_id)
    if failed:
        return 5
    logger.info("storage_maintenance_stopped | worker_id=%s", worker_id)
    return 0


def main() -> int:
    settings = get_settings()
    logging.basicConfig(level=settings.logging.level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    stop = threading.Event()
    def on_signal(signum, _frame):
        logger.info("storage_maintenance_shutdown_signal | signum=%s", signum)
        stop.set()
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    return run(settings=settings, stop=stop)


if __name__ == "__main__":
    raise SystemExit(main())
