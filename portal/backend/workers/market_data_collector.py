"""Long-running bounded market-data collection worker."""

from __future__ import annotations

import logging
import os
import signal
import socket
import time
from typing import Any

from core.settings import get_settings

from portal.backend.service.async_jobs import wait_for_database_ready
from portal.backend.service.market.collector_service import market_data_collector


logger = logging.getLogger(__name__)
_STOP = False
_SETTINGS = get_settings()
_WORKER_SETTINGS = _SETTINGS.workers.collectors
_LEASE_SECONDS = 90.0


def _on_signal(signum: int, _frame: Any) -> None:
    global _STOP
    _STOP = True
    logger.info("market_data_collector_shutdown_signal | signum=%s", signum)


def _worker_id() -> str:
    return f"market-data:{socket.gethostname()}:{os.getpid()}"


def main() -> int:
    logging.basicConfig(
        level=_SETTINGS.logging.level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    worker_id = _worker_id()
    if not wait_for_database_ready(
        timeout_seconds=_WORKER_SETTINGS.db_wait_timeout_seconds,
        poll_interval_seconds=0.5,
    ):
        logger.error(
            "market_data_collector_db_timeout | worker_id=%s timeout_seconds=%s",
            worker_id,
            _WORKER_SETTINGS.db_wait_timeout_seconds,
        )
        return 2

    idle = float(_WORKER_SETTINGS.idle_sleep_seconds)
    idle_max = max(idle, float(_WORKER_SETTINGS.idle_sleep_max_seconds))
    current_idle = idle
    logger.info(
        "market_data_collector_ready | worker_id=%s concurrency=1",
        worker_id,
    )
    while not _STOP:
        try:
            claim = market_data_collector.claim_due(
                owner_id=worker_id,
                lease_seconds=_LEASE_SECONDS,
            )
        except Exception as exc:  # noqa: BLE001 - durable worker loop
            logger.warning(
                "market_data_collector_claim_retry | worker_id=%s error=%s",
                worker_id,
                exc,
            )
            time.sleep(current_idle)
            current_idle = min(idle_max, current_idle * 2)
            continue
        if claim is None:
            time.sleep(current_idle)
            current_idle = min(idle_max, current_idle * 2)
            continue

        current_idle = idle
        started = time.monotonic()
        try:
            result = market_data_collector.collect(
                claim,
                lease_seconds=_LEASE_SECONDS,
            )
            logger.info(
                "market_data_collection_succeeded | definition_id=%s attempt_id=%s "
                "series_id=%s scheduled_for=%s commit_seq=%s duration_ms=%.3f",
                claim.definition_id,
                claim.attempt_id,
                claim.series_id,
                claim.scheduled_for.isoformat(),
                result["outcome"]["max_commit_seq"],
                (time.monotonic() - started) * 1000.0,
            )
        except Exception as exc:  # noqa: BLE001 - failure is persisted by service
            logger.warning(
                "market_data_collection_failed | definition_id=%s attempt_id=%s "
                "scheduled_for=%s duration_ms=%.3f error=%s",
                claim.definition_id,
                claim.attempt_id,
                claim.scheduled_for.isoformat(),
                (time.monotonic() - started) * 1000.0,
                exc,
            )

    logger.info("market_data_collector_stopped | worker_id=%s", worker_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
