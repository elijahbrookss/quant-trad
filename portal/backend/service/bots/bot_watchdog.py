"""BotWatchdog - run lease monitoring and orphan detection for bot runtimes.

This service provides observability into bot runtime health across distributed
servers. It detects orphaned bots (bots that claim to be running but have no
active runtime) and marks them as crashed.

Key responsibilities:
1. Generate unique runner IDs for each server instance
2. Track locally registered bot IDs for process observability
3. Detect expired run leases on startup
4. Background monitoring for expired run leases and missing containers

Usage:
    from portal.backend.service.bots.bot_watchdog import BotWatchdog

    # Initialize on server startup
    watchdog = BotWatchdog.instance()

    # Recover orphaned bots owned by this server
    watchdog.recover_local_orphans()

    # Start background monitoring (call once)
    watchdog.start_background_monitor()

    # When a bot starts running
    watchdog.register_bot(bot_id)

    # Track local process liveness (run leases are renewed by the runtime)
    watchdog.tick(bot_id)

    # When a bot stops normally
    watchdog.unregister_bot(bot_id)
"""

from __future__ import annotations

import logging
import socket
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Mapping, Optional, Set

from core.settings import get_settings
from ..storage.storage import (
    find_expired_bot_run_leases,
    get_bot_run,
    get_bot_run_lifecycle,
    get_bot_run_lease,
    list_active_bot_run_leases,
    mark_bot_crashed,
    run_lease_is_active,
)
from .runner import DockerBotRunner
from .runner_observability import latest_docker_lifecycle_event_for_bot, latest_runner_clock_gap
from .startup_lifecycle import is_active_run_state

logger = logging.getLogger(__name__)


# Configuration
_SETTINGS = get_settings().bot_runtime.watchdog
LOCAL_TICK_INTERVAL_SECONDS = _SETTINGS.heartbeat_interval_seconds
STALE_THRESHOLD_SECONDS = _SETTINGS.stale_threshold_seconds
MONITOR_INTERVAL_SECONDS = _SETTINGS.monitor_interval_seconds
STARTUP_CONTAINER_GRACE_SECONDS = max(float(LOCAL_TICK_INTERVAL_SECONDS * 2), float(MONITOR_INTERVAL_SECONDS))


def _parse_bot_timestamp(value: object) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        return parsed.replace(tzinfo=None)
    return parsed


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _startup_launch_grace_active(run_context: Mapping[str, Any]) -> bool:
    started_at = _active_startup_started_at(run_context)
    if started_at is None:
        return False
    return _utcnow_naive() - started_at < timedelta(seconds=STARTUP_CONTAINER_GRACE_SECONDS)


def _active_startup_started_at(run_context: Mapping[str, Any]) -> Optional[datetime]:
    for key in ("started_at", "checkpoint_at", "created_at"):
        started_at = _parse_bot_timestamp(run_context.get(key))
        if started_at is not None:
            return started_at
    lifecycle = run_context.get("lifecycle") if isinstance(run_context.get("lifecycle"), Mapping) else {}
    run = run_context.get("run") if isinstance(run_context.get("run"), Mapping) else {}
    return (
        _parse_bot_timestamp(run.get("started_at"))
        or _parse_bot_timestamp(lifecycle.get("checkpoint_at"))
        or _parse_bot_timestamp(run.get("created_at"))
    )


def _active_startup_run_id(run_context: Mapping[str, Any]) -> Optional[str]:
    run_id = str(run_context.get("run_id") or "").strip()
    return run_id or None


def _utc_iso(value: datetime) -> str:
    return value.isoformat() + "Z"


def _stale_run_lease_diagnostics(lease: Mapping[str, Any], *, detected_runner_id: str) -> Dict[str, Any]:
    bot_id = str(lease.get("bot_id") or "").strip()
    run_id = str(lease.get("run_id") or "").strip()
    previous_runner = str(lease.get("runner_id") or "").strip() or None
    expires_at = _parse_bot_timestamp(lease.get("expires_at"))
    detected_at = _utcnow_naive()
    max_recent_age = max(float(STALE_THRESHOLD_SECONDS) * 10.0, 900.0)
    diagnostics: Dict[str, Any] = {
        "detected_at": _utc_iso(detected_at),
        "detected_runner_id": str(detected_runner_id or "").strip() or None,
        "previous_runner": previous_runner,
        "lease_expires_at": _utc_iso(expires_at) if expires_at is not None else None,
        "lease_missing_expiry": expires_at is None,
        "lease_stale_threshold_seconds": float(STALE_THRESHOLD_SECONDS),
        "run_lease": dict(lease),
    }
    if bot_id:
        diagnostics["bot_id"] = bot_id
    if run_id:
        diagnostics["run_id"] = run_id
    if expires_at is not None:
        diagnostics["lease_expired_age_seconds"] = round(max((detected_at - expires_at).total_seconds(), 0.0), 3)
    clock_gap = latest_runner_clock_gap(previous_runner, max_age_seconds=max_recent_age)
    if clock_gap is None:
        clock_gap = latest_runner_clock_gap(detected_runner_id, max_age_seconds=max_recent_age)
    if clock_gap is not None:
        diagnostics["runner_clock_gap"] = clock_gap
    docker_event = latest_docker_lifecycle_event_for_bot(bot_id, max_age_seconds=max_recent_age)
    if docker_event is not None:
        diagnostics["docker_lifecycle"] = docker_event
    return diagnostics


def _container_not_running_diagnostics(
    run_context: Mapping[str, Any],
    *,
    container: Mapping[str, Any],
    detected_runner_id: str,
) -> Dict[str, Any]:
    bot_id = str(run_context.get("id") or run_context.get("bot_id") or "").strip()
    max_recent_age = max(float(STALE_THRESHOLD_SECONDS) * 10.0, 900.0)
    run_id = _active_startup_run_id(run_context)
    diagnostics: Dict[str, Any] = {
        "detected_at": _utc_iso(_utcnow_naive()),
        "detected_runner_id": str(detected_runner_id or "").strip() or None,
        "bot_id": bot_id or None,
        "run_id": run_id,
        "container_name": str(container.get("name") or "").strip() or None,
        "container_status": str(container.get("status") or "").strip() or None,
        "container_running": bool(container.get("running")),
        "container_exit_code": container.get("exit_code"),
        "container_oom_killed": bool(container.get("oom_killed")),
        "container_error": str(container.get("error") or "").strip() or None,
        "container_run_id": str(container.get("runtime_run_id") or "").strip() or None,
    }
    docker_event = latest_docker_lifecycle_event_for_bot(bot_id, max_age_seconds=max_recent_age)
    if docker_event is not None:
        diagnostics["docker_lifecycle"] = docker_event
    if run_id:
        lease = get_bot_run_lease(run_id)
        if lease is not None:
            diagnostics["run_lease"] = lease
    return diagnostics


_STARTUP_PHASES = frozenset(
    {
        "start_requested",
        "validating_configuration",
        "resolving_strategy",
        "resolving_runtime_dependencies",
        "preparing_run",
        "stamping_starting_state",
        "launching_container",
        "container_launched",
        "awaiting_container_boot",
        "container_booting",
    }
)


def _startup_container_ownership_pending(
    *,
    run_context: Mapping[str, Any],
    status: str,
    phase: str,
    current_run_id: Optional[str],
    container_run_id: Optional[str],
    ownership_confirmed: bool,
) -> bool:
    if ownership_confirmed:
        return False
    if status == "starting":
        return True
    if status not in {"degraded", "telemetry_degraded"}:
        return False
    if not current_run_id or phase not in _STARTUP_PHASES:
        return False
    return _startup_launch_grace_active(run_context) or container_run_id is None


def _watchdog_run_context_from_lease(lease: Mapping[str, Any]) -> Dict[str, Any]:
    bot_id = str(lease.get("bot_id") or "").strip()
    run_id = str(lease.get("run_id") or "").strip()
    run = get_bot_run(run_id) if run_id else None
    lifecycle = get_bot_run_lifecycle(run_id) if run_id else None
    run_map = dict(run or {})
    lifecycle_map = dict(lifecycle or {})
    return {
        "id": bot_id,
        "bot_id": bot_id,
        "run_id": run_id,
        "status": str(lifecycle_map.get("status") or run_map.get("status") or "").strip().lower(),
        "phase": str(lifecycle_map.get("phase") or "").strip().lower(),
        "started_at": run_map.get("started_at") or lifecycle_map.get("checkpoint_at"),
        "checkpoint_at": lifecycle_map.get("checkpoint_at"),
        "run": run_map,
        "lifecycle": lifecycle_map,
        "lease": dict(lease),
    }


def _generate_runner_id() -> str:
    """Generate a stable runner ID for this server instance."""

    explicit = _SETTINGS.runner_id
    if explicit:
        return explicit.strip()
    return socket.gethostname() or "unknown"


class BotWatchdog:
    """Monitors bot runtime health and detects orphaned bots.

    This is a singleton service that should be initialized once per server.
    It tracks which bots are registered in this process while durable runtime
    liveness is proven by per-run leases.
    """

    _instance: Optional[BotWatchdog] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._runner_id = _generate_runner_id()
        self._registered_bots: Set[str] = set()
        self._local_tick_thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._bot_lock = threading.Lock()
        self._on_orphan_detected: Optional[Callable[[str, Dict], None]] = None

        logger.info(
            "bot_watchdog_initialized | runner_id=%s | local_tick_interval=%s | stale_threshold=%s",
            self._runner_id,
            LOCAL_TICK_INTERVAL_SECONDS,
            STALE_THRESHOLD_SECONDS,
        )

    @classmethod
    def instance(cls) -> BotWatchdog:
        """Return the singleton BotWatchdog instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = BotWatchdog()
        return cls._instance

    @property
    def runner_id(self) -> str:
        """Return this server's unique runner ID."""
        return self._runner_id

    def set_orphan_callback(self, callback: Callable[[str, Dict], None]) -> None:
        """Set a callback to be invoked when an orphan is detected and marked crashed.

        Args:
            callback: Function(bot_id, bot_dict) called for each orphaned bot
        """
        self._on_orphan_detected = callback

    def register_bot(self, bot_id: str) -> None:
        """Register a bot as running on this server.

        Call this when a bot starts. Durable ownership is recorded on the run
        lease, not on the bot definition row.
        """
        with self._bot_lock:
            self._registered_bots.add(bot_id)
        logger.debug("bot_watchdog_registered | bot_id=%s | runner_id=%s", bot_id, self._runner_id)

    def unregister_bot(self, bot_id: str) -> None:
        """Unregister a bot that has stopped normally.

        Call this when a bot stops (completed, stopped, error).
        Run lease release is handled by the runtime control path.
        """
        with self._bot_lock:
            self._registered_bots.discard(bot_id)
        logger.debug("bot_watchdog_unregistered | bot_id=%s", bot_id)

    def tick(self, bot_id: str) -> None:
        """Record a local watchdog tick for a specific bot.

        Runtime liveness is persisted by the per-run lease renewer.
        """
        logger.debug("bot_watchdog_tick | bot_id=%s | runner_id=%s", bot_id, self._runner_id)

    def tick_all(self) -> None:
        """Record watchdog ticks for all registered bots on this server."""
        with self._bot_lock:
            bot_ids = list(self._registered_bots)

        for bot_id in bot_ids:
            try:
                self.tick(bot_id)
            except Exception as exc:
                logger.warning("bot_watchdog_tick_failed | bot_id=%s | error=%s", bot_id, exc)

        if bot_ids:
            logger.debug(
                "bot_watchdog_tick_all | count=%d | runner_id=%s",
                len(bot_ids),
                self._runner_id,
            )

    def recover_local_orphans(self) -> List[str]:
        """Recover bots that were orphaned by this server (e.g., after restart).

        Call this on server startup to mark any bots that this server was
        running with expired leases as crashed.

        Returns:
            List of bot IDs that were marked as crashed
        """
        expired_leases = find_expired_bot_run_leases(
            stale_threshold_seconds=0.0,
            runner_id=self._runner_id,
        )

        crashed_ids = []
        for lease in expired_leases:
            bot_id = str(lease.get("bot_id") or "").strip()
            if bot_id:
                diagnostics = _stale_run_lease_diagnostics(lease, detected_runner_id=self._runner_id)
                if mark_bot_crashed(
                    bot_id,
                    reason=f"server_restart:{self._runner_id}",
                    diagnostics=diagnostics,
                ):
                    crashed_ids.append(bot_id)
                    if self._on_orphan_detected:
                        try:
                            self._on_orphan_detected(bot_id, _watchdog_run_context_from_lease(lease))
                        except Exception as exc:
                            logger.warning(
                                "bot_watchdog_orphan_callback_failed | bot_id=%s | error=%s",
                                bot_id,
                                exc,
                            )

        if crashed_ids:
            logger.warning(
                "bot_watchdog_local_orphans_recovered | count=%d | bot_ids=%s | runner_id=%s",
                len(crashed_ids),
                crashed_ids,
                self._runner_id,
            )
        else:
            logger.info(
                "bot_watchdog_no_local_orphans | runner_id=%s",
                self._runner_id,
            )

        return crashed_ids

    def scan_expired_run_leases(self) -> List[str]:
        """Scan for expired run leases from ANY server.

        This catches bots orphaned by remote servers that died without
        clean shutdown. Should be called periodically by the background monitor.

        Returns:
            List of bot IDs that were marked as crashed
        """
        expired_leases = find_expired_bot_run_leases(
            stale_threshold_seconds=STALE_THRESHOLD_SECONDS,
            runner_id=None,
        )

        crashed_ids = []
        for lease in expired_leases:
            bot_id = str(lease.get("bot_id") or "").strip()
            previous_runner = str(lease.get("runner_id") or "").strip() or "unknown"
            if bot_id:
                diagnostics = _stale_run_lease_diagnostics(lease, detected_runner_id=self._runner_id)
                if mark_bot_crashed(
                    bot_id,
                    reason=f"stale_run_lease:prev={previous_runner}",
                    diagnostics=diagnostics,
                ):
                    crashed_ids.append(bot_id)
                    clock_gap = diagnostics.get("runner_clock_gap")
                    docker_lifecycle = diagnostics.get("docker_lifecycle")
                    logger.warning(
                        "bot_watchdog_stale_run_lease_detected | bot_id=%s | run_id=%s | previous_runner=%s | lease_expires_at=%s | lease_expired_age_seconds=%s | runner_clock_gap_seconds=%s | docker_action=%s",
                        bot_id,
                        diagnostics.get("run_id"),
                        previous_runner,
                        diagnostics.get("lease_expires_at"),
                        diagnostics.get("lease_expired_age_seconds"),
                        clock_gap.get("gap_seconds") if isinstance(clock_gap, Mapping) else None,
                        docker_lifecycle.get("action") if isinstance(docker_lifecycle, Mapping) else None,
                    )
                    if self._on_orphan_detected:
                        try:
                            self._on_orphan_detected(bot_id, _watchdog_run_context_from_lease(lease))
                        except Exception as exc:
                            logger.warning(
                                "bot_watchdog_orphan_callback_failed | bot_id=%s | error=%s",
                                bot_id,
                                exc,
                            )

        if crashed_ids:
            logger.info(
                "bot_watchdog_expired_lease_scan_complete | orphans_found=%d | bot_ids=%s",
                len(crashed_ids),
                crashed_ids,
            )

        return crashed_ids

    def verify_container_ownership(self) -> List[str]:
        """Verify active run leases still map to live docker containers."""

        failed: List[str] = []
        for lease in list_active_bot_run_leases():
            if not run_lease_is_active(lease):
                continue
            run_context = _watchdog_run_context_from_lease(lease)
            status = str(run_context.get("status") or "").strip().lower()
            phase = str(run_context.get("phase") or "").strip().lower()
            if not is_active_run_state(status=status, phase=phase):
                continue
            bot_id = str(run_context.get("bot_id") or "").strip()
            if not bot_id:
                continue
            container = DockerBotRunner.inspect_bot_container(bot_id)
            current_run_id = _active_startup_run_id(run_context)
            container_run_id = str(container.get("runtime_run_id") or "").strip() or None
            ownership_mismatch = bool(current_run_id and container_run_id and container_run_id != current_run_id)
            if bool(container.get("running")):
                if ownership_mismatch:
                    logger.warning(
                        "bot_watchdog_container_ownership_mismatch_running | bot_id=%s | current_run_id=%s | container_run_id=%s",
                        bot_id,
                        current_run_id,
                        container_run_id,
                    )
                    continue
                continue
            container_status = str(container.get("status") or "").strip().lower()
            startup_in_progress = status == "starting" or phase in _STARTUP_PHASES
            launch_grace_active = startup_in_progress and _startup_launch_grace_active(run_context)
            ownership_confirmed = bool(current_run_id and container_run_id == current_run_id)
            ownership_pending = _startup_container_ownership_pending(
                run_context=run_context,
                status=status,
                phase=phase,
                current_run_id=current_run_id,
                container_run_id=container_run_id,
                ownership_confirmed=ownership_confirmed,
            )
            if launch_grace_active:
                logger.warning(
                    "bot_watchdog_startup_container_pending | bot_id=%s | current_run_id=%s | container_run_id=%s | container_status=%s",
                    bot_id,
                    current_run_id,
                    container_run_id,
                    container_status or None,
                )
                continue
            if ownership_pending:
                logger.warning(
                    "bot_watchdog_startup_container_unconfirmed | bot_id=%s | status=%s | current_run_id=%s | container_run_id=%s | container_status=%s",
                    bot_id,
                    status,
                    current_run_id,
                    container_run_id,
                    container_status or None,
                )
                continue
            if ownership_mismatch:
                logger.warning(
                    "bot_watchdog_container_ownership_mismatch_skipped | bot_id=%s | current_run_id=%s | container_run_id=%s | container_status=%s",
                    bot_id,
                    current_run_id,
                    container_run_id,
                    container_status or None,
                )
                continue
            container_name = str(container.get("name") or "").strip()
            diagnostics = _container_not_running_diagnostics(
                run_context,
                container=container,
                detected_runner_id=self._runner_id,
            )
            if mark_bot_crashed(
                bot_id,
                reason=f"container_not_running:{container_name}",
                diagnostics=diagnostics,
            ):
                failed.append(bot_id)
                docker_lifecycle = diagnostics.get("docker_lifecycle")
                logger.error(
                    "bot_watchdog_container_missing | bot_id=%s | container_name=%s | container_status=%s | error=%s | docker_action=%s | docker_exit_code=%s",
                    bot_id,
                    container_name,
                    container.get("status"),
                    container.get("error"),
                    docker_lifecycle.get("action") if isinstance(docker_lifecycle, Mapping) else None,
                    docker_lifecycle.get("exit_code") if isinstance(docker_lifecycle, Mapping) else None,
                )
                if self._on_orphan_detected:
                    try:
                        self._on_orphan_detected(bot_id, run_context)
                    except Exception as exc:
                        logger.warning(
                            "bot_watchdog_orphan_callback_failed | bot_id=%s | error=%s",
                            bot_id,
                            exc,
                        )
        return failed

    def start_background_monitor(self) -> None:
        """Start background threads for local ticks and orphan detection.

        Call this once on server startup after recover_local_orphans().
        """
        if self._local_tick_thread is not None and self._local_tick_thread.is_alive():
            logger.warning("bot_watchdog_already_running")
            return

        self._stop_event.clear()

        self._local_tick_thread = threading.Thread(
            target=self._local_tick_loop,
            name="BotWatchdog-LocalTick",
            daemon=True,
        )
        self._local_tick_thread.start()

        # Monitor thread - scans for expired run leases.
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="BotWatchdog-Monitor",
            daemon=True,
        )
        self._monitor_thread.start()

        logger.info(
            "bot_watchdog_background_started | runner_id=%s | local_tick_interval=%s | monitor_interval=%s",
            self._runner_id,
            LOCAL_TICK_INTERVAL_SECONDS,
            MONITOR_INTERVAL_SECONDS,
        )

    def stop_background_monitor(self) -> None:
        """Stop background monitoring threads."""
        self._stop_event.set()

        if self._local_tick_thread is not None:
            self._local_tick_thread.join(timeout=5)
            self._local_tick_thread = None

        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=5)
            self._monitor_thread = None

        logger.info("bot_watchdog_background_stopped | runner_id=%s", self._runner_id)

    def _local_tick_loop(self) -> None:
        """Background loop that records local watchdog ticks."""
        while not self._stop_event.is_set():
            try:
                self.tick_all()
            except Exception as exc:
                logger.exception("bot_watchdog_tick_loop_error | error=%s", exc)

            self._stop_event.wait(LOCAL_TICK_INTERVAL_SECONDS)

    def _monitor_loop(self) -> None:
        """Background loop that scans for expired run leases."""
        # Initial delay to let servers boot up
        self._stop_event.wait(MONITOR_INTERVAL_SECONDS)

        while not self._stop_event.is_set():
            try:
                self.scan_expired_run_leases()
                self.verify_container_ownership()
            except Exception as exc:
                logger.exception("bot_watchdog_monitor_loop_error | error=%s", exc)

            self._stop_event.wait(MONITOR_INTERVAL_SECONDS)

    def status(self) -> Dict:
        """Return current watchdog status for observability."""
        with self._bot_lock:
            registered_count = len(self._registered_bots)
            registered_ids = list(self._registered_bots)

        return {
            "runner_id": self._runner_id,
            "registered_bots": registered_count,
            "registered_bot_ids": registered_ids,
            "local_tick_interval_seconds": LOCAL_TICK_INTERVAL_SECONDS,
            "stale_threshold_seconds": STALE_THRESHOLD_SECONDS,
            "monitor_interval_seconds": MONITOR_INTERVAL_SECONDS,
            "local_tick_thread_alive": self._local_tick_thread is not None and self._local_tick_thread.is_alive(),
            "monitor_thread_alive": self._monitor_thread is not None and self._monitor_thread.is_alive(),
        }


# Convenience function to get the singleton
def get_watchdog() -> BotWatchdog:
    """Return the singleton BotWatchdog instance."""
    return BotWatchdog.instance()
