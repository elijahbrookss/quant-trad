"""BotWatchdog - Heartbeat monitoring and orphan detection for bot runtimes.

This service provides observability into bot runtime health across distributed
servers. It detects orphaned bots (bots that claim to be running but have no
active runtime) and marks them as crashed.

Key responsibilities:
1. Generate unique runner IDs for each server instance
2. Emit heartbeats for running bots
3. Detect and recover orphaned bots on startup
4. Background monitoring for stale heartbeats from dead remote servers

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

    # Emit heartbeat (call periodically while bot is running)
    watchdog.heartbeat(bot_id)

    # When a bot stops normally
    watchdog.unregister_bot(bot_id)
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
from typing import Callable, Dict, List, Optional, Set

from ..storage.storage import (
    clear_bot_runner,
    find_orphaned_bots,
    mark_bot_crashed,
    update_bot_heartbeat,
)

logger = logging.getLogger(__name__)


# Configuration
HEARTBEAT_INTERVAL_SECONDS = float(os.getenv("BOT_WATCHDOG_HEARTBEAT_INTERVAL", "15"))
STALE_THRESHOLD_SECONDS = float(os.getenv("BOT_WATCHDOG_STALE_THRESHOLD", "60"))
MONITOR_INTERVAL_SECONDS = float(os.getenv("BOT_WATCHDOG_MONITOR_INTERVAL", "30"))


def _generate_runner_id() -> str:
    """Generate a stable runner ID for this server instance."""

    explicit = os.getenv("BOT_RUNNER_ID")
    if explicit:
        return explicit.strip()
    hostname = os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME")
    if hostname:
        return hostname
    return "unknown"


class BotWatchdog:
    """Monitors bot runtime health and detects orphaned bots.

    This is a singleton service that should be initialized once per server.
    It tracks which bots are running on this server and emits heartbeats
    to prove liveness.
    """

    _instance: Optional[BotWatchdog] = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._runner_id = _generate_runner_id()
        self._registered_bots: Set[str] = set()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._bot_lock = threading.Lock()
        self._on_orphan_detected: Optional[Callable[[str, Dict], None]] = None

        logger.info(
            "bot_watchdog_initialized | runner_id=%s | heartbeat_interval=%s | stale_threshold=%s",
            self._runner_id,
            HEARTBEAT_INTERVAL_SECONDS,
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

        Call this when a bot starts. The watchdog will emit heartbeats for it.
        """
        with self._bot_lock:
            self._registered_bots.add(bot_id)
        # Immediately emit first heartbeat
        update_bot_heartbeat(bot_id, self._runner_id)
        logger.debug("bot_watchdog_registered | bot_id=%s | runner_id=%s", bot_id, self._runner_id)

    def unregister_bot(self, bot_id: str) -> None:
        """Unregister a bot that has stopped normally.

        Call this when a bot stops (completed, stopped, error).
        Clears the runner ownership so it won't be flagged as orphaned.
        """
        with self._bot_lock:
            self._registered_bots.discard(bot_id)
        clear_bot_runner(bot_id)
        logger.debug("bot_watchdog_unregistered | bot_id=%s", bot_id)

    def heartbeat(self, bot_id: str) -> None:
        """Emit a heartbeat for a specific bot.

        Call this periodically while a bot is running to prove liveness.
        """
        update_bot_heartbeat(bot_id, self._runner_id)

    def heartbeat_all(self) -> None:
        """Emit heartbeats for all registered bots on this server."""
        with self._bot_lock:
            bot_ids = list(self._registered_bots)

        for bot_id in bot_ids:
            try:
                self.heartbeat(bot_id)
            except Exception as exc:
                logger.warning("bot_watchdog_heartbeat_failed | bot_id=%s | error=%s", bot_id, exc)

        if bot_ids:
            logger.debug(
                "bot_watchdog_heartbeat_all | count=%d | runner_id=%s",
                len(bot_ids),
                self._runner_id,
            )

    def recover_local_orphans(self) -> List[str]:
        """Recover bots that were orphaned by this server (e.g., after restart).

        Call this on server startup to mark any bots that this server was
        running (based on runner_id) as crashed.

        Returns:
            List of bot IDs that were marked as crashed
        """
        orphaned = find_orphaned_bots(
            stale_threshold_seconds=0,  # Any bot owned by us with no heartbeat
            runner_id=self._runner_id,
        )

        crashed_ids = []
        for bot in orphaned:
            bot_id = bot.get("id")
            if bot_id:
                if mark_bot_crashed(bot_id, reason=f"server_restart:{self._runner_id}"):
                    crashed_ids.append(bot_id)
                    if self._on_orphan_detected:
                        try:
                            self._on_orphan_detected(bot_id, bot)
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

    def scan_stale_heartbeats(self) -> List[str]:
        """Scan for bots with stale heartbeats from ANY server.

        This catches bots orphaned by remote servers that died without
        clean shutdown. Should be called periodically by the background monitor.

        Returns:
            List of bot IDs that were marked as crashed
        """
        orphaned = find_orphaned_bots(
            stale_threshold_seconds=STALE_THRESHOLD_SECONDS,
            runner_id=None,  # Check all servers
        )

        crashed_ids = []
        for bot in orphaned:
            bot_id = bot.get("id")
            previous_runner = bot.get("runner_id", "unknown")
            if bot_id:
                if mark_bot_crashed(bot_id, reason=f"stale_heartbeat:prev={previous_runner}"):
                    crashed_ids.append(bot_id)
                    logger.warning(
                        "bot_watchdog_stale_heartbeat_detected | bot_id=%s | previous_runner=%s | heartbeat_at=%s",
                        bot_id,
                        previous_runner,
                        bot.get("heartbeat_at"),
                    )
                    if self._on_orphan_detected:
                        try:
                            self._on_orphan_detected(bot_id, bot)
                        except Exception as exc:
                            logger.warning(
                                "bot_watchdog_orphan_callback_failed | bot_id=%s | error=%s",
                                bot_id,
                                exc,
                            )

        if crashed_ids:
            logger.info(
                "bot_watchdog_stale_scan_complete | orphans_found=%d | bot_ids=%s",
                len(crashed_ids),
                crashed_ids,
            )

        return crashed_ids

    def verify_container_ownership(self) -> List[str]:
        """Verify running bot rows still map to live docker containers."""

        from ..storage.storage import load_bots

        failed: List[str] = []
        for bot in load_bots():
            if str(bot.get("status") or "").lower() != "running":
                continue
            container_id = str(bot.get("runner_id") or "").strip()
            if not container_id:
                continue
            proc = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", container_id],
                capture_output=True,
                text=True,
                check=False,
            )
            is_running = proc.returncode == 0 and str(proc.stdout or "").strip().lower() == "true"
            if is_running:
                continue
            bot_id = str(bot.get("id") or "")
            if not bot_id:
                continue
            if mark_bot_crashed(bot_id, reason=f"container_not_running:{container_id}"):
                failed.append(bot_id)
                logger.error(
                    "bot_watchdog_container_missing | bot_id=%s | container_id=%s | stderr=%s",
                    bot_id,
                    container_id,
                    str(proc.stderr or "").strip(),
                )
        return failed

    def start_background_monitor(self) -> None:
        """Start background threads for heartbeat emission and orphan detection.

        Call this once on server startup after recover_local_orphans().
        """
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            logger.warning("bot_watchdog_already_running")
            return

        self._stop_event.clear()

        # Heartbeat thread - emits heartbeats for registered bots
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name="BotWatchdog-Heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

        # Monitor thread - scans for stale heartbeats
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="BotWatchdog-Monitor",
            daemon=True,
        )
        self._monitor_thread.start()

        logger.info(
            "bot_watchdog_background_started | runner_id=%s | heartbeat_interval=%s | monitor_interval=%s",
            self._runner_id,
            HEARTBEAT_INTERVAL_SECONDS,
            MONITOR_INTERVAL_SECONDS,
        )

    def stop_background_monitor(self) -> None:
        """Stop background monitoring threads."""
        self._stop_event.set()

        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None

        if self._monitor_thread is not None:
            self._monitor_thread.join(timeout=5)
            self._monitor_thread = None

        logger.info("bot_watchdog_background_stopped | runner_id=%s", self._runner_id)

    def _heartbeat_loop(self) -> None:
        """Background loop that emits heartbeats for all registered bots."""
        while not self._stop_event.is_set():
            try:
                self.heartbeat_all()
            except Exception as exc:
                logger.exception("bot_watchdog_heartbeat_loop_error | error=%s", exc)

            self._stop_event.wait(HEARTBEAT_INTERVAL_SECONDS)

    def _monitor_loop(self) -> None:
        """Background loop that scans for stale heartbeats."""
        # Initial delay to let servers boot up
        self._stop_event.wait(MONITOR_INTERVAL_SECONDS)

        while not self._stop_event.is_set():
            try:
                self.scan_stale_heartbeats()
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
            "heartbeat_interval_seconds": HEARTBEAT_INTERVAL_SECONDS,
            "stale_threshold_seconds": STALE_THRESHOLD_SECONDS,
            "monitor_interval_seconds": MONITOR_INTERVAL_SECONDS,
            "heartbeat_thread_alive": self._heartbeat_thread is not None and self._heartbeat_thread.is_alive(),
            "monitor_thread_alive": self._monitor_thread is not None and self._monitor_thread.is_alive(),
        }


# Convenience function to get the singleton
def get_watchdog() -> BotWatchdog:
    """Return the singleton BotWatchdog instance."""
    return BotWatchdog.instance()
