"""Runner-level operational diagnostics for bot runtime ownership.

These signals explain runner/container liveness. They are diagnostic evidence,
not strategy, wallet, order, trade, or report truth.
"""

from __future__ import annotations

import json
import logging
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional

from core.settings import get_settings

from ..observability import BackendObserver, normalize_failure_mode

logger = logging.getLogger(__name__)

_OBSERVER = BackendObserver(component="runner_observability", event_logger=logger)
_RECENT_LOCK = threading.Lock()
_LATEST_CLOCK_GAP_BY_RUNNER: Dict[str, Dict[str, Any]] = {}
_LATEST_DOCKER_EVENT_BY_BOT: Dict[str, Dict[str, Any]] = {}
_LATEST_DOCKER_EVENT_BY_CONTAINER: Dict[str, Dict[str, Any]] = {}
_START_STOP_LOCK = threading.Lock()
_CLOCK_SENTINEL: "RunnerClockGapSentinel | None" = None
_DOCKER_OBSERVER: "DockerLifecycleObserver | None" = None

_BOT_CONTAINER_PREFIX = "quant-trad-bots-"
_QUANT_TRAD_NAME_PREFIXES = (
    "quant-trad-",
    "quanttrad-",
    "docker-backend",
    "docker-frontend",
    "docker-tsdb",
    "docker-loki",
    "docker-promtail",
    "docker-grafana",
)
_TRACKED_DOCKER_ACTIONS = {
    "create",
    "start",
    "restart",
    "die",
    "stop",
    "kill",
    "oom",
    "destroy",
    "health_status: healthy",
    "health_status: unhealthy",
}
_DOCKER_UNAVAILABLE_MARKERS = (
    "cannot connect to the docker daemon",
    "permission denied",
    "no such file or directory",
)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_seconds(diagnostic: Mapping[str, Any]) -> Optional[float]:
    detected_at = _parse_iso(diagnostic.get("detected_at"))
    if detected_at is None:
        return None
    return max((datetime.now(timezone.utc) - detected_at).total_seconds(), 0.0)


def _round_seconds(value: float) -> float:
    return round(max(float(value), 0.0), 3)


def build_clock_gap_diagnostic(
    *,
    runner_id: str,
    wall_delta_seconds: float,
    monotonic_delta_seconds: float,
    expected_interval_seconds: float,
    threshold_seconds: float,
    detected_at: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return a bounded diagnostic when a runner loop wakes up late enough."""

    expected = max(float(expected_interval_seconds), 0.0)
    wall_delta = max(float(wall_delta_seconds), 0.0)
    monotonic_delta = max(float(monotonic_delta_seconds), 0.0)
    wall_gap = max(wall_delta - expected, 0.0)
    monotonic_gap = max(monotonic_delta - expected, 0.0)
    gap_seconds = max(wall_gap, monotonic_gap)
    if gap_seconds < max(float(threshold_seconds), 0.0):
        return None
    return {
        "runner_id": str(runner_id or "").strip() or "unknown",
        "detected_at": detected_at or _utcnow_iso(),
        "expected_interval_seconds": _round_seconds(expected),
        "threshold_seconds": _round_seconds(threshold_seconds),
        "wall_delta_seconds": _round_seconds(wall_delta),
        "monotonic_delta_seconds": _round_seconds(monotonic_delta),
        "wall_gap_seconds": _round_seconds(wall_gap),
        "monotonic_gap_seconds": _round_seconds(monotonic_gap),
        "gap_seconds": _round_seconds(gap_seconds),
    }


def record_runner_clock_gap(diagnostic: Mapping[str, Any]) -> None:
    runner_id = str(diagnostic.get("runner_id") or "").strip() or "unknown"
    with _RECENT_LOCK:
        _LATEST_CLOCK_GAP_BY_RUNNER[runner_id] = dict(diagnostic)


def latest_runner_clock_gap(
    runner_id: Any = None,
    *,
    max_age_seconds: float = 900.0,
) -> Optional[Dict[str, Any]]:
    runner_text = str(runner_id or "").strip()
    with _RECENT_LOCK:
        if runner_text:
            candidate = _LATEST_CLOCK_GAP_BY_RUNNER.get(runner_text)
        else:
            candidate = max(
                _LATEST_CLOCK_GAP_BY_RUNNER.values(),
                key=lambda item: str(item.get("detected_at") or ""),
                default=None,
            )
    if not candidate:
        return None
    age = _age_seconds(candidate)
    if age is not None and age > max(float(max_age_seconds), 0.0):
        return None
    payload = dict(candidate)
    if age is not None:
        payload["age_seconds"] = _round_seconds(age)
    return payload


def bot_id_from_container_name(name: Any) -> Optional[str]:
    normalized = str(name or "").strip().lstrip("/")
    if not normalized.startswith(_BOT_CONTAINER_PREFIX):
        return None
    bot_id = normalized[len(_BOT_CONTAINER_PREFIX) :].strip()
    return bot_id or None


def _container_family(name: str, bot_id: Optional[str]) -> str:
    normalized = str(name or "").strip().lower()
    if bot_id:
        return "bot"
    if "backend" in normalized:
        return "backend"
    if "frontend" in normalized:
        return "frontend"
    if "tsdb" in normalized or "postgres" in normalized:
        return "database"
    if "loki" in normalized or "promtail" in normalized or "grafana" in normalized:
        return "observability"
    return "other"


def _coerce_exit_code(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _docker_event_time(payload: Mapping[str, Any]) -> Optional[str]:
    raw = payload.get("time")
    try:
        timestamp = float(raw)
    except (TypeError, ValueError):
        return None
    try:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        return None


def normalize_docker_container_event(payload: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    event_type = str(payload.get("Type") or payload.get("type") or "").strip().lower()
    if event_type and event_type != "container":
        return None
    actor = payload.get("Actor") if isinstance(payload.get("Actor"), Mapping) else {}
    attributes = actor.get("Attributes") if isinstance(actor.get("Attributes"), Mapping) else {}
    action = str(payload.get("Action") or payload.get("status") or "").strip()
    if not action or action not in _TRACKED_DOCKER_ACTIONS:
        return None
    name = str(attributes.get("name") or payload.get("name") or "").strip().lstrip("/")
    image = str(attributes.get("image") or payload.get("from") or "").strip()
    container_id = str(actor.get("ID") or payload.get("id") or "").strip()
    bot_id = str(attributes.get("quanttrad.bot_id") or "").strip() or bot_id_from_container_name(name)
    event: Dict[str, Any] = {
        "action": action,
        "container_id": container_id[:12] or None,
        "container_name": name or None,
        "container_family": _container_family(name, bot_id),
        "image": image or None,
        "bot_id": bot_id,
        "exit_code": _coerce_exit_code(attributes.get("exitCode")),
        "docker_time": _docker_event_time(payload),
        "observed_at": _utcnow_iso(),
    }
    project = str(attributes.get("com.docker.compose.project") or "").strip()
    service = str(attributes.get("com.docker.compose.service") or "").strip()
    loki_job = str(attributes.get("loki.job") or "").strip()
    runtime = str(attributes.get("quanttrad.runtime") or "").strip()
    run_id = str(attributes.get("quanttrad.run_id") or "").strip()
    if project:
        event["compose_project"] = project
    if service:
        event["compose_service"] = service
    if loki_job:
        event["loki_job"] = loki_job
    if runtime:
        event["runtime"] = runtime
    if run_id:
        event["run_id"] = run_id
    return event


def is_quant_trad_container_event(event: Mapping[str, Any]) -> bool:
    if event.get("bot_id"):
        return True
    name = str(event.get("container_name") or "").strip().lower()
    image = str(event.get("image") or "").strip().lower()
    project = str(event.get("compose_project") or "").strip().lower()
    service = str(event.get("compose_service") or "").strip().lower()
    loki_job = str(event.get("loki_job") or "").strip().lower()
    runtime = str(event.get("runtime") or "").strip().lower()
    if loki_job == "quanttrad" or runtime == "bot":
        return True
    if project in {"quant-trad", "quanttrad"}:
        return True
    if any(name.startswith(prefix) for prefix in _QUANT_TRAD_NAME_PREFIXES):
        return True
    return "quanttrad" in image or "quant-trad" in image or service in {
        "backend",
        "frontend",
        "tsdb",
        "loki",
        "promtail",
        "grafana",
    }


def record_docker_lifecycle_event(event: Mapping[str, Any]) -> None:
    event_payload = dict(event)
    container_name = str(event_payload.get("container_name") or "").strip()
    bot_id = str(event_payload.get("bot_id") or "").strip()
    with _RECENT_LOCK:
        if container_name:
            _LATEST_DOCKER_EVENT_BY_CONTAINER[container_name] = event_payload
        if bot_id:
            _LATEST_DOCKER_EVENT_BY_BOT[bot_id] = event_payload


def latest_docker_lifecycle_event_for_bot(
    bot_id: Any,
    *,
    max_age_seconds: float = 900.0,
) -> Optional[Dict[str, Any]]:
    key = str(bot_id or "").strip()
    if not key:
        return None
    with _RECENT_LOCK:
        candidate = _LATEST_DOCKER_EVENT_BY_BOT.get(key)
    if not candidate:
        return None
    age = _age_seconds({"detected_at": candidate.get("observed_at")})
    if age is not None and age > max(float(max_age_seconds), 0.0):
        return None
    payload = dict(candidate)
    if age is not None:
        payload["age_seconds"] = _round_seconds(age)
    return payload


def docker_lifecycle_level(event: Mapping[str, Any]) -> int:
    action = str(event.get("action") or "").strip()
    exit_code = _coerce_exit_code(event.get("exit_code"))
    if action in {"kill", "oom"}:
        return logging.WARNING
    if action == "die" and exit_code not in (None, 0):
        return logging.WARNING
    if action == "health_status: unhealthy":
        return logging.WARNING
    return logging.INFO


class RunnerClockGapSentinel:
    """Detect process pause/suspend gaps for one backend runner."""

    def __init__(
        self,
        *,
        runner_id: str,
        interval_seconds: float,
        threshold_seconds: float,
        observer: BackendObserver = _OBSERVER,
    ) -> None:
        self.runner_id = str(runner_id or "").strip() or "unknown"
        self.interval_seconds = max(float(interval_seconds), 0.1)
        self.threshold_seconds = max(float(threshold_seconds), 0.1)
        self._observer = observer
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="RunnerClockGapSentinel",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "runner_clock_gap_sentinel_started | runner_id=%s | interval_seconds=%s | threshold_seconds=%s",
            self.runner_id,
            self.interval_seconds,
            self.threshold_seconds,
        )

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("runner_clock_gap_sentinel_stopped | runner_id=%s", self.runner_id)

    def _loop(self) -> None:
        previous_wall = datetime.now(timezone.utc)
        previous_monotonic = time.monotonic()
        while not self._stop_event.wait(self.interval_seconds):
            now_wall = datetime.now(timezone.utc)
            now_monotonic = time.monotonic()
            wall_delta = (now_wall - previous_wall).total_seconds()
            monotonic_delta = now_monotonic - previous_monotonic
            previous_wall = now_wall
            previous_monotonic = now_monotonic
            diagnostic = build_clock_gap_diagnostic(
                runner_id=self.runner_id,
                wall_delta_seconds=wall_delta,
                monotonic_delta_seconds=monotonic_delta,
                expected_interval_seconds=self.interval_seconds,
                threshold_seconds=self.threshold_seconds,
            )
            if diagnostic is None:
                continue
            record_runner_clock_gap(diagnostic)
            logger.warning(
                "runner_clock_gap_detected | runner_id=%s | gap_seconds=%s | wall_delta_seconds=%s | monotonic_delta_seconds=%s | expected_interval_seconds=%s",
                diagnostic["runner_id"],
                diagnostic["gap_seconds"],
                diagnostic["wall_delta_seconds"],
                diagnostic["monotonic_delta_seconds"],
                diagnostic["expected_interval_seconds"],
            )
            self._observer.gauge(
                "runner_clock_gap_seconds",
                float(diagnostic["gap_seconds"]),
                gap_type="runner_pause",
            )
            self._observer.event(
                "runner_clock_gap_detected",
                level=logging.WARNING,
                log_to_logger=False,
                **diagnostic,
            )


class DockerLifecycleObserver:
    """Stream Docker container lifecycle events for Quant-Trad containers."""

    def __init__(
        self,
        *,
        runner_id: str,
        retry_interval_seconds: float,
        observer: BackendObserver = _OBSERVER,
    ) -> None:
        self.runner_id = str(runner_id or "").strip() or "unknown"
        self.retry_interval_seconds = max(float(retry_interval_seconds), 0.1)
        self._observer = observer
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._process: Optional[subprocess.Popen[str]] = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.running:
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="DockerLifecycleObserver",
            daemon=True,
        )
        self._thread.start()
        logger.info("docker_lifecycle_observer_started | runner_id=%s", self.runner_id)

    def stop(self) -> None:
        self._stop_event.set()
        process = self._process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("docker_lifecycle_observer_stopped | runner_id=%s", self.runner_id)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_once()
            except FileNotFoundError:
                self._observer_unavailable("docker_cli_missing")
                return
            except Exception as exc:
                failure_mode = normalize_failure_mode(exc)
                logger.warning(
                    "docker_lifecycle_observer_error | runner_id=%s | failure_mode=%s | error=%s",
                    self.runner_id,
                    failure_mode,
                    exc,
                )
                self._observer.event(
                    "docker_lifecycle_observer_error",
                    level=logging.WARNING,
                    log_to_logger=False,
                    runner_id=self.runner_id,
                    failure_mode=failure_mode,
                    error=str(exc),
                )
            if self._stop_event.wait(self.retry_interval_seconds):
                break

    def _run_once(self) -> None:
        command = ["docker", "events", "--format", "{{json .}}", "--filter", "type=container"]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self._process = process
        assert process.stdout is not None
        for line in process.stdout:
            if self._stop_event.is_set():
                break
            text = line.strip()
            if not text:
                continue
            try:
                raw = json.loads(text)
            except json.JSONDecodeError:
                lowered = text.lower()
                if any(marker in lowered for marker in _DOCKER_UNAVAILABLE_MARKERS):
                    self._observer_unavailable("docker_events_unavailable")
                    self._stop_event.set()
                    return
                logger.warning(
                    "docker_lifecycle_observer_non_json_line | runner_id=%s | line=%s",
                    self.runner_id,
                    text[:240],
                )
                continue
            if not isinstance(raw, Mapping):
                continue
            event = normalize_docker_container_event(raw)
            if event is None or not is_quant_trad_container_event(event):
                continue
            event["runner_id"] = self.runner_id
            record_docker_lifecycle_event(event)
            level = docker_lifecycle_level(event)
            logger.log(
                level,
                "docker_lifecycle_event | runner_id=%s | container_name=%s | action=%s | exit_code=%s | bot_id=%s | container_family=%s",
                self.runner_id,
                event.get("container_name"),
                event.get("action"),
                event.get("exit_code"),
                event.get("bot_id"),
                event.get("container_family"),
            )
            self._observer.increment(
                "docker_lifecycle_events_total",
                outcome=str(event.get("action") or "unknown"),
            )
            self._observer.event(
                "docker_lifecycle_event",
                level=level,
                log_to_logger=False,
                **event,
            )
        return_code = process.wait()
        self._process = None
        if not self._stop_event.is_set():
            logger.warning(
                "docker_lifecycle_observer_exited | runner_id=%s | return_code=%s",
                self.runner_id,
                return_code,
            )

    def _observer_unavailable(self, reason: str) -> None:
        logger.warning(
            "docker_lifecycle_observer_unavailable | runner_id=%s | reason=%s",
            self.runner_id,
            reason,
        )
        self._observer.event(
            "docker_lifecycle_observer_unavailable",
            level=logging.WARNING,
            log_to_logger=False,
            runner_id=self.runner_id,
            reason=reason,
        )


def start_runner_observability(*, runner_id: str) -> None:
    settings = get_settings()
    watchdog_settings = settings.bot_runtime.watchdog
    runtime_target = str(settings.bot_runtime.target or "").strip().lower()
    global _CLOCK_SENTINEL, _DOCKER_OBSERVER
    with _START_STOP_LOCK:
        if watchdog_settings.clock_gap_enabled:
            if _CLOCK_SENTINEL is None or not _CLOCK_SENTINEL.running:
                _CLOCK_SENTINEL = RunnerClockGapSentinel(
                    runner_id=runner_id,
                    interval_seconds=watchdog_settings.clock_gap_interval_seconds,
                    threshold_seconds=watchdog_settings.clock_gap_threshold_seconds,
                )
                _CLOCK_SENTINEL.start()
        if watchdog_settings.docker_lifecycle_enabled:
            if runtime_target != "docker":
                logger.info(
                    "docker_lifecycle_observer_skipped | runner_id=%s | runtime_target=%s",
                    runner_id,
                    runtime_target or "unknown",
                )
            elif _DOCKER_OBSERVER is None or not _DOCKER_OBSERVER.running:
                _DOCKER_OBSERVER = DockerLifecycleObserver(
                    runner_id=runner_id,
                    retry_interval_seconds=watchdog_settings.docker_lifecycle_retry_interval_seconds,
                )
                _DOCKER_OBSERVER.start()


def stop_runner_observability() -> None:
    global _CLOCK_SENTINEL, _DOCKER_OBSERVER
    with _START_STOP_LOCK:
        if _DOCKER_OBSERVER is not None:
            _DOCKER_OBSERVER.stop()
            _DOCKER_OBSERVER = None
        if _CLOCK_SENTINEL is not None:
            _CLOCK_SENTINEL.stop()
            _CLOCK_SENTINEL = None
