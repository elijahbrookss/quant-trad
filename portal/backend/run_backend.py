"""Process supervisor for backend API + internal worker pools."""

from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Dict, List, Optional


logger = logging.getLogger(__name__)


@dataclass
class ManagedProcess:
    name: str
    popen: subprocess.Popen


_STOP = False


def _configure_logging() -> None:
    level_name = os.getenv("PORTAL_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _on_signal(signum: int, _frame) -> None:
    global _STOP
    _STOP = True
    logger.info("backend_supervisor_shutdown_signal | signum=%s", signum)


def _spawn_process(name: str, cmd: List[str], env_overrides: Optional[Dict[str, str]] = None) -> ManagedProcess:
    env = os.environ.copy()
    if env_overrides:
        env.update({k: str(v) for k, v in env_overrides.items()})
    popen = subprocess.Popen(cmd, env=env)
    logger.info("backend_supervisor_spawned | name=%s pid=%s cmd=%s", name, popen.pid, " ".join(cmd))
    return ManagedProcess(name=name, popen=popen)


def _terminate_all(processes: List[ManagedProcess], timeout_seconds: float = 8.0) -> None:
    alive = [p for p in processes if p.popen.poll() is None]
    for proc in alive:
        try:
            proc.popen.terminate()
            logger.info("backend_supervisor_terminate_sent | name=%s pid=%s", proc.name, proc.popen.pid)
        except Exception:
            logger.exception("backend_supervisor_terminate_failed | name=%s", proc.name)

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if all(p.popen.poll() is not None for p in processes):
            return
        time.sleep(0.1)

    for proc in processes:
        if proc.popen.poll() is None:
            try:
                proc.popen.kill()
                logger.warning("backend_supervisor_kill_sent | name=%s pid=%s", proc.name, proc.popen.pid)
            except Exception:
                logger.exception("backend_supervisor_kill_failed | name=%s", proc.name)


def main() -> int:
    global _STOP
    _configure_logging()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    host = os.getenv("BACKEND_HOST", "0.0.0.0")
    port = os.getenv("BACKEND_PORT", "8000")
    quantlab_workers = max(1, int(os.getenv("QUANTLAB_WORKER_PROCESSES", "3")))
    stats_workers = max(1, int(os.getenv("STATS_WORKER_PROCESSES", "2")))
    node = socket.gethostname()

    logger.info(
        "backend_supervisor_starting | node=%s api=%s:%s quantlab_workers=%s stats_workers=%s",
        node,
        host,
        port,
        quantlab_workers,
        stats_workers,
    )

    processes: List[ManagedProcess] = []
    api_cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "portal.backend.main:app",
        "--host",
        host,
        "--port",
        str(port),
    ]
    processes.append(_spawn_process("api", api_cmd))

    for idx in range(quantlab_workers):
        processes.append(
            _spawn_process(
                f"quantlab-worker-{idx}",
                [sys.executable, "-m", "portal.backend.workers.quantlab_worker"],
                env_overrides={
                    "QUANTLAB_WORKER_INDEX": str(idx),
                    "QUANTLAB_WORKER_TOTAL": str(quantlab_workers),
                },
            )
        )

    for idx in range(stats_workers):
        processes.append(
            _spawn_process(
                f"stats-worker-{idx}",
                [sys.executable, "-m", "portal.backend.workers.stats_worker"],
                env_overrides={
                    "STATS_WORKER_INDEX": str(idx),
                    "STATS_WORKER_TOTAL": str(stats_workers),
                },
            )
        )

    exit_code = 0
    try:
        while not _STOP:
            time.sleep(0.25)
            for proc in processes:
                rc = proc.popen.poll()
                if rc is not None:
                    logger.error(
                        "backend_supervisor_child_exited | name=%s pid=%s returncode=%s",
                        proc.name,
                        proc.popen.pid,
                        rc,
                    )
                    _STOP = True
                    exit_code = rc if rc is not None else 1
                    raise RuntimeError(f"child process exited: {proc.name}")
    except RuntimeError:
        pass
    finally:
        _terminate_all(processes)

    logger.info("backend_supervisor_stopped | exit_code=%s", exit_code)
    return int(exit_code)


if __name__ == "__main__":
    raise SystemExit(main())
