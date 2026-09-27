"""Actual worker SIGTERM/exit with isolated discovery and no infrastructure I/O."""
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

import pytest


SCRIPT = r'''import asyncio
import os
from pathlib import Path
import sys
from types import SimpleNamespace
from portal.backend.workers import market_data_collector as worker
from portal.backend.service.market.collector_supervisor import ContinuousCollectorSupervisor, CollectorAdapterRegistry
from tests.test_market_data.test_continuous_collector_supervisor import _Repository, _OperationsRepository, _Adapter

root = Path(sys.argv[1])
fail = sys.argv[2] == "fail"
host_fixture = os.environ.get("QT_SIGNAL_HOST_FIXTURE") == "1"
class Adapter(_Adapter):
    async def run(self, *, stop_requested, **kwargs):
        (root/"ready").write_text("ready")
        while not stop_requested():
            if os.environ.get("QT_SIGNAL_REAL_PUBLICATION") == "1" and (root/"real-publication.json").exists():
                from tests.test_market_data.online_signal_publication_fixture import run_publication
                return await run_publication(root, stop_requested=stop_requested, **kwargs)
            if host_fixture:
                with (root/"native-intake").open("ab") as stream:
                    stream.write(b"x")
            await asyncio.sleep(0.05)
        if fail or (host_fixture and (root/"fail-final-drain").exists()):
            if host_fixture:
                spool = root.parent/"spool"/"qt-signal-owned-fixture"
                spool.mkdir(parents=True, exist_ok=True)
                (spool/"pending.sealed").write_bytes(b"owned failed-finalizer WAL fixture")
            raise RuntimeError("fixture signal-time publication failure")
        return {"status": "stopped"}

class Lifecycle:
    def __init__(self, **kwargs): pass
    def start(self): pass
    def snapshot(self): return {}
    def stop(self): (root/"lifecycle-stopped").write_text("stopped")

class Heartbeat:
    def __init__(self, *args, **kwargs): pass
    def start(self): pass
    def set_state(self, *args, **kwargs): pass
    def stop(self): (root/"heartbeat-stopped").write_text("stopped")

worker.require_configured_archive_mount = lambda: None
worker.require_configured_working_mount = lambda: None
worker.wait_for_database_ready = lambda **kwargs: True
worker.storage_maintenance_runners = lambda *args, **kwargs: {}
worker.MarketStorageLifecycleSupervisor = Lifecycle
worker._WorkerHeartbeat = Heartbeat
worker._WORKER_SETTINGS = SimpleNamespace(db_wait_timeout_seconds=1, idle_sleep_seconds=0.01,
    idle_sleep_max_seconds=0.01, shutdown_drain_timeout_seconds=15 if os.environ.get("QT_SIGNAL_REAL_PUBLICATION") == "1" else 5)
worker.market_data_collector.claim_due = lambda **kwargs: None
worker.ContinuousCollectorSupervisor = lambda **kwargs: ContinuousCollectorSupervisor(
    owner_id="signal-fixture", repository=_Repository(), operations_repository=_OperationsRepository(),
    registry=CollectorAdapterRegistry((Adapter(),)), poll_seconds=0.25)
raise SystemExit(worker.main())
'''


@pytest.mark.skipif(sys.platform != "linux", reason="native Linux SIGTERM contract")
@pytest.mark.parametrize("failed", [False, True])
def test_worker_sigterm_exit_reports_supervisor_drain_failure(tmp_path, failed):
    root = Path(__file__).resolve().parents[2]
    env = dict(os.environ, QT_DISABLE_DOTENV="1", QT_LOGGING_LOKI_URL="", QT_LOGGING_LEVEL="INFO",
               PG_DSN="postgresql+psycopg2://disposable:disposable@127.0.0.1:1/disposable",
               PYTHONPATH=os.pathsep.join((str(root), str(root/"src"))))
    process = subprocess.Popen([sys.executable, "-c", SCRIPT, str(tmp_path),
                                "fail" if failed else "clean"], cwd=root, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        deadline = time.monotonic()+15
        while not (tmp_path/"ready").exists() and time.monotonic()<deadline and process.poll() is None:
            time.sleep(0.02)
        assert (tmp_path/"ready").exists(), "owned signal fixture did not start"
        process.send_signal(signal.SIGTERM)
        output, _ = process.communicate(timeout=10)
        assert process.returncode == (5 if failed else 0), output
        assert "market_data_collector_shutdown_signal" in output
        assert (tmp_path/"lifecycle-stopped").exists()
        assert (tmp_path/"heartbeat-stopped").exists()
        if failed:
            assert "market_data_collector_shutdown_failed" in output
            assert "market_data_collector_stopped |" not in output
        else:
            assert "market_data_collector_stopped |" in output
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)
