"""Provider-free application peers for the existing two-filesystem fixture."""
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
from urllib.error import URLError
from urllib.request import urlopen

import pytest


@pytest.fixture
def server_peers(storage, tmp_path):
    """Start the real supervisor/initializer as the same UID as maintenance."""
    assert os.getuid() == 70 and os.getenv("QT_STORAGE_DEMO") == "1"
    processes = []
    handles = []

    def start(env):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        peer_env = dict(env)
        peer_env.update(
            QT_CONFIG_PROFILE="prod",
            QT_BOT_RUNTIME_IMAGE="qt-disposable-runtime:"+os.environ["QT_IMAGE_SOURCE_REVISION"],
            QT_BACKEND_HOST="127.0.0.1", QT_BACKEND_PORT=str(port),
            QT_WORKERS_INDICATORS_PROCESSES="1", QT_WORKERS_RESEARCH_PROCESSES="1",
            QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA="true",
            QT_SINGLE_NODE_ENABLE_SCHEDULED_FACTS="false",
            QT_SINGLE_NODE_ENABLE_STRUCTURED_FACTS="false",
            QT_SINGLE_NODE_ENABLE_TRADE_STREAMS="false",
            QT_SINGLE_NODE_ENABLE_L2_STREAMS="false",
        )
        path = tmp_path/"backend-peers.log"
        handle = path.open("w")
        handles.append(handle)
        process = subprocess.Popen([sys.executable, "-m", "portal.backend.run_backend"],
                                   env=peer_env, stdout=handle, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        processes.append((process, path))
        def read_status():
            with urlopen(f"http://127.0.0.1:{port}/api/storage", timeout=5) as response:
                return json.load(response)
        deadline = time.monotonic()+120
        while time.monotonic() < deadline:
            assert process.poll() is None, path.read_text()[-6000:]
            try:
                status = read_status()
                break
            except (URLError, TimeoutError):
                time.sleep(.25)
        else:
            pytest.fail("shared-owner backend did not become ready: "+path.read_text()[-6000:])
        assert status["schema_version"] == "qt.storage_status.v1"
        assert {row["target_id"] for row in status["targets"]} == {"ssd", "hdd"}
        assert all(row["status"] == "available" for row in status["targets"])
        initialized = subprocess.run(
            [sys.executable, "-m", "portal.backend.workers.single_node_initializer"],
            env=peer_env, capture_output=True, text=True, timeout=90)
        assert initialized.returncode == 0, initialized.stderr[-6000:]+initialized.stdout[-3000:]
        result = json.loads(initialized.stdout.strip().splitlines()[-1])
        assert result["status"] == "initialized" and result["instruments"]
        assert result["trade_streams"] is result["level2_streams"] is None
        assert not result["scheduled_facts"]

        def frozen_rows(dataset_id, series_id):
            # Use a fresh process and the production repository, without the
            # parent's monkeypatched object-store or database objects.
            code = """
import json,sys
from portal.backend.service.storage.repos.market_data import market_data_repo
rows=market_data_repo.read_dataset_fact_revisions(dataset_id=sys.argv[1],series_id=int(sys.argv[2]))
print(json.dumps([[r.fact_version_id,r.row_hash,r.market_commit_seq,r.revision,r.fact.known_at.isoformat()] for r in rows]))
"""
            result = subprocess.run([sys.executable, "-c", code, dataset_id, str(series_id)],
                                    env=peer_env, capture_output=True, text=True, timeout=60)
            assert result.returncode == 0, result.stderr[-6000:]
            return json.loads(result.stdout.strip().splitlines()[-1])
        return read_status, frozen_rows

    yield start
    failures = []
    for process, path in reversed(processes):
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                # Kill only this explicitly created process group, including
                # supervisor children, before the disposable database is dropped.
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
                failures.append("backend did not stop within its deadline")
        if process.returncode != 0:
            failures.append(path.read_text()[-6000:])
    for handle in handles:
        handle.close()
    assert not failures, "\n".join(failures)
