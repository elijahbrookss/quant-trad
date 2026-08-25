from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]


def test_strategy_facade_import_does_not_bootstrap_persistence() -> None:
    """Proof collection may import strategy routes without resolving a DSN."""

    environment = os.environ.copy()
    environment.update(
        {
            "PG_DSN": "postgresql://qt:qt@127.0.0.1:1/qt_collection_forbidden",
            "PGCONNECT_TIMEOUT": "1",
            "PYTHONPATH": os.pathsep.join(
                [
                    str(ROOT),
                    str(ROOT / "src"),
                    str(ROOT / "portal" / "backend"),
                ]
            ),
        }
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "from portal.backend.service.strategies.strategy_service "
                "import facade; "
                "assert facade._REGISTRY._instance is None; "
                "print('strategy_registry_bootstrap=deferred')"
            ),
        ],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    output = f"{completed.stdout}\n{completed.stderr}"
    assert completed.returncode == 0, output
    assert "strategy_registry_bootstrap=deferred" in completed.stdout
    assert "portal_db_initialise_failed" not in output
    assert "PG_DSN is required" not in output

