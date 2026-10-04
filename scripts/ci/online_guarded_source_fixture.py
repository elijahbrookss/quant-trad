"""Controlled source peers for the owned Docker handoff integration only.

The test image installs this file at the three fixed source module paths. These
are synthetic peers using the real process-lifetime guard, not application or
legacy-image qualification. No production image includes these replacements.
"""
import os
from pathlib import Path
import signal
import time

from core.storage_writer_fence import retain_source_writer_fence


def main():
    if os.environ.get("QT_ONLINE_GUARDED_SOURCE_FIXTURE") != "1":
        raise RuntimeError("owned_guarded_source_fixture_required")
    root = Path(os.environ["QT_STORAGE_SOURCE_FENCE_ROOT"])
    assert retain_source_writer_fence()
    if Path(__file__).stem == "single_node_initializer":
        return 0
    stopped = False
    def stop(_signal, _frame):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    while not stopped:
        if Path(__file__).stem == "market_data_collector":
            with (root / "objects" / "native-intake").open("ab") as stream:
                stream.write(b"x")
        time.sleep(0.05)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
