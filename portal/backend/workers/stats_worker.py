from __future__ import annotations

import logging
import os

from portal.backend.service.market.stats_queue import StatsWorker


def _configure_logging() -> None:
    level_name = os.getenv("PORTAL_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> int:
    _configure_logging()
    worker = StatsWorker()
    worker.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
