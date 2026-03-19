from __future__ import annotations

import logging

from core.settings import get_settings
from portal.backend.service.market.stats_queue import StatsWorker

_SETTINGS = get_settings()


def _configure_logging() -> None:
    level = _SETTINGS.logging.level
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
