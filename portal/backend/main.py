"""FastAPI entrypoint that wires routers and shared middleware."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
import logging
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.settings import get_settings
from .controller import bots, candles, indicators as ind_controller, instruments, providers, reports, strategies
from .service.bots.bot_watchdog import get_watchdog
from .service.db.postgres_extensions import ensure_postgres_extensions

# Auto-discover indicators via package imports.
import indicators  # noqa: F401
from overlays.builtins import ensure_builtin_overlays_registered

_SETTINGS = get_settings()


class _HealthAccessFilter(logging.Filter):
    """Drop routine health-probe access logs without muting real API traffic."""

    def filter(self, record: logging.LogRecord) -> bool:
        args = getattr(record, "args", ())
        if not isinstance(args, tuple) or len(args) < 3:
            return True
        path = str(args[2] or "")
        return "/api/health" not in path


def _allowed_origins() -> List[str]:
    """Load allowed origins from centralized settings."""

    return list(_SETTINGS.backend.allowed_origins)


def _configure_logging() -> None:
    """Configure basic logging once for the API server."""

    level = _SETTINGS.logging.level
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    access_logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(existing, _HealthAccessFilter) for existing in access_logger.filters):
        access_logger.addFilter(_HealthAccessFilter())

def _startup_watchdog() -> None:
    ensure_builtin_overlays_registered()
    ensure_postgres_extensions()
    watchdog = get_watchdog()
    watchdog.recover_local_orphans()
    watchdog.start_background_monitor()
    logger.info("bot_watchdog_ready | runner_id=%s", watchdog.runner_id)


def _shutdown_watchdog() -> None:
    watchdog = get_watchdog()
    watchdog.stop_background_monitor()
    logger.info("bot_watchdog_stopped | runner_id=%s", watchdog.runner_id)


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    _startup_watchdog()
    try:
        yield
    finally:
        _shutdown_watchdog()


app = FastAPI(
    title="Quant-Trad API",
    description="FastAPI for OHLCV and signal services",
    version="0.1.0",
    lifespan=_lifespan,
)


# NOTE: Normalizing duplicate "/api/api" prefixes is a frontend/proxy
# responsibility; middleware-based rewrites were removed to avoid hiding
# client configuration issues.

_configure_logging()
logger = logging.getLogger(__name__)

origins = _allowed_origins()
allow_credentials = "*" not in origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if allow_credentials else ["*"],
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(candles.router, prefix="/api/candles")
app.include_router(ind_controller.router, prefix="/api/indicators")
app.include_router(strategies.router, prefix="/api/strategies")
app.include_router(instruments.router, prefix="/api/instruments")
app.include_router(bots.router, prefix="/api/bots")
app.include_router(providers.router, prefix="/api/providers")
app.include_router(reports.router, prefix="/api/reports")


@app.get("/api/health")
def health() -> dict:
    """Simple health check endpoint for uptime probes."""
    return {"status": "ok", "timestamp": datetime.now(UTC).isoformat().replace("+00:00", "Z")}
