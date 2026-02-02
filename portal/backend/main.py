"""FastAPI entrypoint that wires routers and shared middleware."""

from datetime import datetime
import logging
import os
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request

from .controller import bots, candles, indicators as ind_controller, instruments, providers, reports, strategies
from .service.bots.bot_watchdog import get_watchdog
from .service.market.stats_queue import start_pipeline, stop_pipeline

# Auto-discover indicators and signal rules via package imports
# Indicators: pure computation, returns domain objects
import indicators  # noqa: F401

# Signals: auto-discovers all @signal_rule decorated functions
# This triggers decorator execution and registration in _REGISTRY
import signals  # noqa: F401


def _allowed_origins() -> List[str]:
    """Load allowed origins from env, defaulting to common dev hosts."""

    raw = os.getenv("PORTAL_ALLOWED_ORIGINS", "")
    if raw:
        origins = [item.strip() for item in raw.split(",") if item.strip()]
        if origins:
            return origins
    # default to typical local dev hosts/ports
    return [
        "http://localhost",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1",
        "http://127.0.0.1:5173",
    ]


def _configure_logging() -> None:
    """Configure basic logging once for the API server."""

    level_name = os.getenv("PORTAL_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

app = FastAPI(
    title="Quant-Trad API",
    description="FastAPI for OHLCV and signal services",
    version="0.1.0",
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

@app.on_event("startup")
def _startup_watchdog() -> None:
    watchdog = get_watchdog()
    watchdog.recover_local_orphans()
    watchdog.start_background_monitor()
    start_pipeline()
    logger.info("bot_watchdog_ready | runner_id=%s", watchdog.runner_id)


@app.on_event("shutdown")
def _shutdown_watchdog() -> None:
    watchdog = get_watchdog()
    watchdog.stop_background_monitor()
    stop_pipeline()
    logger.info("bot_watchdog_stopped | runner_id=%s", watchdog.runner_id)


@app.get("/api/health")
def health() -> dict:
    """Simple health check endpoint for uptime probes."""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}
