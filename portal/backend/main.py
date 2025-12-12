"""FastAPI entrypoint that wires routers and shared middleware."""

from datetime import datetime
import os
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .controller import bots, candles, indicators, instruments, providers, strategies

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

app = FastAPI(
    title="Quant-Trad API",
    description="FastAPI for OHLCV and signal services",
    version="0.1.0",
)

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
app.include_router(indicators.router, prefix="/api/indicators")
app.include_router(strategies.router, prefix="/api/strategies")
app.include_router(instruments.router, prefix="/api/instruments")
app.include_router(bots.router, prefix="/api/bots")
app.include_router(providers.router, prefix="/api/providers")


@app.get("/api/health")
def health() -> dict:
    """Simple health check endpoint for uptime probes."""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}
