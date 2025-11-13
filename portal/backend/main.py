from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .controller import bots, candles, indicators, strategies

app = FastAPI(
    title="Quant-Trad API",
    description="FastAPI for OHLCV and signal services",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # for dev only
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(candles.router, prefix="/api/candles")
app.include_router(indicators.router, prefix="/api/indicators")
app.include_router(strategies.router, prefix="/api/strategies")
app.include_router(bots.router, prefix="/api/bots")


@app.get("/api/health")
def health() -> dict:
    """Simple health check endpoint for uptime probes."""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat() + "Z"}
