from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .controller import candles, indicators

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
