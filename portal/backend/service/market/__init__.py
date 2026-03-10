"""Market data services."""

__all__ = ["fetch_ohlcv", "list_instruments", "resolve_instrument"]


def __getattr__(name: str):
    if name == "fetch_ohlcv":
        from .candle_service import fetch_ohlcv

        return fetch_ohlcv
    if name == "list_instruments":
        from .instrument_service import list_instruments

        return list_instruments
    if name == "resolve_instrument":
        from .instrument_service import resolve_instrument

        return resolve_instrument
    raise AttributeError(name)
