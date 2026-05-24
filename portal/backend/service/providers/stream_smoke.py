"""Short-lived provider stream smoke checks for CLI/operator workflows."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Sequence

from core.logger import logger
from data_providers.streams import (
    CanonicalMarketEvent,
    CoinbaseAdvancedTradeStream,
    MarketSubscription,
    ProviderMarketDataStream,
)


StreamFactory = Callable[[], ProviderMarketDataStream]


async def run_provider_stream_smoke(
    *,
    provider_id: str,
    venue_id: str,
    symbol: str,
    product_id: str | None = None,
    channels: Sequence[str] | None = None,
    duration_seconds: float = 10.0,
    timeframe: str | None = None,
    auth_mode: str = "public",
    sample_limit: int = 10,
    stream_factory: StreamFactory | None = None,
) -> dict[str, Any]:
    """Run a bounded read-only stream check and return compact diagnostics."""

    provider = str(provider_id or "").strip().upper()
    venue = str(venue_id or "").strip().upper()
    if provider != "COINBASE":
        raise ValueError("Provider stream smoke checks currently support only COINBASE.")
    if venue and venue != "COINBASE_DIRECT":
        raise ValueError("Coinbase stream smoke checks require venue COINBASE_DIRECT.")

    duration = max(0.1, min(float(duration_seconds or 10.0), 300.0))
    sample_size = max(0, min(int(sample_limit or 0), 50))
    subscription = MarketSubscription.from_values(
        provider=provider,
        venue=venue or "COINBASE_DIRECT",
        symbol=symbol,
        product_id=product_id,
        channels=channels,
        timeframe=timeframe,
        auth_mode=auth_mode,
    )
    if subscription.auth_mode != "public":
        raise ValueError("Only public Coinbase stream smoke checks are supported in v1.")

    stream = stream_factory() if stream_factory is not None else CoinbaseAdvancedTradeStream(provider=provider, venue=subscription.venue)
    started_monotonic = time.monotonic()
    started_at = _utc_iso()
    counts: dict[str, int] = {}
    latest: dict[str, Any] = {}
    samples: list[dict[str, Any]] = []
    diagnostics: dict[str, Any] = {
        "heartbeat_count": 0,
        "largest_heartbeat_gap_seconds": None,
        "sequence_gap_count": 0,
        "out_of_order_count": 0,
        "unsupported_message_count": 0,
        "malformed_message_count": 0,
        "timeout_count": 0,
        "stream_ended_early": False,
        "stream_end_remaining_seconds": None,
    }
    last_heartbeat_at: float | None = None

    logger.info(
        "provider_stream_smoke_started | provider=%s venue=%s symbol=%s product_id=%s duration_seconds=%s channels=%s",
        subscription.provider,
        subscription.venue,
        subscription.symbol,
        subscription.product_id,
        duration,
        ",".join(subscription.channels),
    )
    status = "completed"
    error: str | None = None
    try:
        await stream.connect()
        _increment(counts, "provider_connected")
        await stream.subscribe([subscription])
        iterator = stream.events().__aiter__()
        deadline = started_monotonic + duration
        pending_event: asyncio.Task[CanonicalMarketEvent] | None = None
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            if pending_event is None:
                pending_event = asyncio.create_task(anext(iterator))
            try:
                done, _pending = await asyncio.wait({pending_event}, timeout=min(remaining, 1.0))
            except asyncio.TimeoutError:
                diagnostics["timeout_count"] = int(diagnostics["timeout_count"] or 0) + 1
                continue
            if not done:
                diagnostics["timeout_count"] = int(diagnostics["timeout_count"] or 0) + 1
                continue
            try:
                event = pending_event.result()
            except StopAsyncIteration:
                remaining_at_end = max(deadline - time.monotonic(), 0.0)
                if remaining_at_end > 0.05:
                    diagnostics["stream_ended_early"] = True
                    diagnostics["stream_end_remaining_seconds"] = round(remaining_at_end, 3)
                break
            finally:
                pending_event = None
            payload = event.to_dict()
            kind = str(payload.get("event_kind") or "")
            _increment(counts, kind)
            if sample_size and len(samples) < sample_size:
                samples.append(payload)
            elif sample_size:
                samples.pop(0)
                samples.append(payload)

            if kind == "provider_heartbeat":
                now = time.monotonic()
                diagnostics["heartbeat_count"] = int(diagnostics["heartbeat_count"] or 0) + 1
                if last_heartbeat_at is not None:
                    gap = now - last_heartbeat_at
                    current_largest = diagnostics.get("largest_heartbeat_gap_seconds")
                    if current_largest is None or gap > float(current_largest):
                        diagnostics["largest_heartbeat_gap_seconds"] = round(gap, 3)
                last_heartbeat_at = now
                latest["heartbeat"] = payload
            elif kind == "market_ticker":
                latest["ticker"] = payload
            elif kind == "market_candle_update":
                latest["candle"] = payload
            elif kind == "provider_subscription_ack":
                latest["subscription_ack"] = payload
            elif kind == "provider_disconnected":
                latest["disconnected"] = payload
            elif kind == "provider_sequence_gap":
                event_status = str((event.payload or {}).get("status") or "")
                if event_status == "out_of_order":
                    diagnostics["out_of_order_count"] = int(diagnostics["out_of_order_count"] or 0) + 1
                else:
                    diagnostics["sequence_gap_count"] = int(diagnostics["sequence_gap_count"] or 0) + 1
            elif kind == "provider_unsupported_message":
                diagnostics["unsupported_message_count"] = int(diagnostics["unsupported_message_count"] or 0) + 1
            elif kind == "provider_malformed_message":
                diagnostics["malformed_message_count"] = int(diagnostics["malformed_message_count"] or 0) + 1
        if pending_event is not None:
            pending_event.cancel()
    except Exception as exc:  # noqa: BLE001
        status = "failed"
        error = str(exc)
        logger.error(
            "provider_stream_smoke_failed | provider=%s venue=%s symbol=%s product_id=%s error=%s",
            subscription.provider,
            subscription.venue,
            subscription.symbol,
            subscription.product_id,
            exc,
        )
    finally:
        try:
            await stream.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "provider_stream_smoke_close_failed | provider=%s venue=%s symbol=%s product_id=%s error=%s",
                subscription.provider,
                subscription.venue,
                subscription.symbol,
                subscription.product_id,
                exc,
            )

    elapsed_seconds = round(time.monotonic() - started_monotonic, 3)
    if status == "completed" and diagnostics.get("stream_ended_early"):
        status = "ended_early"
    payload = {
        "schema_version": "provider_stream_smoke.v1",
        "status": status,
        "provider": subscription.provider,
        "venue": subscription.venue,
        "symbol": subscription.symbol,
        "product_id": subscription.product_id,
        "channels": list(subscription.channels),
        "timeframe": subscription.timeframe,
        "auth_mode": subscription.auth_mode,
        "duration_seconds": duration,
        "elapsed_seconds": elapsed_seconds,
        "started_at": started_at,
        "ended_at": _utc_iso(),
        "counts": counts,
        "latest": latest,
        "diagnostics": diagnostics,
        "samples": samples,
    }
    if error:
        payload["error"] = error
    logger.info(
        "provider_stream_smoke_finished | provider=%s venue=%s symbol=%s product_id=%s status=%s elapsed_seconds=%s counts=%s",
        subscription.provider,
        subscription.venue,
        subscription.symbol,
        subscription.product_id,
        status,
        elapsed_seconds,
        counts,
    )
    return payload


def summarize_events(events: Sequence[CanonicalMarketEvent]) -> dict[str, Any]:
    """Build the same compact summary shape from pre-collected events for tests."""

    counts: dict[str, int] = {}
    latest: dict[str, Any] = {}
    for event in events:
        payload = event.to_dict()
        kind = str(payload.get("event_kind") or "")
        _increment(counts, kind)
        if kind == "provider_heartbeat":
            latest["heartbeat"] = payload
        elif kind == "market_ticker":
            latest["ticker"] = payload
        elif kind == "market_candle_update":
            latest["candle"] = payload
    return {"counts": counts, "latest": latest}


def _increment(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _utc_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


__all__ = ["run_provider_stream_smoke", "summarize_events"]
