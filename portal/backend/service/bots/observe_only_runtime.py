"""Container-owned observe-only market intake for paper bot runs."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from typing import Any, Awaitable, Callable, Mapping, Sequence

from data_providers.streams import CoinbaseAdvancedTradeStream, MarketSubscription

from .execution_behavior import OBSERVE_ONLY_BEHAVIOR
from .startup_lifecycle import BotLifecyclePhase, BotLifecycleStatus, LifecycleOwner, build_failure_payload
from ..market import instrument_service

logger = logging.getLogger(__name__)

DEFAULT_OBSERVE_ONLY_CHANNELS = ("heartbeats", "ticker", "candles")

LifecycleRecorder = Callable[
    [str, str, str, str, str, Mapping[str, Any] | None, Mapping[str, Any] | None, str | None],
    Awaitable[Mapping[str, Any]] | Mapping[str, Any],
]
RunSummaryRecorder = Callable[[Mapping[str, Any]], Awaitable[None] | None]


async def run_observe_only_market_intake(
    *,
    bot_id: str,
    run_id: str,
    request_id: str | None,
    strategy: Any,
    readiness: Mapping[str, Any],
    record_lifecycle: LifecycleRecorder,
    record_run_summary: RunSummaryRecorder,
    duration_seconds: float | None = None,
    channels: Sequence[str] = DEFAULT_OBSERVE_ONLY_CHANNELS,
) -> int:
    """Subscribe to provider market data without entering execution semantics."""

    datasource = str(readiness.get("datasource") or getattr(strategy, "datasource", "") or "").strip()
    exchange = str(readiness.get("exchange") or getattr(strategy, "exchange", "") or "").strip()
    timeframe = str(readiness.get("timeframe") or getattr(strategy, "timeframe", "") or "").strip()
    subscriptions = _subscriptions_from_strategy(
        provider=datasource,
        venue=exchange,
        strategy=strategy,
        channels=tuple(channels),
        timeframe=timeframe,
    )
    counts: dict[str, int] = {}
    latest: dict[str, Any] = {}
    first_market_event = False
    started_monotonic = time.monotonic()
    deadline = time.monotonic() + float(duration_seconds) if duration_seconds and float(duration_seconds) > 0 else None
    stream = CoinbaseAdvancedTradeStream(provider="COINBASE", venue="COINBASE_DIRECT")
    pending_event: asyncio.Task[Any] | None = None

    async def _record_phase(
        phase: str,
        message: str,
        metadata: Mapping[str, Any] | None = None,
        failure: Mapping[str, Any] | None = None,
        status: str | None = None,
    ) -> None:
        result = record_lifecycle(
            bot_id,
            run_id,
            phase,
            LifecycleOwner.RUNTIME.value,
            message,
            metadata,
            failure,
            status,
        )
        if hasattr(result, "__await__"):
            await result

    async def _record_summary(status: str, *, ended_at: str | None = None, error: str | None = None) -> None:
        summary: dict[str, Any] = {
            "execution_behavior": OBSERVE_ONLY_BEHAVIOR,
            "market_event_counts": dict(counts),
            "latest_market_events": dict(latest),
            "duration_seconds": round(time.monotonic() - started_monotonic, 3),
            "orders_submitted": 0,
            "fills_recorded": 0,
            "wallet_mutations": 0,
        }
        if error:
            summary["error"] = error
        result = record_run_summary(
            {
                "run_id": run_id,
                "bot_id": bot_id,
                "status": status,
                "summary": summary,
                "ended_at": ended_at,
            }
        )
        if hasattr(result, "__await__"):
            await result

    try:
        await _record_phase(
            BotLifecyclePhase.RUNTIME_SUBSCRIBING.value,
            "Observe-only runtime subscribing to provider market data.",
            {
                "request_id": request_id,
                "execution_behavior": OBSERVE_ONLY_BEHAVIOR,
                "provider": "COINBASE",
                "venue": "COINBASE_DIRECT",
                "timeframe": timeframe,
                "symbols": [subscription.symbol for subscription in subscriptions],
                "channels": list(channels),
                "orders_enabled": False,
                "fills_enabled": False,
                "wallet_mutation_enabled": False,
            },
        )
        await stream.connect()
        _increment(counts, "provider_connected")
        await stream.subscribe(subscriptions)
        iterator = stream.events().__aiter__()
        while True:
            if deadline is not None and time.monotonic() >= deadline:
                break
            if pending_event is None:
                pending_event = asyncio.create_task(anext(iterator))
            wait_seconds = 1.0
            if deadline is not None:
                wait_seconds = max(min(deadline - time.monotonic(), 1.0), 0.05)
            done, _pending = await asyncio.wait({pending_event}, timeout=wait_seconds)
            if not done:
                continue
            try:
                event = pending_event.result()
            except StopAsyncIteration as exc:
                raise RuntimeError("provider stream ended before observe-only run stopped") from exc
            finally:
                pending_event = None
            payload = event.to_dict()
            kind = str(payload.get("event_kind") or "").strip()
            _increment(counts, kind)
            if kind == "provider_heartbeat":
                latest["heartbeat"] = payload
            elif kind == "provider_subscription_ack":
                latest["subscription_ack"] = payload
            elif kind == "market_ticker":
                latest["ticker"] = payload
            elif kind == "market_candle_update":
                latest["candle"] = payload
            elif kind == "provider_sequence_gap":
                latest["sequence_gap"] = payload
            if not first_market_event and kind in {"market_ticker", "market_candle_update"}:
                first_market_event = True
                await _record_phase(
                    BotLifecyclePhase.LIVE.value,
                    "Observe-only runtime received first live market data.",
                    {
                        "request_id": request_id,
                        "execution_behavior": OBSERVE_ONLY_BEHAVIOR,
                        "first_event_kind": kind,
                        "symbol": payload.get("symbol"),
                        "product_id": payload.get("product_id"),
                        "orders_enabled": False,
                        "fills_enabled": False,
                        "wallet_mutation_enabled": False,
                    },
                    status=BotLifecycleStatus.RUNNING.value,
                )
            if sum(counts.values()) % 100 == 0:
                await _record_summary(BotLifecycleStatus.RUNNING.value)
        ended_at = _utc_now_iso()
        await _record_summary(BotLifecycleStatus.COMPLETED.value, ended_at=ended_at)
        await _record_phase(
            BotLifecyclePhase.COMPLETED.value,
            "Observe-only runtime completed configured duration.",
            {
                "request_id": request_id,
                "execution_behavior": OBSERVE_ONLY_BEHAVIOR,
                "market_event_counts": dict(counts),
                "terminal_actor": "observe_only_runtime",
                "terminal_reason_text": "Observe-only runtime completed configured duration.",
                "orders_submitted": 0,
                "fills_recorded": 0,
                "wallet_mutations": 0,
            },
            status=BotLifecycleStatus.COMPLETED.value,
        )
        return 0
    except Exception as exc:  # noqa: BLE001
        ended_at = _utc_now_iso()
        message = str(exc)
        await _record_summary(BotLifecycleStatus.FAILED.value, ended_at=ended_at, error=message)
        await _record_phase(
            BotLifecyclePhase.FAILED.value,
            message,
            {
                "request_id": request_id,
                "execution_behavior": OBSERVE_ONLY_BEHAVIOR,
                "market_event_counts": dict(counts),
                "terminal_actor": "observe_only_runtime",
                "terminal_reason_text": message,
            },
            build_failure_payload(
                phase=BotLifecyclePhase.FAILED.value,
                message=message,
                error_type=type(exc).__name__,
                type="observe_only_exception",
                reason_code="observe_only_runtime_failed",
                owner=LifecycleOwner.RUNTIME.value,
                exception_type=type(exc).__name__,
            ),
            status=BotLifecycleStatus.FAILED.value,
        )
        logger.exception(
            "observe_only_runtime_failed | bot_id=%s | run_id=%s | error=%s",
            bot_id,
            run_id,
            exc,
        )
        return 1
    finally:
        if pending_event is not None:
            pending_event.cancel()
        try:
            await stream.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "observe_only_stream_close_failed | bot_id=%s | run_id=%s | error=%s",
                bot_id,
                run_id,
                exc,
            )


def _subscriptions_from_strategy(
    *,
    provider: str,
    venue: str,
    strategy: Any,
    channels: tuple[str, ...],
    timeframe: str,
) -> list[MarketSubscription]:
    provider_id = str(provider or "").strip().upper()
    venue_id = str(venue or "").strip().upper()
    if provider_id != "COINBASE" or venue_id != "COINBASE_DIRECT":
        raise ValueError(
            "observe-only paper v1 supports only COINBASE/COINBASE_DIRECT live streams "
            f"(got provider={provider or None} venue={venue or None})"
        )
    subscriptions: list[MarketSubscription] = []
    for link in list(getattr(strategy, "instrument_links", []) or []):
        snapshot = dict(getattr(link, "instrument_snapshot", {}) or {})
        symbol = str(snapshot.get("symbol") or getattr(link, "symbol", "") or "").strip()
        if not symbol:
            raise ValueError("observe-only strategy instrument link is missing symbol")
        instrument = {}
        instrument_id = str(getattr(link, "instrument_id", "") or "").strip()
        if instrument_id:
            try:
                instrument = instrument_service.get_instrument_record(instrument_id)
            except KeyError:
                instrument = {}
        instrument = dict(instrument or snapshot)
        product_id = _product_id_from_instrument(instrument) or symbol
        subscriptions.append(
            MarketSubscription.from_values(
                provider="COINBASE",
                venue="COINBASE_DIRECT",
                symbol=symbol,
                product_id=product_id,
                channels=channels,
                timeframe=timeframe or None,
                auth_mode="public",
            )
        )
    if not subscriptions:
        raise ValueError("observe-only paper run requires at least one strategy instrument")
    return subscriptions


def _product_id_from_instrument(instrument: Mapping[str, Any]) -> str | None:
    metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
    provider_metadata = metadata.get("provider_metadata") if isinstance(metadata.get("provider_metadata"), Mapping) else metadata
    for key in ("product_id", "coinbase_product_id", "venue_symbol", "provider_symbol"):
        value = provider_metadata.get(key) if isinstance(provider_metadata, Mapping) else None
        text = str(value or "").strip()
        if text:
            return text
    return None


def _increment(counts: dict[str, int], key: str) -> None:
    counts[key] = int(counts.get(key, 0)) + 1


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(tzinfo=None).isoformat() + "Z"


__all__ = [
    "DEFAULT_OBSERVE_ONLY_CHANNELS",
    "run_observe_only_market_intake",
]
