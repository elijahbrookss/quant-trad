"""Credential-free production-path runtime correctness harness."""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

import pandas as pd

from engines.bot_runtime.adapters import PaperAdapter
from engines.bot_runtime.core.runtime_events import (
    RuntimeEventName,
    WalletInitializedContext,
    build_correlation_id,
    new_runtime_event,
)
from engines.bot_runtime.deps import BotRuntimeDeps
from engines.bot_runtime.runtime.runtime import BotRuntime
from engines.bot_runtime.strategy.models import (
    Strategy,
    StrategyIndicatorLink,
    StrategyInstrumentLink,
)
from indicators.candle_stats.definition import CandleStatsIndicator
from indicators.candle_stats.runtime import TypedCandleStatsIndicator
from portal.backend.service.reports import artifacts


BOT_ID = "persisted-reference-bot"
RUN_ID = "persisted-reference-run"
STRATEGY_ID = "persisted-reference-strategy"
INDICATOR_ID = "persisted-reference-candle-stats"
INSTRUMENT_ID = "persisted-reference-instrument"
SYMBOL = "REFERENCE-PERP"
TIMEFRAME = "1m"
STARTING_CASH = 1_000.0
EVALUATION_START = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _Counter:
    def __init__(self, value: int = 0) -> None:
        self._value = int(value)

    def get(self) -> int:
        return self._value

    def set(self, value: int) -> None:
        self._value = int(value)


class HarnessStorage:
    """Test-owned persistence gateway implementing production storage ports."""

    def __init__(self) -> None:
        self.trades: dict[str, dict[str, Any]] = {}
        self.trade_events: list[dict[str, Any]] = []
        self.step_rollups: list[dict[str, Any]] = []
        self.canonical_fact_batches: list[dict[str, Any]] = []
        self.run_upserts: list[dict[str, Any]] = []

    def get_indicator(self, indicator_id: str) -> dict[str, Any]:
        if indicator_id != INDICATOR_ID:
            raise KeyError(indicator_id)
        return _indicator_meta()

    def record_trade(self, payload: Mapping[str, Any]) -> None:
        trade_id = str(payload.get("trade_id") or "").strip()
        if not trade_id:
            raise ValueError("persisted harness trade_id is required")
        current = self.trades.setdefault(trade_id, {})
        current.update(dict(payload))

    def record_trade_event(self, payload: Mapping[str, Any]) -> None:
        self.trade_events.append(dict(payload))

    def record_step_rollups(self, payloads: list[dict[str, Any]]) -> int:
        self.step_rollups.extend(dict(payload) for payload in payloads)
        return len(payloads)

    def append_canonical_fact_batch(self, **payload: Any) -> dict[str, Any]:
        self.canonical_fact_batches.append(dict(payload))
        return {"inserted_rows": 1, "row_count": 1}

    def list_bot_trades_for_run(self, run_id: str) -> list[dict[str, Any]]:
        return [
            dict(payload)
            for payload in self.trades.values()
            if str(payload.get("run_id") or "") == str(run_id)
        ]

    def list_bot_trade_events_for_trades(
        self,
        trade_ids: list[str],
    ) -> list[dict[str, Any]]:
        wanted = {str(trade_id) for trade_id in trade_ids}
        return [
            dict(payload)
            for payload in self.trade_events
            if str(payload.get("trade_id") or "") in wanted
        ]

    def upsert_bot_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        copied = dict(payload)
        self.run_upserts.append(copied)
        return copied


@dataclass(frozen=True)
class PersistedRuntimeResult:
    run_dir: Path
    artifact: dict[str, Any]
    manifest: dict[str, Any]
    summary: dict[str, Any]
    series_snapshot: dict[str, Any]
    trades: tuple[dict[str, Any], ...]
    step_rollups: tuple[dict[str, Any], ...]
    run_upsert: dict[str, Any]


def reference_frame(*, include_future_suffix: bool) -> pd.DataFrame:
    warmup_times = [
        EVALUATION_START - timedelta(minutes=offset)
        for offset in range(100, 0, -1)
    ]
    warmup_rows = [
        {
            "open": 100.0,
            "high": 100.5,
            "low": 99.5,
            "close": 100.0,
            "volume": 1_000.0,
            "atr": 2.0,
        }
        for _ in warmup_times
    ]
    price_ranges = [
        (100.0, 0.5),
        (100.5, 0.5),
        (101.0, 5.0),
        (111.0, 5.0),
        (111.5, 0.5),
        (112.0, 0.5),
        (1_000.0, 500.0),
    ]
    if not include_future_suffix:
        price_ranges = price_ranges[:4]
    evaluation_times = [
        EVALUATION_START + timedelta(minutes=index)
        for index in range(len(price_ranges))
    ]
    evaluation_rows = [
        {
            "open": close,
            "high": close + half_range,
            "low": close - half_range,
            "close": close,
            "volume": 1_000.0,
            "atr": 2.0,
        }
        for close, half_range in price_ranges
    ]
    frame = pd.DataFrame(
        [*warmup_rows, *evaluation_rows],
        index=pd.DatetimeIndex([*warmup_times, *evaluation_times]),
    )
    frame.attrs["gap_classification"] = []
    return frame


def run_persisted_reference(
    *,
    monkeypatch: Any,
    root: Path,
    include_future_suffix: bool,
    adapter_kind: str = "backtest",
) -> PersistedRuntimeResult:
    if adapter_kind not in {"backtest", "paper"}:
        raise ValueError(f"unsupported adapter_kind={adapter_kind!r}")
    storage = HarnessStorage()
    _configure_artifacts(monkeypatch, root, storage)
    frame = reference_frame(include_future_suffix=include_future_suffix)
    strategy = _strategy()
    deps = _runtime_deps(frame=frame, strategy=strategy, storage=storage)
    evaluation_bars = 7 if include_future_suffix else 4
    evaluation_end = EVALUATION_START + timedelta(minutes=evaluation_bars)
    runtime = BotRuntime(
        BOT_ID,
        {
            "run_id": RUN_ID,
            "request_id": "persisted-reference-request",
            "name": "Persisted reference bot",
            "mode": "instant",
            "execution_mode": "fast",
            "run_type": "backtest",
            "series_runner": "inline",
            "strategy_ids": [STRATEGY_ID],
            "backtest_start": _iso(EVALUATION_START),
            # Canonical evaluation windows are half-open. Express N one-minute
            # bars as [start, start + N minutes) so the final expected bar is
            # decision-producing rather than accidentally excluded.
            "backtest_end": _iso(evaluation_end),
            "backtest_warmup_bars": 100,
            "wallet_config": {"balances": {"USD": STARTING_CASH}},
            "shared_wallet_proxy": _shared_wallet_proxy(),
            "canonical_fact_async_enabled": False,
            "step_trace_flush_interval_ms": 10,
        },
        deps=deps,
    )
    runtime.warm_up()
    if adapter_kind == "paper":
        for series in runtime._series:
            profile = series.execution_profile
            if profile is None or series.risk_engine is None:
                raise RuntimeError("persisted harness execution profile is required")
            constraints = profile.constraints
            series.risk_engine.attach_execution_adapter(
                PaperAdapter(
                    tick_size=constraints.tick_size,
                    qty_step=constraints.qty_step,
                    min_qty=constraints.min_order_size,
                    min_notional=constraints.min_notional,
                    contract_size=constraints.contract_size,
                    short_requires_borrow=profile.capabilities.short_requires_borrow,
                    max_qty=constraints.max_qty,
                    amount_precision=constraints.amount_precision,
                    slippage_bps=0.0,
                )
            )
    runtime.start()
    if runtime.state.get("status") != "completed":
        raise RuntimeError(f"persisted harness runtime failed: {runtime.state}")

    run_dir = root / f"bot_id={BOT_ID}" / f"run_id={RUN_ID}"
    artifact = _read_json(run_dir / "run" / "runtime_artifact.json")
    manifest = _read_json(run_dir / "manifest.json")
    summary = _read_json(run_dir / "summary" / "summary.json")
    series_snapshot = _read_json(run_dir / "run" / "series.json")
    if not storage.run_upserts:
        raise RuntimeError("persisted harness report run was not upserted")
    return PersistedRuntimeResult(
        run_dir=run_dir,
        artifact=artifact,
        manifest=manifest,
        summary=summary,
        series_snapshot=series_snapshot,
        trades=tuple(
            sorted(
                (dict(payload) for payload in storage.trades.values()),
                key=lambda payload: str(payload.get("entry_time") or ""),
            )
        ),
        step_rollups=tuple(storage.step_rollups),
        run_upsert=dict(storage.run_upserts[-1]),
    )


def semantic_trace(result: PersistedRuntimeResult) -> dict[str, Any]:
    artifact = result.artifact
    summary = dict(result.summary.get("summary") or {})
    summary.pop("performance", None)
    return _canonicalize(
        {
            "runtime_metadata": artifact.get("runtime_metadata"),
            "runtime_events": artifact.get("runtime_event_stream"),
            "decision_artifacts": artifact.get("decision_artifacts"),
            "rejection_artifacts": artifact.get("rejection_artifacts"),
            "wallet_state": artifact.get("wallet_state"),
            "trades": list(result.trades),
            "summary": summary,
            "series": result.series_snapshot.get("series"),
            "manifest_files": [
                {
                    "path": entry.get("path"),
                    "rows": entry.get("rows"),
                    "source": entry.get("source"),
                    "kind": entry.get("kind"),
                }
                for entry in result.manifest.get("files") or []
            ],
        }
    )


def semantic_trace_through(
    result: PersistedRuntimeResult,
    *,
    known_at: datetime,
) -> dict[str, Any]:
    boundary = _iso(known_at)
    events = []
    for event in result.artifact.get("runtime_event_stream") or []:
        context = event.get("context") if isinstance(event, Mapping) else {}
        event_known_at = (
            context.get("bar_ts")
            or context.get("bar_time")
            or event.get("event_ts")
        )
        if str(event.get("event_name") or "") == RuntimeEventName.WALLET_INITIALIZED.value:
            events.append(event)
        elif event_known_at and str(event_known_at) <= boundary:
            events.append(event)
    decisions = [
        artifact
        for artifact in result.artifact.get("decision_artifacts") or []
        if int(artifact.get("bar_epoch") or 0) <= int(known_at.timestamp())
    ]
    trades = [
        trade
        for trade in result.trades
        if str(trade.get("exit_time") or "") <= boundary
    ]
    return _canonicalize(
        {
            "known_at": boundary,
            "runtime_events": events,
            "decision_artifacts": decisions,
            "trades": trades,
        }
    )


def semantic_fingerprint(result: PersistedRuntimeResult) -> str:
    material = json.dumps(
        semantic_trace(result),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _configure_artifacts(
    monkeypatch: Any,
    root: Path,
    storage: HarnessStorage,
) -> None:
    settings = SimpleNamespace(
        enabled=True,
        capture_backtest=True,
        capture_live=True,
        root_dir=str(root),
        output_format="csv",
        include_candles=True,
        include_runtime_events=True,
        include_indicator_outputs=True,
        include_overlays=True,
        include_decision_trace=True,
        include_trades=True,
        include_trade_events=True,
        compress_zip_on_finalize=False,
    )
    monkeypatch.setattr(artifacts, "_ARTIFACT_SETTINGS", settings)
    monkeypatch.setattr(
        artifacts,
        "get_settings",
        lambda: SimpleNamespace(reports=SimpleNamespace(artifacts=settings)),
    )
    monkeypatch.setattr(artifacts, "_storage", lambda: storage)


def _runtime_deps(
    *,
    frame: pd.DataFrame,
    strategy: Strategy,
    storage: HarnessStorage,
) -> BotRuntimeDeps:
    def fetch_frame(
        _instrument_id: str,
        start: str,
        end: str,
        _timeframe: str,
    ) -> pd.DataFrame:
        start_ts = pd.to_datetime(start, utc=True)
        end_ts = pd.to_datetime(end, utc=True)
        selected = frame.loc[
            (frame.index >= start_ts) & (frame.index <= end_ts)
        ].copy()
        selected.attrs.update(frame.attrs)
        return selected

    return BotRuntimeDeps(
        fetch_strategy=lambda _strategy_id, _config=None: strategy,
        fetch_ohlcv=lambda *_args, **_kwargs: frame.copy(),
        fetch_ohlcv_by_instrument=fetch_frame,
        get_instrument_record=lambda _instrument_id: _instrument(),
        resolve_instrument=lambda _datasource, _exchange, _symbol: _instrument(),
        strategy_evaluate=lambda *_args, **_kwargs: {},
        strategy_run_preview=lambda *_args, **_kwargs: {},
        indicator_get_instance_meta=lambda *_args, **_kwargs: _indicator_meta(),
        indicator_build_runtime_graph=lambda *_args, **_kwargs: (
            {INDICATOR_ID: _indicator_meta()},
            [_indicator()],
        ),
        indicator_build_runtime_instance=lambda *_args, **_kwargs: _indicator(),
        indicator_collect_runtime_diagnostics=lambda _indicators: [],
        indicator_runtime_input_plan_for_instance=lambda *_args, **_kwargs: {},
        build_indicator_context=lambda bot_id, _overlay_cache: SimpleNamespace(
            cache_owner="persisted-correctness-harness",
            cache_scope_id=bot_id,
        ),
        record_bot_runtime_event=lambda _payload: None,
        record_bot_runtime_events_batch=lambda payloads: len(payloads),
        append_botlens_canonical_fact_batch=storage.append_canonical_fact_batch,
        record_bot_trade=storage.record_trade,
        record_bot_trade_event=storage.record_trade_event,
        record_bot_run_steps_batch=storage.record_step_rollups,
        build_run_artifact_bundle=artifacts.build_run_artifact_bundle,
    )


def _strategy() -> Strategy:
    return Strategy(
        id=STRATEGY_ID,
        name="Persisted known-at reference",
        timeframe=TIMEFRAME,
        datasource="fixture",
        exchange="fixture",
        atm_template_id="persisted-reference-atm",
        atm_template={
            "name": "Persisted reference ATM",
            "execution_mode": "market",
            "initial_stop": {"atr_multiplier": 2.0},
            "take_profit_orders": [
                {"id": "tp-1", "ticks": 10, "size_fraction": 1.0}
            ],
            "breakeven": {"enabled": False},
            "trailing": {"enabled": False},
            "stop_adjustments": [],
        },
        risk_config={"base_risk_per_trade": 4.0},
        indicator_links=[
            StrategyIndicatorLink(
                id="persisted-reference-indicator-link",
                strategy_id=STRATEGY_ID,
                indicator_id=INDICATOR_ID,
            )
        ],
        instrument_links=[
            StrategyInstrumentLink(
                id="persisted-reference-instrument-link",
                strategy_id=STRATEGY_ID,
                instrument_id=INSTRUMENT_ID,
                instrument_snapshot=_instrument(),
            )
        ],
        rules={
            "atr-expansion-entry": {
                "id": "atr-expansion-entry",
                "name": "Enter on known ATR expansion",
                "intent": "enter_long",
                "priority": 10,
                "trigger": {
                    "type": "signal_match",
                    "indicator_id": INDICATOR_ID,
                    "output_name": "atr_expansion",
                    "event_key": "atr_expansion_long",
                },
                "guards": [],
            }
        },
    )


def _instrument() -> dict[str, Any]:
    return {
        "id": INSTRUMENT_ID,
        "symbol": SYMBOL,
        "datasource": "fixture",
        "exchange": "fixture",
        "venue": "fixture",
        "instrument_type": "future",
        "tick_size": 1.0,
        "contract_size": 1.0,
        "tick_value": 1.0,
        "min_order_size": 1.0,
        "qty_step": 1.0,
        "min_notional": 0.0,
        "base_currency": "REFERENCE",
        "quote_currency": "USD",
        "can_short": True,
        "short_requires_borrow": False,
        "maker_fee_rate": 0.001,
        "taker_fee_rate": 0.002,
        "margin_rates": {
            "intraday": {
                "long_margin_rate": 0.1,
                "short_margin_rate": 0.1,
            },
            "overnight": {
                "long_margin_rate": 0.2,
                "short_margin_rate": 0.2,
            },
        },
        "metadata": {"info": {"base_increment": "1"}},
    }


def _indicator_meta() -> dict[str, Any]:
    return {
        "id": INDICATOR_ID,
        "type": "candle_stats",
        "version": "v1",
        "name": "Persisted reference candle stats",
        "enabled": True,
        "params": {
            "atr_short_window": 1,
            "atr_long_window": 3,
            "atr_z_window": 3,
            "directional_efficiency_window": 1,
            "slope_window": 1,
            "range_window": 1,
            "expansion_window": 1,
            "volume_window": 1,
            "overlap_window": 1,
            "slope_stability_lookback": 1,
            "warmup_bars": 3,
            "atr_expansion_signal_threshold": 0.5,
        },
        "typed_outputs": [
            {
                "name": "atr_expansion",
                "type": "signal",
                "event_keys": ["atr_expansion_long"],
            }
        ],
    }


def _indicator() -> TypedCandleStatsIndicator:
    params = CandleStatsIndicator.resolve_config(
        _indicator_meta()["params"],
        strict_unknown=True,
    )
    return TypedCandleStatsIndicator(
        indicator_id=INDICATOR_ID,
        version="v1",
        params=params,
    )


def _shared_wallet_proxy() -> dict[str, Any]:
    initialized = new_runtime_event(
        event_name=RuntimeEventName.WALLET_INITIALIZED,
        correlation_id=build_correlation_id(
            run_id=RUN_ID,
            symbol=None,
            timeframe=None,
            bar_ts=None,
        ),
        context=WalletInitializedContext(
            run_id=RUN_ID,
            bot_id=BOT_ID,
            strategy_id="__runtime__",
            symbol=None,
            timeframe=None,
            bar_ts=None,
            balances={"USD": STARTING_CASH},
            source="persisted_reference_fixture",
            wallet_commit_seq=0,
            wallet_commit_seq_status="runtime_assigned",
            wallet_eval_seq=0,
        ),
    ).serialize()
    initialized["seq"] = 0
    return {
        "runtime_events": [dict(initialized)],
        "wallet_events": [dict(initialized)],
        "runtime_event_seq": _Counter(0),
        "wallet_event_seq": _Counter(0),
        "reservations": {},
        "lock": threading.RLock(),
    }


def _canonicalize(value: Any) -> Any:
    id_maps: dict[str, dict[str, str]] = {}
    instance_id_keys = {
        "event_id",
        "root_id",
        "parent_id",
        "correlation_id",
        "trade_id",
        "order_intent_id",
        "entry_request_id",
        "attempt_id",
        "settlement_attempt_id",
        "reservation_id",
        "wallet_correlation_id",
    }
    timing_keys = {
        "started_at",
        "ended_at",
        "runtime_started_at",
        "runtime_ended_at",
        "runtime_loop_started_at",
        "runtime_loop_ended_at",
        "user_wall_clock_seconds",
        "db_run_started_ended_seconds",
        "runtime_loop_duration_seconds",
        "async_projection_flush_drain_seconds",
        "event_ts",
    }

    def visit(item: Any, key: str | None = None) -> Any:
        if isinstance(item, Mapping):
            if key == "margin_positions":
                positions = [visit(child) for child in item.values()]
                return sorted(
                    positions,
                    key=lambda child: json.dumps(
                        child,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                )
            return {
                str(child_key): visit(child_value, str(child_key))
                for child_key, child_value in sorted(
                    item.items(),
                    key=lambda entry: str(entry[0]),
                )
                if str(child_key) not in timing_keys
            }
        if isinstance(item, (list, tuple)):
            return [visit(child) for child in item]
        if isinstance(item, datetime):
            return _iso(item)
        if key in instance_id_keys and item not in (None, ""):
            raw = str(item)
            mapping = id_maps.setdefault(str(key), {})
            if raw not in mapping:
                mapping[raw] = f"{key}-{len(mapping) + 1}"
            return mapping[raw]
        return item

    return visit(value)


def _read_json(path: Path) -> dict[str, Any]:
    return dict(json.loads(path.read_text(encoding="utf-8")))


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
