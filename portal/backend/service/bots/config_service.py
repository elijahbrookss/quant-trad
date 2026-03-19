"""Bot configuration service: validation + persistence only."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from core.settings import env_is_set, env_value, get_settings
from engines.bot_runtime.core.execution_profile import compile_runtime_profile_or_error

from .strategy_loader import StrategyLoader
from ..market import instrument_service
from ..storage.storage import delete_bot, load_bots, load_strategies, upsert_bot

MIN_STARTING_WALLET = 10.0
_DERIVATIVE_TYPES = {"perp", "perps", "swap", "future", "futures", "derivative", "derivatives"}
_RUNTIME_ALLOWED_DERIVATIVE_TYPES = {"future", "futures", "perp", "perps"}
_SETTINGS = get_settings()


class BotConfigService:
    def list_bots(self) -> List[Dict[str, object]]:
        bots = load_bots()
        for bot in bots:
            bot["instrument_type"] = self.instrument_policy_from_bot(bot)
        return bots

    def get_bot(self, bot_id: str) -> Dict[str, object]:
        for bot in load_bots():
            if bot["id"] == bot_id:
                bot["instrument_type"] = self.instrument_policy_from_bot(bot)
                return bot
        raise KeyError(f"Bot {bot_id} was not found")

    def create_bot(self, name: str, **payload: object) -> Dict[str, object]:
        bot_id = payload.get("id") or str(uuid.uuid4())
        strategy_id = self.validate_strategy_id(payload.get("strategy_id"))
        run_type = str(payload.get("run_type") or "backtest").lower()
        wallet_config = self.validate_wallet_config(payload.get("wallet_config"))

        record: Dict[str, object] = {
            "id": bot_id,
            "name": name,
            "strategy_id": strategy_id,
            "timeframe": None,
            "mode": (payload.get("mode") or "instant").lower(),
            "run_type": run_type,
            "playback_speed": self.coerce_playback_speed(payload.get("playback_speed") or payload.get("fetch_seconds")),
            "backtest_start": self.coerce_isoformat(payload.get("backtest_start")),
            "backtest_end": self.coerce_isoformat(payload.get("backtest_end")),
            "risk": dict(payload.get("risk") or {}),
            "wallet_config": wallet_config,
            "snapshot_interval_ms": int(payload.get("snapshot_interval_ms") or 0),
            "bot_env": self.validate_bot_env(payload.get("bot_env") if isinstance(payload.get("bot_env"), Mapping) else {}),
            "status": "idle",
            "last_stats": {},
        }
        if int(record.get("snapshot_interval_ms") or 0) <= 0:
            raise ValueError("snapshot_interval_ms is required and must be > 0")
        self.apply_instrument_policy(record, payload.get("instrument_type"))
        self.validate_backtest_window(record)
        upsert_bot(record)
        return record

    def update_bot(self, bot_id: str, **payload: object) -> Dict[str, object]:
        bots = {bot["id"]: bot for bot in load_bots()}
        if bot_id not in bots:
            raise KeyError(f"Bot {bot_id} was not found")
        record = bots[bot_id]

        if "strategy_id" in payload and payload["strategy_id"] is not None:
            record["strategy_id"] = self.validate_strategy_id(payload.get("strategy_id"))
        if "name" in payload and payload["name"] is not None:
            record["name"] = payload["name"]
        if "instrument_type" in payload:
            self.apply_instrument_policy(record, payload.get("instrument_type"))
        if "run_type" in payload and payload["run_type"] is not None:
            record["run_type"] = str(payload["run_type"]).lower()
        if "mode" in payload and payload["mode"] is not None:
            record["mode"] = str(payload["mode"]).lower()
        if "playback_speed" in payload and payload["playback_speed"] is not None:
            record["playback_speed"] = self.coerce_playback_speed(payload["playback_speed"])
        elif "fetch_seconds" in payload and payload["fetch_seconds"] is not None:
            record["playback_speed"] = self.coerce_playback_speed(payload["fetch_seconds"])
        if "focus_symbol" in payload:
            record["focus_symbol"] = payload.get("focus_symbol") or None
        if "backtest_start" in payload:
            record["backtest_start"] = self.coerce_isoformat(payload.get("backtest_start"))
        if "backtest_end" in payload:
            record["backtest_end"] = self.coerce_isoformat(payload.get("backtest_end"))
        if "wallet_config" in payload and payload["wallet_config"] is not None:
            record["wallet_config"] = self.validate_wallet_config(payload.get("wallet_config"))
        if "snapshot_interval_ms" in payload and payload["snapshot_interval_ms"] is not None:
            interval = int(payload["snapshot_interval_ms"])
            if interval <= 0:
                raise ValueError("snapshot_interval_ms is required and must be > 0")
            record["snapshot_interval_ms"] = interval
        if "bot_env" in payload:
            next_env = self.validate_bot_env(payload.get("bot_env") if isinstance(payload.get("bot_env"), Mapping) else {})
            current_env = dict(record.get("bot_env") or {})
            if str(record.get("status") or "").lower() == "running" and next_env != current_env:
                raise ValueError("Bot env settings changed. Stop and restart the bot to apply new env vars.")
            record["bot_env"] = next_env
        self.validate_backtest_window(record)
        upsert_bot(record)
        return record

    def delete_bot_record(self, bot_id: str) -> None:
        delete_bot(bot_id)

    @staticmethod
    def coerce_playback_speed(value: Optional[object]) -> float:
        _ = value
        return 0.0

    @staticmethod
    def coerce_isoformat(value: Optional[object]) -> Optional[str]:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value.replace(tzinfo=None).isoformat() + "Z"
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            return text
        try:
            parsed = datetime.fromisoformat(text)
            return parsed.replace(tzinfo=None).isoformat() + "Z"
        except ValueError:
            return text

    @staticmethod
    def normalize_instrument_policy(value: Optional[object]) -> Optional[str]:
        if value in (None, ""):
            return None
        text = str(value).strip().lower()
        if not text:
            return None
        if text == "spot":
            return "spot"
        if text in _DERIVATIVE_TYPES:
            return "derivatives"
        raise ValueError(f"Unsupported instrument_type '{value}'")

    def instrument_policy_from_bot(self, bot: Mapping[str, object]) -> Optional[str]:
        direct = bot.get("instrument_type")
        if direct:
            return self.normalize_instrument_policy(direct)
        risk = bot.get("risk")
        if isinstance(risk, Mapping):
            return self.normalize_instrument_policy(risk.get("instrument_type"))
        return None

    def apply_instrument_policy(self, record: Dict[str, object], value: Optional[object]) -> None:
        policy = self.normalize_instrument_policy(value)
        if policy is None:
            return
        record["instrument_type"] = policy
        risk = dict(record.get("risk") or {})
        risk["instrument_type"] = policy
        record["risk"] = risk

    @staticmethod
    def validate_backtest_window(record: Mapping[str, object]) -> None:
        if str(record.get("run_type") or "backtest").lower() != "backtest":
            return
        if not record.get("backtest_start") or not record.get("backtest_end"):
            raise ValueError("Backtests require both start and end timestamps.")

    @staticmethod
    def validate_wallet_config(wallet_config: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        if not isinstance(wallet_config, Mapping):
            raise ValueError("wallet_config is required and must be an object")
        balances = wallet_config.get("balances")
        if not isinstance(balances, Mapping) or not balances:
            raise ValueError("wallet_config.balances is required and cannot be empty")
        normalized: Dict[str, float] = {}
        total = 0.0
        for currency, amount in balances.items():
            code = str(currency).strip().upper()
            if not code:
                raise ValueError("wallet_config.balances contains an empty currency key")
            try:
                numeric = float(amount)
            except (TypeError, ValueError):
                raise ValueError(f"wallet_config.balances[{code}] must be numeric")
            if numeric < 0:
                raise ValueError(f"wallet_config.balances[{code}] must be non-negative")
            normalized[code] = numeric
            total += numeric
        if total < MIN_STARTING_WALLET:
            raise ValueError(f"wallet_config balances must sum to at least {MIN_STARTING_WALLET}")
        return {"balances": normalized}

    @staticmethod
    def validate_bot_env(value: Optional[Mapping[str, Any]]) -> Dict[str, str]:
        if value in (None, ""):
            return {}
        if not isinstance(value, Mapping):
            raise ValueError("bot_env must be an object map")
        normalized: Dict[str, str] = {}
        for raw_key, raw_value in value.items():
            key = str(raw_key or "").strip().upper()
            if not key:
                continue
            if not key.replace("_", "").isalnum() or not (key[0].isalpha() or key[0] == "_"):
                raise ValueError(f"Invalid env var key: {raw_key}")
            normalized[key] = "" if raw_value is None else str(raw_value)
        return normalized

    @staticmethod
    def mask_env_value(key: str, value: Optional[str]) -> str:
        k = str(key or "").upper()
        secret_hint = any(token in k for token in ("SECRET", "TOKEN", "KEY", "PASSWORD", "PASS", "DSN"))
        if secret_hint:
            return "***"
        return "" if value is None else str(value)

    def settings_catalog(self) -> Dict[str, Any]:
        exposed = [
            "QT_BOT_RUNTIME_IMAGE",
            "QT_BOT_RUNTIME_NETWORK",
            "QT_BOT_RUNTIME_TELEMETRY_WS_URL",
            "QT_BOT_RUNTIME_TELEMETRY_EVENT_POLL_MS",
            "QT_BOT_RUNTIME_MAX_SYMBOLS_PER_STRATEGY",
            "QT_BOT_RUNTIME_SYMBOL_PROCESS_MAX",
            "QT_BOT_RUNTIME_STATUS_HEARTBEAT_STALE_MS",
            "QT_BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY",
            "QT_BOT_RUNTIME_BOTLENS_MAX_SERIES",
            "QT_BOT_RUNTIME_BOTLENS_MAX_CANDLES",
            "QT_BOT_RUNTIME_BOTLENS_MAX_OVERLAYS",
            "QT_BOT_RUNTIME_BOTLENS_MAX_OVERLAY_POINTS",
            "QT_BOT_RUNTIME_BOTLENS_MAX_CLOSED_TRADES",
            "QT_BOT_RUNTIME_BOTLENS_MAX_LOGS",
            "QT_BOT_RUNTIME_BOTLENS_MAX_DECISIONS",
            "QT_BOT_RUNTIME_BOTLENS_MAX_WARNINGS",
            "QT_BOT_RUNTIME_STEP_TRACE_QUEUE_MAX",
            "QT_BOT_RUNTIME_STEP_TRACE_BATCH_SIZE",
            "QT_BOT_RUNTIME_STEP_TRACE_FLUSH_INTERVAL_MS",
            "QT_BOT_RUNTIME_STEP_TRACE_OVERFLOW_POLICY",
            "QT_BOT_RUNTIME_WATCHDOG_HEARTBEAT_INTERVAL_SECONDS",
            "QT_BOT_RUNTIME_WATCHDOG_STALE_THRESHOLD_SECONDS",
            "QT_BOT_RUNTIME_WATCHDOG_MONITOR_INTERVAL_SECONDS",
            "PG_DSN",
            "QT_SECURITY_PROVIDER_CREDENTIAL_KEY",
        ]
        env_rows: List[Dict[str, Any]] = []
        for key in exposed:
            current = env_value(key)
            masked = self.mask_env_value(key, current)
            env_rows.append(
                {
                    "key": key,
                    "value": masked,
                    "is_secret": masked == "***",
                    "is_set": env_is_set(key),
                }
            )
        return {
            "bot_defaults": {
                "snapshot_interval_ms": _SETTINGS.bot_runtime.snapshot.default_interval_ms,
                "env_templates": [
                    {"key": "SNAPSHOT_INTERVAL_MS", "default": _SETTINGS.bot_runtime.snapshot.default_interval_ms},
                    {
                        "key": "SNAPSHOT_FAST_INTERVAL_MS",
                        "default": _SETTINGS.bot_runtime.snapshot.fast_interval_ms,
                    },
                    {
                        "key": "SNAPSHOT_IDLE_INTERVAL_MS",
                        "default": _SETTINGS.bot_runtime.snapshot.idle_interval_ms,
                    },
                    {
                        "key": "SNAPSHOT_IDLE_CYCLES",
                        "default": _SETTINGS.bot_runtime.snapshot.idle_cycles,
                    },
                    {
                        "key": "BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY",
                        "default": _SETTINGS.bot_runtime.push.payload_bytes_sample_every,
                    },
                    {"key": "BOTLENS_STREAM_MAX_SERIES", "default": _SETTINGS.bot_runtime.botlens.max_series},
                    {"key": "BOTLENS_STREAM_MAX_CANDLES", "default": _SETTINGS.bot_runtime.botlens.max_candles},
                    {"key": "BOTLENS_STREAM_MAX_OVERLAYS", "default": _SETTINGS.bot_runtime.botlens.max_overlays},
                    {
                        "key": "BOTLENS_STREAM_MAX_OVERLAY_POINTS",
                        "default": _SETTINGS.bot_runtime.botlens.max_overlay_points,
                    },
                    {
                        "key": "BOTLENS_STREAM_MAX_CLOSED_TRADES",
                        "default": _SETTINGS.bot_runtime.botlens.max_closed_trades,
                    },
                    {"key": "BOTLENS_STREAM_MAX_LOGS", "default": _SETTINGS.bot_runtime.botlens.max_logs},
                    {
                        "key": "BOTLENS_STREAM_MAX_DECISIONS",
                        "default": _SETTINGS.bot_runtime.botlens.max_decisions,
                    },
                    {
                        "key": "BOTLENS_STREAM_MAX_WARNINGS",
                        "default": _SETTINGS.bot_runtime.botlens.max_warnings,
                    },
                    {
                        "key": "BOT_RUNTIME_STEP_TRACE_QUEUE_MAX",
                        "default": _SETTINGS.bot_runtime.step_trace.queue_max,
                    },
                    {
                        "key": "BOT_RUNTIME_STEP_TRACE_BATCH_SIZE",
                        "default": _SETTINGS.bot_runtime.step_trace.batch_size,
                    },
                    {
                        "key": "BOT_RUNTIME_STEP_TRACE_FLUSH_INTERVAL_MS",
                        "default": _SETTINGS.bot_runtime.step_trace.flush_interval_ms,
                    },
                    {
                        "key": "BOT_RUNTIME_STEP_TRACE_OVERFLOW_POLICY",
                        "default": _SETTINGS.bot_runtime.step_trace.overflow_policy,
                    },
                ],
            },
            "runtime_env": env_rows,
        }

    def validate_strategy_id(self, strategy_id: Optional[object]) -> str:
        candidate = str(strategy_id or "").strip()
        if not candidate:
            raise ValueError("Bots require a strategy_id.")
        available = {strategy["id"] for strategy in load_strategies()}
        if candidate not in available:
            raise ValueError(f"Strategy does not exist: {candidate}")
        return candidate

    def validate_strategy_existence(self, bot: Mapping[str, object]) -> None:
        strategy_id = str(bot.get("strategy_id") or "").strip()
        if not strategy_id:
            raise ValueError("Bots require a strategy_id.")
        if not StrategyLoader.strategy_exists(strategy_id):
            raise ValueError(f"Strategy not found: {strategy_id}")

    def validate_instrument_policy(self, bot: Mapping[str, object]) -> None:
        policy = self.instrument_policy_from_bot(bot)
        if not policy:
            return

        strategy_id = str(bot.get("strategy_id") or "").strip()
        if not strategy_id:
            raise ValueError("Bots require a strategy_id.")
        strategy = StrategyLoader.fetch_strategy(strategy_id)
        for link in strategy.instrument_links:
            snapshot = link.instrument_snapshot or {}
            instrument_type = str(snapshot.get("instrument_type") or "").lower()
            symbol = snapshot.get("symbol") or link.symbol
            if not instrument_type:
                resolved = instrument_service.resolve_instrument(strategy.datasource, strategy.exchange, symbol or "")
                instrument_type = str((resolved or {}).get("instrument_type") or "").lower()
            if not instrument_type:
                raise ValueError(
                    f"Instrument type missing for {symbol or link.instrument_id}. Validate the instrument before running this bot."
                )
            is_spot = instrument_type == "spot"
            if policy == "derivatives" and is_spot:
                raise ValueError(
                    f"Derivatives-only bot cannot run on spot instrument {symbol or link.instrument_id}."
                )
            if policy == "spot" and not is_spot:
                raise ValueError(
                    f"Spot-only bot cannot run on derivatives instrument {symbol or link.instrument_id}."
                )

    @staticmethod
    def _normalize_runtime_instrument_type(value: Optional[object]) -> str:
        text = str(value or "").strip().lower()
        if text == "futures":
            return "future"
        if text == "perps":
            return "perp"
        return text

    def validate_runtime_readiness(self, bot: Mapping[str, object]) -> None:
        """Validate bot runtime prerequisites for v1 derivatives execution."""

        strategy_id = str(bot.get("strategy_id") or "").strip()
        if not strategy_id:
            raise ValueError("Bots require a strategy_id.")

        strategy = StrategyLoader.fetch_strategy(strategy_id)
        errors: List[str] = []

        if not strategy.instrument_links:
            raise ValueError("Strategy has no instruments attached. Add at least one instrument before bot start.")

        for link in strategy.instrument_links:
            snapshot = dict(link.instrument_snapshot or {})
            symbol = str(snapshot.get("symbol") or link.symbol or link.instrument_id or "").strip()
            resolved = (
                instrument_service.resolve_instrument(strategy.datasource, strategy.exchange, symbol)
                if symbol
                else None
            )
            instrument = dict(resolved or snapshot or {})
            if not instrument:
                errors.append(
                    f"{symbol or link.instrument_id}: instrument metadata missing. Refresh instrument metadata in Strategy."
                )
                continue
            try:
                compile_runtime_profile_or_error(
                    instrument,
                    allowed_derivative_types=_RUNTIME_ALLOWED_DERIVATIVE_TYPES,
                )
            except ValueError as exc:
                message = str(exc)
                prefix = f"{symbol}:".lower() if symbol else ""
                if prefix and message.lower().startswith(prefix):
                    errors.append(message)
                else:
                    errors.append(f"{symbol or link.instrument_id}: {message}")

        if errors:
            raise ValueError("Bot startup preflight failed: " + " | ".join(errors))
