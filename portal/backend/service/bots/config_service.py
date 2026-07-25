"""Bot configuration service: validation + persistence only."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional

from core.settings import env_is_set, env_value, get_settings
from engines.bot_runtime.core.execution_profile import compile_series_execution_profile, normalize_execution_semantics
from engines.bot_runtime.runtime.components.runtime_policy import ExecutionMode

from .strategy_loader import StrategyLoader
from .startup_validation import validate_wallet_config as normalize_wallet_config
from .execution_behavior import execution_behavior_from_bot, normalize_execution_behavior
from .market_data_stream_policy import normalize_market_data_stream_policy
from .startup_lifecycle import is_active_run_state
from ..market import instrument_service
from ..storage.storage import (
    delete_bot,
    get_bot as get_bot_record,
    get_atm_template,
    get_latest_bot_run_lifecycle,
    get_strategy_variant,
    list_strategy_variants,
    load_bots,
    load_strategies,
    upsert_bot,
)
from ..strategy_variant_resolution import EffectiveStrategyConfig, resolve_strategy_variant
from risk import normalise_risk_config

_SETTINGS = get_settings()


class BotConfigService:
    @staticmethod
    def _resolve_variant(
        strategy_id: str,
        variant_id: Optional[str],
        variant_name: Optional[str] = None,
    ) -> Optional[Mapping[str, object]]:
        normalized_variant_id = str(variant_id or "").strip() or None
        normalized_variant_name = str(variant_name or "").strip() or None
        variant = get_strategy_variant(normalized_variant_id) if normalized_variant_id else None
        if variant and str(variant.get("strategy_id") or "").strip() != strategy_id:
            raise ValueError("strategy_variant_id does not belong to the selected strategy.")
        if normalized_variant_name:
            if variant:
                if str(variant.get("name") or "").strip() != normalized_variant_name:
                    raise ValueError("strategy_variant_name does not match strategy_variant_id.")
                return variant
            for candidate in list_strategy_variants(strategy_id):
                if str(candidate.get("name") or "").strip() == normalized_variant_name:
                    return candidate
            raise ValueError("Strategy variant not found.")
        return variant

    @staticmethod
    def _resolve_effective_atm_template_id(
        strategy_atm_template_id: Optional[str],
        requested_atm_template_id: Optional[object],
    ) -> Optional[str]:
        candidate = str(requested_atm_template_id or "").strip() or str(strategy_atm_template_id or "").strip() or None
        if candidate and not get_atm_template(candidate):
            raise ValueError("ATM template not found.")
        return candidate

    @staticmethod
    def _resolve_effective_strategy_config(
        strategy: Any,
        variant: Optional[Mapping[str, object]],
    ) -> EffectiveStrategyConfig:
        strategy_id = str(getattr(strategy, "id", "") or "").strip()
        variants = list_strategy_variants(strategy_id) if strategy_id else []
        default_variant = next((item for item in variants if bool(item.get("is_default", False))), None)
        return resolve_strategy_variant(
            strategy,
            variant,
            default_variant=default_variant,
        )

    def list_bots(self) -> List[Dict[str, object]]:
        bots = load_bots()
        for bot in bots:
            bot["execution_semantics"] = self.execution_semantics_from_bot(bot)
            bot["market_data_stream_policy"] = normalize_market_data_stream_policy(
                bot.get("market_data_stream_policy")
                if isinstance(bot.get("market_data_stream_policy"), Mapping)
                else None
            )
        return bots

    def get_bot(self, bot_id: str) -> Dict[str, object]:
        bot = get_bot_record(bot_id)
        if bot:
            bot["execution_semantics"] = self.execution_semantics_from_bot(bot)
            bot["market_data_stream_policy"] = normalize_market_data_stream_policy(
                bot.get("market_data_stream_policy")
                if isinstance(bot.get("market_data_stream_policy"), Mapping)
                else None
            )
            return bot
        raise KeyError(f"Bot {bot_id} was not found")

    def create_bot(self, name: str, **payload: object) -> Dict[str, object]:
        bot_id = payload.get("id") or str(uuid.uuid4())
        strategy_id = self.validate_strategy_id(payload.get("strategy_id"))
        run_type = str(payload.get("run_type") or "backtest").lower()
        wallet_config = self.validate_wallet_config(payload.get("wallet_config"))
        strategy = StrategyLoader.fetch_strategy(strategy_id)
        variant_id = str(payload.get("strategy_variant_id") or "").strip() or None
        variant = self._resolve_variant(
            strategy_id,
            variant_id,
            str(payload.get("strategy_variant_name") or "").strip() or None,
        )
        effective_strategy_config = self._resolve_effective_strategy_config(
            strategy,
            variant,
        )
        effective_atm_template_id = self._resolve_effective_atm_template_id(
            getattr(strategy, "atm_template_id", None),
            payload.get("atm_template_id"),
        )
        variant_name = (
            str(variant.get("name") or "").strip()
            if isinstance(variant, Mapping)
            else str(payload.get("strategy_variant_name") or "").strip()
        ) or None
        risk_config_payload = (
            payload.get("risk_config") if isinstance(payload.get("risk_config"), Mapping) else strategy.risk_config
        )

        execution_mode = self.validate_execution_mode(
            payload.get("execution_mode"),
            legacy_mode=payload.get("mode"),
        )
        risk_payload = dict(payload.get("risk") or {})
        risk_payload["execution_mode"] = execution_mode
        execution_behavior = normalize_execution_behavior(payload.get("execution_behavior") or risk_payload.get("execution_behavior"))
        risk_payload["execution_behavior"] = execution_behavior
        market_data_stream_policy = normalize_market_data_stream_policy(
            payload.get("market_data_stream_policy")
            if isinstance(payload.get("market_data_stream_policy"), Mapping)
            else None
        )

        record: Dict[str, object] = {
            "id": bot_id,
            "name": name,
            "strategy_id": strategy_id,
            "strategy_variant_id": (
                str(variant.get("id") or "").strip()
                if isinstance(variant, Mapping)
                else None
            ) or None,
            "strategy_variant_name": variant_name,
            "atm_template_id": effective_atm_template_id,
            "resolved_params": effective_strategy_config.effective_params,
            "risk_config": normalise_risk_config(risk_config_payload),
            "timeframe": None,
            "mode": (payload.get("mode") or "instant").lower(),
            "execution_mode": execution_mode,
            "execution_behavior": execution_behavior,
            "run_type": run_type,
            "playback_speed": self.coerce_playback_speed(payload.get("playback_speed") or payload.get("fetch_seconds")),
            "backtest_start": self.coerce_isoformat(payload.get("backtest_start")),
            "backtest_end": self.coerce_isoformat(payload.get("backtest_end")),
            "risk": risk_payload,
            "wallet_config": wallet_config,
            "market_data_stream_policy": market_data_stream_policy,
            "snapshot_interval_ms": int(payload.get("snapshot_interval_ms") or 0),
            "bot_env": self.validate_bot_env(payload.get("bot_env") if isinstance(payload.get("bot_env"), Mapping) else {}),
        }
        if int(record.get("snapshot_interval_ms") or 0) <= 0:
            raise ValueError("snapshot_interval_ms is required and must be > 0")
        self.apply_execution_semantics(record, payload.get("execution_semantics"))
        self.validate_backtest_window(record)
        upsert_bot(record)
        return record

    def update_bot(self, bot_id: str, **payload: object) -> Dict[str, object]:
        existing = get_bot_record(bot_id)
        if not existing:
            raise KeyError(f"Bot {bot_id} was not found")
        record = dict(existing)

        if "strategy_id" in payload and payload["strategy_id"] is not None:
            record["strategy_id"] = self.validate_strategy_id(payload.get("strategy_id"))
        if "strategy_variant_id" in payload:
            record["strategy_variant_id"] = str(payload.get("strategy_variant_id") or "").strip() or None
            if "strategy_variant_name" not in payload:
                record["strategy_variant_name"] = None
        if "strategy_variant_name" in payload:
            record["strategy_variant_name"] = str(payload.get("strategy_variant_name") or "").strip() or None
            if "strategy_variant_id" not in payload:
                record["strategy_variant_id"] = None
        if "atm_template_id" in payload:
            record["atm_template_id"] = str(payload.get("atm_template_id") or "").strip() or None
        if "risk_config" in payload:
            config = payload.get("risk_config") if isinstance(payload.get("risk_config"), Mapping) else {}
            record["risk_config"] = normalise_risk_config(config)
        if "name" in payload and payload["name"] is not None:
            record["name"] = payload["name"]
        if "execution_semantics" in payload:
            self.apply_execution_semantics(record, payload.get("execution_semantics"))
        if "run_type" in payload and payload["run_type"] is not None:
            record["run_type"] = str(payload["run_type"]).lower()
        if "mode" in payload and payload["mode"] is not None:
            record["mode"] = str(payload["mode"]).lower()
        if "execution_mode" in payload:
            execution_mode = self.validate_execution_mode(
                payload.get("execution_mode"),
                legacy_mode=record.get("mode"),
            )
            record["execution_mode"] = execution_mode
            risk = dict(record.get("risk") or {})
            risk["execution_mode"] = execution_mode
            record["risk"] = risk
        if "execution_behavior" in payload:
            execution_behavior = normalize_execution_behavior(payload.get("execution_behavior"))
            record["execution_behavior"] = execution_behavior
            risk = dict(record.get("risk") or {})
            risk["execution_behavior"] = execution_behavior
            record["risk"] = risk
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
        if "market_data_stream_policy" in payload and payload["market_data_stream_policy"] is not None:
            record["market_data_stream_policy"] = normalize_market_data_stream_policy(
                payload.get("market_data_stream_policy")
                if isinstance(payload.get("market_data_stream_policy"), Mapping)
                else {}
            )
        if "snapshot_interval_ms" in payload and payload["snapshot_interval_ms"] is not None:
            interval = int(payload["snapshot_interval_ms"])
            if interval <= 0:
                raise ValueError("snapshot_interval_ms is required and must be > 0")
            record["snapshot_interval_ms"] = interval
        if "bot_env" in payload:
            next_env = self.validate_bot_env(payload.get("bot_env") if isinstance(payload.get("bot_env"), Mapping) else {})
            current_env = dict(record.get("bot_env") or {})
            lifecycle = get_latest_bot_run_lifecycle(bot_id)
            if is_active_run_state(
                status=(lifecycle or {}).get("status"),
                phase=(lifecycle or {}).get("phase"),
            ) and next_env != current_env:
                raise ValueError("Bot env settings changed. Stop and restart the bot to apply new env vars.")
            record["bot_env"] = next_env
        strategy_id = self.validate_strategy_id(record.get("strategy_id"))
        record["strategy_id"] = strategy_id
        strategy = StrategyLoader.fetch_strategy(strategy_id)
        variant = self._resolve_variant(
            strategy_id,
            record.get("strategy_variant_id"),
            record.get("strategy_variant_name"),
        )
        effective_strategy_config = self._resolve_effective_strategy_config(
            strategy,
            variant,
        )
        record["atm_template_id"] = self._resolve_effective_atm_template_id(
            getattr(strategy, "atm_template_id", None),
            record.get("atm_template_id"),
        )
        record["strategy_variant_name"] = (
            str(variant.get("name") or "").strip()
            if isinstance(variant, Mapping)
            else str(record.get("strategy_variant_name") or "").strip()
        ) or None
        record["strategy_variant_id"] = (
            str(variant.get("id") or "").strip()
            if isinstance(variant, Mapping)
            else str(record.get("strategy_variant_id") or "").strip()
        ) or None
        record["resolved_params"] = effective_strategy_config.effective_params
        record["execution_behavior"] = execution_behavior_from_bot(record)
        record["market_data_stream_policy"] = normalize_market_data_stream_policy(
            record.get("market_data_stream_policy")
            if isinstance(record.get("market_data_stream_policy"), Mapping)
            else None
        )
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
    def validate_execution_mode(value: Optional[object], *, legacy_mode: Optional[object] = None) -> str:
        return ExecutionMode.from_config(value, legacy_mode=legacy_mode).value

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
    def normalize_execution_semantics_policy(value: Optional[object]) -> Optional[str]:
        if value in (None, ""):
            return None
        resolved = normalize_execution_semantics(value)
        if resolved not in {"spot", "derivative", "proxy_derivative"}:
            raise ValueError(f"Unsupported execution_semantics '{value}'")
        return resolved

    @staticmethod
    def execution_semantics_from_bot(bot: Mapping[str, object]) -> Optional[str]:
        direct = bot.get("execution_semantics")
        if direct:
            return normalize_execution_semantics(direct)
        risk = bot.get("risk")
        if isinstance(risk, Mapping) and risk.get("execution_semantics"):
            return normalize_execution_semantics(risk.get("execution_semantics"))
        return None

    def apply_execution_semantics(self, record: Dict[str, object], value: Optional[object]) -> None:
        execution_semantics = self.normalize_execution_semantics_policy(value)
        if execution_semantics is None:
            return
        record["execution_semantics"] = execution_semantics
        risk = dict(record.get("risk") or {})
        risk["execution_semantics"] = execution_semantics
        record["risk"] = risk

    @staticmethod
    def _instrument_fields(instrument: Mapping[str, Any]) -> Dict[str, Any]:
        metadata = instrument.get("metadata") if isinstance(instrument.get("metadata"), Mapping) else {}
        fields = metadata.get("instrument_fields") if isinstance(metadata.get("instrument_fields"), Mapping) else {}
        return dict(fields)

    @classmethod
    def _has_proxy_derivative_reference(cls, instrument: Mapping[str, Any]) -> bool:
        fields = cls._instrument_fields(instrument)
        margin_rates = instrument.get("proxy_derivative_margin_rates") or fields.get("proxy_derivative_margin_rates")
        execution_fields = instrument.get("proxy_derivative_instrument_fields") or fields.get("proxy_derivative_instrument_fields")
        return isinstance(margin_rates, Mapping) and bool(margin_rates) and isinstance(execution_fields, Mapping) and bool(execution_fields)

    def _execution_semantics_for_runtime_instrument(
        self,
        bot: Mapping[str, object],
        instrument: Mapping[str, Any],
        *,
        run_type: str,
    ) -> str:
        instrument_type = self._normalize_runtime_instrument_type(instrument.get("instrument_type"))
        if not instrument_type:
            symbol = str(instrument.get("symbol") or instrument.get("id") or "unknown").strip()
            raise ValueError(
                f"{symbol}: instrument_type missing. Validate the instrument before running this bot."
            )

        requested = self.execution_semantics_from_bot(bot)
        if requested:
            execution_semantics = normalize_execution_semantics(requested, instrument_type=instrument_type)
        elif run_type == "backtest" and instrument_type == "spot" and self._has_proxy_derivative_reference(instrument):
            execution_semantics = "proxy_derivative"
        else:
            execution_semantics = normalize_execution_semantics(None, instrument_type=instrument_type)

        if execution_semantics == "proxy_derivative" and run_type != "backtest":
            symbol = str(instrument.get("symbol") or instrument.get("id") or "unknown").strip()
            raise ValueError(
                f"{symbol}: proxy_derivative execution is currently supported for backtest runs only. "
                "Use spot execution semantics outside historical research until paper/live proxy execution is modeled."
            )
        return execution_semantics

    @staticmethod
    def validate_backtest_window(record: Mapping[str, object]) -> None:
        if str(record.get("run_type") or "backtest").lower() != "backtest":
            return
        if not record.get("backtest_start") or not record.get("backtest_end"):
            raise ValueError("Backtests require both start and end timestamps.")

    @staticmethod
    def validate_wallet_config(wallet_config: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        return normalize_wallet_config(wallet_config)

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
            "QT_BOT_RUNTIME_MARKET_DATA_RECONNECT_ENABLED",
            "QT_BOT_RUNTIME_MARKET_DATA_INITIAL_BACKOFF_SECONDS",
            "QT_BOT_RUNTIME_MARKET_DATA_MAX_BACKOFF_SECONDS",
            "QT_BOT_RUNTIME_MARKET_DATA_CONTINUOUS_DISCONNECT_BUDGET_SECONDS",
            "QT_BOT_RUNTIME_MARKET_DATA_HEARTBEAT_STALE_SECONDS",
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
            "QT_BOT_RUNTIME_WATCHDOG_CLOCK_GAP_ENABLED",
            "QT_BOT_RUNTIME_WATCHDOG_CLOCK_GAP_INTERVAL_SECONDS",
            "QT_BOT_RUNTIME_WATCHDOG_CLOCK_GAP_THRESHOLD_SECONDS",
            "QT_BOT_RUNTIME_WATCHDOG_DOCKER_LIFECYCLE_ENABLED",
            "QT_BOT_RUNTIME_WATCHDOG_DOCKER_LIFECYCLE_RETRY_INTERVAL_SECONDS",
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
                "market_data_stream_policy": normalize_market_data_stream_policy(None),
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
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_ENABLED",
                        "default": _SETTINGS.bot_runtime.indicator_guard.enabled,
                    },
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_TIME_SOFT_LIMIT_MS",
                        "default": _SETTINGS.bot_runtime.indicator_guard.time_soft_limit_ms,
                    },
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_TIME_CONSECUTIVE_BARS",
                        "default": _SETTINGS.bot_runtime.indicator_guard.time_consecutive_bars,
                    },
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_TIME_WINDOW_BARS",
                        "default": _SETTINGS.bot_runtime.indicator_guard.time_window_bars,
                    },
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_TIME_WINDOW_BREACH_COUNT",
                        "default": _SETTINGS.bot_runtime.indicator_guard.time_window_breach_count,
                    },
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_OVERLAY_POINTS_SOFT_LIMIT",
                        "default": _SETTINGS.bot_runtime.indicator_guard.overlay_points_soft_limit,
                    },
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_OVERLAY_POINTS_HARD_LIMIT",
                        "default": _SETTINGS.bot_runtime.indicator_guard.overlay_points_hard_limit,
                    },
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_OVERLAY_PAYLOAD_SOFT_LIMIT_BYTES",
                        "default": _SETTINGS.bot_runtime.indicator_guard.overlay_payload_soft_limit_bytes,
                    },
                    {
                        "key": "BOT_RUNTIME_INDICATOR_GUARD_OVERLAY_PAYLOAD_HARD_LIMIT_BYTES",
                        "default": _SETTINGS.bot_runtime.indicator_guard.overlay_payload_hard_limit_bytes,
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

    @staticmethod
    def _runtime_loader_config(bot: Mapping[str, object]) -> Dict[str, Any]:
        return {
            "strategy_variant_id": str(bot.get("strategy_variant_id") or "").strip() or None,
            "strategy_variant_name": str(bot.get("strategy_variant_name") or "").strip() or None,
            "atm_template_id": str(bot.get("atm_template_id") or "").strip() or None,
            "risk_config": dict(bot.get("risk_config") or {}) if isinstance(bot.get("risk_config"), Mapping) else {},
        }

    @staticmethod
    def _resolve_runtime_instrument(strategy: Any, link: Any) -> Dict[str, Any]:
        snapshot = dict(getattr(link, "instrument_snapshot", {}) or {})
        instrument_id = str(
            getattr(link, "instrument_id", "")
            or snapshot.get("id")
            or snapshot.get("instrument_id")
            or ""
        ).strip()
        if instrument_id:
            try:
                resolved_by_id = instrument_service.get_instrument_record(instrument_id)
                if resolved_by_id:
                    return {**snapshot, **dict(resolved_by_id)}
            except Exception:
                pass
        symbol = str(
            snapshot.get("symbol")
            or getattr(link, "symbol", "")
            or instrument_id
            or ""
        ).strip()
        resolved = (
            instrument_service.resolve_instrument(strategy.datasource, strategy.exchange, symbol)
            if symbol
            else None
        )
        return dict(resolved or snapshot or {})

    def prepare_startup_artifacts(self, bot: Mapping[str, object]) -> Dict[str, Any]:
        strategy_id = self.validate_strategy_id(bot.get("strategy_id"))
        wallet_config = self.validate_wallet_config(bot.get("wallet_config"))
        self.validate_backtest_window(bot)

        strategy = StrategyLoader.fetch_strategy(
            strategy_id,
            runtime_config=self._runtime_loader_config(bot),
        )
        if not strategy.instrument_links:
            raise ValueError("Strategy has no instruments attached. Add at least one instrument before bot start.")

        run_type = str(bot.get("run_type") or "backtest").strip().lower()
        symbols: List[str] = []
        readiness_entries: List[Dict[str, Any]] = []
        errors: List[str] = []

        for link in strategy.instrument_links:
            instrument = self._resolve_runtime_instrument(strategy, link)
            symbol = str(
                instrument.get("symbol")
                or getattr(link, "symbol", "")
                or getattr(link, "instrument_id", "")
                or ""
            ).strip()
            if symbol and symbol not in symbols:
                symbols.append(symbol)
            instrument_type = self._normalize_runtime_instrument_type(instrument.get("instrument_type"))
            if not instrument:
                errors.append(
                    f"{symbol or getattr(link, 'instrument_id', None)}: instrument metadata missing. Refresh instrument metadata in Strategy."
                )
                continue
            try:
                execution_semantics = self._execution_semantics_for_runtime_instrument(
                    bot,
                    instrument,
                    run_type=run_type,
                )
                profile = compile_series_execution_profile(
                    instrument,
                    risk_config=bot.get("risk_config") if isinstance(bot.get("risk_config"), Mapping) else None,
                    require_margin_accounting=execution_semantics in {"derivative", "proxy_derivative"},
                    execution_semantics=execution_semantics,
                )
                readiness_entries.append(
                    {
                        "symbol": symbol or None,
                        "instrument_id": getattr(link, "instrument_id", None),
                        "instrument_type": instrument_type or None,
                        "source_instrument_type": profile.instrument.source_instrument_type,
                        "execution_semantics": profile.instrument.execution_semantics,
                        "research_market_role": profile.instrument.research_market_role,
                        "accounting_mode": profile.accounting_mode,
                        "margin_calc_type": profile.margin_calc_type,
                        "profile": profile.to_dict() if hasattr(profile, "to_dict") else {"instrument_type": instrument_type or None},
                    }
                )
            except ValueError as exc:
                message = str(exc)
                prefix = f"{symbol}:".lower() if symbol else ""
                if prefix and message.lower().startswith(prefix):
                    errors.append(message)
                else:
                    errors.append(f"{symbol or getattr(link, 'instrument_id', None)}: {message}")

        if errors:
            raise ValueError("Bot startup preflight failed: " + " | ".join(errors))

        return {
            "strategy_id": strategy_id,
            "strategy": strategy,
            "wallet_config": wallet_config,
            "symbols": symbols,
            "run_strategy_snapshot": dict(getattr(strategy, "run_strategy_snapshot", {}) or {}),
            "effective_strategy_config": dict(getattr(strategy, "effective_strategy_config", {}) or {}),
            "runtime_readiness": {
                "datasource": strategy.datasource,
                "exchange": strategy.exchange,
                "timeframe": strategy.timeframe,
                "symbols": symbols,
                "profiles": readiness_entries,
            },
        }

    @staticmethod
    def _normalize_runtime_instrument_type(value: Optional[object]) -> str:
        text = str(value or "").strip().lower()
        if text == "futures":
            return "future"
        if text == "perps":
            return "perp"
        return text

    def validate_runtime_readiness(self, bot: Mapping[str, object]) -> None:
        """Validate bot runtime prerequisites for configured execution semantics."""
        self.prepare_startup_artifacts(bot)
