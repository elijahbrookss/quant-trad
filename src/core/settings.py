"""Centralized application settings.

This module is the single configuration boundary for Quant-Trad application
code. Non-secret defaults live in YAML files under ``config/`` while
deployment-specific overrides and secrets still come from environment
variables.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

import yaml
from dotenv import load_dotenv


logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_DIR = _REPO_ROOT / "config"
_DEFAULTS_FILE = _CONFIG_DIR / "defaults.yaml"
_ENV_LOADED = False
_SETTINGS_CACHE: "AppSettings | None" = None
_ENV_BINDINGS: list[tuple[str, tuple[str, ...]]] = [
    ("QT_LOGGING_LEVEL", ("logging", "level")),
    ("QT_LOGGING_DEBUG", ("logging", "debug")),
    ("QT_LOGGING_ENV_NAME", ("logging", "env_name")),
    ("QT_LOGGING_LOKI_URL", ("logging", "loki_url")),
    ("QT_BACKEND_HOST", ("backend", "host")),
    ("QT_BACKEND_PORT", ("backend", "port")),
    ("QT_DATABASE_APPLICATION_NAME", ("database", "application_name")),
    ("QT_DATABASE_POOL_RECYCLE_SECONDS", ("database", "pool_recycle_seconds")),
    ("QT_DATABASE_POOL_TIMEOUT_SECONDS", ("database", "pool_timeout_seconds")),
    ("QT_DATABASE_CONNECT_TIMEOUT_SECONDS", ("database", "connect_timeout_seconds")),
    ("QT_DATABASE_POOL_PRE_PING", ("database", "pool_pre_ping")),
    ("QT_DATABASE_TCP_KEEPALIVE_ENABLED", ("database", "tcp_keepalive_enabled")),
    ("QT_DATABASE_TCP_KEEPALIVE_IDLE_SECONDS", ("database", "tcp_keepalive_idle_seconds")),
    ("QT_DATABASE_TCP_KEEPALIVE_INTERVAL_SECONDS", ("database", "tcp_keepalive_interval_seconds")),
    ("QT_DATABASE_TCP_KEEPALIVE_COUNT", ("database", "tcp_keepalive_count")),
    ("QT_DATABASE_WRITE_RETRY_ATTEMPTS", ("database", "write_retry_attempts")),
    ("QT_OBSERVABILITY_ENABLED", ("observability", "enabled")),
    ("QT_OBSERVABILITY_STEP_SAMPLE_RATE", ("observability", "step_sample_rate")),
    ("QT_OBSERVABILITY_SLOW_MS", ("observability", "slow_ms")),
    ("QT_OBSERVABILITY_LOG_THROTTLE_SECONDS", ("observability", "log_throttle_seconds")),
    ("QT_ASYNC_JOBS_RUNNING_TIMEOUT_SECONDS", ("async_jobs", "running_timeout_seconds")),
    ("QT_ASYNC_JOBS_QUANTLAB_JOB_WAIT_TIMEOUT_SECONDS", ("async_jobs", "quantlab_job_wait_timeout_seconds")),
    ("QT_ASYNC_JOBS_QUANTLAB_JOB_POLL_INTERVAL_SECONDS", ("async_jobs", "quantlab_job_poll_interval_seconds")),
    ("QT_WORKERS_QUANTLAB_PROCESSES", ("workers", "quantlab", "processes")),
    ("QT_WORKERS_QUANTLAB_INDEX", ("workers", "quantlab", "index")),
    ("QT_WORKERS_QUANTLAB_TOTAL", ("workers", "quantlab", "total")),
    ("QT_WORKERS_QUANTLAB_IDLE_SLEEP_SECONDS", ("workers", "quantlab", "idle_sleep_seconds")),
    ("QT_WORKERS_QUANTLAB_DB_WAIT_TIMEOUT_SECONDS", ("workers", "quantlab", "db_wait_timeout_seconds")),
    ("QT_WORKERS_STATS_PROCESSES", ("workers", "stats", "processes")),
    ("QT_WORKERS_STATS_INDEX", ("workers", "stats", "index")),
    ("QT_WORKERS_STATS_TOTAL", ("workers", "stats", "total")),
    ("QT_WORKERS_STATS_IDLE_SLEEP_SECONDS", ("workers", "stats", "idle_sleep_seconds")),
    ("QT_WORKERS_STATS_DB_WAIT_TIMEOUT_SECONDS", ("workers", "stats", "db_wait_timeout_seconds")),
    ("QT_BOT_RUNTIME_MODE", ("bot_runtime", "mode")),
    ("QT_BOT_RUNTIME_TARGET", ("bot_runtime", "target")),
    ("QT_BOT_RUNTIME_IMAGE", ("bot_runtime", "image")),
    ("QT_BOT_RUNTIME_NETWORK", ("bot_runtime", "network")),
    ("QT_BOT_RUNTIME_BOT_ID", ("bot_runtime", "bot_id")),
    ("QT_BOT_RUNTIME_MAX_SYMBOLS_PER_STRATEGY", ("bot_runtime", "max_symbols_per_strategy")),
    ("QT_BOT_RUNTIME_SYMBOL_PROCESS_MAX", ("bot_runtime", "symbol_process_max")),
    ("QT_BOT_RUNTIME_STATUS_HEARTBEAT_STALE_MS", ("bot_runtime", "status_heartbeat_stale_ms")),
    ("QT_BOT_RUNTIME_TELEMETRY_WS_URL", ("bot_runtime", "telemetry", "ws_url")),
    ("QT_BOT_RUNTIME_TELEMETRY_EVENT_POLL_MS", ("bot_runtime", "telemetry", "event_poll_ms")),
    ("QT_BOT_RUNTIME_TELEMETRY_EMIT_QUEUE_MAX", ("bot_runtime", "telemetry", "emit_queue_max")),
    ("QT_BOT_RUNTIME_TELEMETRY_EMIT_QUEUE_TIMEOUT_MS", ("bot_runtime", "telemetry", "emit_queue_timeout_ms")),
    ("QT_BOT_RUNTIME_TELEMETRY_EMIT_RETRY_MS", ("bot_runtime", "telemetry", "emit_retry_ms")),
    ("QT_BOT_RUNTIME_SNAPSHOT_DEFAULT_INTERVAL_MS", ("bot_runtime", "snapshot", "default_interval_ms")),
    ("QT_BOT_RUNTIME_SNAPSHOT_FAST_INTERVAL_MS", ("bot_runtime", "snapshot", "fast_interval_ms")),
    ("QT_BOT_RUNTIME_SNAPSHOT_IDLE_INTERVAL_MS", ("bot_runtime", "snapshot", "idle_interval_ms")),
    ("QT_BOT_RUNTIME_SNAPSHOT_IDLE_CYCLES", ("bot_runtime", "snapshot", "idle_cycles")),
    ("QT_BOT_RUNTIME_PUSH_PAYLOAD_BYTES_SAMPLE_EVERY", ("bot_runtime", "push", "payload_bytes_sample_every")),
    ("QT_BOT_RUNTIME_BOTLENS_MAX_SERIES", ("bot_runtime", "botlens", "max_series")),
    ("QT_BOT_RUNTIME_BOTLENS_MAX_CANDLES", ("bot_runtime", "botlens", "max_candles")),
    ("QT_BOT_RUNTIME_BOTLENS_MAX_OVERLAYS", ("bot_runtime", "botlens", "max_overlays")),
    ("QT_BOT_RUNTIME_BOTLENS_MAX_OVERLAY_POINTS", ("bot_runtime", "botlens", "max_overlay_points")),
    ("QT_BOT_RUNTIME_BOTLENS_MAX_CLOSED_TRADES", ("bot_runtime", "botlens", "max_closed_trades")),
    ("QT_BOT_RUNTIME_BOTLENS_MAX_LOGS", ("bot_runtime", "botlens", "max_logs")),
    ("QT_BOT_RUNTIME_BOTLENS_MAX_DECISIONS", ("bot_runtime", "botlens", "max_decisions")),
    ("QT_BOT_RUNTIME_BOTLENS_MAX_WARNINGS", ("bot_runtime", "botlens", "max_warnings")),
    ("QT_BOT_RUNTIME_BOTLENS_RING_SIZE", ("bot_runtime", "botlens", "ring_size")),
    ("QT_BOT_RUNTIME_BOTLENS_INGEST_QUEUE_MAX", ("bot_runtime", "botlens", "ingest_queue_max")),
    ("QT_BOT_RUNTIME_STEP_TRACE_QUEUE_MAX", ("bot_runtime", "step_trace", "queue_max")),
    ("QT_BOT_RUNTIME_STEP_TRACE_BATCH_SIZE", ("bot_runtime", "step_trace", "batch_size")),
    ("QT_BOT_RUNTIME_STEP_TRACE_FLUSH_INTERVAL_MS", ("bot_runtime", "step_trace", "flush_interval_ms")),
    ("QT_BOT_RUNTIME_STEP_TRACE_OVERFLOW_POLICY", ("bot_runtime", "step_trace", "overflow_policy")),
    ("QT_BOT_RUNTIME_WATCHDOG_HEARTBEAT_INTERVAL_SECONDS", ("bot_runtime", "watchdog", "heartbeat_interval_seconds")),
    ("QT_BOT_RUNTIME_WATCHDOG_STALE_THRESHOLD_SECONDS", ("bot_runtime", "watchdog", "stale_threshold_seconds")),
    ("QT_BOT_RUNTIME_WATCHDOG_MONITOR_INTERVAL_SECONDS", ("bot_runtime", "watchdog", "monitor_interval_seconds")),
    ("QT_BOT_RUNTIME_WATCHDOG_RUNNER_ID", ("bot_runtime", "watchdog", "runner_id")),
    ("QT_PROVIDERS_RUNTIME_HISTORY_SEGMENT_POINTS", ("providers", "runtime", "history_segment_points")),
    ("QT_PROVIDERS_RUNTIME_CANDLES_RAW_TABLE", ("providers", "runtime", "persistence", "candles_raw_table")),
    ("QT_PROVIDERS_RUNTIME_CANDLE_STATS_TABLE", ("providers", "runtime", "persistence", "candle_stats_table")),
    ("QT_PROVIDERS_RUNTIME_REGIME_STATS_TABLE", ("providers", "runtime", "persistence", "regime_stats_table")),
    ("QT_PROVIDERS_RUNTIME_REGIME_BLOCKS_TABLE", ("providers", "runtime", "persistence", "regime_blocks_table")),
    ("QT_PROVIDERS_RUNTIME_DERIVATIVES_STATE_TABLE", ("providers", "runtime", "persistence", "derivatives_state_table")),
    ("QT_PROVIDERS_RUNTIME_CLOSURES_TABLE", ("providers", "runtime", "persistence", "closures_table")),
    ("QT_PROVIDERS_IBKR_HOST", ("providers", "ibkr", "host")),
    ("QT_PROVIDERS_IBKR_PORT", ("providers", "ibkr", "port")),
    ("QT_PROVIDERS_IBKR_CLIENT_ID", ("providers", "ibkr", "client_id")),
    ("QT_PROVIDERS_IBKR_DEFAULT_CURRENCY", ("providers", "ibkr", "default_currency")),
    ("QT_PROVIDERS_IBKR_DEFAULT_SEC_TYPE", ("providers", "ibkr", "default_sec_type")),
    ("QT_PROVIDERS_IBKR_DEFAULT_EXCHANGE", ("providers", "ibkr", "default_exchange")),
    ("QT_PROVIDERS_IBKR_WHAT_TO_SHOW", ("providers", "ibkr", "what_to_show")),
    ("QT_PROVIDERS_IBKR_SYMBOL_OVERRIDES", ("providers", "ibkr", "symbol_overrides")),
    ("QT_PROVIDERS_CCXT_SANDBOX_MODE", ("providers", "ccxt", "sandbox_mode")),
    ("QT_PROVIDERS_CCXT_OHLCV_LIMIT", ("providers", "ccxt", "ohlcv_limit")),
    ("QT_PROVIDERS_CCXT_API_KEY", ("providers", "ccxt", "api_key")),
    ("QT_PROVIDERS_CCXT_API_SECRET", ("providers", "ccxt", "api_secret")),
    ("QT_PROVIDERS_CCXT_SECRET", ("providers", "ccxt", "secret")),
    ("QT_PROVIDERS_CCXT_PASSWORD", ("providers", "ccxt", "password")),
    ("QT_PROVIDERS_ALPACA_API_KEY", ("providers", "alpaca", "api_key")),
    ("QT_PROVIDERS_ALPACA_SECRET_KEY", ("providers", "alpaca", "secret_key")),
    ("QT_PROVIDERS_ALPACA_PAPER", ("providers", "alpaca", "paper")),
    ("QT_SECURITY_PROVIDER_CREDENTIAL_KEY", ("security", "provider_credential_key")),
]


def _deep_merge(base: Dict[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        existing = result.get(key)
        if isinstance(existing, dict) and isinstance(value, Mapping):
            result[key] = _deep_merge(existing, value)
            continue
        result[key] = value
    return result


def _path_get(mapping: Mapping[str, Any], path: Sequence[str], default: Any = None) -> Any:
    current: Any = mapping
    for segment in path:
        if not isinstance(current, Mapping):
            return default
        if segment not in current:
            return default
        current = current[segment]
    return current


def _path_set(mapping: Dict[str, Any], path: Sequence[str], value: Any) -> None:
    current = mapping
    for segment in path[:-1]:
        next_value = current.get(segment)
        if not isinstance(next_value, dict):
            next_value = {}
            current[segment] = next_value
        current = next_value
    current[path[-1]] = value


def _yaml_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    payload = yaml.safe_load(path.read_text()) or {}
    if not isinstance(payload, dict):
        raise RuntimeError(f"Configuration file must contain a mapping: {path}")
    return payload


def _parse_env_value(raw: str) -> Any:
    if raw == "":
        return ""
    try:
        return yaml.safe_load(raw)
    except yaml.YAMLError:
        return raw


def _coerce_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    return bool(default)


def _coerce_int(value: Any, default: int, *, minimum: int | None = None) -> int:
    try:
        resolved = int(value)
    except (TypeError, ValueError):
        resolved = int(default)
    if minimum is not None:
        return max(minimum, resolved)
    return resolved


def _coerce_float(value: Any, default: float, *, minimum: float | None = None) -> float:
    try:
        resolved = float(value)
    except (TypeError, ValueError):
        resolved = float(default)
    if minimum is not None:
        return max(minimum, resolved)
    return resolved


def _coerce_str(value: Any, default: str = "") -> str:
    if value in (None, ""):
        return str(default)
    return str(value)


def _coerce_optional_str(value: Any) -> Optional[str]:
    text = str(value).strip() if value is not None else ""
    return text or None


def _coerce_string_list(value: Any, default: Sequence[str]) -> list[str]:
    if value in (None, ""):
        return [str(item) for item in default]
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Sequence):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(item) for item in default]


def _coerce_mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _coerce_optional_mapping(value: Any) -> dict[str, Any]:
    payload = _coerce_mapping(value)
    return payload if payload else {}


def ensure_env_loaded() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    if not os.getenv("GITHUB_ACTIONS"):
        load_dotenv(_REPO_ROOT / "secrets.env")
        load_dotenv(_REPO_ROOT / ".env")
    _ENV_LOADED = True


def _profile_name() -> str:
    ensure_env_loaded()
    explicit = str(os.getenv("QT_CONFIG_PROFILE") or "").strip()
    if explicit:
        return explicit
    return "dev"


def _load_raw_config() -> Dict[str, Any]:
    ensure_env_loaded()
    if not _DEFAULTS_FILE.exists():
        raise RuntimeError(f"Missing configuration defaults file: {_DEFAULTS_FILE}")

    payload = _yaml_file(_DEFAULTS_FILE)
    profile = _profile_name()
    profile_file = _CONFIG_DIR / f"{profile}.yaml"
    if profile_file.exists():
        payload = _deep_merge(payload, _yaml_file(profile_file))

    custom_file = str(os.getenv("QT_CONFIG_FILE") or "").strip()
    if custom_file:
        custom_path = Path(custom_file)
        if not custom_path.is_absolute():
            custom_path = (_REPO_ROOT / custom_path).resolve()
        if not custom_path.exists():
            raise RuntimeError(f"QT_CONFIG_FILE does not exist: {custom_path}")
        payload = _deep_merge(payload, _yaml_file(custom_path))

    env_overrides: Dict[str, Any] = {}
    for env_name, path in _ENV_BINDINGS:
        if env_name not in os.environ:
            continue
        _path_set(env_overrides, path, _parse_env_value(os.environ[env_name]))

    # PG_DSN remains the one canonical non-prefixed infrastructure override.
    if "PG_DSN" in os.environ:
        _path_set(env_overrides, ("database", "dsn"), _parse_env_value(os.environ["PG_DSN"]))

    if env_overrides:
        payload = _deep_merge(payload, env_overrides)
    payload["profile"] = profile
    return payload


@dataclass(frozen=True)
class LoggingSettings:
    level_name: str
    debug: bool
    env_name: str
    loki_url: Optional[str]

    @property
    def effective_level_name(self) -> str:
        base = str(self.level_name or "INFO").strip().upper() or "INFO"
        if self.debug and base == "INFO":
            return "DEBUG"
        return base

    @property
    def level(self) -> int:
        return getattr(logging, self.effective_level_name, logging.INFO)


@dataclass(frozen=True)
class BackendSettings:
    host: str
    port: int
    allowed_origins: list[str]


@dataclass(frozen=True)
class DatabaseSettings:
    dsn: Optional[str]
    application_name: str
    pool_recycle_seconds: int
    pool_timeout_seconds: int
    connect_timeout_seconds: int
    pool_pre_ping: bool
    tcp_keepalive_enabled: bool
    tcp_keepalive_idle_seconds: int
    tcp_keepalive_interval_seconds: int
    tcp_keepalive_count: int
    write_retry_attempts: int


@dataclass(frozen=True)
class ObservabilitySettings:
    enabled: bool
    step_sample_rate: float
    slow_ms: float
    log_throttle_seconds: float


@dataclass(frozen=True)
class ProviderPersistenceSettings:
    candles_raw_table: str
    candle_stats_table: str
    regime_stats_table: str
    regime_blocks_table: str
    derivatives_state_table: str
    closures_table: str


@dataclass(frozen=True)
class ProviderRuntimeSettings:
    history_segment_points: int
    persistence: ProviderPersistenceSettings


@dataclass(frozen=True)
class AsyncJobSettings:
    running_timeout_seconds: float
    quantlab_job_wait_timeout_seconds: float
    quantlab_job_poll_interval_seconds: float


@dataclass(frozen=True)
class WorkerGroupSettings:
    processes: int
    index: int
    total: int
    idle_sleep_seconds: float
    db_wait_timeout_seconds: float


@dataclass(frozen=True)
class WorkersSettings:
    quantlab: WorkerGroupSettings
    stats: WorkerGroupSettings


@dataclass(frozen=True)
class SnapshotSettings:
    default_interval_ms: int
    fast_interval_ms: int
    idle_interval_ms: int
    idle_cycles: int


@dataclass(frozen=True)
class PushSettings:
    payload_bytes_sample_every: int


@dataclass(frozen=True)
class BotlensSettings:
    max_series: int
    max_candles: int
    max_overlays: int
    max_overlay_points: int
    max_closed_trades: int
    max_logs: int
    max_decisions: int
    max_warnings: int
    ring_size: int
    ingest_queue_max: int


@dataclass(frozen=True)
class StepTraceSettings:
    queue_max: int
    batch_size: int
    flush_interval_ms: int
    overflow_policy: str


@dataclass(frozen=True)
class TelemetrySettings:
    ws_url: str
    event_poll_ms: int
    emit_queue_max: int
    emit_queue_timeout_ms: int
    emit_retry_ms: int


@dataclass(frozen=True)
class WatchdogSettings:
    heartbeat_interval_seconds: float
    stale_threshold_seconds: float
    monitor_interval_seconds: float
    runner_id: Optional[str]


@dataclass(frozen=True)
class BotRuntimeSettings:
    mode: str
    target: str
    image: Optional[str]
    network: Optional[str]
    bot_id: Optional[str]
    max_symbols_per_strategy: int
    symbol_process_max: Optional[int]
    status_heartbeat_stale_ms: int
    snapshot: SnapshotSettings
    push: PushSettings
    botlens: BotlensSettings
    step_trace: StepTraceSettings
    telemetry: TelemetrySettings
    watchdog: WatchdogSettings


@dataclass(frozen=True)
class IbkrSettings:
    host: str
    port: int
    client_id: int
    default_currency: str
    default_sec_type: str
    default_exchange: str
    what_to_show: str
    symbol_overrides: dict[str, Any]


@dataclass(frozen=True)
class CcxtSettings:
    sandbox_mode: bool
    ohlcv_limit: Optional[int]
    api_key: Optional[str]
    api_secret: Optional[str]
    secret: Optional[str]
    password: Optional[str]


@dataclass(frozen=True)
class AlpacaSettings:
    api_key: Optional[str]
    secret_key: Optional[str]
    paper: bool


@dataclass(frozen=True)
class ProviderSettings:
    runtime: ProviderRuntimeSettings
    ibkr: IbkrSettings
    ccxt: CcxtSettings
    alpaca: AlpacaSettings


@dataclass(frozen=True)
class SecuritySettings:
    provider_credential_key: Optional[str]


@dataclass(frozen=True)
class FrontendBotlensSettings:
    auto_fit_overlay_extents: bool
    debug: bool
    target_render_lag_ms: int
    catchup_render_lag_ms: int
    catchup_seq_behind: int
    catchup_queue_depth: int
    normal_apply_interval_ms: int
    catchup_apply_interval_ms: int
    max_catchup_batch: int
    metrics_publish_ms: int
    snap_candles_behind: int
    ledger_poll_ms: int
    ledger_poll_limit: int
    ledger_max_events: int
    live_resubscribe_limit: int
    live_resubscribe_window_ms: int


@dataclass(frozen=True)
class FrontendSettings:
    api_base_url: str
    botlens: FrontendBotlensSettings


@dataclass(frozen=True)
class AppSettings:
    profile: str
    logging: LoggingSettings
    backend: BackendSettings
    database: DatabaseSettings
    observability: ObservabilitySettings
    async_jobs: AsyncJobSettings
    workers: WorkersSettings
    bot_runtime: BotRuntimeSettings
    providers: ProviderSettings
    security: SecuritySettings
    frontend: FrontendSettings


def _build_settings(payload: Mapping[str, Any]) -> AppSettings:
    logging_payload = _coerce_mapping(payload.get("logging"))
    backend_payload = _coerce_mapping(payload.get("backend"))
    database_payload = _coerce_mapping(payload.get("database"))
    observability_payload = _coerce_mapping(payload.get("observability"))
    async_jobs_payload = _coerce_mapping(payload.get("async_jobs"))
    workers_payload = _coerce_mapping(payload.get("workers"))
    quantlab_payload = _coerce_mapping(workers_payload.get("quantlab"))
    stats_payload = _coerce_mapping(workers_payload.get("stats"))
    bot_runtime_payload = _coerce_mapping(payload.get("bot_runtime"))
    snapshot_payload = _coerce_mapping(bot_runtime_payload.get("snapshot"))
    push_payload = _coerce_mapping(bot_runtime_payload.get("push"))
    botlens_payload = _coerce_mapping(bot_runtime_payload.get("botlens"))
    step_trace_payload = _coerce_mapping(bot_runtime_payload.get("step_trace"))
    telemetry_payload = _coerce_mapping(bot_runtime_payload.get("telemetry"))
    watchdog_payload = _coerce_mapping(bot_runtime_payload.get("watchdog"))
    providers_payload = _coerce_mapping(payload.get("providers"))
    provider_runtime_payload = _coerce_mapping(providers_payload.get("runtime"))
    persistence_payload = _coerce_mapping(provider_runtime_payload.get("persistence"))
    ibkr_payload = _coerce_mapping(providers_payload.get("ibkr"))
    ccxt_payload = _coerce_mapping(providers_payload.get("ccxt"))
    alpaca_payload = _coerce_mapping(providers_payload.get("alpaca"))
    security_payload = _coerce_mapping(payload.get("security"))
    frontend_payload = _coerce_mapping(payload.get("frontend"))
    frontend_botlens_payload = _coerce_mapping(frontend_payload.get("botlens"))

    default_origins = [
        "http://localhost",
        "http://localhost:3000",
        "http://localhost:5173",
        "http://127.0.0.1",
        "http://127.0.0.1:5173",
    ]

    return AppSettings(
        profile=_coerce_str(payload.get("profile"), "dev"),
        logging=LoggingSettings(
            level_name=_coerce_str(logging_payload.get("level"), "INFO"),
            debug=_coerce_bool(logging_payload.get("debug"), False),
            env_name=_coerce_str(logging_payload.get("env_name"), "dev"),
            loki_url=_coerce_optional_str(logging_payload.get("loki_url")),
        ),
        backend=BackendSettings(
            host=_coerce_str(backend_payload.get("host"), "0.0.0.0"),
            port=_coerce_int(backend_payload.get("port"), 8000, minimum=1),
            allowed_origins=_coerce_string_list(backend_payload.get("allowed_origins"), default_origins),
        ),
        database=DatabaseSettings(
            dsn=_coerce_optional_str(database_payload.get("dsn")),
            application_name=_coerce_str(database_payload.get("application_name"), "quant_trad_portal"),
            pool_recycle_seconds=_coerce_int(database_payload.get("pool_recycle_seconds"), 900, minimum=-1),
            pool_timeout_seconds=_coerce_int(database_payload.get("pool_timeout_seconds"), 30, minimum=1),
            connect_timeout_seconds=_coerce_int(database_payload.get("connect_timeout_seconds"), 5, minimum=1),
            pool_pre_ping=_coerce_bool(database_payload.get("pool_pre_ping"), True),
            tcp_keepalive_enabled=_coerce_bool(database_payload.get("tcp_keepalive_enabled"), True),
            tcp_keepalive_idle_seconds=_coerce_int(database_payload.get("tcp_keepalive_idle_seconds"), 30, minimum=1),
            tcp_keepalive_interval_seconds=_coerce_int(
                database_payload.get("tcp_keepalive_interval_seconds"), 10, minimum=1
            ),
            tcp_keepalive_count=_coerce_int(database_payload.get("tcp_keepalive_count"), 3, minimum=1),
            write_retry_attempts=_coerce_int(database_payload.get("write_retry_attempts"), 2, minimum=1),
        ),
        observability=ObservabilitySettings(
            enabled=_coerce_bool(observability_payload.get("enabled"), True),
            step_sample_rate=_coerce_float(observability_payload.get("step_sample_rate"), 0.01, minimum=0.0),
            slow_ms=_coerce_float(observability_payload.get("slow_ms"), 250.0, minimum=0.0),
            log_throttle_seconds=_coerce_float(
                observability_payload.get("log_throttle_seconds"), 30.0, minimum=0.0
            ),
        ),
        async_jobs=AsyncJobSettings(
            running_timeout_seconds=_coerce_float(async_jobs_payload.get("running_timeout_seconds"), 1800.0, minimum=0.0),
            quantlab_job_wait_timeout_seconds=_coerce_float(
                async_jobs_payload.get("quantlab_job_wait_timeout_seconds"), 180.0, minimum=0.1
            ),
            quantlab_job_poll_interval_seconds=_coerce_float(
                async_jobs_payload.get("quantlab_job_poll_interval_seconds"), 0.2, minimum=0.05
            ),
        ),
        workers=WorkersSettings(
            quantlab=WorkerGroupSettings(
                processes=_coerce_int(quantlab_payload.get("processes"), 3, minimum=1),
                index=_coerce_int(quantlab_payload.get("index"), 0, minimum=0),
                total=_coerce_int(quantlab_payload.get("total"), 1, minimum=1),
                idle_sleep_seconds=_coerce_float(quantlab_payload.get("idle_sleep_seconds"), 0.2, minimum=0.05),
                db_wait_timeout_seconds=_coerce_float(
                    quantlab_payload.get("db_wait_timeout_seconds"), 120.0, minimum=0.5
                ),
            ),
            stats=WorkerGroupSettings(
                processes=_coerce_int(stats_payload.get("processes"), 2, minimum=1),
                index=_coerce_int(stats_payload.get("index"), 0, minimum=0),
                total=_coerce_int(stats_payload.get("total"), 1, minimum=1),
                idle_sleep_seconds=_coerce_float(stats_payload.get("idle_sleep_seconds"), 0.25, minimum=0.05),
                db_wait_timeout_seconds=_coerce_float(stats_payload.get("db_wait_timeout_seconds"), 120.0, minimum=0.5),
            ),
        ),
        bot_runtime=BotRuntimeSettings(
            mode=_coerce_str(bot_runtime_payload.get("mode"), "backtest"),
            target=_coerce_str(bot_runtime_payload.get("target"), "docker"),
            image=_coerce_optional_str(bot_runtime_payload.get("image")),
            network=_coerce_optional_str(bot_runtime_payload.get("network")),
            bot_id=_coerce_optional_str(bot_runtime_payload.get("bot_id")),
            max_symbols_per_strategy=_coerce_int(bot_runtime_payload.get("max_symbols_per_strategy"), 10, minimum=1),
            symbol_process_max=(
                None
                if bot_runtime_payload.get("symbol_process_max") in (None, "")
                else _coerce_int(bot_runtime_payload.get("symbol_process_max"), 8, minimum=1)
            ),
            status_heartbeat_stale_ms=_coerce_int(
                bot_runtime_payload.get("status_heartbeat_stale_ms"), 45000, minimum=5000
            ),
            snapshot=SnapshotSettings(
                default_interval_ms=_coerce_int(snapshot_payload.get("default_interval_ms"), 250, minimum=1),
                fast_interval_ms=_coerce_int(snapshot_payload.get("fast_interval_ms"), 250, minimum=1),
                idle_interval_ms=_coerce_int(snapshot_payload.get("idle_interval_ms"), 1000, minimum=1),
                idle_cycles=_coerce_int(snapshot_payload.get("idle_cycles"), 2, minimum=1),
            ),
            push=PushSettings(
                payload_bytes_sample_every=_coerce_int(
                    push_payload.get("payload_bytes_sample_every"), 10, minimum=1
                )
            ),
            botlens=BotlensSettings(
                max_series=_coerce_int(botlens_payload.get("max_series"), 12, minimum=1),
                max_candles=_coerce_int(botlens_payload.get("max_candles"), 320, minimum=50),
                max_overlays=_coerce_int(botlens_payload.get("max_overlays"), 400, minimum=50),
                max_overlay_points=_coerce_int(botlens_payload.get("max_overlay_points"), 160, minimum=20),
                max_closed_trades=_coerce_int(botlens_payload.get("max_closed_trades"), 240, minimum=20),
                max_logs=_coerce_int(botlens_payload.get("max_logs"), 300, minimum=50),
                max_decisions=_coerce_int(botlens_payload.get("max_decisions"), 600, minimum=100),
                max_warnings=_coerce_int(botlens_payload.get("max_warnings"), 120, minimum=20),
                ring_size=_coerce_int(botlens_payload.get("ring_size"), 2048, minimum=32),
                ingest_queue_max=_coerce_int(botlens_payload.get("ingest_queue_max"), 4096, minimum=64),
            ),
            step_trace=StepTraceSettings(
                queue_max=_coerce_int(step_trace_payload.get("queue_max"), 8192, minimum=1),
                batch_size=_coerce_int(step_trace_payload.get("batch_size"), 200, minimum=1),
                flush_interval_ms=_coerce_int(step_trace_payload.get("flush_interval_ms"), 200, minimum=1),
                overflow_policy=_coerce_str(step_trace_payload.get("overflow_policy"), "drop_oldest"),
            ),
            telemetry=TelemetrySettings(
                ws_url=_coerce_str(
                    telemetry_payload.get("ws_url"),
                    "ws://backend.quanttrad:8000/api/bots/ws/telemetry/ingest",
                ),
                event_poll_ms=_coerce_int(telemetry_payload.get("event_poll_ms"), 50, minimum=10),
                emit_queue_max=_coerce_int(telemetry_payload.get("emit_queue_max"), 256, minimum=8),
                emit_queue_timeout_ms=_coerce_int(telemetry_payload.get("emit_queue_timeout_ms"), 1000, minimum=10),
                emit_retry_ms=_coerce_int(telemetry_payload.get("emit_retry_ms"), 250, minimum=50),
            ),
            watchdog=WatchdogSettings(
                heartbeat_interval_seconds=_coerce_float(
                    watchdog_payload.get("heartbeat_interval_seconds"), 15.0, minimum=0.1
                ),
                stale_threshold_seconds=_coerce_float(
                    watchdog_payload.get("stale_threshold_seconds"), 60.0, minimum=0.1
                ),
                monitor_interval_seconds=_coerce_float(
                    watchdog_payload.get("monitor_interval_seconds"), 30.0, minimum=0.1
                ),
                runner_id=_coerce_optional_str(watchdog_payload.get("runner_id")),
            ),
        ),
        providers=ProviderSettings(
            runtime=ProviderRuntimeSettings(
                history_segment_points=_coerce_int(
                    provider_runtime_payload.get("history_segment_points"), 1000, minimum=1
                ),
                persistence=ProviderPersistenceSettings(
                    candles_raw_table=_coerce_str(persistence_payload.get("candles_raw_table"), "market_candles_raw"),
                    candle_stats_table=_coerce_str(persistence_payload.get("candle_stats_table"), "candle_stats"),
                    regime_stats_table=_coerce_str(persistence_payload.get("regime_stats_table"), "regime_stats"),
                    regime_blocks_table=_coerce_str(persistence_payload.get("regime_blocks_table"), "regime_blocks"),
                    derivatives_state_table=_coerce_str(
                        persistence_payload.get("derivatives_state_table"), "derivatives_market_state"
                    ),
                    closures_table=_coerce_str(
                        persistence_payload.get("closures_table"), "portal_candle_closures"
                    ),
                ),
            ),
            ibkr=IbkrSettings(
                host=_coerce_str(ibkr_payload.get("host"), "ibkr-gateway"),
                port=_coerce_int(ibkr_payload.get("port"), 4002, minimum=1),
                client_id=_coerce_int(ibkr_payload.get("client_id"), 1, minimum=0),
                default_currency=_coerce_str(ibkr_payload.get("default_currency"), "USD").upper(),
                default_sec_type=_coerce_str(ibkr_payload.get("default_sec_type"), "STK").upper(),
                default_exchange=_coerce_str(ibkr_payload.get("default_exchange"), "SMART").upper(),
                what_to_show=_coerce_str(ibkr_payload.get("what_to_show"), "TRADES"),
                symbol_overrides=_coerce_optional_mapping(ibkr_payload.get("symbol_overrides")),
            ),
            ccxt=CcxtSettings(
                sandbox_mode=_coerce_bool(ccxt_payload.get("sandbox_mode"), False),
                ohlcv_limit=(
                    None
                    if ccxt_payload.get("ohlcv_limit") in (None, "")
                    else _coerce_int(ccxt_payload.get("ohlcv_limit"), 500, minimum=1)
                ),
                api_key=_coerce_optional_str(ccxt_payload.get("api_key")),
                api_secret=_coerce_optional_str(ccxt_payload.get("api_secret")),
                secret=_coerce_optional_str(ccxt_payload.get("secret")),
                password=_coerce_optional_str(ccxt_payload.get("password")),
            ),
            alpaca=AlpacaSettings(
                api_key=_coerce_optional_str(alpaca_payload.get("api_key")),
                secret_key=_coerce_optional_str(alpaca_payload.get("secret_key")),
                paper=_coerce_bool(alpaca_payload.get("paper"), True),
            ),
        ),
        security=SecuritySettings(
            provider_credential_key=_coerce_optional_str(security_payload.get("provider_credential_key"))
        ),
        frontend=FrontendSettings(
            api_base_url=_coerce_str(frontend_payload.get("api_base_url"), "/api"),
            botlens=FrontendBotlensSettings(
                auto_fit_overlay_extents=_coerce_bool(
                    frontend_botlens_payload.get("auto_fit_overlay_extents"), False
                ),
                debug=_coerce_bool(frontend_botlens_payload.get("debug"), False),
                target_render_lag_ms=_coerce_int(
                    frontend_botlens_payload.get("target_render_lag_ms"), 120, minimum=1
                ),
                catchup_render_lag_ms=_coerce_int(
                    frontend_botlens_payload.get("catchup_render_lag_ms"), 1200, minimum=1
                ),
                catchup_seq_behind=_coerce_int(frontend_botlens_payload.get("catchup_seq_behind"), 6, minimum=1),
                catchup_queue_depth=_coerce_int(frontend_botlens_payload.get("catchup_queue_depth"), 8, minimum=1),
                normal_apply_interval_ms=_coerce_int(
                    frontend_botlens_payload.get("normal_apply_interval_ms"), 33, minimum=1
                ),
                catchup_apply_interval_ms=_coerce_int(
                    frontend_botlens_payload.get("catchup_apply_interval_ms"), 12, minimum=1
                ),
                max_catchup_batch=_coerce_int(frontend_botlens_payload.get("max_catchup_batch"), 2, minimum=1),
                metrics_publish_ms=_coerce_int(frontend_botlens_payload.get("metrics_publish_ms"), 120, minimum=1),
                snap_candles_behind=_coerce_int(
                    frontend_botlens_payload.get("snap_candles_behind"), 30, minimum=1
                ),
                ledger_poll_ms=_coerce_int(frontend_botlens_payload.get("ledger_poll_ms"), 800, minimum=1),
                ledger_poll_limit=_coerce_int(frontend_botlens_payload.get("ledger_poll_limit"), 500, minimum=1),
                ledger_max_events=_coerce_int(frontend_botlens_payload.get("ledger_max_events"), 3000, minimum=1),
                live_resubscribe_limit=_coerce_int(
                    frontend_botlens_payload.get("live_resubscribe_limit"), 3, minimum=1
                ),
                live_resubscribe_window_ms=_coerce_int(
                    frontend_botlens_payload.get("live_resubscribe_window_ms"), 30000, minimum=1
                ),
            ),
        ),
    )


def get_settings(*, force_reload: bool = False) -> AppSettings:
    global _SETTINGS_CACHE
    if force_reload or _SETTINGS_CACHE is None:
        raw = _load_raw_config()
        _SETTINGS_CACHE = _build_settings(raw)
        logger.debug("settings_loaded | profile=%s", _SETTINGS_CACHE.profile)
    return _SETTINGS_CACHE


def clear_settings_cache() -> None:
    global _SETTINGS_CACHE
    _SETTINGS_CACHE = None


def env_value(name: str) -> Optional[str]:
    ensure_env_loaded()
    value = os.environ.get(str(name))
    if value is None:
        return None
    return str(value)


def env_is_set(name: str) -> bool:
    value = env_value(name)
    return value not in (None, "")


def resolve_ccxt_credentials(exchange_id: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    ensure_env_loaded()
    settings = get_settings()
    upper = str(exchange_id or "").strip().upper()
    if upper:
        prefix = f"QT_PROVIDERS_CCXT_{upper}_"
        api_key = _coerce_optional_str(os.getenv(prefix + "API_KEY"))
        api_secret = _coerce_optional_str(os.getenv(prefix + "API_SECRET"))
        api_password = _coerce_optional_str(os.getenv(prefix + "API_PASSWORD"))
        if api_key or api_secret or api_password:
            return api_key, api_secret, api_password
    return settings.providers.ccxt.api_key, settings.providers.ccxt.api_secret or settings.providers.ccxt.secret, settings.providers.ccxt.password


__all__ = [
    "AppSettings",
    "AsyncJobSettings",
    "BackendSettings",
    "BotRuntimeSettings",
    "BotlensSettings",
    "DatabaseSettings",
    "FrontendSettings",
    "IbkrSettings",
    "LoggingSettings",
    "ObservabilitySettings",
    "ProviderRuntimeSettings",
    "SecuritySettings",
    "TelemetrySettings",
    "WatchdogSettings",
    "clear_settings_cache",
    "ensure_env_loaded",
    "env_is_set",
    "env_value",
    "get_settings",
    "resolve_ccxt_credentials",
]
