"""Explicit runtime composition root for portal bot/runtime services.

This module centralizes runtime wiring so service modules can stay focused on
behavior and avoid hidden deep imports for persistence and runtime control.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Mapping, Optional, Protocol

from .bot_stream import BotStreamManager

if TYPE_CHECKING:
    from .bot_watchdog import BotWatchdog


class RuntimeMode(str, Enum):
    """Supported runtime composition modes."""

    BACKTEST = "backtest"
    PAPER = "paper"
    LIVE = "live"


class BotStorageGateway(Protocol):
    """Storage boundary used by bot service/runtime control composition."""

    def upsert_bot(self, payload: Mapping[str, Any]) -> None: ...

    def list_bot_runs(self, *, bot_id: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]: ...

    def get_latest_bot_run_view_state(
        self,
        *,
        bot_id: str,
        run_id: Optional[str] = None,
        series_key: Optional[str] = None,
    ) -> Optional[Mapping[str, Any]]: ...


@dataclass(frozen=True)
class RuntimeComposition:
    """Composed runtime collaborators used by bot-facing portal services."""

    mode: RuntimeMode
    stream_manager: BotStreamManager
    config_service: Any
    runtime_control_service: Any
    storage: BotStorageGateway
    watchdog: Any


def _normalize_mode(value: Optional[str]) -> RuntimeMode:
    raw = str(value or RuntimeMode.BACKTEST.value).strip().lower()
    try:
        return RuntimeMode(raw)
    except ValueError as exc:
        raise RuntimeError(
            f"Unsupported runtime mode '{raw}'. "
            f"Expected one of: {', '.join(mode.value for mode in RuntimeMode)}"
        ) from exc


def _runtime_mode_from_env() -> RuntimeMode:
    return _normalize_mode(os.getenv("BOT_RUNTIME_MODE", RuntimeMode.BACKTEST.value))


def _build_storage_gateway() -> BotStorageGateway:
    """Build the default storage gateway with lazy import-time wiring."""

    from ..storage import storage as storage_module

    class _Gateway:
        def upsert_bot(self, payload: Mapping[str, Any]) -> None:
            storage_module.upsert_bot(dict(payload))

        def list_bot_runs(self, *, bot_id: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
            rows = storage_module.list_bot_runs(bot_id=bot_id)
            if limit and int(limit) > 0:
                return list(rows)[: int(limit)]
            return list(rows)

        def get_latest_bot_run_view_state(
            self,
            *,
            bot_id: str,
            run_id: Optional[str] = None,
            series_key: Optional[str] = None,
        ) -> Optional[Mapping[str, Any]]:
            return storage_module.get_latest_bot_run_view_state(
                bot_id=bot_id,
                run_id=run_id,
                series_key=series_key,
            )

    return _Gateway()


def _build_common_composition(
    *,
    mode: RuntimeMode,
    stream_manager: Optional[BotStreamManager] = None,
    config_service: Optional[Any] = None,
    storage: Optional[BotStorageGateway] = None,
    watchdog: Optional[Any] = None,
    runner_factory: Optional[Callable[[], Any]] = None,
) -> RuntimeComposition:
    resolved_stream = stream_manager or BotStreamManager()
    if config_service is None:
        from .config_service import BotConfigService

        resolved_config = BotConfigService()
    else:
        resolved_config = config_service

    resolved_storage = storage or _build_storage_gateway()
    if watchdog is None:
        from .bot_watchdog import get_watchdog

        resolved_watchdog = get_watchdog()
    else:
        resolved_watchdog = watchdog

    from .runtime_control_service import BotRuntimeControlService

    runtime_control = BotRuntimeControlService(
        resolved_config,
        resolved_stream,
        storage=resolved_storage,
        watchdog=resolved_watchdog,
        runner_factory=runner_factory,
    )
    return RuntimeComposition(
        mode=mode,
        stream_manager=resolved_stream,
        config_service=resolved_config,
        runtime_control_service=runtime_control,
        storage=resolved_storage,
        watchdog=resolved_watchdog,
    )


def build_backtest_runtime_composition(
    **kwargs: Any,
) -> RuntimeComposition:
    """Build runtime composition for backtest mode."""

    return _build_common_composition(mode=RuntimeMode.BACKTEST, **kwargs)


def build_paper_runtime_composition(
    **kwargs: Any,
) -> RuntimeComposition:
    """Build runtime composition for paper mode.

    Note: this currently reuses the backtest wiring shape while reserving an
    explicit composition seam for paper-specific collaborators.
    """

    return _build_common_composition(mode=RuntimeMode.PAPER, **kwargs)


def build_live_runtime_composition(
    **kwargs: Any,
) -> RuntimeComposition:
    """Build runtime composition for live mode.

    Note: this currently reuses the backtest wiring shape while reserving an
    explicit composition seam for live-specific collaborators.
    """

    return _build_common_composition(mode=RuntimeMode.LIVE, **kwargs)


def build_runtime_composition(
    *,
    mode: RuntimeMode | str | None = None,
    **kwargs: Any,
) -> RuntimeComposition:
    """Build runtime composition for production or tests."""

    resolved_mode = _normalize_mode(mode if isinstance(mode, str) or mode is None else mode.value)
    if resolved_mode == RuntimeMode.BACKTEST:
        return build_backtest_runtime_composition(**kwargs)
    if resolved_mode == RuntimeMode.PAPER:
        return build_paper_runtime_composition(**kwargs)
    if resolved_mode == RuntimeMode.LIVE:
        return build_live_runtime_composition(**kwargs)
    raise RuntimeError(f"Unsupported runtime mode '{resolved_mode.value}'")


_RUNTIME_COMPOSITIONS: Dict[RuntimeMode, RuntimeComposition] = {}


def get_runtime_composition(*, mode: RuntimeMode | str | None = None) -> RuntimeComposition:
    """Return process-level runtime composition singleton per mode."""

    resolved_mode = _normalize_mode(mode if isinstance(mode, str) or mode is None else mode.value)
    if resolved_mode not in _RUNTIME_COMPOSITIONS:
        _RUNTIME_COMPOSITIONS[resolved_mode] = build_runtime_composition(mode=resolved_mode)
    return _RUNTIME_COMPOSITIONS[resolved_mode]


def set_runtime_composition_for_tests(composition: RuntimeComposition) -> None:
    """Install a custom runtime composition for tests."""

    _RUNTIME_COMPOSITIONS[composition.mode] = composition


def clear_runtime_compositions_for_tests() -> None:
    """Clear cached runtime compositions for deterministic tests."""

    _RUNTIME_COMPOSITIONS.clear()


__all__ = [
    "BotStorageGateway",
    "RuntimeMode",
    "RuntimeComposition",
    "build_runtime_composition",
    "build_backtest_runtime_composition",
    "build_paper_runtime_composition",
    "build_live_runtime_composition",
    "get_runtime_composition",
    "set_runtime_composition_for_tests",
    "clear_runtime_compositions_for_tests",
]
