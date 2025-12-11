from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Set, Tuple

from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator
from signals.base import BaseSignal
from signals.engine.market_profile_generator import build_value_area_payloads
from signals.rules.market_profile import MarketProfileBreakoutConfig
from signals.rules.pivot import PivotBreakoutConfig

from .context import IndicatorServiceContext, _context
from .utils import (
    coerce_int,
    ensure_color,
    get_indicator_entry,
    normalize_datasource,
    normalize_exchange,
    resolve_data_provider,
)

logger = logging.getLogger(__name__)

_DEFAULT_PIVOT_BREAKOUT_CONFIG = PivotBreakoutConfig()
_DEFAULT_MARKET_PROFILE_BREAKOUT_CONFIG = MarketProfileBreakoutConfig()


@dataclass
class BreakoutCacheContext:
    cache_spec: Optional[Any]
    cache_key: Optional[Tuple[Any, ...]]
    requested_rule_ids: Optional[Set[str]]
    using_cached_breakouts: bool = False
    drop_breakout_from_response: bool = False


class IndicatorSignalExecutor:
    """Execute indicator signal rules with breakout cache support."""

    def __init__(self, ctx: IndicatorServiceContext = _context) -> None:
        self._ctx = ctx

    def execute(
        self,
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        *,
        symbol: Optional[str] = None,
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        entry = self._load_entry(inst_id, start, end, interval, symbol)
        sym = self._resolve_symbol(entry, symbol)
        provider, data_ctx = self._prepare_provider(
            entry.meta, sym, start, end, interval, datasource, exchange
        )
        df = self._load_candles(provider, data_ctx, inst_id, sym, interval)
        rule_config, cache_ctx = self._prepare_rule_config(
            entry.instance, entry.meta, df, sym, interval, start, end, config
        )
        signals_all = self._run_rules(
            entry.instance, df, rule_config, inst_id, sym, interval, start, end
        )
        filtered = self._filter_signals(signals_all, cache_ctx)
        self._persist_breakout_cache(signals_all, cache_ctx, inst_id)
        payload = ensure_color(dict(entry.meta), ctx=self._ctx)
        payload["signals"] = filtered
        return payload

    def _load_entry(
        self, inst_id: str, start: str, end: str, interval: str, symbol: Optional[str]
    ):
        return get_indicator_entry(
            inst_id,
            fallback_context={
                "symbol": symbol,
                "start": start,
                "end": end,
                "interval": interval,
            },
            ctx=self._ctx,
        )

    def _resolve_symbol(self, entry, symbol: Optional[str]) -> str:
        base_params = entry.meta.get("params", {})
        sym = symbol or base_params.get("symbol")
        if not sym:
            raise ValueError("Stored indicator has no symbol and none was provided")
        return sym

    def _prepare_provider(
        self,
        meta: Mapping[str, Any],
        symbol: str,
        start: str,
        end: str,
        interval: str,
        datasource: Optional[str],
        exchange: Optional[str],
    ):
        stored_params = meta.get("params", {})
        stored_datasource = normalize_datasource(
            meta.get("datasource") or stored_params.get("datasource"), ctx=self._ctx
        )
        stored_exchange = normalize_exchange(
            meta.get("exchange") or stored_params.get("exchange"), ctx=self._ctx
        )

        req_datasource = normalize_datasource(datasource, ctx=self._ctx)
        req_exchange = normalize_exchange(exchange, ctx=self._ctx)

        effective_datasource = req_datasource or stored_datasource
        effective_exchange = req_exchange or stored_exchange
        if effective_exchange and not effective_datasource:
            effective_datasource = "ccxt"

        provider = resolve_data_provider(
            effective_datasource,
            exchange=effective_exchange,
            ctx=self._ctx,
        )
        data_ctx = DataContext(symbol=symbol, start=start, end=end, interval=interval)
        return provider, data_ctx

    def _load_candles(self, provider, data_ctx: DataContext, inst_id: str, symbol: str, interval: str):
        logger.info(
            "event=indicator_signal_prepare indicator=%s symbol=%s interval=%s start=%s end=%s",
            inst_id,
            symbol,
            interval,
            data_ctx.start,
            data_ctx.end,
        )
        df = provider.get_ohlcv(data_ctx)
        if df is None or df.empty:
            raise LookupError("No candles available for given window")
        return df

    def _prepare_rule_config(
        self,
        instance,
        meta: Mapping[str, Any],
        df,
        symbol: str,
        interval: str,
        start: str,
        end: str,
        config: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], BreakoutCacheContext]:
        rule_config: Dict[str, Any] = dict(config or {})
        stored_params = meta.get("params", {}) if isinstance(meta, Mapping) else {}
        self._apply_breakout_defaults(instance, stored_params, rule_config)
        self._maybe_add_market_profile_payloads(instance, df, rule_config, interval)
        rule_config.setdefault("symbol", symbol)

        indicator_name = getattr(instance, "NAME", instance.__class__.__name__)
        cache_spec = self._ctx.breakout_cache.spec_for(indicator_name)
        requested_rule_ids = self._normalise_enabled_rules(rule_config)
        cache_ctx = BreakoutCacheContext(
            cache_spec=cache_spec,
            cache_key=None,
            requested_rule_ids=requested_rule_ids,
        )
        if cache_spec is not None:
            cache_ctx.cache_key = self._ctx.breakout_cache.build_cache_key(
                meta.get("id"),
                indicator_name,
                symbol,
                interval,
                start,
                end,
                cache_spec.config_signature_builder(rule_config),
            )
            self._maybe_hydrate_cache_context(rule_config, cache_ctx, inst_id=meta.get("id"))
        return rule_config, cache_ctx

    def _apply_breakout_defaults(
        self, instance, stored_params: Mapping[str, Any], rule_config: Dict[str, Any]
    ) -> None:
        if "pivot_breakout_confirmation_bars" not in rule_config:
            pivot_bars = stored_params.get("pivot_breakout_confirmation_bars")
            if pivot_bars is None and hasattr(instance, "pivot_breakout_confirmation_bars"):
                pivot_bars = getattr(instance, "pivot_breakout_confirmation_bars")
            rule_config["pivot_breakout_confirmation_bars"] = coerce_int(
                pivot_bars,
                _DEFAULT_PIVOT_BREAKOUT_CONFIG.confirmation_bars,
                minimum=1,
            )
        if "market_profile_breakout_confirmation_bars" not in rule_config:
            mp_bars = stored_params.get("market_profile_breakout_confirmation_bars")
            if mp_bars is None and hasattr(instance, "market_profile_breakout_confirmation_bars"):
                mp_bars = getattr(instance, "market_profile_breakout_confirmation_bars")
            rule_config["market_profile_breakout_confirmation_bars"] = coerce_int(
                mp_bars,
                _DEFAULT_MARKET_PROFILE_BREAKOUT_CONFIG.confirmation_bars,
                minimum=1,
            )

    def _maybe_add_market_profile_payloads(
        self, instance, df, rule_config: Dict[str, Any], interval: str
    ) -> None:
        if isinstance(instance, MarketProfileIndicator) and "rule_payloads" not in rule_config:
            rule_config.setdefault(
                "market_profile_use_merged_value_areas",
                getattr(instance, "use_merged_value_areas", True),
            )
            rule_config.setdefault(
                "market_profile_merge_threshold",
                getattr(instance, "merge_threshold", 0.6),
            )
            payloads = build_value_area_payloads(
                instance,
                df,
                interval=interval,
                use_merged=rule_config.get("market_profile_use_merged_value_areas"),
                merge_threshold=rule_config.get("market_profile_merge_threshold"),
                min_merge_sessions=rule_config.get("market_profile_merge_min_sessions"),
            )
            rule_config["rule_payloads"] = payloads

    def _normalise_enabled_rules(
        self, rule_config: Dict[str, Any]
    ) -> Optional[Set[str]]:
        enabled_rules_config = rule_config.get("enabled_rules")
        if enabled_rules_config is None:
            return None
        normalised_rules: List[str] = []
        seen: Set[str] = set()
        for rule_id in enabled_rules_config:
            if rule_id is None:
                continue
            rule_str = str(rule_id).strip()
            if not rule_str:
                continue
            norm = rule_str.lower()
            if norm not in seen:
                normalised_rules.append(norm)
                seen.add(norm)
        if normalised_rules:
            rule_config["enabled_rules"] = normalised_rules
            return set(normalised_rules)
        rule_config.pop("enabled_rules", None)
        return None

    def _maybe_hydrate_cache_context(
        self, rule_config: Dict[str, Any], cache_ctx: BreakoutCacheContext, *, inst_id: Optional[str]
    ) -> None:
        cache_spec = cache_ctx.cache_spec
        if cache_spec is None or cache_ctx.cache_key is None:
            return
        if (
            cache_ctx.requested_rule_ids
            and cache_spec.retest_rule_id in cache_ctx.requested_rule_ids
            and cache_spec.breakout_rule_id not in cache_ctx.requested_rule_ids
        ):
            cached_breakouts = self._ctx.breakout_cache.get_cached_breakouts(cache_ctx.cache_key)
            if cached_breakouts:
                cache_ctx.using_cached_breakouts = True
                rule_config[cache_spec.cache_context_key] = cached_breakouts
                rule_config[cache_spec.ready_flag_key] = True
                if cache_spec.initialised_flag_key:
                    rule_config[cache_spec.initialised_flag_key] = True
                for extra_key, extra_value in cache_spec.context_defaults.items():
                    rule_config.setdefault(extra_key, extra_value)
                logger.debug(
                    "event=indicator_breakout_cache_hit indicator=%s rule=%s entries=%d",
                    inst_id,
                    cache_spec.breakout_rule_id,
                    len(cached_breakouts),
                )
            else:
                cache_ctx.drop_breakout_from_response = True
                current_rules = list(rule_config.get("enabled_rules", []))
                if cache_spec.breakout_rule_id not in current_rules:
                    current_rules.append(cache_spec.breakout_rule_id)
                if current_rules:
                    rule_config["enabled_rules"] = current_rules
                    cache_ctx.requested_rule_ids = set(current_rules)
                logger.debug(
                    "event=indicator_breakout_cache_miss indicator=%s rule=%s",
                    inst_id,
                    cache_spec.breakout_rule_id,
                    )

    def _run_rules(
        self,
        instance,
        df,
        rule_config: Dict[str, Any],
        inst_id: str,
        symbol: str,
        interval: str,
        start: str,
        end: str,
    ):
        indicator_name = getattr(instance, "NAME", instance.__class__.__name__)
        noisy_keys = {"rule_payloads"}
        cache_spec = self._ctx.breakout_cache.spec_for(indicator_name)
        if cache_spec is not None:
            noisy_keys.add(cache_spec.cache_context_key)
        log_config: Dict[str, Any] = {}
        for key, value in rule_config.items():
            if key in noisy_keys:
                try:
                    length = len(value)  # type: ignore[arg-type]
                except Exception:
                    length = "?"
                log_config[key] = f"<{key}:len={length}>"
            else:
                log_config[key] = value
        logger.info(
            "event=indicator_signal_execute indicator=%s name=%s symbol=%s interval=%s start=%s end=%s config=%s",
            inst_id,
            indicator_name,
            symbol,
            interval,
            start,
            end,
            log_config,
        )
        return self._ctx.signal_runner.run_rules(instance, df, **rule_config)

    def _persist_breakout_cache(
        self, signals_all: Sequence[BaseSignal], cache_ctx: BreakoutCacheContext, inst_id: str
    ) -> None:
        cache_spec = cache_ctx.cache_spec
        if cache_spec is None or cache_ctx.cache_key is None or cache_ctx.using_cached_breakouts:
            return
        enabled_for_run = cache_ctx.requested_rule_ids
        ran_breakout = enabled_for_run is None or cache_spec.breakout_rule_id in enabled_for_run
        if ran_breakout:
            breakout_payloads = [
                self._ctx.breakout_cache.flatten_breakout_signal(sig)
                for sig in signals_all
                if sig.type == "breakout"
            ]
            self._ctx.breakout_cache.store_breakout_cache(cache_ctx.cache_key, breakout_payloads)
            logger.debug(
                "event=indicator_breakout_cache_store indicator=%s rule=%s entries=%d",
                inst_id,
                cache_spec.breakout_rule_id,
                len(breakout_payloads),
            )

    def _filter_signals(
        self, signals_all: Sequence[BaseSignal], cache_ctx: BreakoutCacheContext
    ) -> Sequence[BaseSignal]:
        filtered_signals = signals_all
        if cache_ctx.requested_rule_ids is not None:
            filtered_signals = [
                sig for sig in signals_all if sig.type in cache_ctx.requested_rule_ids
            ]
            if cache_ctx.drop_breakout_from_response:
                filtered_signals = [
                    sig for sig in filtered_signals if sig.type != "breakout"
                ]
        if len(filtered_signals) != len(signals_all):
            logger.debug(
                "event=indicator_signal_filtered indicator=%s total=%d returned=%d",
                cache_ctx.cache_key,
                len(signals_all),
                len(filtered_signals),
            )
        return filtered_signals


__all__ = ["IndicatorSignalExecutor", "BreakoutCacheContext"]
