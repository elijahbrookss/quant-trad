from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from indicators.config import DataContext
from indicators.market_profile import MarketProfileIndicator
from signals.base import BaseSignal
from signals.engine.market_profile import resolve_market_profile_params
from signals.engine.market_profile_generator import build_value_area_payloads
from signals.rules.market_profile import MarketProfileBreakoutConfig
from signals.rules.pivot import PivotBreakoutConfig

from .context import IndicatorServiceContext, _context
from ...market import instrument_service
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
        entry = self._load_entry(inst_id, start, end, interval, symbol, datasource, exchange)
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
        overlays = self._ctx.signal_runner.build_overlays(
            entry.instance, filtered, df, **rule_config
        )
        payload = ensure_color(dict(entry.meta), ctx=self._ctx)
        # Convert BaseSignal objects to dicts for JSON serialization and strategy evaluation
        payload["signals"] = [sig.to_dict() if hasattr(sig, "to_dict") else sig for sig in filtered]
        payload["overlays"] = overlays
        return payload

    def _load_entry(
        self,
        inst_id: str,
        start: str,
        end: str,
        interval: str,
        symbol: Optional[str],
        datasource: Optional[str] = None,
        exchange: Optional[str] = None,
    ):
        return get_indicator_entry(
            inst_id,
            fallback_context={
                "symbol": symbol,
                "start": start,
                "end": end,
                "interval": interval,
                "datasource": datasource,
                "exchange": exchange,
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

        logger.info(
            "event=signal_executor_prepare_provider indicator_id=%s symbol=%s "
            "req_datasource=%s req_exchange=%s stored_datasource=%s stored_exchange=%s "
            "effective_datasource=%s effective_exchange=%s",
            meta.get("id"),
            symbol,
            req_datasource,
            req_exchange,
            stored_datasource,
            stored_exchange,
            effective_datasource,
            effective_exchange,
        )

        # resolve_data_provider will raise ValueError if effective_datasource is None
        provider = resolve_data_provider(
            effective_datasource,
            exchange=effective_exchange,
            ctx=self._ctx,
        )
        instrument_id = instrument_service.require_instrument_id(
            effective_datasource,
            effective_exchange,
            symbol,
        )
        data_ctx = DataContext(
            symbol=symbol,
            start=start,
            end=end,
            interval=interval,
            instrument_id=instrument_id,
        )
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
        self._maybe_add_market_profile_payloads(instance, df, rule_config, interval, symbol)
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
        self, instance, df, rule_config: Dict[str, Any], interval: str, symbol: str
    ) -> None:
        if isinstance(instance, MarketProfileIndicator) and "rule_payloads" not in rule_config:
            params = resolve_market_profile_params(
                instance,
                use_merged_value_areas=rule_config.get("market_profile_use_merged_value_areas"),
                merge_threshold=rule_config.get("market_profile_merge_threshold"),
                min_merge_sessions=rule_config.get("market_profile_merge_min_sessions"),
            )
            rule_config["market_profile_use_merged_value_areas"] = params.use_merged_value_areas
            rule_config["market_profile_merge_threshold"] = params.merge_threshold
            rule_config["market_profile_merge_min_sessions"] = params.min_merge_sessions

            # Log merge parameters to diagnose discrepancies between Signal Preview and Generate Signals
            logger.info(
                "market_profile_merge_config | use_merged=%s | threshold=%s | min_sessions=%s | from_config=%s | instance_values=[use_merged=%s, threshold=%s, min_sessions=%s]",
                rule_config.get("market_profile_use_merged_value_areas"),
                rule_config.get("market_profile_merge_threshold"),
                rule_config.get("market_profile_merge_min_sessions"),
                {
                    "has_use_merged": "market_profile_use_merged_value_areas" in rule_config,
                    "has_threshold": "market_profile_merge_threshold" in rule_config,
                    "has_min_sessions": "market_profile_merge_min_sessions" in rule_config,
                },
                getattr(instance, "use_merged_value_areas", None),
                getattr(instance, "merge_threshold", None),
                getattr(instance, "min_merge_sessions", None),
            )

            payloads = build_value_area_payloads(
                instance,
                df,
                runtime_indicator=instance,
                interval=interval,
                symbol=symbol,
                use_merged=params.use_merged_value_areas,
                merge_threshold=params.merge_threshold,
                min_merge_sessions=params.min_merge_sessions,
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
        requested: Set[str] = set()
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
                requested.update(self._expand_rule_identifier(norm))
        if normalised_rules:
            rule_config["enabled_rules"] = normalised_rules
            return requested
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
            logger.info(
                "signal_filtering | raw_signals=%d | requested_rule_ids=%s | drop_breakout=%s",
                len(signals_all),
                sorted(cache_ctx.requested_rule_ids) if cache_ctx.requested_rule_ids else None,
                cache_ctx.drop_breakout_from_response,
            )
            filtered_signals = []
            matched_count = 0
            for idx, sig in enumerate(signals_all):
                identifiers = self._collect_signal_identifiers(sig)
                intersection = identifiers.intersection(cache_ctx.requested_rule_ids)
                matched = bool(intersection)
                if matched:
                    filtered_signals.append(sig)
                    matched_count += 1
                # Log first 3 signals for debugging
                if idx < 3:
                    # Extract metadata for debugging
                    sig_metadata = getattr(sig, "metadata", None)
                    metadata_keys = list(sig_metadata.keys()) if isinstance(sig_metadata, dict) else None
                    metadata_rule_id = sig_metadata.get("rule_id") if isinstance(sig_metadata, dict) else None
                    metadata_aliases = sig_metadata.get("aliases") if isinstance(sig_metadata, dict) else None

                    logger.info(
                        "signal_filtering_debug | signal_idx=%d | signal_type=%s | has_metadata=%s | metadata_keys=%s | metadata_rule_id=%s | metadata_aliases=%s | collected_identifiers=%s | requested_ids=%s | matched=%s | intersection=%s",
                        idx,
                        getattr(sig, "type", None),
                        sig_metadata is not None,
                        metadata_keys,
                        metadata_rule_id,
                        metadata_aliases,
                        sorted(identifiers) if identifiers else [],
                        sorted(cache_ctx.requested_rule_ids),
                        matched,
                        sorted(intersection) if intersection else [],
                    )
            logger.info(
                "signal_filtering_after_rules | filtered_signals=%d | matched=%d | dropped=%d",
                len(filtered_signals),
                matched_count,
                len(signals_all) - matched_count,
            )
            if cache_ctx.drop_breakout_from_response:
                before_drop = len(filtered_signals)
                filtered_signals = [
                    sig for sig in filtered_signals if sig.type != "breakout"
                ]
                logger.info(
                    "signal_filtering_after_breakout_drop | before=%d | after=%d",
                    before_drop,
                    len(filtered_signals),
                )
        if len(filtered_signals) != len(signals_all):
            logger.debug(
                "event=indicator_signal_filtered indicator=%s total=%d returned=%d requested_rules=%s",
                cache_ctx.cache_key,
                len(signals_all),
                len(filtered_signals),
                sorted(cache_ctx.requested_rule_ids) if cache_ctx.requested_rule_ids else None,
            )
        return filtered_signals

    def _collect_signal_identifiers(self, signal: BaseSignal) -> Set[str]:
        identifiers: Set[str] = set()

        def _append(value: Any) -> None:
            if isinstance(value, str):
                normalised = value.strip().lower()
                if normalised:
                    identifiers.add(normalised)
            elif isinstance(value, Iterable) and not isinstance(
                value, (str, bytes, Mapping)
            ):
                for item in value:
                    _append(item)

        base_fields: Dict[str, Any] = {}
        if getattr(signal, "type", None):
            base_fields["type"] = signal.type

        sources: List[Mapping[str, Any]] = [base_fields]
        metadata = getattr(signal, "metadata", None)
        if isinstance(metadata, Mapping):
            sources.append(metadata)

        keys = ("rule_id", "pattern_id", "signal_id", "pattern", "id", "type")
        alias_keys = ("aliases", "rule_aliases", "pattern_aliases", "signal_aliases")

        for source in sources:
            for key in keys:
                _append(source.get(key))
            for alias_key in alias_keys:
                _append(source.get(alias_key))

        expanded: Set[str] = set(identifiers)
        for identifier in identifiers:
            expanded.update(self._expand_rule_identifier(identifier))

        return expanded

    @staticmethod
    def _expand_rule_identifier(identifier: str) -> Set[str]:
        variants = {identifier}
        if identifier.endswith("_rule"):
            variants.add(identifier[: -len("_rule")])
        else:
            variants.add(f"{identifier}_rule")
        return variants


__all__ = ["IndicatorSignalExecutor", "BreakoutCacheContext"]
