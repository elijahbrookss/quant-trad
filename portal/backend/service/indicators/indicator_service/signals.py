from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from engines.bot_runtime.core.domain import Candle
from engines.bot_runtime.core.indicator_state import ensure_builtin_indicator_plugins_registered
from engines.bot_runtime.core.indicator_state.plugins import plugin_registry
from indicators.config import DataContext
from signals.base import BaseSignal

from .context import IndicatorServiceContext, _context
from ...market import instrument_service
from .utils import (
    ensure_color,
    get_indicator_entry,
    normalize_datasource,
    normalize_exchange,
    resolve_data_provider,
)

logger = logging.getLogger(__name__)


@dataclass
class BreakoutCacheContext:
    cache_spec: Optional[Any]
    cache_key: Optional[Tuple[Any, ...]]
    requested_rule_ids: Optional[Set[str]]
    requested_rule_identities: Optional[List["RuleIdentity"]] = None
    using_cached_breakouts: bool = False
    drop_breakout_from_response: bool = False


@dataclass(frozen=True)
class RuleIdentity:
    raw_id: str
    family: str
    version: Optional[int]


class IndicatorSignalExecutor:
    """Execute indicator signals via runtime state-engine semantics."""

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
        runtime_plan = self._ctx.factory.build_runtime_input_plan(
            entry.meta,
            strategy_interval=interval,
            start=start,
            end=end,
        )
        plan_start = str(runtime_plan.get("start") or start)
        plan_end = str(runtime_plan.get("end") or end)
        plan_interval = str(runtime_plan.get("source_timeframe") or interval)
        requested_rule_ids = self._normalise_enabled_rules(dict(config or {}))
        requested_rule_identities = (
            [self._rule_identity_from_id(rule_id) for rule_id in sorted(requested_rule_ids)]
            if requested_rule_ids
            else None
        )
        provider, data_ctx = self._prepare_provider(
            entry.meta, sym, plan_start, plan_end, plan_interval, datasource, exchange
        )
        df = self._load_candles(provider, data_ctx, inst_id, sym, plan_interval)
        logger.info(
            "event=indicator_signal_mode indicator_id=%s indicator_type=%s mode=runtime_state source_timeframe=%s requested_rules=%s",
            inst_id,
            entry.meta.get("type"),
            plan_interval,
            sorted(requested_rule_ids) if requested_rule_ids else None,
        )
        signals_all = self._run_runtime_state_signals(
            inst_id=inst_id,
            meta=entry.meta,
            df=df,
            symbol=sym,
            timeframe=plan_interval,
        )
        cache_ctx = BreakoutCacheContext(
            cache_spec=None,
            cache_key=None,
            requested_rule_ids=requested_rule_ids,
            requested_rule_identities=requested_rule_identities,
        )
        filtered = self._filter_signals(signals_all, cache_ctx)
        overlays = self._ctx.signal_runner.build_overlays(
            entry.instance, filtered, df, **(config or {})
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

    def _run_runtime_state_signals(
        self,
        *,
        inst_id: str,
        meta: Mapping[str, Any],
        df: Any,
        symbol: str,
        timeframe: str,
    ) -> List[BaseSignal]:
        indicator_type = str(meta.get("type") or "").strip().lower()
        if not indicator_type:
            raise RuntimeError(f"indicator_signal_runtime_invalid: indicator_id={inst_id} missing type")
        ensure_builtin_indicator_plugins_registered()
        try:
            plugin = plugin_registry().resolve(indicator_type)
        except Exception as exc:
            raise RuntimeError(
                f"indicator_signal_runtime_plugin_missing: indicator_id={inst_id} indicator_type={indicator_type}"
            ) from exc
        if getattr(plugin, "signal_emitter", None) is None:
            raise RuntimeError(
                f"indicator_signal_runtime_emitter_missing: indicator_id={inst_id} indicator_type={indicator_type}"
            )

        engine = plugin.engine_factory(meta)
        engine_state = engine.initialize(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "indicator_id": inst_id,
            }
        )
        emitted: List[BaseSignal] = []
        previous_candle: Optional[Candle] = None
        for timestamp, row in df.iterrows():
            candle_time = timestamp.to_pydatetime() if hasattr(timestamp, "to_pydatetime") else timestamp
            if getattr(candle_time, "tzinfo", None) is None:
                candle_time = candle_time.replace(tzinfo=timezone.utc)
            else:
                candle_time = candle_time.astimezone(timezone.utc)
            candle = Candle(
                time=candle_time,
                open=float(row.get("open")),
                high=float(row.get("high")),
                low=float(row.get("low")),
                close=float(row.get("close")),
                volume=float(row.get("volume")) if row.get("volume") is not None else None,
            )
            engine.apply_bar(engine_state, candle)
            snapshot = engine.snapshot(engine_state)
            payload = dict(snapshot.payload)
            result = plugin.signal_emitter(payload, candle, previous_candle)
            signals = result.get("signals") if isinstance(result, Mapping) else []
            if isinstance(signals, Sequence):
                for signal in signals:
                    if not isinstance(signal, Mapping):
                        continue
                    converted = self._signal_from_runtime_payload(
                        signal,
                        default_symbol=symbol,
                    )
                    if converted is not None:
                        emitted.append(converted)
            previous_candle = candle
        logger.info(
            "event=indicator_signal_runtime_complete indicator_id=%s indicator_type=%s signals=%s bars=%s",
            inst_id,
            indicator_type,
            len(emitted),
            len(df),
        )
        return emitted

    def _signal_from_runtime_payload(
        self,
        signal: Mapping[str, Any],
        *,
        default_symbol: str,
    ) -> Optional[BaseSignal]:
        signal_type = str(signal.get("type") or "").strip()
        if not signal_type:
            return None
        ts = self._coerce_signal_time(signal.get("time"))
        if ts is None:
            return None
        symbol = str(signal.get("symbol") or default_symbol or "").strip()
        if not symbol:
            return None
        confidence = signal.get("confidence")
        try:
            confidence_value = float(confidence) if confidence is not None else 1.0
        except (TypeError, ValueError):
            confidence_value = 1.0
        metadata = {
            key: value
            for key, value in signal.items()
            if key not in {"type", "symbol", "time", "confidence"}
        }
        identity = self._rule_identity_from_id(str(metadata.get("rule_id") or signal_type))
        metadata.setdefault("rule_family", identity.family)
        metadata.setdefault("rule_version", identity.version)
        return BaseSignal(
            type=signal_type,
            symbol=symbol,
            time=ts,
            confidence=confidence_value,
            metadata=metadata,
        )

    @staticmethod
    def _coerce_signal_time(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            try:
                parsed = datetime.fromisoformat(text)
            except ValueError:
                return None
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        return None

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
            compatible_family_matches = 0
            for idx, sig in enumerate(signals_all):
                identifiers = self._collect_signal_identifiers(sig)
                intersection = identifiers.intersection(cache_ctx.requested_rule_ids)
                matched = bool(intersection)
                compatibility_reason: Optional[str] = None
                if (
                    not matched
                    and cache_ctx.requested_rule_identities
                    and self._signal_matches_requested_identity(sig, cache_ctx.requested_rule_identities)
                ):
                    matched = True
                    compatible_family_matches += 1
                    compatibility_reason = "family_version_compat"
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
                    if compatibility_reason:
                        logger.info(
                            "signal_filtering_compat_match | reason=%s | requested=%s | signal_rule_id=%s | signal_family=%s | signal_version=%s",
                            compatibility_reason,
                            sorted(cache_ctx.requested_rule_ids),
                            metadata_rule_id,
                            sig_metadata.get("rule_family") if isinstance(sig_metadata, dict) else None,
                            sig_metadata.get("rule_version") if isinstance(sig_metadata, dict) else None,
                        )
            logger.info(
                "signal_filtering_after_rules | filtered_signals=%d | matched=%d | dropped=%d | compatible_family_matches=%d",
                len(filtered_signals),
                matched_count,
                len(signals_all) - matched_count,
                compatible_family_matches,
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

    @staticmethod
    def _rule_identity_from_id(rule_id: str) -> RuleIdentity:
        raw = str(rule_id or "").strip().lower()
        if not raw:
            return RuleIdentity(raw_id="", family="", version=None)
        base = raw[:-5] if raw.endswith("_rule") else raw
        match = re.match(r"^(?P<family>.+)_v(?P<version>\d+)$", base)
        if match:
            family = str(match.group("family") or "").strip().lower()
            version = int(match.group("version"))
            return RuleIdentity(raw_id=raw, family=family, version=version)
        return RuleIdentity(raw_id=raw, family=base, version=None)

    def _signal_matches_requested_identity(
        self,
        signal: BaseSignal,
        requested: Sequence[RuleIdentity],
    ) -> bool:
        metadata = signal.metadata if isinstance(signal.metadata, Mapping) else {}
        rule_id = str(metadata.get("rule_id") or "").strip().lower()
        family = str(metadata.get("rule_family") or "").strip().lower()
        version_raw = metadata.get("rule_version")
        version: Optional[int]
        try:
            version = int(version_raw) if version_raw is not None else None
        except (TypeError, ValueError):
            version = None
        if not family and rule_id:
            derived = self._rule_identity_from_id(rule_id)
            family = derived.family
            version = derived.version if version is None else version
        if not family:
            return False
        for selector in requested:
            if selector.family != family:
                continue
            if selector.version is None:
                return True
            if version is None:
                # Allow unversioned runtime emissions to satisfy explicit version
                # selectors within the same family while version rollout is in
                # progress.
                return True
            if selector.version == version:
                return True
        return False


__all__ = ["IndicatorSignalExecutor", "BreakoutCacheContext"]
