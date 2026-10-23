"""Factory and immutable source-frame projection for the causal runtime."""
from collections.abc import Mapping
from datetime import timedelta

from indicators.manifest import resolve_manifest_params
from research_science.recognition import RecognitionCandle, RecognitionContext, plain, utc
from research_science.reversal_recognition import ReversalSettings

from .manifest import MANIFEST
from .runtime import TypedReversalIndicator


class ReversalIndicator:
    NAME = MANIFEST.type
    MANIFEST = MANIFEST
    RUNTIME_REPLAY_FROM_ORIGIN = True

    @classmethod
    def resolve_config(cls, params, *, strict_unknown=False):
        # Preserve strict scalar types before the generic manifest resolver can
        # coerce a bool/string into a supported resource bound.
        for name, maximum in (("max_bars", 19104), ("max_candidates", 10000)):
            value = (params or {}).get(name, maximum)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"reversal_config_invalid: {name} out of bounds")
        result = resolve_manifest_params(cls.MANIFEST, params, strict_unknown=strict_unknown)
        result["computation_origin"] = plain(utc(result["computation_origin"]))
        if result["reference_basis"] != "rising_leg_plus_retreat":
            raise ValueError("reversal_config_invalid: only rising_leg_plus_retreat is admitted")
        ReversalSettings(result["reference_basis"], result["ownership"],
                         result["max_bars"], result["max_candidates"])
        return result

    @staticmethod
    def _execution_context(execution_context):
        if execution_context is None:
            raise ValueError("reversal_source_invalid: execution_context required")
        execution_context.validate()
        if not isinstance(execution_context.instrument_id, str) or not execution_context.instrument_id.strip():
            raise ValueError("reversal_source_invalid: canonical instrument_id required; symbol is not identity")
        if execution_context.interval != "15m":
            raise ValueError("reversal_source_invalid: native 15m input required")
        return execution_context

    @classmethod
    def build_runtime_data_request(cls, *, resolved_params, execution_context):
        context = cls._execution_context(execution_context)
        params = cls.resolve_config(resolved_params, strict_unknown=True)
        origin = utc(params["computation_origin"])
        if origin > utc(context.start) or origin >= utc(context.end):
            raise ValueError("reversal_source_invalid: origin must precede the evaluation end and not follow its start")
        span = (utc(context.end)-origin).total_seconds()
        if ((utc(context.start)-origin).total_seconds() % 900 or span % 900
                or span/900 > params["max_bars"]):
            raise ValueError("reversal_source_invalid: origin window must be aligned and within max_bars")
        return context.data_context(start=plain(origin), interval="15m")

    @classmethod
    def build_runtime_source_facts(cls, *, resolved_params, execution_context, source_frame,
                                   source_diagnostics=None):
        context = cls._execution_context(execution_context)
        params = cls.resolve_config(resolved_params, strict_unknown=True)
        required = {"instrument_id", "source_identity_key", "close_time", "known_at",
                    "open", "high", "low", "close"}
        if source_frame is None or not required <= set(source_frame.columns):
            raise ValueError("reversal_source_invalid: frame requires canonical instrument/source, clocks and OHLC")
        if not 1 <= len(source_frame) <= params["max_bars"]:
            raise ValueError("reversal_source_limit: source frame bar bound exceeded or empty")
        candles = []
        source = None
        expected = utc(params["computation_origin"])
        for timestamp, raw in source_frame.iterrows():
            row = raw.to_dict()
            if row["instrument_id"] != context.instrument_id:
                raise ValueError("reversal_source_invalid: instrument identity mismatch")
            key = row["source_identity_key"]
            if not isinstance(key, str) or not key.strip() or (source is not None and key != source):
                raise ValueError("reversal_source_invalid: missing or mixed source identity")
            source = key
            if "interval" in row and row["interval"] != "15m":
                raise ValueError("reversal_source_invalid: native interval mismatch")
            if "timestamp" in row and utc(row["timestamp"]) != utc(timestamp):
                raise ValueError("reversal_source_invalid: frame timestamp/index mismatch")
            row["open_time"] = timestamp
            candle = RecognitionCandle.from_mapping(row)
            if (candle.open_time != expected or candle.close_time-candle.open_time != timedelta(seconds=900)
                    or candle.known_at != candle.close_time):
                raise ValueError("reversal_source_invalid: origin/gap/native interval/close availability mismatch")
            candles.append(candle)
            expected = candle.close_time
        recognition_context = RecognitionContext(context.instrument_id, 900,
            utc(params["computation_origin"]), source)
        # Conversion only: no recognizer, features, context or outcomes run here.
        return {"context": recognition_context, "candles": tuple(candles)}

    @classmethod
    def build_runtime_indicator(cls, *, indicator_id, meta, resolved_params,
                                strategy_indicator_metas, execution_context=None, source_facts=None):
        context = cls._execution_context(execution_context)
        params = cls.resolve_config(resolved_params, strict_unknown=True)
        if not isinstance(source_facts, Mapping) or set(source_facts) != {"context", "candles"}:
            raise ValueError("reversal_source_invalid: immutable source frame projection required")
        recognition_context = source_facts["context"]
        if (not isinstance(recognition_context, RecognitionContext)
                or recognition_context.instrument_id != context.instrument_id
                or recognition_context.computation_origin != utc(params["computation_origin"])
                or recognition_context.timeframe_seconds != 900):
            raise ValueError("reversal_source_invalid: projected context disagrees with execution")
        version = str(meta.get("version") or cls.MANIFEST.version)
        if version != cls.MANIFEST.version:
            raise ValueError("reversal_config_invalid: unsupported runtime version")
        return TypedReversalIndicator(indicator_id=indicator_id, version=version,
            params=params, context=recognition_context, candles=source_facts["candles"])
