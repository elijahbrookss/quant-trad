"""O(1) walk-forward daily UTC VWAP from HLC3 and native candle volume."""

from datetime import datetime, timedelta, timezone
import math

from engines.bot_runtime.core.domain import Candle
from engines.indicator_engine.contracts import Indicator, IndicatorGapRejectedError, RuntimeOutput
from indicators.manifest import build_runtime_spec

from .manifest import MANIFEST, NATIVE_TIMEFRAMES


class TypedCandleVWAPIndicator(Indicator):
    def __init__(self, *, indicator_id, version, params):
        if version != MANIFEST.version:
            raise ValueError("candle_vwap_config_invalid: unsupported runtime version")
        self.runtime_spec = build_runtime_spec(MANIFEST, instance_id=indicator_id, version=version)
        self._timeframe = params["source_timeframe"]
        self._step = timedelta(seconds=NATIVE_TIMEFRAMES[self._timeframe])
        self._previous_end = None
        self._previous_known_at = None
        self._session_start = None
        self._previous_distance = None
        self._volume = 0.0
        self._weighted_price = 0.0
        self._observed_bars = 0
        self._positive_bars = 0
        self._complete = False
        self._outputs = {output.name: RuntimeOutput(datetime.min, False, {}) for output in MANIFEST.outputs}

    def _invalid(self, reason, bar=None):
        raise ValueError(f"candle_vwap_source_invalid: indicator_id={self.runtime_spec.instance_id} "
                         f"timeframe={self._timeframe} bar_time={getattr(bar, 'time', None)} {reason}")

    def handle_gap(self, *, policy, gap, next_bar_time, rewarm_bars):
        # v1 never manufactures a new session anchor after missing source bars.
        raise IndicatorGapRejectedError(indicator_id=self.runtime_spec.instance_id, gap=gap)

    def apply_bar(self, bar, inputs):
        if not isinstance(bar, Candle) or inputs:
            self._invalid("Candle input without dependencies required", bar)
        clocks = (bar.time, bar.end_time, bar.known_at or bar.end_time)
        if any(not isinstance(t, datetime) or t.tzinfo is None or t.utcoffset() is None for t in clocks):
            self._invalid("timezone-aware source clocks required", bar)
        start, end, known_at = (t.astimezone(timezone.utc) for t in clocks)
        if (end-start != self._step or start.timestamp() % self._step.total_seconds()
                or known_at < end):
            self._invalid("native interval/alignment/availability mismatch", bar)
        if self._previous_end is not None and start != self._previous_end:
            raise IndicatorGapRejectedError(indicator_id=self.runtime_spec.instance_id,
                gap={"classification": "source_discontinuity", "expected": self._previous_end.isoformat(),
                     "actual": start.isoformat()})
        if self._previous_known_at is not None and known_at < self._previous_known_at:
            self._invalid("availability mismatch: source known_at moved backwards", bar)
        raw = (bar.open, bar.high, bar.low, bar.close, bar.volume)
        if any(isinstance(v, bool) or v is None for v in raw):
            self._invalid("finite OHLC and nonnegative volume required", bar)
        try:
            opening, high, low, close, volume = map(float, raw)
        except (TypeError, ValueError, OverflowError):
            self._invalid("numeric OHLC and volume required", bar)
        if (not all(math.isfinite(v) for v in (opening, high, low, close, volume))
                or low <= 0 or not low <= min(opening, close) <= max(opening, close) <= high
                or volume < 0):
            self._invalid("finite positive consistent OHLC and nonnegative volume required", bar)
        session = start.replace(hour=0, minute=0, second=0, microsecond=0)
        new_session = session != self._session_start
        old_volume = 0.0 if new_session else self._volume
        old_weighted = 0.0 if new_session else self._weighted_price
        # Divide first to avoid overflowing a finite H+L+C unnecessarily.
        basis = high/3.0 + low/3.0 + close/3.0
        cumulative_volume = old_volume + volume
        weighted_price = old_weighted + basis*volume
        if not math.isfinite(cumulative_volume) or not math.isfinite(weighted_price):
            self._invalid("VWAP accumulator overflow", bar)
        if new_session:
            self._session_start = session
            self._complete = start == session
            self._observed_bars = self._positive_bars = 0
            self._previous_distance = None
        self._volume, self._weighted_price = cumulative_volume, weighted_price
        self._observed_bars += 1
        self._positive_bars += int(volume > 0)
        ready = self._complete and self._volume > 0
        vwap = weighted_price/cumulative_volume if ready else None
        distance = close-vwap if ready else None
        if ready and (vwap <= 0 or not math.isfinite(vwap)):
            self._invalid("VWAP precision underflow or overflow", bar)
        distance_bps = (distance/vwap)*10000 if ready else None
        if ready and not math.isfinite(distance_bps):
            self._invalid("distance precision overflow", bar)
        state = ("warming_session" if not self._complete else "awaiting_volume" if not ready
                 else "above" if distance > 0 else "below" if distance < 0 else "at")
        events = []
        if ready and volume > 0 and self._previous_distance is not None:
            crossing = ("above" if self._previous_distance <= 0 < distance else
                        "below" if self._previous_distance >= 0 > distance else None)
            if crossing:
                events.append({"key": f"close_cross_{crossing}_vwap",
                    "direction": "long" if crossing == "above" else "short",
                    "pattern_id": f"{self.runtime_spec.instance_id}:{start.isoformat()}:{crossing}",
                    "known_at": known_at.isoformat(), "metadata": {
                        "session_start": session.isoformat(), "price_basis": "hlc3",
                        "source_timeframe": self._timeframe, "vwap": vwap,
                        "close_distance": distance, "trigger_price": close,
                        "reference": {"kind": "session_vwap", "price": vwap,
                                      "known_at": known_at.isoformat()}}})
        # No cross is inferred across an unready or zero-volume bar.
        self._previous_distance = distance if ready and volume > 0 else None
        self._previous_end, self._previous_known_at = end, known_at
        fields = {"session_start": session.isoformat(),
                  "session_end": (session+timedelta(days=1)).isoformat(), "session_timezone": "UTC",
                  "price_basis": "hlc3", "source_timeframe": self._timeframe,
                  "volume_semantics": "provider_reported_candle_volume",
                  "session_complete_from_anchor": self._complete, "observed_bars": self._observed_bars,
                  "positive_volume_bars": self._positive_bars, "zero_volume_bar": volume == 0,
                  "close_time": end.isoformat(), "known_at": known_at.isoformat(),
                  "gap_contract": "candle_vwap.reject.v1"}
        self._outputs = {
            "vwap_context": RuntimeOutput(bar.time, ready, {"state_key": state, "fields": fields}),
            "vwap_metrics": RuntimeOutput(bar.time, ready, {"vwap": vwap, "close_distance": distance,
                "close_distance_bps": distance_bps,
                "cumulative_volume": self._volume}),
            "vwap_cross": RuntimeOutput(bar.time, ready, {"events": events}),
        }

    def snapshot(self):
        return {key: output.copy() for key, output in self._outputs.items()}
