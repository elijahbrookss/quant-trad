"""Call-scoped completed-event context shared by selection and measurement.

No matching or forward path is read here. Historical selector arithmetic and
serialization are retained, including strict prior volatility and support clocks.
"""
from array import array
from bisect import bisect_left
from collections import OrderedDict, deque
from datetime import timedelta
import math
from .recognition import plain


def _log_ratio(current, previous):
    ratio = current/previous
    return math.log(ratio) if 0 < ratio < math.inf else math.log(current)-math.log(previous)


def locked_reference_geometry(entry, low, known_at):
    """Completion-known reference geometry; caller owns the admitted low window."""
    return {"E": entry, "L": low, "upper": entry+(entry-low), "known_at": plain(known_at)}


class CompletedEventContext:
    def __init__(self, candles, events, *, evaluation_start, evaluation_end, checkpoint=lambda: None):
        by_close = {b.close_time: i for i, b in enumerate(candles)}
        by_open = {b.open_time: i for i, b in enumerate(candles)}
        all_clocks = sorted({by_close[e.known_at] for e in events})
        completed_indices = set(all_clocks)
        closes = [b.close for b in candles]
        base = {}
        for i, bar in enumerate(candles):
            checkpoint()
            if i < 97 or not evaluation_start <= bar.close_time < evaluation_end:
                continue
            sigma = math.sqrt(math.fsum(_log_ratio(closes[k], closes[k-1])**2 for k in range(i-96, i))/96)
            if not sigma:
                continue
            count = bisect_left(all_clocks, i)-bisect_left(all_clocks, i-12)
            base[i] = {"sigma": sigma, "z_pre12": _log_ratio(closes[i-1], closes[i-13])/(sigma*math.sqrt(12)),
                "z_pre96": _log_ratio(closes[i-1], closes[i-97])/(sigma*math.sqrt(96)),
                "body": _log_ratio(bar.close, bar.open)/sigma, "range": _log_ratio(bar.high, bar.low)/sigma,
                "prior_completion_stratum": min(count, 2),
                "block": int((bar.close_time-evaluation_start)//timedelta(days=28)),
                "utc6h": bar.close_time.hour//6}
        cache = OrderedDict()
        def trailing_lows(n):
            if n in cache:
                cache.move_to_end(n)
                return cache[n]
            values = array('d', [math.nan])*len(candles)
            queue = deque()
            for i, bar in enumerate(candles):
                if i % 128 == 0:
                    checkpoint()
                while queue and queue[0] <= i-n:
                    queue.popleft()
                while queue and candles[queue[-1]].low >= bar.low:
                    queue.pop()
                queue.append(i)
                if i >= n-1:
                    values[i] = candles[queue[0]].low
            cache[n] = values
            if len(cache) > 8:
                cache.popitem(last=False)
            return values
        def geometry(i, n):
            low = trailing_lows(n)[i] if i >= n-1 else math.nan
            r = (closes[i]-low)/closes[i]
            if not math.isfinite(r) or r <= 0 or i not in base:
                return None
            features = {k: base[i][k] for k in ("sigma", "z_pre12", "z_pre96", "body", "range")}
            features.update(r=r, q=r/(features["sigma"]*math.sqrt(12)), duration_hours=n/4)
            return features
        self.candles, self.evaluation_start = candles, evaluation_start
        self.by_close, self.by_open, self.completed_indices = by_close, by_open, completed_indices
        self.base, self.closes = base, closes
        self.trailing_lows, self.geometry = trailing_lows, geometry

    def record(self, event, *, timing_view, order_keys=None):
        candles, evaluation_start = self.candles, self.evaluation_start
        by_close, by_open, closes = self.by_close, self.by_open, self.closes
        trailing_lows, geometry = self.trailing_lows, self.geometry
        step = timedelta(minutes=15)
        i = by_close[event.known_at]
        n = i-by_open[event.formation_start]+1
        start_index = max(0, min(i-n+1, i-97))
        record = {"event_id": event.event_id, "decision_at": plain(event.known_at),
            "block": int((event.known_at-evaluation_start)//timedelta(days=28)),
            "formation_start": plain(event.formation_start), "formation_candles": n,
            "boundary_geometry": locked_reference_geometry(closes[i], trailing_lows(n)[i], event.known_at),
            "support_start": plain(candles[start_index].open_time), "support_end": plain(event.known_at+12*step),
            "features": geometry(i, n), "timing": timing_view(event),
            "source_reference_ids": [s.event_id for s in event.source_events], "match_status": "unmatched"}
        if order_keys is not None:
            record["b2_compatibility_order_key"] = order_keys[event.event_id]
        return record
