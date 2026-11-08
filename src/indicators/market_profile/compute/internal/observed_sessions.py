"""Immutable causal revisions of observed daily TPO profiles, without filling gaps."""
from __future__ import annotations

from math import ceil, isfinite

import pandas as pd

from data_providers.utils.ohlcv import interval_to_timedelta
from market_data.frozen import semantic_hash

from .bin_size import infer_precision_from_step
from .computation import build_tpo_histogram, extract_value_area

STEP = pd.Timedelta(minutes=30)


def _lineage_value(row, field):
    value = row.get(field)
    if pd.isna(value):
        return None
    if field in {"revision", "market_commit_seq"}:
        # A future nullable row may change pandas' dtype for the whole column.
        # That must not change the identity of already visible integer metadata.
        if isinstance(value, bool) or not isfinite(float(value)) or float(value) != int(value):
            raise ValueError(f"market_profile_source_invalid: integer {field} required")
        return int(value)
    return str(value)


def observed_session_events(frame, *, bin_size, precision, chart_timeframe):
    """Each event contains only rows available by its publication cutoff.

    Absence is disclosed, never interpreted as a verified venue closure. Unknown
    clocks fail the source build; import/detection times cannot repair them.
    """
    required = {"close_time", "known_at", "open", "high", "low", "close"}
    if not required <= set(frame.columns):
        raise ValueError("market_profile_availability_unknown: v4 requires explicit source close_time/known_at and OHLC")
    if not isfinite(bin_size) or bin_size <= 0:
        raise ValueError("market_profile_source_contract_invalid: v4 requires fixed positive bin_size")
    if ("source_identity_key" not in frame or frame.source_identity_key.isna().any()
            or any(not isinstance(value, str) or not value.strip() for value in frame.source_identity_key)
            or frame.source_identity_key.nunique() != 1):
        raise ValueError("market_profile_source_contract_invalid: v4 requires one explicit source identity")
    chart_step = pd.Timedelta(interval_to_timedelta(chart_timeframe))
    if chart_step <= pd.Timedelta(0) or chart_step > pd.Timedelta(days=1):
        raise ValueError("market_profile_source_contract_invalid: v4 chart interval must be positive and at most one day")
    source = frame.copy()
    source.index = pd.to_datetime(frame.index, utc=True, errors="raise")
    if source.index.hasnans or source.index.has_duplicates or any(source.index.asi8 % STEP.value):
        raise ValueError("market_profile_source_clock_invalid: unique aligned native 30m opens required")
    source = source.sort_index()
    for field in ("close_time", "known_at"):
        try:
            if any(pd.Timestamp(value).tzinfo is None for value in source[field] if not pd.isna(value)):
                raise ValueError("explicit timezone required")
            source[field] = pd.to_datetime(source[field], utc=True, errors="raise", format="mixed")
        except (TypeError, ValueError) as exc:
            raise ValueError(f"market_profile_availability_unknown: invalid {field}") from exc
        if source[field].isna().any():
            raise ValueError(f"market_profile_availability_unknown: missing {field}")
    if not pd.DatetimeIndex(source.close_time).equals(source.index + STEP):
        raise ValueError("market_profile_source_clock_invalid: native close_time mismatch")
    if (source.known_at < source.close_time).any():
        raise ValueError("market_profile_source_clock_invalid: known_at precedes native close")
    for field in ("open", "high", "low", "close"):
        if not all(isfinite(float(value)) for value in source[field]):
            raise ValueError(f"market_profile_source_invalid: nonfinite {field}")
    if ((source.high < source.low) | (source.high < source.open) | (source.high < source.close)
            | (source.low > source.open) | (source.low > source.close)).any():
        raise ValueError("market_profile_source_invalid: inconsistent OHLC")

    def project(epoch, mode):
        step = int(chart_step.total_seconds())
        return epoch // step * step if mode == "floor" else (epoch + step - 1) // step * step

    events = []
    for day, group in source.groupby(source.index.normalize(), sort=True):
        session_close = day + pd.Timedelta(days=1)
        boundaries = sorted({project(ceil(max(session_close, value).timestamp()), "ceil") for value in group.known_at})
        expected = pd.date_range(day, periods=48, freq=STEP)
        for revision, boundary in enumerate(boundaries, 1):
            cutoff = pd.Timestamp(boundary, unit="s", tz="UTC")
            visible = group.loc[group.known_at <= cutoff]
            if visible.empty:
                continue
            histogram = build_tpo_histogram(visible, bin_size, infer_precision_from_step(bin_size))
            area = extract_value_area(histogram, precision)
            if area is None:
                raise ValueError("market_profile_source_invalid: empty observed histogram")
            lineage = []
            for timestamp, row in visible.iterrows():
                lineage.append({
                    "open_time": timestamp.isoformat(), "close_time": row.close_time.isoformat(),
                    "known_at": row.known_at.isoformat(),
                    **{field: float(row[field]) for field in ("open", "high", "low", "close")},
                    **{field: _lineage_value(row, field)
                       for field in ("source_identity_key", "revision", "market_commit_seq", "known_at_method")},
                })
            missing = expected.difference(visible.index)
            methods = sorted({"unspecified" if pd.isna(row.get("known_at_method")) else (str(row["known_at_method"]).strip() or "unspecified")
                              for _, row in visible.iterrows()})
            quality = {
                "session": day.isoformat(), "session_revision": revision,
                "state": "complete_observed_slots" if not len(missing) else "observed_incomplete",
                "observed_bars": len(visible), "expected_bars": 48,
                "missing_open_times": [value.isoformat() for value in missing],
                "source_lineage_hash": semantic_hash(lineage),
                "source_identity_key": str(visible.source_identity_key.iloc[0]),
                "source_known_at": ceil(max(session_close, visible.known_at.max()).timestamp()),
                "availability_methods": methods,
                "availability_inferred": any(method in {"interval_close_inferred", "unspecified"} for method in methods),
                "closure_verified": False,
            }
            source_start = int(visible.index[0].timestamp())
            source_end = int(visible.index[-1].timestamp())
            events.append({
                "start": project(source_start, "floor"), "end": project(source_end, "ceil"),
                "source_start": source_start, "source_end": source_end,
                "formed_at": int(session_close.timestamp()), "known_at": boundary,
                "source_known_at": quality["source_known_at"],
                "session_key": day.isoformat(), "session_revision": revision,
                "VAH": float(area.vah), "VAL": float(area.val), "POC": float(area.poc),
                "session_count": 1, "precision": precision, "quality": quality,
            })
    return sorted(events, key=lambda event: (event["known_at"], event["session_key"], event["session_revision"]))
