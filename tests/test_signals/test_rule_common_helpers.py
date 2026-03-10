import pytest

pd = pytest.importorskip("pandas")

from signals.rules.common.cache import append_to_cache, ensure_cache, mark_ready
from signals.rules.common.utils import (
    as_timestamp,
    clean_numeric,
    normalise_meta_timestamp,
    resolve_index_position,
)


def test_cache_helpers_manage_flags_and_population():
    context: dict = {}

    mutable = ensure_cache(
        context,
        "cache",
        list,
        ready_flag="ready",
        initialised_flag="init",
    )

    assert mutable is context
    assert context["cache"] == []
    assert context["init"] is True
    assert context["ready"] is False

    append_to_cache(context, "cache", [1, 2])
    assert context["cache"] == [1, 2]

    mark_ready(context, "ready", ready=True)
    assert context["ready"] is True


def test_timestamp_and_numeric_normalisation():
    tz = "UTC"
    ts = as_timestamp("2024-01-01T00:00:00", tz)
    meta_ts = normalise_meta_timestamp("2024-01-02 00:00:00", tz)

    assert ts is not None and ts.tzinfo is not None
    assert meta_ts is not None and meta_ts.tzinfo is not None

    idx = pd.date_range("2024-01-01", periods=3, tz=tz)
    nearest = resolve_index_position(idx, ts)
    assert nearest == 0

    assert clean_numeric("not-a-number") is None
    assert clean_numeric(float("inf"), default=0.0) == 0.0
    assert clean_numeric(1.25) == 1.25
