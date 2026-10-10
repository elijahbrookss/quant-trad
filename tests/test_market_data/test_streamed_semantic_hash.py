from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
import tracemalloc

import pytest

from market_data.frozen import _semantic_json, semantic_hash


def _previous_hash(payload):
    return hashlib.sha256(json.dumps(_semantic_json(payload, field="payload"),
        sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode()).hexdigest()


@pytest.mark.parametrize("payload", [
    {}, {"unicode": "\u00e9\n\"", "time": datetime(2024, 1, 1, tzinfo=UTC)},
    {"rows": [{"value": -0.0, "missing": None, "ready": False}, {"value": 1e-30}], "tuple": (1, 2)},
    {1: "first", "1": "last"},
])
def test_streamed_hash_preserves_exact_existing_json_bytes(payload):
    assert semantic_hash(payload) == _previous_hash(payload)


def test_hash_does_not_duplicate_whole_normalized_history():
    payload = {"rows": [{"time": i, "value": [1, 2, 3], "text": "evidence"} for i in range(5000)]}
    peaks, hashes = [], []
    for method in (_previous_hash, semantic_hash):
        tracemalloc.start()
        hashes.append(method(payload))
        peaks.append(tracemalloc.get_traced_memory()[1])
        tracemalloc.stop()
    assert hashes[0] == hashes[1]
    assert peaks[1] < peaks[0] / 2


def test_streamed_hash_still_rejects_nonfinite_material():
    with pytest.raises(ValueError, match="finite"):
        semantic_hash({"rows": [float("nan")]})
