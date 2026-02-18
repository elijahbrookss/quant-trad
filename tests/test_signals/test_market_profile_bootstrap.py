from __future__ import annotations

from signals.rules.market_profile import _bootstrap as bootstrap_module


def test_ensure_breakouts_ready_passes_payload_sequence_to_v2(monkeypatch):
    calls = {"v1": None, "v2": None}

    def _v1(context, payload):  # noqa: ANN001
        _ = context
        calls["v1"] = payload
        return []

    def _v2(context, payloads):  # noqa: ANN001
        _ = context
        calls["v2"] = payloads
        return []

    monkeypatch.setattr(bootstrap_module, "market_profile_breakout_rule", _v1)
    monkeypatch.setattr(bootstrap_module, "market_profile_breakout_v2_rule", _v2)

    payloads = [{"VAH": 100, "VAL": 90}, {"VAH": 101, "VAL": 91}]
    context = {}
    bootstrap_module.ensure_breakouts_ready(context, payloads)

    assert calls["v1"] == payloads[0]
    assert calls["v2"] == payloads
