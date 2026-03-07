from __future__ import annotations

from signals.rules.market_profile._meta import ensure_market_profile_rule_metadata


def test_ensure_market_profile_rule_metadata_normalizes_ids_and_aliases() -> None:
    payload = ensure_market_profile_rule_metadata(
        {
            "type": "breakout",
            "symbol": "CL",
            "time": "2025-01-08T00:00:00Z",
            "aliases": [" top_level_alias "],
            "metadata": {"aliases": ["meta_alias", "top_level_alias"]},
        },
        rule_id="Market_Profile_Breakout",
        pattern_id="Value_Area_Breakout",
        aliases=("market_profile_breakout_rule",),
    )

    assert payload["rule_id"] == "market_profile_breakout"
    assert payload["pattern_id"] == "value_area_breakout"
    assert payload["aliases"] == [
        "market_profile_breakout_rule",
        "meta_alias",
        "top_level_alias",
    ]

    metadata = payload["metadata"]
    assert metadata["rule_id"] == "market_profile_breakout"
    assert metadata["pattern_id"] == "value_area_breakout"
    assert metadata["rule_aliases"] == payload["aliases"]
    assert metadata["aliases"] == payload["aliases"]
