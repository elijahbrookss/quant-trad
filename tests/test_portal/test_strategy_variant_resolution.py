from __future__ import annotations

from types import SimpleNamespace

from portal.backend.service.strategy_variant_resolution import resolve_strategy_variant


def test_resolve_strategy_variant_uses_base_default_selected_then_bot_overrides() -> None:
    strategy = SimpleNamespace(
        id="strategy-1",
        name="Strategy 1",
        timeframe="15m",
        atm_template_id="atm-base",
        base_params={"lookback": 20},
    )
    default_variant = {
        "id": "variant-default",
        "strategy_id": "strategy-1",
        "name": "default",
        "param_overrides": {"conviction_min": 0.5, "risk_multiple": 1.0},
        "atm_template_id": "atm-default",
        "is_default": True,
    }
    selected_variant = {
        "id": "variant-fast",
        "strategy_id": "strategy-1",
        "name": "fast",
        "param_overrides": {"conviction_min": 0.65},
        "atm_template_id": "atm-fast",
        "is_default": False,
    }

    effective = resolve_strategy_variant(
        strategy,
        selected_variant,
        default_variant=default_variant,
        bot_overrides={"debug_param": "kept-for-legacy"},
    )

    assert effective.effective_params == {
        "lookback": 20,
        "conviction_min": 0.65,
        "risk_multiple": 1.0,
        "debug_param": "kept-for-legacy",
    }
    assert effective.param_source_map == {
        "lookback": "base_params",
        "conviction_min": "variant_overrides",
        "risk_multiple": "default_variant",
        "debug_param": "bot_overrides",
    }
    assert effective.effective_atm_template_id == "atm-fast"
    assert effective.atm_template_source == "variant_overrides"


def test_resolve_strategy_variant_hash_is_stable_for_mapping_order() -> None:
    strategy = SimpleNamespace(id="strategy-1", name="Strategy 1", timeframe="15m")
    default_variant = {
        "id": "variant-default",
        "strategy_id": "strategy-1",
        "name": "default",
        "param_overrides": {"b": 2, "a": 1},
        "is_default": True,
    }

    left = resolve_strategy_variant(strategy, None, default_variant=default_variant)
    right = resolve_strategy_variant(
        strategy,
        None,
        default_variant={
            **default_variant,
            "param_overrides": {"a": 1, "b": 2},
        },
    )

    assert left.effective_params == {"b": 2, "a": 1}
    assert left.effective_strategy_config_hash == right.effective_strategy_config_hash
