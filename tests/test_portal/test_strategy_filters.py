"""Tests for strategy filter evaluation helpers."""

from datetime import datetime

from portal.backend.service.strategies.strategy_service.filters import (
    FilterContext,
    FilterDefinition,
    evaluate_filter_definition,
    evaluate_filter_definitions,
    summarize_filter_results,
    validate_filter_dsl,
)


def _build_context(
    *,
    instrument_id: str = "inst-1",
    candle_time: datetime | None = None,
    stats: dict | None = None,
    regime: dict | None = None,
    stats_version: str = "v1",
    regime_version: str = "v1",
    include_versions: bool = True,
) -> FilterContext:
    candle_time = candle_time or datetime(2025, 1, 1, 12, 0, 0)
    stats_latest = {}
    regime_latest = {}
    stats_by_version = {}
    regime_by_version = {}
    if stats is not None:
        stats_latest[(instrument_id, candle_time)] = {"stats": stats, "version": stats_version}
        if include_versions:
            stats_by_version[(instrument_id, candle_time, stats_version)] = stats
    if regime is not None:
        regime_latest[(instrument_id, candle_time)] = {"regime": regime, "version": regime_version}
        if include_versions:
            regime_by_version[(instrument_id, candle_time, regime_version)] = regime
    return FilterContext(
        instrument_id=instrument_id,
        candle_time=candle_time,
        candle_stats_latest=stats_latest,
        candle_stats_by_version=stats_by_version,
        regime_stats_latest=regime_latest,
        regime_stats_by_version=regime_by_version,
    )


def test_filter_predicate_operators() -> None:
    stats = {"foo": 5, "bar": 10, "flag": True, "bucket": "A"}
    context = _build_context(stats=stats)

    definitions = [
        FilterDefinition(
            id="eq",
            scope="GLOBAL",
            name="eq",
            dsl={"source": "candle_stats", "path": "$.foo", "operator": "eq", "value": 5},
        ),
        FilterDefinition(
            id="gt",
            scope="GLOBAL",
            name="gt",
            dsl={"source": "candle_stats", "path": "$.bar", "operator": "gt", "value": 5},
        ),
        FilterDefinition(
            id="between",
            scope="GLOBAL",
            name="between",
            dsl={"source": "candle_stats", "path": "$.bar", "operator": "between", "value": [5, 15]},
        ),
        FilterDefinition(
            id="in",
            scope="GLOBAL",
            name="in",
            dsl={"source": "candle_stats", "path": "$.bucket", "operator": "in", "value": ["A", "B"]},
        ),
        FilterDefinition(
            id="exists",
            scope="GLOBAL",
            name="exists",
            dsl={"source": "candle_stats", "path": "$.flag", "operator": "exists"},
        ),
        FilterDefinition(
            id="missing",
            scope="GLOBAL",
            name="missing",
            dsl={"source": "candle_stats", "path": "$.missing", "operator": "missing"},
        ),
    ]

    for definition in definitions:
        result = evaluate_filter_definition(definition, context)
        assert result["passed"] is True


def test_nested_group_logic_with_not() -> None:
    stats = {"foo": 5, "bar": 1}
    context = _build_context(stats=stats)
    dsl = {
        "all": [
            {"source": "candle_stats", "path": "$.foo", "operator": "gt", "value": 1},
            {"not": {"source": "candle_stats", "path": "$.bar", "operator": "gt", "value": 10}},
        ]
    }
    validate_filter_dsl(dsl)
    definition = FilterDefinition(id="grp", scope="GLOBAL", name="group", dsl=dsl)
    result = evaluate_filter_definition(definition, context)
    assert result["passed"] is True


def test_missing_data_policy_handling() -> None:
    context = _build_context(stats={"foo": 5})
    fail_def = FilterDefinition(
        id="fail",
        scope="GLOBAL",
        name="fail",
        dsl={
            "source": "candle_stats",
            "path": "$.missing",
            "operator": "eq",
            "value": 1,
            "missing_data_policy": "fail",
        },
    )
    pass_def = FilterDefinition(
        id="pass",
        scope="GLOBAL",
        name="pass",
        dsl={
            "source": "candle_stats",
            "path": "$.missing",
            "operator": "eq",
            "value": 1,
            "missing_data_policy": "pass",
        },
    )
    ignore_def = FilterDefinition(
        id="ignore",
        scope="GLOBAL",
        name="ignore",
        dsl={
            "source": "candle_stats",
            "path": "$.missing",
            "operator": "eq",
            "value": 1,
            "missing_data_policy": "ignore",
        },
    )

    assert evaluate_filter_definition(fail_def, context)["passed"] is False
    assert evaluate_filter_definition(pass_def, context)["passed"] is True
    assert evaluate_filter_definition(ignore_def, context)["passed"] is True


def test_version_constraints_treat_mismatch_as_missing() -> None:
    context = _build_context(stats={"foo": 5}, stats_version="v1")
    definition = FilterDefinition(
        id="version",
        scope="GLOBAL",
        name="version",
        dsl={
            "source": "candle_stats",
            "path": "$.foo",
            "operator": "eq",
            "value": 5,
            "stats_version": "v2",
        },
    )
    result = evaluate_filter_definition(definition, context)
    assert result["passed"] is False
    assert result["details"]["missing_reason"] == "version_mismatch"


def test_filters_block_when_global_fails() -> None:
    context = _build_context(stats={"foo": 5})
    global_filters = [
        FilterDefinition(
            id="global",
            scope="GLOBAL",
            name="global",
            dsl={"source": "candle_stats", "path": "$.foo", "operator": "gt", "value": 10},
        )
    ]
    rule_filters = [
        FilterDefinition(
            id="rule",
            scope="RULE",
            name="rule",
            dsl={"source": "candle_stats", "path": "$.foo", "operator": "gt", "value": 1},
        )
    ]

    global_results = evaluate_filter_definitions(global_filters, context)
    rule_results = evaluate_filter_definitions(rule_filters, context)
    global_passed, _ = summarize_filter_results(global_results)
    rule_passed, _ = summarize_filter_results(rule_results)

    assert global_passed is False
    assert rule_passed is True


def test_filters_block_when_rule_fails() -> None:
    context = _build_context(stats={"foo": 5})
    global_filters = [
        FilterDefinition(
            id="global",
            scope="GLOBAL",
            name="global",
            dsl={"source": "candle_stats", "path": "$.foo", "operator": "gt", "value": 1},
        )
    ]
    rule_filters = [
        FilterDefinition(
            id="rule",
            scope="RULE",
            name="rule",
            dsl={"source": "candle_stats", "path": "$.foo", "operator": "gt", "value": 10},
        )
    ]

    global_results = evaluate_filter_definitions(global_filters, context)
    rule_results = evaluate_filter_definitions(rule_filters, context)
    global_passed, _ = summarize_filter_results(global_results)
    rule_passed, _ = summarize_filter_results(rule_results)

    assert global_passed is True
    assert rule_passed is False
