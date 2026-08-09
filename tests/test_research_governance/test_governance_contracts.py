from __future__ import annotations

import pytest

from research_governance import GovernanceState, validate_offline_transition


def test_offline_lifecycle_accepts_only_declared_adjacent_states() -> None:
    source, target = validate_offline_transition("observation", "hypothesis")
    assert source is GovernanceState.OBSERVATION
    assert target is GovernanceState.HYPOTHESIS
    with pytest.raises(ValueError, match="not allowed"):
        validate_offline_transition("OBSERVATION", "RESEARCH_CERTIFIED")


@pytest.mark.parametrize(
    "target",
    ["SHADOW", "PAPER", "CONTROLLED_LIVE", "LIVE", "DEPLOYED", "CAPITAL_APPROVED"],
)
def test_operational_and_capital_states_are_structurally_absent(target: str) -> None:
    with pytest.raises(ValueError, match="structurally closed"):
        validate_offline_transition("RESEARCH_CERTIFIED", target)


def test_research_certified_can_only_degrade_or_archive() -> None:
    assert validate_offline_transition(
        "RESEARCH_CERTIFIED", "RESEARCH_DEGRADED"
    )[1] is GovernanceState.RESEARCH_DEGRADED
    assert validate_offline_transition(
        "RESEARCH_CERTIFIED", "ARCHIVED"
    )[1] is GovernanceState.ARCHIVED
