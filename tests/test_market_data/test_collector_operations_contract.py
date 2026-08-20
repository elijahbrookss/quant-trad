from market_data.collector_operations import (
    CollectorAction,
    CollectorActualState,
    CollectorDesiredState,
    CollectorDiagnosticBoundary,
    CollectorKind,
)
from portal.backend.service.storage.repos.collector_operations import (
    PostgresCollectorOperationsRepository,
)


def test_collector_lifecycle_vocabulary_is_closed_and_provider_neutral():
    assert {item.value for item in CollectorKind} == {
        "scheduled_fact",
        "continuous_stream",
    }
    assert {item.value for item in CollectorDesiredState} == {
        "running",
        "stopped",
        "paused",
    }
    assert {item.value for item in CollectorActualState} == {
        "DISABLED",
        "STOPPED",
        "PAUSED",
        "STARTING",
        "HEALTHY",
        "DEGRADED",
        "RETRYING",
        "RECOVERING",
        "FAILED",
        "STOPPING",
    }


def test_collector_actions_encode_mutation_and_confirmation_boundaries():
    assert CollectorAction.HEALTH_PROBE.mutates is False
    assert CollectorAction.DIAGNOSE.mutates is False
    assert CollectorAction.START.mutates is True
    assert CollectorAction.STOP.requires_confirmation is True
    assert CollectorAction.RESTART.requires_confirmation is True
    assert CollectorAction.RECOVER.requires_confirmation is True
    assert CollectorAction.PAUSE.requires_confirmation is False


def test_collector_diagnostic_boundaries_stop_provider_logic_at_backend():
    assert {item.value for item in CollectorDiagnosticBoundary} == {
        "registration",
        "worker",
        "scheduler",
        "ownership",
        "provider",
        "canonicalization",
        "schema",
        "persistence",
        "freshness",
        "gaps_recovery",
    }


def test_collector_lifecycle_transitions_keep_configuration_code_owned():
    transition = PostgresCollectorOperationsRepository._transition

    target, force_generation, error = transition(
        action=CollectorAction.START,
        configured_enabled=True,
        desired_state=CollectorDesiredState.STOPPED,
    )
    assert (target, force_generation, error) == (
        CollectorDesiredState.RUNNING,
        False,
        None,
    )

    target, force_generation, error = transition(
        action=CollectorAction.RESTART,
        configured_enabled=True,
        desired_state=CollectorDesiredState.RUNNING,
    )
    assert (target, force_generation, error) == (
        CollectorDesiredState.RUNNING,
        True,
        None,
    )

    target, force_generation, error = transition(
        action=CollectorAction.RESUME,
        configured_enabled=False,
        desired_state=CollectorDesiredState.PAUSED,
    )
    assert target == CollectorDesiredState.PAUSED
    assert force_generation is False
    assert error == "collector_configured_disabled"


def test_collector_recovery_cannot_bypass_a_registered_capability_handler():
    target, force_generation, error = (
        PostgresCollectorOperationsRepository._transition(
            action=CollectorAction.RECOVER,
            configured_enabled=True,
            desired_state=CollectorDesiredState.RUNNING,
        )
    )
    assert target == CollectorDesiredState.RUNNING
    assert force_generation is False
    assert error == "collector_recovery_requires_capability_handler"
