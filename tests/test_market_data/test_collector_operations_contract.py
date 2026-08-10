from market_data.collector_operations import (
    CollectorAction,
    CollectorActualState,
    CollectorDesiredState,
    CollectorDiagnosticBoundary,
    CollectorKind,
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
