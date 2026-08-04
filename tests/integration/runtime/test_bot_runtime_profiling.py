from __future__ import annotations

from engines.bot_runtime.runtime.profiling import PythonProfileSession


def _profiled_workload() -> int:
    return sum(index * index for index in range(2_000))


def test_python_profile_session_is_disabled_without_overhead_artifact() -> None:
    session = PythonProfileSession(enabled=False, context={"run_id": "run-1"})

    session.start()
    result = _profiled_workload()
    summary = session.stop()

    assert result > 0
    assert summary is None


def test_python_profile_session_records_bounded_standard_library_evidence() -> None:
    session = PythonProfileSession(
        enabled=True,
        context={"run_id": "run-1", "dataset_id": "mds-1"},
        work_units=2_000,
        top_limit=5,
    )

    session.start()
    result = _profiled_workload()
    summary = session.stop()

    assert result > 0
    assert summary is not None
    assert summary["schema_version"] == "python_profile.v1"
    assert summary["status"] == "completed"
    assert summary["wall_seconds"] > 0.0
    assert summary["cpu_seconds"] >= 0.0
    assert summary["peak_memory_bytes"] is not None
    assert summary["memory_scope"] == "process_peak_rss"
    assert summary["current_memory_bytes"] is None
    assert "peak_memory_is_process_lifetime_rss_not_profile_session_allocations" in summary[
        "caveats"
    ]
    assert summary["work_units"] == 2_000
    assert summary["work_units_per_second"] > 0.0
    assert 1 <= len(summary["top_by_cumulative_time"]) <= 5
    assert 1 <= len(summary["top_by_self_time"]) <= 5
    assert summary["context"] == {"run_id": "run-1", "dataset_id": "mds-1"}


def test_python_profile_failure_is_bounded_and_does_not_raise() -> None:
    session = PythonProfileSession(
        enabled=True,
        context={"run_id": "run-1"},
        work_units=2_000,
    )
    session.start()

    summary = session.record_failure(RuntimeError("profile serialization failed"))

    assert summary is not None
    assert summary["status"] == "failed"
    assert summary["error"] == "RuntimeError: profile serialization failed"
    assert summary["context"] == {"run_id": "run-1"}
    assert "trading_run_outcome_is_authoritative_over_profile_failure" in summary[
        "caveats"
    ]
