from datetime import date, datetime
from types import SimpleNamespace

import pytest

from portal.backend.service.storage.repos.fact_reclamation import (
    FactReclamationLimits, PostgresCanonicalFactReclamationRepository, unproven_reclamation_fact_types,
)


@pytest.mark.parametrize("value", [0, -1, True, "1", 1.5])
@pytest.mark.parametrize("field", ["statement_timeout_ms", "pinned_ranges_statement_timeout_ms",
                                   "max_handoff_seconds"])
def test_reclamation_limits_require_positive_integers(field, value):
    with pytest.raises(ValueError, match="canonical_reclaim_limit_invalid"):
        FactReclamationLimits(**{field: value})


@pytest.mark.parametrize("value", [0, 1, None, "true"])
def test_destructive_enablement_requires_a_boolean(value):
    with pytest.raises(ValueError, match="enabled_must_be_boolean"):
        PostgresCanonicalFactReclamationRepository(archive_repository=None, enabled=value)


def test_execution_is_disabled_before_accessing_any_storage():
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=None)
    with pytest.raises(RuntimeError, match="canonical_reclaim_disabled"):
        reclaimer.reclaim_partition(date(2026, 1, 1), eligible_before=date(2026, 1, 2), execute=True)


@pytest.mark.parametrize("params", [
    {"day": "2026-01-01"}, {"day": datetime(2026, 1, 1)},
    {"eligible_before": None}, {"execute": 1},
])
def test_invalid_requests_fail_before_accessing_storage(params):
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=None)
    with pytest.raises(ValueError, match="canonical_reclaim_request_invalid"):
        reclaimer.reclaim_partition(**{"day": date(2026, 1, 1), "eligible_before": date(2026, 1, 2), **params})


@pytest.mark.parametrize("fact_type", [
    "unknown.family",
])
def test_incomplete_transitive_proofs_block_reclamation(fact_type):
    def execute(statement, *_):
        if "clock_timestamp" in str(statement):
            return SimpleNamespace(scalar_one=lambda: date(2026, 1, 3))
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: ["market.trade", fact_type]))
    with pytest.raises(RuntimeError, match="canonical_reclaim_dependency_proof_required"):
        PostgresCanonicalFactReclamationRepository._eligibility(
            SimpleNamespace(execute=execute), {"storage_day": date(2026, 1, 1), "state": "verified"},
            eligible_before=date(2026, 1, 2),
        )


def test_response_admission_does_not_bypass_unproven_families_in_a_mixed_day():
    assert unproven_reclamation_fact_types(["market.market_response", "market.trade"]) == []
    assert unproven_reclamation_fact_types(["market.market_response", "market.normalized.example"]) == []
    assert unproven_reclamation_fact_types(["market.normalized.example", "unknown.family"]) == ["unknown.family"]


@pytest.mark.parametrize("changes", [
    None, {"relkind": "v"}, {"relispartition": False}, {"parents": []},
    {"parents": ["market.other"]}, {"parents": ["market.fact_hot_payloads", "market.other"]},
    {"bounds": "FOR VALUES FROM ('2026-01-02') TO ('2026-01-03')"},
])
def test_only_the_expected_attached_daily_relation_is_eligible(changes):
    row = {"oid": 1, "relkind": "r", "relispartition": True, "parents": ["market.fact_hot_payloads"],
           "bounds": "FOR VALUES FROM ('2026-01-01') TO ('2026-01-02')"}
    result = None if changes is None else {**row, **changes}
    session = SimpleNamespace(execute=lambda *_: SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: result)))
    with pytest.raises(RuntimeError, match="canonical_reclaim_relation_scope_invalid"):
        PostgresCanonicalFactReclamationRepository._relation(session, date(2026, 1, 1))


def test_reclamation_mount_admission_requires_writability_on_every_recheck(monkeypatch):
    from portal.backend.service.storage.repos import fact_reclamation
    from core.storage_mounts import StorageMountError
    events = []
    def mount(*, require_writable):
        events.append(("mount", require_writable))
    monkeypatch.setattr(fact_reclamation, "require_configured_archive_mount", mount)
    objects = SimpleNamespace(assert_unchanged=lambda: events.append(("files", True)))
    reclaimer = PostgresCanonicalFactReclamationRepository(archive_repository=None)
    reclaimer._assert_archive_admission()
    reclaimer._assert_archive_admission(objects)
    assert events == [("mount", True), ("mount", True), ("files", True)]
    def read_only(**_):
        raise StorageMountError("storage_mount_read_only")
    monkeypatch.setattr(fact_reclamation, "require_configured_archive_mount", read_only)
    with pytest.raises(StorageMountError, match="storage_mount_read_only"):
        reclaimer._assert_archive_admission(objects)
    assert len(events) == 3


def test_reclamation_timeout_defaults_and_upper_bounds():
    assert FactReclamationLimits().statement_timeout_ms == 1000
    assert FactReclamationLimits().pinned_ranges_statement_timeout_ms == 1000
    assert FactReclamationLimits(statement_timeout_ms=60000,
                                 pinned_ranges_statement_timeout_ms=60000).statement_timeout_ms == 60000
    for field in ("statement_timeout_ms", "pinned_ranges_statement_timeout_ms"):
        with pytest.raises(ValueError, match=f"field={field}"):
            FactReclamationLimits(**{field: 60001})
    assert FactReclamationLimits().max_handoff_seconds == 10
    assert FactReclamationLimits(max_handoff_seconds=60).max_handoff_seconds == 60
    with pytest.raises(ValueError, match="field=max_handoff_seconds"):
        FactReclamationLimits(max_handoff_seconds=61)



class _PinnedSession:
    def __init__(self, count=3):
        self.count = count
        self.calls = []

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters or {}))
        if "canonical_reclaim_pinned_ranges" in sql:
            return SimpleNamespace(scalar_one=lambda: self.count)
        return SimpleNamespace()


def _pin_reclaimer(*, run_remaining, checked=None):
    checked = checked if checked is not None else []
    archive = SimpleNamespace(_check_budget=lambda: checked.append("checked"))
    limits = FactReclamationLimits(statement_timeout_ms=5000,
                                   pinned_ranges_statement_timeout_ms=1000)
    return PostgresCanonicalFactReclamationRepository(
        archive_repository=archive, limits=limits,
        remaining_run_seconds=lambda: run_remaining[0],
    )


def test_shared_pin_audit_recomputes_run_budget_and_exposes_effective_timeout():
    remaining = [0.25]
    checked = []
    session = _PinnedSession()
    reclaimer = _pin_reclaimer(run_remaining=remaining, checked=checked)
    assert reclaimer._pinned_ranges(session, date(2026, 1, 1)) == (3, 250)
    assert checked == ["checked"]
    assert [params["timeout"] for sql, params in session.calls if "set_config" in sql] == ["250", "5000"]

    remaining[0] = 0
    expired = _PinnedSession()
    with pytest.raises(RuntimeError, match="pinned_ranges_budget_exhausted"):
        reclaimer._pinned_ranges(expired, date(2026, 1, 1))
    assert expired.calls == []


def test_handoff_pin_audit_recomputes_run_and_handoff_budgets_before_query():
    run_remaining = [0.25]
    handoff_remaining = [0.125]
    session = _PinnedSession(count=4)
    reclaimer = _pin_reclaimer(run_remaining=run_remaining)
    assert reclaimer._pinned_ranges(
        session, date(2026, 1, 1), remaining_seconds=lambda: handoff_remaining[0]
    ) == (4, 125)
    assert [params["timeout"] for sql, params in session.calls if "set_config" in sql] == ["125", "5000"]

    handoff_remaining[0] = 0
    expired = _PinnedSession()
    with pytest.raises(RuntimeError, match="pinned_ranges_budget_exhausted"):
        reclaimer._pinned_ranges(
            expired, date(2026, 1, 1), remaining_seconds=lambda: handoff_remaining[0]
        )
    assert expired.calls == []
