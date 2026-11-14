from types import SimpleNamespace

import pytest

from portal.backend.service.storage import cold_stage_pressure as module


@pytest.fixture
def stage(monkeypatch):
    recent = SimpleNamespace(id="recent", medium="ssd", state="active", reserved_bytes=20,
                             auxiliary_reserved_bytes=10)
    state = SimpleNamespace(free=4_000, wal=0, role_target="recent", caps=[])
    targets = [recent, SimpleNamespace(id="history")]
    filesystem = SimpleNamespace(inspect=lambda **_: SimpleNamespace(device_id="8:1", available_bytes=state.free))
    monkeypatch.setattr(module, "_read", lambda _: (SimpleNamespace(movement_enabled=True, recent=["recent"]), None))
    monkeypatch.setattr(module, "registered_header_targets", lambda _: targets)
    monkeypatch.setattr(module, "_target", lambda _: filesystem)

    def observe(*args, **kwargs):
        return SimpleNamespace(capacity={"recent": SimpleNamespace(device_id="8:1")}, bindings=[
            {"role": role, "target_id": state.role_target} for role in (
                "database", "database_default", "wal", "temporary_files",
            )])

    monkeypatch.setattr(module, "observe_header_resources", observe)

    def scalar(statement, parameters=None):
        query = str(statement)
        if "set_config" in query:
            state.caps.append(parameters["cap"])
            return "1"
        if "pg_wal_lsn_diff" in query:
            return state.wal
        if "pg_current_wal_insert_lsn" in query:
            return "1/1"
        return 1024

    session = SimpleNamespace(scalar=scalar, connection=object, execute=lambda _: None)

    def admit():
        return module.admit_cold_stage_pressure(session, pg_controldata="/private/pg_controldata",
                                                temporary_bytes={"recent": 2048, "history": 0}, wal_bytes=1000)

    return state, admit


def test_stage_uses_approved_envelope_and_checks_wal_advance_between_pages(stage):
    state, admit = stage
    check = admit()
    assert state.caps == [2]
    state.free = 1_900
    state.wal = 400
    check()
    state.wal = 1000
    with pytest.raises(module.ColdStagePressure, match="wal_budget_reached"):
        check()


def test_stage_refuses_tight_actual_runway_or_misdirected_wal(stage):
    state, admit = stage
    state.free = 1000 + 1024 + 20 + 10
    with pytest.raises(module.ColdStagePressure, match="ssd_runway_unavailable"):
        admit()
    state.role_target = "history"
    with pytest.raises(RuntimeError, match="wal_or_temporary_storage_not_on_recent_ssd"):
        admit()
