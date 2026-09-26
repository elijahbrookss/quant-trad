"""Synthetic resource envelopes; no disks, credentials or database access."""
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from core.storage_mounts import FilesystemEvidence
from core.storage_move_budget import assess_header_move_resources
from core.storage_targets import StoragePolicy, StorageTarget


@pytest.fixture
def request_data():
    targets = tuple(StorageTarget(name, name, "uuid-" + name, "/storage/" + name, medium)
                    for name, medium in [("ssd", "ssd"), ("hdd", "hdd")])
    capacity = {item.target_id: FilesystemEvidence(item.root, "8:" + str(index),
                item.filesystem_uuid, 10000, 2000, 8000, False)
                for index, item in enumerate(targets)}
    now = datetime(2026, 9, 17, tzinfo=UTC)
    return dict(targets=targets, capacity=capacity,
        policy=StoragePolicy(("ssd",), ("hdd",), ("hdd",), ("hdd",)),
        observed_at=now, now=now, copy_target_id="hdd", copy_bytes=1000,
        own_reserved_bytes=1200, reserved_bytes={"ssd": 100, "hdd": 1500},
        auxiliary_reserved_bytes={"ssd": 0, "hdd": 0},
        own_auxiliary_reserved_bytes={"ssd": 0, "hdd": 0},
        wal_target_id="ssd", wal_bytes=400,
        temporary_bytes={"ssd": 200, "hdd": 50},
        growth_bytes_per_second={"ssd": 10, "hdd": 20},
        maintenance_bytes={"ssd": 100, "hdd": 150},
        timeout_seconds=10, cancellation_grace_seconds=2)


def rows(result):
    return {item["target_id"]: item for item in result["filesystems"]}


def test_demands_share_physical_capacity_and_own_reservation_is_counted_once(request_data):
    result = assess_header_move_resources(**request_data)
    budget = rows(result)
    assert result["capacity_sufficient_for_declared_limits"]
    assert budget["ssd"]["required_bytes"] == 100 + 400 + 200 + 120 + 100 + 2000
    assert budget["hdd"]["required_bytes"] == 1000 + 300 + 50 + 240 + 150 + 2000
    assert result["source_space_credited_bytes"] == 0
    assert not result["execution_available"] and not result["activation_ready"]
    assert result["uncovered"]


def test_wal_on_history_accumulates_with_copy_and_temp_instead_of_reusing_free_space(request_data):
    request_data.update(wal_target_id="hdd", wal_bytes=5000)
    result = assess_header_move_resources(**request_data)
    assert not result["capacity_sufficient_for_declared_limits"]
    assert result["blockers"] == [{"code": "filesystem_headroom_insufficient",
                                   "target_id": "hdd", "shortfall_bytes": 740}]
    assert rows(result)["ssd"]["additional_wal_bytes"] == 0


def test_source_drive_can_block_move_even_with_plenty_of_destination_space(request_data):
    request_data["capacity"]["ssd"] = replace(request_data["capacity"]["ssd"],
                                            used_bytes=8000, available_bytes=2000)
    result = assess_header_move_resources(**request_data)
    assert not result["capacity_sufficient_for_declared_limits"]
    assert result["blockers"][0]["target_id"] == "ssd"


@pytest.mark.parametrize("name", ["capacity", "reserved_bytes", "temporary_bytes",
                                  "growth_bytes_per_second", "maintenance_bytes",
                                  "auxiliary_reserved_bytes", "own_auxiliary_reserved_bytes"])
@pytest.mark.parametrize("mutation", ["missing", "unknown"])
def test_incomplete_or_unknown_resource_accounting_refuses_instead_of_assuming_zero(request_data, name, mutation):
    if mutation == "missing":
        request_data[name].pop("ssd")
    else:
        request_data[name]["unknown"] = next(iter(request_data[name].values()))
    with pytest.raises(ValueError, match="complete"):
        assess_header_move_resources(**request_data)


@pytest.mark.parametrize("seconds", [-1, 31])
def test_future_or_stale_capacity_cannot_authorize_a_budget(request_data, seconds):
    request_data["now"] += timedelta(seconds=seconds)
    with pytest.raises(ValueError, match="stale or future"):
        assess_header_move_resources(**request_data)


@pytest.mark.parametrize("name,value", [
    ("timeout_seconds", True), ("timeout_seconds", 0), ("timeout_seconds", 4*86400+1),
    ("cancellation_grace_seconds", 0), ("cancellation_grace_seconds", 61),
    ("copy_bytes", -1), ("copy_bytes", 1201), ("own_reserved_bytes", 0),
    ("own_reserved_bytes", 1600), ("wal_bytes", 0), ("wal_bytes", 1.5),
    ("copy_target_id", "ssd"), ("wal_target_id", "missing"), ("wal_target_id", []),
])
def test_invalid_or_unreserved_limits_are_rejected(request_data, name, value):
    request_data[name] = value
    with pytest.raises(ValueError, match="storage_move_budget_invalid"):
        assess_header_move_resources(**request_data)


@pytest.mark.parametrize("change", [
    {"filesystem_uuid": "wrong"}, {"device_id": "8:0"},
    {"total_bytes": 0}, {"used_bytes": 9000}, {"available_bytes": -1},
    {"device_id": ""}, {"device_id": "08:0"}, {"read_only": 1},
])
def test_duplicate_or_inconsistent_filesystem_observation_refuses(request_data, change):
    request_data["capacity"]["hdd"] = replace(request_data["capacity"]["hdd"], **change)
    with pytest.raises(ValueError, match="filesystem"):
        assess_header_move_resources(**request_data)


def test_readonly_target_blocks_without_claiming_success(request_data):
    request_data["capacity"]["ssd"] = replace(request_data["capacity"]["ssd"], read_only=True)
    result = assess_header_move_resources(**request_data)
    assert result["blockers"] == [{"code": "filesystem_not_writable_active", "target_id": "ssd"}]


def test_cancellation_grace_counts_growth_and_demand_never_credits_existing_used_bytes(request_data):
    before = rows(assess_header_move_resources(**request_data))
    request_data["cancellation_grace_seconds"] += 5
    after = rows(assess_header_move_resources(**request_data))
    assert after["ssd"]["required_bytes"] - before["ssd"]["required_bytes"] == 50
    assert after["hdd"]["required_bytes"] - before["hdd"]["required_bytes"] == 100


def test_overflow_and_boolean_allowances_refuse(request_data):
    request_data["growth_bytes_per_second"]["ssd"] = 2**63 - 1
    with pytest.raises(ValueError, match="combined"):
        assess_header_move_resources(**request_data)
    request_data["growth_bytes_per_second"]["ssd"] = True
    with pytest.raises(ValueError, match="growth rates"):
        assess_header_move_resources(**request_data)


def test_exact_headroom_boundary_is_accepted_but_one_byte_less_blocks(request_data):
    required = rows(assess_header_move_resources(**request_data))["hdd"]["required_bytes"]
    request_data["capacity"]["hdd"] = replace(request_data["capacity"]["hdd"],
                                            available_bytes=required, used_bytes=10000-required)
    assert assess_header_move_resources(**request_data)["capacity_sufficient_for_declared_limits"]
    request_data["capacity"]["hdd"] = replace(request_data["capacity"]["hdd"],
                                            available_bytes=required-1, used_bytes=10001-required)
    result = assess_header_move_resources(**request_data)
    assert result["blockers"][0]["shortfall_bytes"] == 1


@pytest.mark.parametrize("age,rounded", [(0.001, 1), (20, 20), (30, 30)])
def test_observation_age_is_rounded_up_and_reserved_as_additional_growth(request_data, age, rounded):
    before = rows(assess_header_move_resources(**request_data))
    request_data["now"] += timedelta(seconds=age)
    result = assess_header_move_resources(**request_data)
    after = rows(result)
    assert result["observation_age_seconds"] == rounded
    assert result["growth_window_seconds"] == 12 + rounded
    assert after["ssd"]["required_bytes"] - before["ssd"]["required_bytes"] == 10 * rounded
    assert after["hdd"]["required_bytes"] - before["hdd"]["required_bytes"] == 20 * rounded


def test_competing_auxiliary_claims_count_once_and_own_claim_is_replaced(request_data):
    before = rows(assess_header_move_resources(**request_data))
    request_data["auxiliary_reserved_bytes"] = {"ssd": 800, "hdd": 400}
    request_data["own_auxiliary_reserved_bytes"] = {"ssd": 500, "hdd": 200}
    after = rows(assess_header_move_resources(**request_data))
    assert after["ssd"]["required_bytes"] == before["ssd"]["required_bytes"] + 300
    assert after["hdd"]["required_bytes"] == before["hdd"]["required_bytes"] + 200
    request_data["own_auxiliary_reserved_bytes"]["ssd"] = 801
    with pytest.raises(ValueError, match="below own auxiliary"):
        assess_header_move_resources(**request_data)


def test_initial_migration_horizon_counts_growth_without_relaxing_routine_limits(request_data):
    from portal.backend.service.storage.header_resource_claims import _limits
    limits={key:request_data[key] for key in ("wal_bytes","temporary_bytes",
        "growth_bytes_per_second","maintenance_bytes","cancellation_grace_seconds")}
    limits["movement_timeout_seconds"]=4*86400
    with pytest.raises(ValueError,match="limits_invalid"):_limits(limits)
    assert _limits(limits,migration=True)["movement_timeout_seconds"]==4*86400
    result=assess_header_move_resources(**{**request_data,"timeout_seconds":4*86400})
    assert rows(result)["ssd"]["ingestion_and_other_growth_bytes"]==10*(4*86400+2)
    assert rows(result)["hdd"]["ingestion_and_other_growth_bytes"]==20*(4*86400+2)
    assert not result["capacity_sufficient_for_declared_limits"]
    with pytest.raises(ValueError,match="limits_invalid"):
        _limits({**limits,"movement_timeout_seconds":4*86400+1},migration=True)
