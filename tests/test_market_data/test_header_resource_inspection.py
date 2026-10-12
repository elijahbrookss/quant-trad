"""Composition binding checks use synthetic resource observations only."""
from dataclasses import replace

import pytest

from portal.backend.service.storage.header_inspection import _bound_resource_targets
from portal.backend.service.storage.header_resources import VerifiedHeaderResources
from portal.backend.service.storage_management import StorageConflict
from tests.test_storage_move_budget import request_data


def make_bindings(capacity):
    result = []
    for role, oid in (("database", None), ("wal", None), ("database_default", 1663),
                      ("temporary_files", 1663), ("temporary_relations", 1663)):
        evidence = capacity["ssd"]
        result.append({"role": role, "tablespace_oid": oid, "target_id": "ssd",
                       "filesystem_uuid": evidence.filesystem_uuid, "device_id": evidence.device_id})
    return tuple(result)


@pytest.fixture
def bindings(request_data):
    resources = VerifiedHeaderResources("123/42", 77, request_data["now"], request_data["now"],
                                        "", request_data["capacity"], make_bindings(request_data["capacity"]))
    return request_data, resources


def test_resource_targets_are_derived_from_verified_bindings(bindings):
    request, resources = bindings
    assert _bound_resource_targets(resources, request["targets"], request["capacity"]) == ("ssd", ("ssd",))


@pytest.mark.parametrize("role", ["wal", "temporary_relations"])
def test_resource_roles_can_share_history_or_use_different_filesystems(bindings, role):
    request, resources = bindings
    evidence = request["capacity"]["hdd"]
    changed = tuple({**item, "target_id": "hdd", "filesystem_uuid": evidence.filesystem_uuid,
                     "device_id": evidence.device_id} if item["role"] == role else item
                    for item in resources.bindings)
    result = _bound_resource_targets(replace(resources, bindings=changed), request["targets"], request["capacity"])
    assert result == (("hdd", ("ssd",)) if role == "wal" else ("ssd", ("hdd", "ssd")))


@pytest.mark.parametrize("change", ["missing_capacity", "device", "uuid", "path", "readonly",
                                    "missing_wal", "extra_wal", "unknown_target", "binding_uuid",
                                    "missing_temp", "extra_temp", "default_space", "unknown_role"])
def test_mismatched_or_incomplete_binding_evidence_refuses(bindings, change):
    request, resources = bindings
    capacity = dict(resources.capacity)
    rows = [dict(item) for item in resources.bindings]
    if change == "missing_capacity":
        capacity.pop("hdd")
    elif change in ("device", "uuid", "path", "readonly"):
        fields = {"device": {"device_id": "8:9"}, "uuid": {"filesystem_uuid": "other"},
                  "path": {"path": "/different"}, "readonly": {"read_only": True}}
        capacity["hdd"] = replace(capacity["hdd"], **fields[change])
    elif change == "missing_wal":
        rows = [item for item in rows if item["role"] != "wal"]
    elif change == "extra_wal":
        rows.append(dict(rows[1]))
    elif change == "unknown_target":
        rows[1]["target_id"] = "unknown"
    elif change == "binding_uuid":
        rows[1]["filesystem_uuid"] = "wrong"
    elif change == "missing_temp":
        rows = [item for item in rows if item["role"] != "temporary_relations"]
    elif change == "extra_temp":
        rows.append(dict(rows[-1]))
    elif change == "default_space":
        rows[2]["tablespace_oid"] = 9000
    else:
        rows[1]["role"] = "unknown"
    with pytest.raises(StorageConflict, match="resource_"):
        _bound_resource_targets(replace(resources, capacity=capacity, bindings=tuple(rows)),
                                request["targets"], request["capacity"])
