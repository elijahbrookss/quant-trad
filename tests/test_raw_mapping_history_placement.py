"""Reject malformed or already-cancelled physical requests before database use."""
import pytest

from scripts.db import raw_mapping_v2_placement as move


class NoDatabase:
    def connect(self):
        raise AssertionError("invalid physical request reached the database")


def options():
    return dict(request_id="reviewed-raw-placement", handoff_sha256="a"*64, policy=None,
        resource_limits={"wal_bytes":1, "temporary_bytes":{"ssd":0,"hdd":0},
            "growth_bytes_per_second":{"ssd":0,"hdd":0},
            "maintenance_bytes":{"ssd":0,"hdd":0}, "movement_timeout_seconds":60,
            "cancellation_grace_seconds":1})


@pytest.mark.parametrize("request_id", [None, True, "", "a"*81, "../request", "request;select", "request\n"])
def test_invalid_request_identity_never_opens_database(request_id):
    with pytest.raises(ValueError, match="request_id_invalid"):
        move.move_retained_raw_history(NoDatabase(), **{**options(), "request_id":request_id})
    with pytest.raises(ValueError, match="request_id_invalid"):
        move.cancel_retained_raw_history(NoDatabase(), request_id=request_id, handoff_sha256="a"*64)


@pytest.mark.parametrize("digest", [None, "A"*64, "g"*64, "a"*63, "a"*65])
def test_invalid_handoff_never_opens_database(digest):
    with pytest.raises(ValueError, match="handoff_hash_invalid"):
        move.move_retained_raw_history(NoDatabase(), **{**options(), "handoff_sha256":digest})


@pytest.mark.parametrize("seconds", [0, -1, True, 3601, 1.5])
def test_move_cannot_borrow_extended_migration_duration(seconds):
    args = options()
    args["resource_limits"]["movement_timeout_seconds"] = seconds
    with pytest.raises(ValueError, match="limits_invalid"):
        move.move_retained_raw_history(NoDatabase(), **args)


def test_cancelled_move_never_opens_database():
    with pytest.raises(RuntimeError, match="storage_move_cancelled"):
        move.move_retained_raw_history(NoDatabase(), cancelled=lambda:True, **options())
