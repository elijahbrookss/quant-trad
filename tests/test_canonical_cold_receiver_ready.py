from time import time

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.automation import canonical_cold_receiver_ready as ready
from scripts.db.canonical_cold_replayed_checkpoint import build_replayed_checkpoint


@pytest.fixture
def replayed_receiver():
    return replayed_receiver_evidence()


def replayed_receiver_evidence():
    first = "000000010000000000000003"
    through = "000000010000000000000004"
    layout = {"layout_version": "market.fact_storage_tiers.v2", "certificate_sha256": "c" * 64}
    baseline = {"schema": "qt.encrypted_recovery_repository_verification.v1", "database_identity": "123/42",
                "pair_receipt_sha256": "b" * 64, "storage_layout": layout, "anchor_sha256": None,
                "wal_start": first, "wal_backup_stop": first, "wal_verified_through": first, "wal_segment_bytes": 2**24}
    baseline["verification_sha256"] = archive_evidence_hash(baseline)
    scope = ready.prepared_receiver_scope(
        approved_package_sha256="d" * 64, receiver_preflight_sha256="e" * 64,
        site_policy=baseline | {"base_transfer_sha256": "a" * 64,
                                "baseline_verification_sha256": baseline["verification_sha256"]},
    )
    endpoint = {"database_identity": "123/42", "timeline": 1, "target_lsn": "0/4017B28", "replay_lsn": "0/4017B80",
                "accepted_commit_seq": 13, "storage_layout": layout}
    archive = endpoint | {"schema": "qt.paused_archive_suffix.v1", "pair_receipt_sha256": baseline["pair_receipt_sha256"],
                          "catalog_inventory_sha256": "1" * 64, "catalog_objects": 7, "catalog_bytes": 555}
    archive["verification_sha256"] = archive_evidence_hash(archive)
    control = f"""pg_control version number: 1300
Database cluster state: shut down in recovery
Database system identifier: 123
Latest checkpoint location: 0/03000060
Latest checkpoint's REDO location: 0/03000028
Latest checkpoint's REDO WAL file: {first}
Latest checkpoint's TimeLineID: 1
Minimum recovery ending location: 0/04017B80
Min recovery ending loc's timeline: 1
Bytes per WAL segment: 16777216
"""
    checkpoint = build_replayed_checkpoint(
        pair_receipt_sha256=baseline["pair_receipt_sha256"], database_identity="123/42", timeline=1,
        wal_first=first, wal_verified_through=through, wal_segment_bytes=2**24, wal_verification_sha256="2" * 64,
        paused_receipt=endpoint, archive_receipt=archive, stopped_control=control, stopped_control_sha256="3" * 64,
    )
    return scope, baseline, checkpoint


def test_ready_versions_bind_exact_digest_and_verified_checkpoint(replayed_receiver):
    scope, baseline, checkpoint = replayed_receiver
    options = {"scope": scope, "materialized_sha256": "4" * 64, "expires_at": int(time()) + 60}
    validator = {"scope": scope, "baseline_verification": baseline}
    old = ready.make_prepared_receiver_ready(**options)
    assert old["schema"] == ready.PREPARED_READY_VERSION
    assert ready.verify_prepared_receiver_ready(old, scope=scope) == archive_evidence_hash(old)
    body = ready.make_prepared_receiver_ready(**options, replayed_checkpoint=checkpoint, baseline_verification=baseline)
    assert body["schema"] == ready.PREPARED_READY_REPLAYED_VERSION
    assert ready.verify_prepared_receiver_ready(body, **validator) == archive_evidence_hash(body)
    assert checkpoint["wal_verified_through"] != checkpoint["redo_wal_segment"]

    with pytest.raises(ValueError, match="original_baseline_changed"):
        ready.verify_prepared_receiver_ready(body, scope=scope)
    with pytest.raises(ValueError, match="prepared_scope_changed"):
        ready.verify_prepared_receiver_ready(body | {"schema": old["schema"]}, **validator)
    with pytest.raises(TimeoutError, match="prepared_receiver_expired"):
        ready.verify_prepared_receiver_ready(body | {"expires_at": 0}, **validator)
    with pytest.raises(ValueError, match="original_baseline_changed"):
        ready.verify_prepared_receiver_ready(body, scope=scope, baseline_verification=baseline | {"anchor_sha256": "a" * 64})
    with pytest.raises(ValueError, match="checkpoint_digest_changed"):
        ready.verify_prepared_receiver_ready(body | {"replayed_checkpoint": checkpoint | {"accepted_commit_seq": 14}}, **validator)


@pytest.mark.parametrize(("change", "reason"), [
    ({"pair_receipt_sha256": "1" * 64}, "checkpoint_scope_changed"),
    ({"database_identity": "123/43"}, "checkpoint_scope_changed"),
    ({"wal_first": "000000010000000000000004"}, "checkpoint_scope_changed"),
    ({"timeline": 2}, "checkpoint_scope_invalid"),
    ({"wal_verified_through": "000000010000000000000005"}, "wal_coverage_changed"),
    ({"redo_wal_segment": "000000010000000000000004"}, "wal_coverage_changed"),
    ({"minimum_recovery_lsn": "0/4017B81"}, "checkpoint_lsn_changed"),
])
def test_replayed_checkpoint_refuses_rehashed_wrong_scope(replayed_receiver, change, reason):
    _, baseline, checkpoint = replayed_receiver
    changed = {key: value for key, value in checkpoint.items() if key != "verification_sha256"} | change
    digest = archive_evidence_hash(changed)
    changed["verification_sha256"] = digest
    with pytest.raises(ValueError, match="checkpoint_digest_changed"):
        ready.verify_replayed_checkpoint_baseline(
            changed, baseline_verification=baseline, expected_checkpoint_sha256=checkpoint["verification_sha256"],
        )
    with pytest.raises(ValueError, match=reason):
        ready.verify_replayed_checkpoint_baseline(changed, baseline_verification=baseline, expected_checkpoint_sha256=digest)
