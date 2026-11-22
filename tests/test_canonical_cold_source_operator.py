import hashlib
from pathlib import Path
from types import SimpleNamespace

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.automation import canonical_cold_source_operator as operator
from scripts.automation.canonical_cold_named_recovery import _publish, _sign
from scripts.automation.canonical_cold_receiver_operator import _replay_selection
from scripts.automation.stage_encrypted_recovery_handoff import TransferLimits
from scripts.db.canonical_cold_recovery_suffix import _encode
from tests.test_canonical_cold_receiver_ready import replayed_receiver_evidence


@pytest.fixture
def site(tmp_path, monkeypatch):
    scope, earlier, checkpoint = replayed_receiver_evidence()
    baseline = {key: value for key, value in earlier.items() if key != "verification_sha256"} | {"database_archive_id": "15-1"}
    baseline["verification_sha256"] = archive_evidence_hash(baseline)
    receiving = tmp_path / "receiving"
    receiving.mkdir(mode=0o700)
    signer = Ed25519PrivateKey.generate()
    public = signer.public_key().public_bytes_raw().hex()
    wal = TransferLimits(10, 2**26, 4, 0)
    pins = {"base_transfer_sha256": scope["base_transfer_sha256"], "receiver_public_key": public,
            "baseline_verification_sha256": baseline["verification_sha256"], "pair_receipt_sha256": baseline["pair_receipt_sha256"]}
    policy = SimpleNamespace(receiving=receiving, receiver_public_key=public, baseline_verification=baseline,
                             base_transfer_sha256=pins["base_transfer_sha256"], preflight_scope=lambda: pins,
                             max_archive_objects=8, wal=wal)
    measured = {"measurement_sha256": "f" * 64, "pending_expected": False}
    monkeypatch.setattr(operator, "_verified_preflight", lambda policy, signed: measured)
    return policy, checkpoint, signer, measured


def _selection(policy, checkpoint, measured):
    return {"schema": operator.REPLAY_SELECTION_VERSION,
            "baseline_verification_sha256": policy.baseline_verification["verification_sha256"],
            "receiver_preflight_sha256": measured["measurement_sha256"],
            **{key: checkpoint[key] for key in ("timeline", "target_lsn", "replay_lsn")},
            "prefixes": [{"after": policy.baseline_verification["wal_backup_stop"], "through": checkpoint["wal_verified_through"],
                          "prefix_sha256": "a" * 64, "previous_prefix_sha256": None}]}


def test_stage_publishes_only_after_target_verification(site, tmp_path, monkeypatch):
    policy, checkpoint, _, measured = site
    source, state = tmp_path / "source", tmp_path / "state"
    source.mkdir(mode=0o700)
    state.mkdir(mode=0o700)
    selection = _selection(policy, checkpoint, measured)
    terminal = checkpoint["wal_verified_through"]
    archive_sha1 = "1" * 40
    pg_waldump, pgbackrest, database_key = (tmp_path / kind for kind in ("pg_waldump", "pgbackrest", "database-key"))

    def stage(**options):
        assert options["target_through"] == terminal
        assert options["progress_path"] == state / "wal-prefix-progress.json"
        return {"target_through": terminal, "terminal": {"segment": terminal, "wal_checksum": archive_sha1},
                "prefixes": selection["prefixes"]}

    def qualify(**options):
        assert options["repository"] == policy.receiving / "database"
        assert options["segment"] == terminal and options["expected_wal_sha1"] == archive_sha1
        assert options["archive_id"] == "15-1" and options["expected_system_identifier"] == "123"
        assert options["record_kind"] == "checkpoint_online" and options["restore_name"] is None
        return {"segment": terminal, **{key: selection[key] for key in ("timeline", "target_lsn", "replay_lsn")}}

    monkeypatch.setattr(operator, "stage_source_wal_prefixes", stage)
    monkeypatch.setattr(operator, "qualify_source_wal_target", qualify)
    options = dict(control=None, expected_database={}, source=source, policy=policy, receiver_preflight={},
        state_directory=state, limits=policy.wal, pg_waldump=pg_waldump, pgbackrest=pgbackrest, pgdata=Path("/local-control"),
        database_key=database_key,
        max_batches=2, timeout_seconds=5, target={"segment": terminal, "record_kind": "checkpoint_online", "restore_name": None,
                                                **{key: selection[key] for key in ("target_lsn", "replay_lsn")}})
    first = operator.stage_recovery_selection(**options)
    assert operator.stage_recovery_selection(**options) == first
    assert hashlib.sha256(first["selection_path"].read_bytes()).hexdigest() == first["selection_sha256"]
    assert _replay_selection(first["selection_path"], policy, measured, lambda: None) == selection
    assert len(list(state.glob("replay-selection-*.json"))) == 1


def test_review_binds_signed_offer_manifest_and_current_source_budget(site, monkeypatch):
    policy, checkpoint, signer, measured = site
    manifest_bytes = _encode("raw/parquet", ("a" * 64, 10))
    manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
    body = {key: value for key, value in checkpoint.items() if key != "verification_sha256"}
    body.update(catalog_inventory_sha256=manifest_digest, catalog_objects=1, catalog_bytes=10)
    checkpoint = body | {"verification_sha256": archive_evidence_hash(body)}
    selection = _selection(policy, checkpoint, measured)
    receiver_plan = "d" * 64
    catalog = {"filename": f"replayed-catalog-{manifest_digest}.jsonl", "sha256": manifest_digest, "bytes": len(manifest_bytes)}
    manifest = policy.receiving / catalog["filename"]
    manifest.write_bytes(manifest_bytes)
    manifest.chmod(0o600)
    offer = policy.receiving / f"replayed-candidate-{checkpoint['verification_sha256']}.json"
    _publish(offer, _sign({"schema": operator.OFFER_VERSION, "checkpoint": checkpoint, "site_plan_sha256": receiver_plan,
                          "receiver_preflight_sha256": measured["measurement_sha256"], "catalog": catalog}, signer))
    calls = []

    def current_capacity(control, **options):
        calls.append(options)
        assert options["backup_stop"] == checkpoint["wal_verified_through"]
        assert options["restart_from"] == checkpoint["redo_wal_segment"]
        assert options["max_restart_segments"] == 3
        return 1

    monkeypatch.setattr(operator, "check_source_wal_capacity", current_capacity)
    options = dict(control=None, expected_database={}, policy=policy, receiver_preflight={}, receiver_plan_sha256=receiver_plan,
                   selection=selection, offer_path=offer, pending_spool_scope_sha256=None, max_restart_segments=3,
                   timeout_seconds=5)
    report = operator.review_replayed_offer(**options)
    assert report["replayed_inventory"] == manifest
    assert report["checkpoint"] == checkpoint and report["archive_objects"] == 1
    assert report["independent_recovery"]["replayed_checkpoint_sha256"] == checkpoint["verification_sha256"]
    assert report["source_wal_files_now"] == 1
    with pytest.raises(ValueError, match="offer_scope_changed"):
        operator.review_replayed_offer(**(options | {"receiver_plan_sha256": "b" * 64}))
    with pytest.raises(ValueError, match="offer_replay_changed"):
        operator.review_replayed_offer(**(options | {"selection": selection | {"target_lsn": "0/4000001"}}))
    with pytest.raises(ValueError, match="restart_budget_has_no_final_wal_room"):
        operator.review_replayed_offer(**(options | {"max_restart_segments": 2}))
    assert len(calls) == 1


def test_source_connection_requires_private_control_database_pin(tmp_path):
    credential = Path(tmp_path) / "fixture-dsn"
    credential.write_text("postgresql://user@localhost/target", encoding="ascii")
    credential.chmod(0o600)
    with pytest.raises(ValueError, match="control_database_invalid"), operator._source_control(credential):
        pytest.fail("target database connection was opened")
