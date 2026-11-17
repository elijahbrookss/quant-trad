import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from market_data.fact_archive import archive_evidence_hash
from scripts.automation import canonical_cold_receiver_archive as receiver
from scripts.automation.stage_encrypted_recovery_handoff import _encode, _identity
from scripts.db import canonical_cold_recovery_suffix as suffix


@pytest.fixture
def archive_materializer(tmp_path, monkeypatch):
    base, pgdata, output = (tmp_path / name for name in ("base", "pgdata", "materialized"))
    for path in (base, pgdata, output):
        path.mkdir(mode=0o700)
    for path in (base / "objects", base / "objects" / "raw"):
        path.mkdir(mode=0o700)
    contents = {"raw/base": b"receiver base fixture", "raw/suffix": b"receiver suffix fixture"}
    (base / "objects" / "raw" / "base").write_bytes(contents["raw/base"])
    descriptors = {key: (hashlib.sha256(data).hexdigest(), len(data)) for key, data in contents.items()}
    inventory = suffix._encode("raw/base", descriptors["raw/base"])
    (base / "inventory.jsonl").write_bytes(inventory)
    layout = {"layout_version": "fixture", "certificate_sha256": "a" * 64}
    pair = {"schema_version": "qt.encrypted_recovery_pair.v1", "restore_endpoint": "immediate",
            "database_label": "20261115-000000F", "database_identity": "100/20", "storage_layout": layout,
            "archive_snapshot": "b" * 64, "archive_objects": 1, "archive_bytes": descriptors["raw/base"][1],
            "archive_source_root": "/objects", "inventory_snapshot_path": "/inventory.jsonl",
            "inventory_sha256": hashlib.sha256(inventory).hexdigest()}
    receipt = tmp_path / "complete.json"
    receipt.write_bytes(_encode(pair))
    native = {"target_lsn": "0/3000050", "replay_lsn": "0/3000090", "database": "qt", "database_identity": "100/20",
              "pgdata": str(pgdata), "read_only": "on", "isolation": "repeatable read", "paused": True, "archive_only": True,
              "isolated_socket": True, "exact_target": True, "version": 150006, "target_timeline": "1", "timeline": 1,
              "postmaster_started": "123.5"}

    class Connection:
        engine = SimpleNamespace(url=SimpleNamespace(database="qt"))

        def in_transaction(self):
            return True

        def execute(self, sql, parameters=None):
            if sql is receiver._NATIVE_PAUSE:
                assert parameters == {"target": "qt"}
                return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: native))
            if "set_config" in str(sql):
                return None
            if "to_regclass" in str(sql):
                return SimpleNamespace(scalar_one=lambda: False)
            records = [
                {"id": str(index), "object_key": key, "object_sha256": sha, "byte_count": size}
                for index, (key, (sha, size)) in enumerate(descriptors.items(), start=1)
            ] if "market.raw_archive_manifests" in str(sql) and parameters["after"] == "" else []
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: records))

    monkeypatch.setattr(receiver, "_snapshot_layout", lambda _: layout)
    monkeypatch.setattr(receiver.PostgresMarketDataRepository, "_current_commit_seq_with_session", lambda _: 2)
    options = {"pgdata": pgdata, "pair_receipt": receipt, "pair_receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
               "base_restore": base, "output": output, "max_objects": 3, "max_work_bytes": 1024 * 1024, "reserve_bytes": 0}
    return options, Connection(), native, contents


def test_receiver_archive_refuses_writable_source(archive_materializer, tmp_path):
    options, _, _, _ = archive_materializer
    source = tmp_path / "source"
    source.mkdir()
    with pytest.raises(ValueError, match="readonly_source_bind_required"):
        receiver.bind_replayed_archive_materializer(**options, source_archive=source, source_identity=_identity(source.lstat()))


@pytest.mark.skipif(not os.environ.get("QT_TEST_RECEIVER_ARCHIVE_BIND"), reason="requires an explicit local read-only archive bind")
def test_receiver_archive_copies_reuses_and_rejects_tamper(archive_materializer):
    options, conn, native, contents = archive_materializer
    source = Path(os.environ["QT_TEST_RECEIVER_ARCHIVE_BIND"])
    bind_options = options | {"source_archive": source, "source_identity": _identity(source.lstat())}
    materialize = receiver.bind_replayed_archive_materializer(**bind_options)
    native["read_only"] = "off"
    with pytest.raises(RuntimeError, match="native_endpoint_changed"):
        materialize(conn, lambda: 20)
    assert not any(options["output"].iterdir())
    native["read_only"] = "on"
    first = materialize(conn, lambda: 20)
    assert first["copied_objects"] == first["copied_baseline_objects"] == 1
    assert first["catalog_objects"] == 2
    inventory, objects = Path(first["catalog_inventory_path"]), Path(first["suffix_root"])
    assert hashlib.sha256(inventory.read_bytes()).hexdigest() == first["catalog_inventory_sha256"]
    assert first["verification_sha256"] == archive_evidence_hash({key: value for key, value in first.items()
                                                                  if key != "verification_sha256"})
    for key, expected in contents.items():
        assert (objects / key).read_bytes() == expected
    retry = materialize(conn, lambda: 20)
    assert retry["reused_objects"] == retry["reused_baseline_objects"] == 1
    assert retry["catalog_inventory_sha256"] == first["catalog_inventory_sha256"]
    extra = objects / "raw" / "unregistered"
    extra.touch(mode=0o600)
    with pytest.raises(RuntimeError, match="materialized_namespace_changed"):
        materialize(conn, lambda: 20)
    extra.unlink()
    (objects / "raw" / "suffix").write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="checkpoint_bytes_changed"):
        materialize(conn, lambda: 20)
