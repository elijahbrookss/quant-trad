from __future__ import annotations

from types import SimpleNamespace

from portal.backend.service.storage.repos.report_materializations import (
    _input_fingerprint_status_payload,
)


def test_report_input_fingerprint_exposes_frozen_dataset_identity() -> None:
    run = SimpleNamespace(
        run_id="run-1",
        bot_id="bot-1",
        status="completed",
        run_type="backtest",
        started_at=None,
        ended_at=None,
        updated_at=None,
        config_hash="config-hash",
        material_config_hash="material-config-hash",
        strategy_hash="strategy-hash",
        data_snapshot_hash=None,
        runtime_contract_version="runtime-contract",
        runtime_source_revision="revision",
        runtime_image="image",
        storage_schema_version="storage-contract",
        summary={},
        config_snapshot={
            "dataset_binding": {
                "dataset_id": "mds_test",
                "dataset_hash": "a" * 64,
            }
        },
    )

    payload = _input_fingerprint_status_payload(
        run=run,
        event_count=0,
        event_high_water_run_seq=0,
        event_high_water_id=0,
        event_updated_at=None,
        trade_count=0,
        trade_updated_at=None,
    )

    assert payload["dataset_id"] == "mds_test"
    assert payload["dataset_hash"] == "a" * 64
