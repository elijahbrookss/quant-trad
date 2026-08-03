from __future__ import annotations

from portal.backend.service.bots import botlens_bootstrap_service


def test_botlens_run_meta_exposes_execution_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        botlens_bootstrap_service,
        "get_bot_run",
        lambda _run_id: {
            "run_id": "run-1",
            "bot_id": "bot-1",
            "status": "running",
            "config_snapshot": {"execution_mode": "full"},
        },
    )

    meta = botlens_bootstrap_service._run_meta(
        run_id="run-1",
        projected_bot={"id": "bot-1"},
        health_state={"status": "running"},
    )

    assert meta["execution_mode"] == "full"
    assert meta["intrabar_execution"] is True


def test_botlens_run_meta_exposes_frozen_dataset_bounds(monkeypatch) -> None:
    monkeypatch.setattr(
        botlens_bootstrap_service,
        'get_bot_run',
        lambda _run_id: {
            'run_id': 'run-1',
            'bot_id': 'bot-1',
            'status': 'completed',
            'config_snapshot': {
                'dataset_binding': {
                    'dataset_id': 'mds-frozen',
                    'dataset_hash': 'hash-frozen',
                    'max_commit_seq': 73,
                    'evaluation_range': {
                        'start': '2025-01-01T00:00:00Z',
                        'end_exclusive': '2026-01-01T00:00:00Z',
                    },
                    'materialization_range': {
                        'start': '2024-12-20T00:00:00Z',
                        'end_exclusive': '2026-01-01T00:00:00Z',
                    },
                }
            },
        },
    )

    meta = botlens_bootstrap_service._run_meta(
        run_id='run-1',
        projected_bot={'id': 'bot-1'},
        health_state={'status': 'completed'},
    )

    assert meta['backtest_start'] == '2025-01-01T00:00:00Z'
    assert meta['backtest_end'] == '2026-01-01T00:00:00Z'
    assert meta['materialization_start'] == '2024-12-20T00:00:00Z'
    assert meta['dataset'] == {
        'dataset_id': 'mds-frozen',
        'dataset_hash': 'hash-frozen',
        'max_commit_seq': 73,
    }
