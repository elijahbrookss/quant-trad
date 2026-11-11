from __future__ import annotations

import json
from cli.main import main


def test_bot_start_transmits_explicit_simulation_fee_schedule(monkeypatch):
    import cli.main as cli_module
    captured = {}
    class Client:
        def request_json(self, method, path, payload):
            captured.update(method=method, path=path, payload=payload)
            return {'status': 'accepted'}
    monkeypatch.setattr(cli_module, '_client', lambda args: Client())
    fees = {'btc': {'source': 'simulation:scenario', 'version': 'v1'}}
    assert main(['--no-audit-log', 'bots', 'start', 'bot', '--economic-claim-intent', 'exploration',
        '--simulation-fee-schedules-json', json.dumps(fees)]) == 0
    assert captured['path'] == '/api/bots/bot/runs/start'
    assert captured['payload']['simulation_fee_schedules'] == fees


def test_dataset_preparation_transmits_same_fee_schedule(monkeypatch):
    import cli.main as cli_module
    captured = {}
    class Client:
        def request_json(self, method, path, payload):
            captured.update(path=path, payload=payload)
            return {'dataset_id': 'fixture'}
    monkeypatch.setattr(cli_module, '_client', lambda args: Client())
    fees = {'btc': {'source': 'simulation:scenario', 'version': 'v1'}}
    assert main(['--no-audit-log', 'data', 'prepare-backtest-dataset', '--bot-id', 'bot',
        '--start', '2024-03-01T00:00:00Z', '--end', '2024-05-31T00:00:00Z',
        '--simulation-fee-schedules-json', json.dumps(fees)]) == 0
    assert captured['path'] == '/api/bots/bot/backtest-dataset/prepare'
    assert captured['payload']['simulation_fee_schedules'] == fees
    assert captured['payload']['acquire_missing'] is False
