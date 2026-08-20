from __future__ import annotations

import json
import urllib.parse
import urllib.request

import pytest

from cli.main import main


class _Response:
    status = 200
    headers: dict[str, str] = {}

    def __init__(self, payload: dict) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self) -> bytes:
        return self._body


def _acquire_args() -> list[str]:
    return [
        "--no-audit-log",
        "data",
        "acquire-numeric-facts",
        "--manifest-path",
        "config/market-data/chainlink.numeric-facts.v1.yaml",
        "--binding-id",
        "chainlink-eth-usd",
        "--mode",
        "historical",
        "--start",
        "2026-01-01T00:00:00Z",
        "--end",
        "2026-01-02T00:00:00Z",
        "--allow-network",
        "--requested-by",
        "operator@example.test",
        "--reason",
        "bounded coverage repair",
        "--max-requests",
        "17",
        "--max-logs",
        "5000",
        "--max-blocks",
        "250000",
        "--max-retries",
        "4",
        "--repair",
    ]


def test_acquire_numeric_facts_sends_explicit_auth_and_budget(monkeypatch) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"result": {"complete": True}})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    assert main(_acquire_args()) == 0
    assert observed == {
        "method": "POST",
        "path": "/api/market-data/numeric-facts/acquire",
        "body": {
            "manifest_path": "config/market-data/chainlink.numeric-facts.v1.yaml",
            "binding_id": "chainlink-eth-usd",
            "mode": "historical",
            "start": "2026-01-01T00:00:00Z",
            "end": "2026-01-02T00:00:00Z",
            "allow_network": True,
            "requested_by": "operator@example.test",
            "reason": "bounded coverage repair",
            "max_requests": 17,
            "max_logs": 5000,
            "max_blocks": 250000,
            "max_retries": 4,
            "repair": True,
        },
    }


def test_acquire_numeric_facts_defaults_to_network_denied_and_exits_nonzero_when_incomplete(
    monkeypatch,
) -> None:
    observed = {}

    def fake_urlopen(request, timeout):
        observed.update(body=json.loads(request.data.decode("utf-8")))
        return _Response(
            {
                "result": {
                    "complete": False,
                    "gaps": [{"reason": "rpc_capability_denied"}],
                }
            }
        )

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "acquire-numeric-facts",
            "--manifest-path",
            "config/market-data/chainlink.numeric-facts.v1.yaml",
            "--binding-id",
            "chainlink-eth-usd",
            "--mode",
            "current",
            "--requested-by",
            "operator@example.test",
            "--reason",
            "offline coverage inspection",
            "--max-requests",
            "1",
            "--max-logs",
            "1",
            "--max-blocks",
            "1",
        ]
    )

    assert exit_code == 1
    assert observed["body"]["allow_network"] is False
    assert observed["body"]["repair"] is False
    assert observed["body"]["max_retries"] == 2
    assert observed["body"]["start"] is None
    assert observed["body"]["end"] is None


@pytest.mark.parametrize(
    "required_flag",
    ["--requested-by", "--reason", "--max-requests", "--max-logs", "--max-blocks"],
)
def test_acquire_numeric_facts_requires_auth_and_budget_flags(
    required_flag: str,
) -> None:
    args = _acquire_args()
    index = args.index(required_flag)
    del args[index : index + 2]

    with pytest.raises(SystemExit) as exc_info:
        main(args)

    assert exc_info.value.code == 2


@pytest.mark.parametrize(
    ("mode", "extra_args", "expected_error"),
    [
        (
            "historical",
            [],
            "--start and --end are required for historical mode",
        ),
        (
            "current",
            ["--start", "2026-01-01T00:00:00Z"],
            "current mode forbids --start, --end, and --repair",
        ),
    ],
)
def test_acquire_numeric_facts_rejects_incoherent_mode_bounds(
    monkeypatch,
    capsys,
    mode: str,
    extra_args: list[str],
    expected_error: str,
) -> None:
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: pytest.fail("invalid request reached the API"),
    )
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "acquire-numeric-facts",
            "--manifest-path",
            "manifest.yaml",
            "--binding-id",
            "binding-1",
            "--mode",
            mode,
            "--requested-by",
            "operator",
            "--reason",
            "contract validation",
            "--max-requests",
            "1",
            "--max-logs",
            "1",
            "--max-blocks",
            "1",
            *extra_args,
        ]
    )

    assert exit_code == 2
    assert json.loads(capsys.readouterr().out)["error"] == expected_error


def _numeric_acquisition() -> dict:
    return {
        "bindings": [
            {
                "manifest_path": "config/market-data/chainlink.numeric-facts.v1.yaml",
                "binding_id": "chainlink-eth-usd",
            }
        ],
        "authorization": {
            "network_allowed": True,
            "actor": "operator@example.test",
            "reason": "fill bounded numeric coverage",
        },
        "budget": {
            "max_requests": 17,
            "max_logs": 5000,
            "max_blocks": 250000,
            "max_retries": 4,
        },
    }


def test_prepare_backtest_dataset_requires_explicit_acquire_missing_for_numeric_json(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: pytest.fail("unauthorized request reached the API"),
    )

    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "prepare-backtest-dataset",
            "--bot-id",
            "bot-1",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-02T00:00:00Z",
            "--numeric-acquisition-json",
            json.dumps(_numeric_acquisition()),
        ]
    )

    assert exit_code == 2
    assert (
        json.loads(capsys.readouterr().out)["error"]
        == "--numeric-acquisition-json requires --acquire-missing; "
        "dataset preparation never contacts providers implicitly"
    )


def test_prepare_backtest_dataset_forwards_numeric_acquisition_json(monkeypatch) -> None:
    observed = {}
    numeric_acquisition = _numeric_acquisition()

    def fake_urlopen(request, timeout):
        observed.update(
            method=request.get_method(),
            path=urllib.parse.urlparse(request.full_url).path,
            body=json.loads(request.data.decode("utf-8")),
        )
        return _Response({"status": "ready", "dataset": {"dataset_id": "mds_123"}})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    exit_code = main(
        [
            "--no-audit-log",
            "data",
            "prepare-backtest-dataset",
            "--bot-id",
            "bot/with space",
            "--start",
            "2026-01-01T00:00:00Z",
            "--end",
            "2026-01-02T00:00:00Z",
            "--acquire-missing",
            "--numeric-acquisition-json",
            json.dumps(numeric_acquisition),
            "--created-by",
            "operator@example.test",
        ]
    )

    assert exit_code == 0
    assert observed == {
        "method": "POST",
        "path": "/api/bots/bot%2Fwith%20space/backtest-dataset/prepare",
        "body": {
            "evaluation_start": "2026-01-01T00:00:00Z",
            "evaluation_end": "2026-01-02T00:00:00Z",
            "acquire_missing": True,
            "created_by": "operator@example.test",
            "numeric_acquisition": numeric_acquisition,
        },
    }
