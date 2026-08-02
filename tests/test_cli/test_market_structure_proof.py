from __future__ import annotations

import asyncio
import json
from collections import Counter
from pathlib import Path

import pytest

from cli.main import build_parser
from cli.market_structure_proof import (
    ProofStreamSpec,
    _ProofParquetSink,
    _StreamAnalyzer,
    _capacity_summary,
    _normalize_products,
    _phase1_admission,
    _phase1_implementation_readiness,
    _proof_implementation,
    _quantity_semantics,
    _replay_proof_archive,
    _run_cde_public_history_proofs,
    _trade_side_semantics,
    run_coinbase_market_structure_proof,
)
from data_providers.streams import ProviderRawMessage


def _message(payload: dict, *, epoch: int = 1, ordinal: int = 1) -> ProviderRawMessage:
    return ProviderRawMessage.build(
        provider="COINBASE",
        venue="COINBASE_DIRECT",
        stream_session_id="session-1",
        connection_epoch=epoch,
        receive_ordinal=ordinal,
        raw_frame=json.dumps(payload, separators=(",", ":")),
        received_at="2026-08-02T12:00:01Z",
    )


def _trade_payload() -> dict:
    return {
        "channel": "market_trades",
        "timestamp": "2026-08-02T12:00:00.250Z",
        "sequence_num": 10,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "trade_id": "trade-1",
                        "product_id": "BIP-20DEC30-CDE",
                        "price": "115000",
                        "size": "3",
                        "side": "BUY",
                        "time": "2026-08-02T12:00:00.100Z",
                    }
                ],
            }
        ],
    }


def _l2_payload(event_type: str, sequence_num: int, quantity: str) -> dict:
    return {
        "channel": "l2_data",
        "timestamp": "2026-08-02T12:00:00.250Z",
        "sequence_num": sequence_num,
        "events": [
            {
                "type": event_type,
                "product_id": "BIP-20DEC30-CDE",
                "updates": [
                    {
                        "side": "bid",
                        "event_time": "2026-08-02T12:00:00.100Z",
                        "price_level": "115000",
                        "new_quantity": quantity,
                    }
                ],
            }
        ],
    }


def test_market_structure_proof_rejects_products_outside_allowlist() -> None:
    with pytest.raises(ValueError, match="outside the bounded allowlist"):
        _normalize_products(["DOGE-USD"])


def test_proof_implementation_fingerprints_sources_and_runtime() -> None:
    proof = _proof_implementation()

    assert set(proof["source_sha256"]) == {
        "proof_harness",
        "coinbase_stream",
        "coinbase_provider",
    }
    assert all(len(value) == 64 for value in proof["source_sha256"].values())
    assert proof["runtime"]["python"]
    assert proof["runtime"]["pyarrow"]


@pytest.mark.parametrize("budget", [0, -1, float("inf"), float("nan")])
def test_market_structure_proof_rejects_invalid_archive_budget(
    tmp_path: Path,
    budget: float,
) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        asyncio.run(
            run_coinbase_market_structure_proof(
                output_dir=tmp_path,
                duration_seconds=1,
                max_annual_archive_gib=budget,
            )
        )


def test_stream_analyzer_preserves_trade_schema_side_batching_and_rates() -> None:
    analyzer = _StreamAnalyzer(
        ProofStreamSpec("BIP-20DEC30-CDE", "market_trades", "public"),
        sample_limit=1,
    )

    observed = analyzer.observe(_message(_trade_payload()))
    report = analyzer.report()

    assert observed == Counter(trades=1, mutations=0)
    assert report["trade_count"] == 1
    assert report["observed_semantics"]["trade_sides"] == ["BUY"]
    assert report["counts"]["trade_batch_size:1"] == 1
    assert report["schema_signatures"]["trades"] == {
        "price,product_id,side,size,time,trade_id": 1
    }
    assert report["samples"][0]["events"][0]["trades"][0]["side"] == "BUY"


def test_l2_analyzer_requires_snapshot_and_applies_absolute_zero_delete() -> None:
    analyzer = _StreamAnalyzer(
        ProofStreamSpec("BIP-20DEC30-CDE", "level2", "public"),
        sample_limit=0,
    )

    first = analyzer.observe(_message(_l2_payload("snapshot", 20, "2"), ordinal=1))
    second = analyzer.observe(_message(_l2_payload("update", 21, "0"), ordinal=2))
    report = analyzer.report()
    book = report["book_by_epoch"]["1"]

    assert first == Counter(trades=0, mutations=1)
    assert second == Counter(trades=0, mutations=1)
    assert report["first_requested_event_type_by_epoch"] == {"1": "snapshot"}
    assert book["snapshot_count"] == 1
    assert book["update_count"] == 1
    assert book["checkpoint"]["valid"] is True
    assert book["checkpoint"]["level_count"] == 0


def test_local_parquet_proof_replays_with_same_content_fingerprint(tmp_path) -> None:
    spec = ProofStreamSpec("BIP-20DEC30-CDE", "market_trades", "public")
    sink = _ProofParquetSink(tmp_path, spec, batch_size=1)
    analyzer = _StreamAnalyzer(spec, sample_limit=0)
    message = _message(_trade_payload())

    sink.append(message)
    analyzer.observe(message)
    result = sink.close()
    replay = _replay_proof_archive(result["path"], spec)

    assert result["complete"] is True
    assert result["local_encoder"]["flush_count"] == 1
    assert result["local_encoder"]["max_buffered_rows"] == 1
    assert result["local_encoder"]["max_buffered_raw_bytes"] > 0
    assert replay["ordering_or_checksum_errors"] == 0
    assert replay["trade_count"] == 1
    assert replay["ordered_content_fingerprint"] == analyzer.report()[
        "ordered_content_fingerprint"
    ]
    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        _ProofParquetSink(tmp_path, spec)


def test_local_parquet_proof_does_not_finalize_incomplete_capture(tmp_path) -> None:
    spec = ProofStreamSpec("BIP-20DEC30-CDE", "market_trades", "public")
    sink = _ProofParquetSink(tmp_path, spec, batch_size=1)
    sink.append(_message(_trade_payload()))

    result = sink.close(finalize=False)

    assert result["complete"] is False
    assert result["path"].endswith(".partial.parquet")
    assert not (tmp_path / "public-BIP-20DEC30-CDE-market_trades.parquet").exists()


def test_capacity_summary_includes_quiet_seconds_and_event_rates() -> None:
    streams = [
        {
            "analysis": {"raw_bytes": 400},
            "raw_file": {"compressed_bytes": 200},
        }
    ]
    rates = {
        0: Counter(frames=2, raw_bytes=300, trades=3, mutations=4),
        2: Counter(frames=1, raw_bytes=100, trades=1, mutations=2),
    }

    summary = _capacity_summary(
        streams,
        rates,
        elapsed_seconds=3.0,
        max_annual_archive_gib=10_000,
    )

    assert summary["frames_per_second"] == {"p50": 1, "p95": 2, "p99": 2, "max": 2}
    assert summary["trades_per_second"]["p50"] == 1
    assert summary["l2_mutations_per_second"]["p50"] == 2
    assert summary["raw_bytes_per_second"]["p50"] == 100
    assert summary["p99_input_bytes_per_second"] == 300
    assert summary["required_3x_six_hour_spool_gib"] == pytest.approx(
        300 * 21_600 * 3 / (1024**3),
        abs=1e-6,
    )
    assert (
        summary["recommended_initial_spool_gib"]
        == summary["required_3x_six_hour_spool_gib"]
    )
    assert summary["local_encoder"]["scope"].startswith("local proof")
    assert (
        summary["deferred_measurements"]["object_upload_latency_and_backlog"][
            "status"
        ]
        == "not_measured"
    )
    assert summary["measurement_duration_gate_pass"] is False


def test_quantity_semantics_confirms_bip_contract_units_from_metadata_and_samples() -> None:
    rest = {
        "products": {
            "BIP-20DEC30-CDE": {
                "product": {
                    "status": "confirmed",
                    "payload": {
                        "base_increment": "1",
                        "future_product_details": {
                            "contract_root_unit": "BTC",
                            "contract_size": "0.01",
                        },
                    },
                }
            }
        }
    }
    streams = [
        {
            "spec": {
                "product_id": "BIP-20DEC30-CDE",
                "channel": "market_trades",
            },
            "analysis": {
                "observed_semantics": {"trade_size_samples": ["1", "13", "100"]}
            },
        },
        {
            "spec": {"product_id": "BIP-20DEC30-CDE", "channel": "level2"},
            "analysis": {
                "observed_semantics": {"l2_quantity_samples": ["0", "62", "2500"]}
            },
        },
    ]

    decision = _quantity_semantics(rest, streams)["products"]["BIP-20DEC30-CDE"]

    assert decision["status"] == "confirmed_contracts"
    assert decision["provider_size_unit"] == "contract"
    assert decision["base_quantity_formula"] == "provider_size * contract_size"
    assert decision["quote_notional_formula"] == "price * provider_size * contract_size"


def test_quantity_semantics_blocks_on_metadata_or_increment_mismatch() -> None:
    rest = {
        "products": {
            "BIP-20DEC30-CDE": {
                "product": {
                    "status": "confirmed",
                    "payload": {
                        "base_increment": "0.5",
                        "future_product_details": {
                            "contract_root_unit": "BTC",
                            "contract_size": "1",
                        },
                    },
                }
            }
        }
    }
    streams = [
        {
            "spec": {"product_id": "BIP-20DEC30-CDE", "channel": "market_trades"},
            "analysis": {"observed_semantics": {"trade_size_samples": ["0.5"]}},
        },
        {
            "spec": {"product_id": "BIP-20DEC30-CDE", "channel": "level2"},
            "analysis": {"observed_semantics": {"l2_quantity_samples": ["1"]}},
        },
    ]

    decision = _quantity_semantics(rest, streams)["products"]["BIP-20DEC30-CDE"]

    assert decision["status"] == "blocked"
    assert decision["provider_size_unit"] is None
    assert set(decision["reasons"]) >= {
        "contract_size_mismatch",
        "base_increment_mismatch",
        "trade_sizes_not_contract_increment_multiples",
    }


def test_phase1_admission_requires_resnapshot_unit_decision_and_full_duration() -> None:
    streams = [
        {
            "status": "completed",
            "spec": {"product_id": "BIP-20DEC30-CDE", "channel": "level2"},
            "analysis": {
                "counts": {"requested_channel_frames": 1},
                "first_requested_event_type_by_epoch": {"1": "update"},
            },
            "replay": {
                "ordering_or_checksum_errors": 0,
                "content_fingerprint_equal": True,
            },
        }
    ]
    rest = {
        "products": {
            "BIP-20DEC30-CDE": {
                "product": {"status": "confirmed"},
                "product_book": {"status": "confirmed"},
                "recent_market_trades": {"status": "confirmed"},
            }
        },
        "cde_public_history": {"status": "unsupported"},
    }

    admission = _phase1_admission(
        streams,
        rest,
        {
            "measurement_duration_gate_pass": False,
            "annual_archive_budget_pass": True,
        },
        duration_seconds=86_400,
    )

    assert admission["status"] == "blocked"
    assert "24_hour_capacity_capture_required" in admission["reasons"]
    assert "l2_resnapshot_not_proven:BIP-20DEC30-CDE" in admission["reasons"]
    assert "futures_quantity_unit_not_proven:BIP-20DEC30-CDE" in admission["reasons"]


def test_phase1_admission_requires_complete_bounded_integrity_evidence() -> None:
    streams = []
    for product_id in ("BIP-20DEC30-CDE", "BTC-USD"):
        for channel in ("market_trades", "level2", "ticker"):
            analysis = {
                "counts": {
                    "channel:heartbeats": 2,
                    "requested_channel_frames": 1,
                    f"sequence_observed:{channel}": 1,
                },
                "first_requested_event_type_by_epoch": {"0": "snapshot"},
            }
            replay = {
                "ordering_or_checksum_errors": 0,
                "content_fingerprint_equal": True,
            }
            if channel == "level2":
                analysis["book_by_epoch"] = {
                    "0": {
                        "checkpoint": {"valid": True},
                        "update_before_snapshot_count": 0,
                        "invalid_mutation_count": 0,
                    }
                }
                replay["book_fingerprints_equal"] = True
            streams.append(
                {
                    "status": "completed",
                    "spec": {"product_id": product_id, "channel": channel},
                    "analysis": analysis,
                    "replay": replay,
                    "raw_file": {"complete": True},
                    "connection_count": 2,
                    "deliberate_reconnect_count": 1,
                }
            )
    rest = {
        "products": {
            product_id: {
                "product": {"status": "confirmed"},
                "product_book": {"status": "confirmed"},
                "recent_market_trades": {"status": "confirmed"},
            }
            for product_id in ("BIP-20DEC30-CDE", "BTC-USD")
        },
        "cde_public_history": {"status": "unsupported"},
    }
    capacity = {
        "measurement_duration_gate_pass": True,
        "annual_archive_budget_pass": True,
        "full_replay": {"one_day_under_one_hour_gate_pass": True},
    }
    quantity_semantics = {
        "products": {"BIP-20DEC30-CDE": {"status": "confirmed_contracts"}}
    }
    trade_side_semantics = {"status": "confirmed_maker_side"}

    admission = _phase1_admission(
        streams,
        rest,
        capacity,
        duration_seconds=86_400,
        quantity_semantics=quantity_semantics,
        trade_side_semantics=trade_side_semantics,
    )

    assert admission == {
        "status": "admitted",
        "reasons": [],
        "scope": "BIP-20DEC30-CDE/BTC-USD only",
    }

    implementation_readiness = _phase1_implementation_readiness(
        streams,
        rest,
        {
            "measurement_duration_gate_pass": False,
            "annual_archive_budget_pass": None,
            "full_replay": {"one_day_under_one_hour_gate_pass": False},
        },
        duration_seconds=3_600,
        quantity_semantics=quantity_semantics,
        trade_side_semantics=trade_side_semantics,
    )
    assert implementation_readiness == {
        "status": "admitted",
        "reasons": [],
        "scope": "BIP-20DEC30-CDE/BTC-USD only",
    }

    too_short = _phase1_implementation_readiness(
        streams,
        rest,
        capacity,
        duration_seconds=3_599,
        quantity_semantics=quantity_semantics,
        trade_side_semantics=trade_side_semantics,
    )
    assert too_short["status"] == "blocked"
    assert "one_hour_provider_capture_required" in too_short["reasons"]

    streams[0]["analysis"]["counts"]["sequence_gap:connection"] = 1
    blocked = _phase1_admission(
        streams,
        rest,
        capacity,
        duration_seconds=86_400,
        quantity_semantics=quantity_semantics,
        trade_side_semantics=trade_side_semantics,
    )

    assert blocked["status"] == "blocked"
    assert (
        "stream_integrity_failed:BIP-20DEC30-CDE:sequence_gap:connection"
        in blocked["reasons"]
    )

    streams[0]["analysis"]["counts"]["sequence_gap:connection"] = 0
    streams[0]["analysis"]["counts"]["sequence_missing:market_trades"] = 1
    blocked = _phase1_admission(
        streams,
        rest,
        capacity,
        duration_seconds=86_400,
        quantity_semantics=quantity_semantics,
        trade_side_semantics=trade_side_semantics,
    )

    assert blocked["status"] == "blocked"
    assert (
        "requested_channel_sequence_missing:BIP-20DEC30-CDE:market_trades"
        in blocked["reasons"]
    )


def test_trade_side_semantics_requires_documented_values_in_captured_schema() -> None:
    streams = [
        {
            "spec": {"product_id": product_id, "channel": "market_trades"},
            "analysis": {
                "trade_count": 2,
                "observed_semantics": {"trade_sides": ["BUY", "SELL"]},
                "schema_signatures": {
                    "trades": {"price,product_id,side,size,time,trade_id": 2}
                },
            },
        }
        for product_id in ("BIP-20DEC30-CDE", "BTC-USD")
    ]

    decision = _trade_side_semantics(streams)

    assert decision["status"] == "confirmed_maker_side"
    assert decision["provider_meaning"] == "maker_side"
    assert decision["aggressor_transform"]["BUY"] == "SELL"

    streams[0]["analysis"]["observed_semantics"]["trade_sides"] = ["UNKNOWN"]
    assert _trade_side_semantics(streams)["status"] == "unproven"


def test_cde_public_history_probe_rejects_challenged_page_and_authenticated_funding(
    monkeypatch,
) -> None:
    responses = iter(
        [
            {"result": "http_response", "http_status": 403},
            {"result": "http_response", "http_status": 401},
        ]
    )
    monkeypatch.setattr(
        "cli.market_structure_proof._bounded_public_probe",
        lambda *_args, **_kwargs: next(responses),
    )

    proof = _run_cde_public_history_proofs()

    assert proof["status"] == "unsupported"
    assert proof["admission"] == "rejected"
    assert proof["sources"] == {
        "daily_market_statistics": {
            "status": "unsupported",
            "reasons": [
                "historical_page_not_machine_accessible_without_auth_or_challenge"
            ],
        },
        "finalized_funding": {
            "status": "unsupported",
            "reasons": ["historical_funding_requires_cde_request_credentials"],
        },
    }
    assert proof["reasons"] == [
        "historical_page_not_machine_accessible_without_auth_or_challenge",
        "historical_funding_requires_cde_request_credentials",
    ]


def test_cde_public_history_probe_rejects_human_page_without_machine_contract(
    monkeypatch,
) -> None:
    responses = iter(
        [
            {"result": "http_response", "http_status": 200},
            {"result": "http_response", "http_status": 401},
        ]
    )
    monkeypatch.setattr(
        "cli.market_structure_proof._bounded_public_probe",
        lambda *_args, **_kwargs: next(responses),
    )

    proof = _run_cde_public_history_proofs()

    assert proof["status"] == "unsupported"
    assert proof["admission"] == "rejected"
    assert proof["sources"]["daily_market_statistics"]["status"] == "unsupported"
    assert proof["reasons"] == [
        "historical_page_has_no_stable_documented_machine_data_contract",
        "historical_funding_requires_cde_request_credentials",
    ]


def test_cli_exposes_bounded_market_structure_proof_command() -> None:
    args = build_parser().parse_args(
        [
            "data",
            "market-structure-proof",
            "--product-id",
            "BIP-20DEC30-CDE",
            "--channel",
            "level2",
            "--duration",
            "10",
            "--auth-mode",
            "authenticated",
        ]
    )

    assert args.product_id == ["BIP-20DEC30-CDE"]
    assert args.channel == ["level2"]
    assert args.duration == 10
    assert args.auth_mode == "authenticated"
