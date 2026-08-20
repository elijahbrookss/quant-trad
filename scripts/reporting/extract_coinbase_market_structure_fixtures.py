#!/usr/bin/env python3
"""Extract bounded exact public Coinbase frames from a Phase 0 proof archive."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow.parquet as pq


PRODUCTS = ("BIP-20DEC30-CDE", "BTC-USD")
SENSITIVE_KEYS = frozenset(
    {
        "api_key",
        "apikey",
        "authorization",
        "credential",
        "jwt",
        "private_key",
        "secret",
        "token",
    }
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract deterministic sanitized Coinbase Phase 0 fixtures."
    )
    parser.add_argument("--proof-report", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    report_path = args.proof_report.expanduser().resolve()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if (report.get("scope") or {}).get("auth_mode") != "public":
        raise ValueError("Exact fixtures must be extracted from a public proof capture.")
    if report.get("status") != "completed":
        raise ValueError("Exact fixtures require a completed proof report.")

    selected = _select_frames(_read_unique_rows(report))
    missing = sorted(_required_fixture_ids() - set(selected))
    if missing:
        raise ValueError("Proof capture lacks required exact frames: " + ", ".join(missing))

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    payload_path = output_dir / "raw_frames.json.gz"
    manifest_path = output_dir / "manifest.json"
    if payload_path.exists() or manifest_path.exists():
        raise FileExistsError(f"Refusing to overwrite existing fixtures in {output_dir}")

    frames = []
    manifest_entries = []
    for fixture_id in sorted(selected):
        row, parsed = selected[fixture_id]
        _assert_no_sensitive_keys(parsed)
        raw_frame = bytes(row["raw_frame"])
        raw_sha = hashlib.sha256(raw_frame).hexdigest()
        if raw_sha != row["raw_frame_sha256"]:
            raise ValueError(f"Raw frame checksum mismatch for {fixture_id}")
        frames.append(
            {
                "fixture_id": fixture_id,
                "raw_frame": raw_frame.decode("utf-8"),
            }
        )
        manifest_entries.append(
            {
                "fixture_id": fixture_id,
                "provider": row["provider"],
                "venue": row["venue"],
                "requested_product_id": row["requested_product_id"],
                "stream_session_id": row["stream_session_id"],
                "connection_epoch": row["connection_epoch"],
                "receive_ordinal": row["receive_ordinal"],
                "received_at": row["received_at"],
                "raw_frame_sha256": raw_sha,
                "raw_frame_bytes": len(raw_frame),
            }
        )

    payload_bytes = _canonical_json_bytes(
        {
            "schema_version": "coinbase_market_structure_fixture_payload.v1",
            "frames": frames,
        }
    )
    payload_path.write_bytes(gzip.compress(payload_bytes, compresslevel=9, mtime=0))
    report_bytes = report_path.read_bytes()
    manifest = {
        "schema_version": "coinbase_market_structure_fixture_manifest.v1",
        "source": {
            "proof_report_sha256": hashlib.sha256(report_bytes).hexdigest(),
            "proof_report_schema_version": report.get("schema_version"),
            "auth_mode": "public",
            "archive_complete": False,
            "dataset_eligible": False,
        },
        "sanitization": {
            "mode": "none_required_public_inbound_market_data",
            "sensitive_key_scan": "passed",
            "raw_frames_changed": False,
        },
        "payload": {
            "path": payload_path.name,
            "sha256": hashlib.sha256(payload_path.read_bytes()).hexdigest(),
            "uncompressed_sha256": hashlib.sha256(payload_bytes).hexdigest(),
            "frame_count": len(frames),
        },
        "entries": manifest_entries,
    }
    manifest_path.write_bytes(_canonical_json_bytes(manifest))
    print(json.dumps({"manifest": str(manifest_path), "frames": len(frames)}))
    return 0


def _read_unique_rows(report: Mapping[str, Any]) -> Iterable[dict[str, Any]]:
    paths = sorted(
        {
            str((row.get("raw_file") or {}).get("path") or "")
            for row in report.get("streams") or []
            if str((row.get("raw_file") or {}).get("path") or "")
        }
    )
    for path_text in paths:
        parquet = pq.ParquetFile(Path(path_text))
        for batch in parquet.iter_batches():
            yield from batch.to_pylist()


def _select_frames(
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]]:
    selected: dict[str, tuple[Mapping[str, Any], Mapping[str, Any]]] = {}
    for row in rows:
        try:
            parsed = json.loads(bytes(row["raw_frame"]))
        except (KeyError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(parsed, Mapping):
            continue
        requested_product = str(row.get("requested_product_id") or "")
        channel = str(parsed.get("channel") or parsed.get("type") or "").lower()
        if channel == "l2_data":
            channel = "level2"
        events = [event for event in parsed.get("events") or [] if isinstance(event, Mapping)]
        event_types = {str(event.get("type") or "").lower() for event in events}

        if channel == "heartbeats" and requested_product == PRODUCTS[0]:
            selected.setdefault("heartbeat", (row, parsed))
        if requested_product not in PRODUCTS:
            continue
        prefix = "bip" if requested_product == PRODUCTS[0] else "btc_spot"
        if channel == "market_trades":
            if "snapshot" in event_types:
                selected.setdefault(f"{prefix}_market_trades_snapshot", (row, parsed))
            if "update" in event_types:
                selected.setdefault(f"{prefix}_market_trades_update", (row, parsed))
        elif channel == "level2":
            if "snapshot" in event_types:
                selected.setdefault(f"{prefix}_level2_snapshot", (row, parsed))
            if "update" in event_types and _contains_zero_quantity(events):
                selected.setdefault(f"{prefix}_level2_zero_delete", (row, parsed))
        elif channel == "ticker":
            selected.setdefault(f"{prefix}_ticker", (row, parsed))
        if set(selected) >= _required_fixture_ids():
            break
    return selected


def _contains_zero_quantity(events: Iterable[Mapping[str, Any]]) -> bool:
    return any(
        str(update.get("new_quantity") or "").strip() in {"0", "0.0", "0.00"}
        for event in events
        for update in event.get("updates") or []
        if isinstance(update, Mapping)
    )


def _required_fixture_ids() -> set[str]:
    required = {"heartbeat"}
    for prefix in ("bip", "btc_spot"):
        required.update(
            {
                f"{prefix}_market_trades_snapshot",
                f"{prefix}_market_trades_update",
                f"{prefix}_level2_snapshot",
                f"{prefix}_level2_zero_delete",
                f"{prefix}_ticker",
            }
        )
    return required


def _assert_no_sensitive_keys(value: Any, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).strip().lower().replace("-", "_")
            if _is_sensitive_key(normalized):
                raise ValueError(f"Sensitive key {path}.{key} prevents fixture publication")
            _assert_no_sensitive_keys(child, path=f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_no_sensitive_keys(child, path=f"{path}[{index}]")
    elif isinstance(value, str) and _looks_like_credential(value):
        raise ValueError(f"Credential-shaped value {path} prevents fixture publication")


def _is_sensitive_key(normalized: str) -> bool:
    return normalized in SENSITIVE_KEYS or normalized.endswith(
        ("_api_key", "_authorization", "_credential", "_jwt", "_secret", "_token")
    )


def _looks_like_credential(value: str) -> bool:
    normalized = value.strip()
    lowered = normalized.lower()
    if lowered.startswith("bearer ") or "-----begin private key-----" in lowered:
        return True
    jwt_parts = normalized.split(".")
    return len(jwt_parts) == 3 and all(
        len(part) >= 8 and set(part) <= set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
        for part in jwt_parts
    )


def _canonical_json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False).encode("utf-8")
        + b"\n"
    )


if __name__ == "__main__":
    raise SystemExit(main())
