"""Idempotently install the reviewed single-node market-data fleet."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from core.storage_mounts import require_configured_archive_mount
from data_providers.structured_facts import load_structured_fact_manifest
from market_data.instrument_enrollment import load_instrument_enrollment_manifest
from market_data.stream_enrollment import load_stream_enrollment_manifest

from portal.backend.service.market.collector_service import market_data_collector
from portal.backend.service.market.market_structure_service import (
    market_structure_service,
)
from portal.backend.service.storage.repos.instruments import (
    install_code_owned_instrument,
)


DEFAULT_INSTRUMENT_MANIFEST = Path(
    "/app/config/market_data/coinbase_perpetual_instruments.v1.json"
)
DEFAULT_TRADE_MANIFEST = Path(
    "/app/config/market_data/coinbase_perpetual_trade_fleet.v1.json"
)
DEFAULT_L2_MANIFEST = Path(
    "/app/config/market_data/coinbase_perpetual_l2_fleet.v1.json"
)
DEFAULT_STRUCTURED_FACT_MANIFESTS = (
    Path(
        "/app/config/market-data/structured-facts/"
        "chainlink-nxtassets-btc-etp-reserves.json"
    ),
)


def _flag(name: str, *, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0")).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"single_node_initializer_invalid: {name} must be boolean")


def _positive_int(name: str, *, default: int) -> int:
    value = int(str(os.environ.get(name, default)).strip())
    if value <= 0:
        raise ValueError(f"single_node_initializer_invalid: {name} must be positive")
    return value


def _manifest(name: str, default: Path) -> Path:
    path = Path(os.environ.get(name, str(default))).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(
            f"single_node_initializer_manifest_missing: {name}={path}"
        )
    return path


def _manifest_list(name: str, defaults: tuple[Path, ...]) -> tuple[Path, ...]:
    raw = str(
        os.environ.get(name, os.pathsep.join(str(path) for path in defaults))
    ).strip()
    if not raw:
        raise ValueError(
            f"single_node_initializer_invalid: {name} must contain a manifest path"
        )
    paths = tuple(
        Path(value.strip()).expanduser().resolve()
        for value in raw.split(os.pathsep)
        if value.strip()
    )
    if not paths or len(paths) != len(set(paths)):
        raise ValueError(
            f"single_node_initializer_invalid: {name} paths must be unique"
        )
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(
                f"single_node_initializer_manifest_missing: {name}={path}"
            )
    return paths


def initialize_single_node_market_data() -> dict[str, Any]:
    """Install definitions without overriding later operator lifecycle choices."""

    if not _flag("QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA", default=True):
        return {
            "schema_version": "qt.single_node_initialization.v1",
            "status": "skipped",
            "reason": "QT_SINGLE_NODE_BOOTSTRAP_MARKET_DATA is disabled",
        }

    instrument_manifest_path = _manifest(
        "QT_SINGLE_NODE_INSTRUMENT_MANIFEST", DEFAULT_INSTRUMENT_MANIFEST
    )
    trade_manifest_path = _manifest(
        "QT_SINGLE_NODE_TRADE_MANIFEST", DEFAULT_TRADE_MANIFEST
    )
    l2_manifest_path = _manifest(
        "QT_SINGLE_NODE_L2_MANIFEST", DEFAULT_L2_MANIFEST
    )
    instrument_manifest = load_instrument_enrollment_manifest(
        instrument_manifest_path
    )
    trade_manifest = load_stream_enrollment_manifest(trade_manifest_path)
    l2_manifest = load_stream_enrollment_manifest(l2_manifest_path)
    scheduled_facts_enabled = _flag(
        "QT_SINGLE_NODE_ENABLE_SCHEDULED_FACTS", default=True
    )
    structured_fact_manifest_paths: tuple[Path, ...] = ()
    structured_fact_manifests = ()
    if scheduled_facts_enabled and _flag(
        "QT_SINGLE_NODE_ENABLE_STRUCTURED_FACTS", default=True
    ):
        structured_fact_manifest_paths = _manifest_list(
            "QT_SINGLE_NODE_STRUCTURED_FACT_MANIFESTS",
            DEFAULT_STRUCTURED_FACT_MANIFESTS,
        )
        structured_fact_manifests = tuple(
            load_structured_fact_manifest(path)
            for path in structured_fact_manifest_paths
        )
    installed_instruments = {
        row.id: install_code_owned_instrument(row.to_dict())
        for row in instrument_manifest.instruments
    }
    for stream_manifest in (trade_manifest, l2_manifest):
        for enrollment in stream_manifest.enrollments:
            installed = installed_instruments.get(enrollment.instrument_id)
            if installed is None or str(installed.get("symbol") or "") != (
                enrollment.product_contract.provider_product_id
            ):
                raise RuntimeError(
                    "single_node_initializer_instrument_mismatch: "
                    f"enrollment_id={enrollment.enrollment_id}"
                )
    result: dict[str, Any] = {
        "schema_version": "qt.single_node_initialization.v1",
        "status": "initialized",
        "instrument_manifest_hash": instrument_manifest.manifest_hash,
        "instruments": list(installed_instruments.values()),
        "trade_streams": None,
        "level2_streams": None,
        "structured_fact_manifests": [
            {
                "id": manifest.id,
                "manifest_hash": manifest.manifest_hash,
                "path": str(path),
            }
            for path, manifest in zip(
                structured_fact_manifest_paths,
                structured_fact_manifests,
                strict=True,
            )
        ],
        "scheduled_facts": [],
    }
    if _flag("QT_SINGLE_NODE_ENABLE_TRADE_STREAMS", default=True):
        result["trade_streams"] = (
            market_structure_service.apply_stream_enrollment_manifest(
                manifest_path=trade_manifest_path
            )
        )
    if _flag("QT_SINGLE_NODE_ENABLE_L2_STREAMS", default=True):
        result["level2_streams"] = (
            market_structure_service.apply_stream_enrollment_manifest(
                manifest_path=l2_manifest_path
            )
        )
    if scheduled_facts_enabled:
        oi_interval = _positive_int(
            "QT_SINGLE_NODE_OI_POLL_SECONDS", default=60
        )
        funding_interval = _positive_int(
            "QT_SINGLE_NODE_FUNDING_POLL_SECONDS", default=60
        )
        for enrollment in trade_manifest.enrollments:
            if enrollment.product_type.lower() != "future":
                continue
            common = {
                "instrument_id": enrollment.instrument_id,
                "provider_product_id": (
                    enrollment.product_contract.provider_product_id
                ),
                "max_attempts": 3,
                "minimum_spacing_seconds": 1.0,
                "enabled": True,
            }
            result["scheduled_facts"].append(
                market_data_collector.create_coinbase_open_interest_definition(
                    poll_interval_seconds=oi_interval,
                    **common,
                )
            )
            result["scheduled_facts"].append(
                market_data_collector.create_coinbase_funding_rate_definition(
                    poll_interval_seconds=funding_interval,
                    **common,
                )
            )
        for path, manifest in zip(
            structured_fact_manifest_paths,
            structured_fact_manifests,
            strict=True,
        ):
            if not manifest.enabled:
                continue
            for binding in manifest.bindings:
                if not binding.enabled:
                    continue
                result["scheduled_facts"].append(
                    market_data_collector.create_structured_fact_definition(
                        manifest_path=str(path),
                        binding_id=binding.id,
                        max_attempts=3,
                        minimum_spacing_seconds=1.0,
                        enabled=True,
                    )
                )
    return result


def main() -> int:
    require_configured_archive_mount()
    print(
        json.dumps(
            initialize_single_node_market_data(),
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
