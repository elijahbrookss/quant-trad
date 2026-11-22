"""Compare cutover filesystem samples from QT's native host capacity observer."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path

from market_data.fact_archive import archive_evidence_hash


_RESOURCES = {
    "docker-engine-storage": ("docker_engine_storage", "engine_storage_filesystem"),
    "market-archive-storage": ("market_archive_storage", "archive_filesystem"),
}


def _sample(records, resource, scope, authority):
    rows = [record for record in records if record.get("resource_id") == resource]
    samples = [record for record in rows if record.get("sample_kind") == "filesystem"]
    if len(samples) != 1 or any(record.get("sample_kind") == "capacity_unavailable" for record in rows):
        raise ValueError(f"cutover_capacity_requires_one_available_sample: {resource}")
    sample = samples[0]
    expected = {
        "runtime_kind": "native_linux", "physical_host_visible": True,
        "capacity_scope": scope, "capacity_authority": authority,
    }
    identity = (sample.get("device_id"), sample.get("filesystem_uuid"), sample.get("path"))
    if (any(sample.get(key) != value for key, value in expected.items())
            or any(not isinstance(value, str) or not value for value in identity)):
        raise ValueError(f"cutover_capacity_requires_native_device_identity: {resource}")
    total, used, available = (sample.get(key) for key in ("total_bytes", "used_bytes", "available_bytes"))
    if (any(type(value) is not int or value < 0 for value in (total, used, available))
            or not 0 < used + available <= total):
        raise ValueError(f"cutover_capacity_invalid_filesystem_bytes: {resource}")
    captured = datetime.fromisoformat(sample["observed_at"])
    if captured.utcoffset() is None:
        raise ValueError(f"cutover_capacity_requires_timestamp_offset: {resource}")
    return sample, identity, captured


def compare_capacity(before, after):
    """Measure net physical change with device identity and allocatable scope pinned."""
    comparisons = {}
    bounds = set()
    for resource, (scope, authority) in _RESOURCES.items():
        prior, prior_identity, start = _sample(before, resource, scope, authority)
        current, current_identity, end = _sample(after, resource, scope, authority)
        if prior_identity != current_identity or not start < end:
            raise ValueError(f"cutover_capacity_sample_identity_or_order_changed: {resource}")
        prior_usable = prior["used_bytes"] + prior["available_bytes"]
        current_usable = current["used_bytes"] + current["available_bytes"]
        if prior["total_bytes"] != current["total_bytes"] or prior_usable != current_usable:
            raise ValueError(f"cutover_capacity_filesystem_scope_changed: {resource}")
        bounds.add((start, end))
        comparisons[resource] = {
            "device_id": prior_identity[0], "filesystem_uuid": prior_identity[1],
            "allocatable_bytes": prior_usable,
            "before_used_bytes": prior["used_bytes"], "after_used_bytes": current["used_bytes"],
            "before_used_percent": 100 * prior["used_bytes"] / prior_usable,
            "after_used_percent": 100 * current["used_bytes"] / current_usable,
            "available_bytes_change": current["available_bytes"] - prior["available_bytes"],
        }
    if len(bounds) != 1 or any(len({value[key] for value in comparisons.values()}) != len(_RESOURCES)
                               for key in ("filesystem_uuid", "device_id")):
        raise ValueError("cutover_capacity_requires_aligned_independent_device_samples")
    (start, end), = bounds
    ssd = comparisons["docker-engine-storage"]
    result = {
        "schema": "qt.canonical_cold_filesystem_delta.v1",
        "measured_from": start.isoformat(), "measured_through": end.isoformat(),
        "resources": comparisons, "ssd_net_reclaimed_bytes": ssd["available_bytes_change"],
        "ssd_below_65_percent": ssd["after_used_bytes"] * 100 < ssd["allocatable_bytes"] * 65,
        "ssd_bytes_above_50_percent": max(0, ssd["after_used_bytes"] - ssd["allocatable_bytes"] // 2),
    }
    return result | {"receipt_hash": archive_evidence_hash(result)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    args = parser.parse_args()
    with args.before.open(encoding="utf-8") as before, args.after.open(encoding="utf-8") as after:
        result = compare_capacity([json.loads(line) for line in before], [json.loads(line) for line in after])
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
