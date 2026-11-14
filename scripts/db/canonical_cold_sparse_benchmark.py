"""Compare full and admitted-ordinal canonical reads on the same 10k-row page."""
from __future__ import annotations

import argparse
from hashlib import sha256
from importlib import metadata
import json
from pathlib import Path
from random import Random
import subprocess
from tempfile import TemporaryDirectory
from time import perf_counter

from core.execution_control import ExecutionControl, controlled_execution
from market_data.archive import FilesystemRawArchiveObjectStore
from market_data.cold_history import ColdFactLocation, ColdHistoryReader, publish_cold_page
from market_data.fact_archive import archive_evidence_hash
from scripts.db.canonical_cold_scale import _row, _stats


def _time(operation):
    control = ExecutionControl()
    control.limit(seconds=30, archive_read_bytes=128 * 1024**2)
    start = perf_counter()
    with controlled_execution(control):
        result = operation()
    return result, perf_counter() - start, control.snapshot()["consumed"]


def run(*, probes=16):
    count = 10_000
    with TemporaryDirectory(prefix="qt-cold-sparse-probe-") as path:
        root = Path(path)
        store = FilesystemRawArchiveObjectStore(root / "objects")
        expected = [_row(index) for index in range(count)]
        manifest = publish_cold_page(expected, object_store=store, temporary_directory=root / "staging")
        reader = ColdHistoryReader(store)
        if reader.read_page(manifest) != tuple(expected):
            raise RuntimeError("sparse benchmark prior full admission with fixture rows differs")
        insertion = {"schema": "qt.cold_sparse_benchmark_fixture.v2", "fixture": "cold-scale.v1",
                     "result_count": probes, "page_rows": count}
        digest = sha256()
        records = {label: {"seconds": [], "read_bytes": [], "decoded_rows": []} for label in ("full", "admitted")}
        offsets = Random(1729).sample(range(count), probes)
        for sequence, offset in enumerate(offsets):
            row = expected[offset]
            location = ColdFactLocation(manifest, offset)
            locator = {row["id"]: location}
            operations = {
                "full": lambda: reader.read_by_ids(locator),
                "admitted": lambda: reader.read_admitted_by_ids(locator),
            }
            for label in (tuple(operations) if sequence % 2 == 0 else tuple(reversed(operations))):
                result, elapsed, resources = _time(operations[label])
                if result != {row["id"]: row}:
                    raise RuntimeError("sparse benchmark exact output differs")
                records[label]["seconds"].append(elapsed)
                records[label]["read_bytes"].append(resources["archive_source_bytes"])
                records[label]["decoded_rows"].append(resources["archive_decoded_rows"])
            digest.update(bytes.fromhex(archive_evidence_hash({"row": row})))
        return insertion | {"object_sha256": manifest.object_sha256, "source_file_bytes": manifest.byte_count,
            "ordered_exact_sha256": digest.hexdigest(), "probe_ordinals": offsets,
            "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
            "pyarrow": metadata.version("pyarrow"), "cache_policy": "no_qt_cache_os_cache_uncontrolled_and_warm_after_full_admission",
            "paths": {key: values | {"metrics": _stats(values["seconds"])} for key, values in records.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probes", type=int, default=16)
    args = parser.parse_args()
    if not 5 <= args.probes <= 50:
        parser.error("probe count must be within 5 and 50")
    print(json.dumps(run(probes=args.probes), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
