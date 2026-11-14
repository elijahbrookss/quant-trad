"""Compare native full-page and experimental verified exact-ID reads on 10k rows."""
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
from scripts.db.canonical_cold_sparse_probe import AdmittedLocator, read_admitted_rows


def _time(operation):
    control = ExecutionControl()
    control.limit(seconds=30, archive_read_bytes=128 * 1024**2)
    start = perf_counter()
    with controlled_execution(control):
        result = operation()
    return result, perf_counter() - start, control.snapshot()["consumed"].get("archive_source_bytes", 0)


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
        insertion = {"schema": "qt.cold_sparse_benchmark_fixture.v1", "fixture": "cold-scale.v1", "result_count": probes, "page_rows": count}
        digest = sha256()
        records = {label: {"seconds": [], "read_bytes": [], "decoded_rows": []} for label in ("native", "sparse")}
        offsets = Random(1729).sample(range(count), probes)
        for sequence, offset in enumerate(offsets):
            row = expected[offset]
            location = ColdFactLocation(manifest, offset)
            locator = AdmittedLocator(row["id"], location, row["series_id"], sha256(row["observation_key"].encode()).digest(),
                                      row["revision"], row["market_commit_seq"], row["row_hash"])
            operations = {
                "native": lambda: (reader.read_by_ids({row["id"]: location}), count),
                "sparse": lambda: read_admitted_rows(store.local_path(manifest.object_key), claims=[locator]),
            }
            for label in (tuple(operations) if sequence % 2 == 0 else tuple(reversed(operations))):
                (result, decoded), elapsed, bytes_read = _time(operations[label])
                if result != {row["id"]: row}:
                    raise RuntimeError("sparse benchmark exact output differs")
                records[label]["seconds"].append(elapsed)
                records[label]["read_bytes"].append(bytes_read)
                records[label]["decoded_rows"].append(decoded)
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
