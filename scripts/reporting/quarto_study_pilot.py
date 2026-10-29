"""Manually authored pilot from a verified saved-record ZIP; no research replay.

Run with the repository Python environment. Quarto runs without network, with an
empty environment and temporary HOME, resource limits, and execution disabled.
Output stays local; publish explicitly with qt research reports attach.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import tempfile
from zipfile import ZipFile

from cli.study_reports import Fragment, render, source_revision

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

CLASSES = ["continuation", "failure", "neither", "unresolved"]
COLORS = ["#147d79", "#bd6246", "#889aab", "#b08b40"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--quarto", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-qmd", type=Path, required=True, help="Private manually authored narrative, kept outside Git.")
    parser.add_argument("--study-id", required=True)
    parser.add_argument("--publication-hash", required=True)
    parser.add_argument("--check-id", required=True)
    parser.add_argument("--result-hash", required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("output already exists")
    bundle = args.bundle.read_bytes()
    if len(bundle) > 32 * 1024**2:
        raise ValueError("bundle exceeds bound")
    with ZipFile(args.bundle) as archive:
        if sum(row.file_size for row in archive.infolist()) > 32 * 1024**2 or len(archive.infolist()) > 160:
            raise ValueError("uncompressed bundle exceeds bound")
        checksums = archive.read("checksums.sha256").decode().splitlines()
        for row in checksums:
            digest, name = row.split("  ", 1)
            if hashlib.sha256(archive.read(name)).hexdigest() != digest:
                raise ValueError("bundle checksum mismatch")
        publication = json.loads(archive.read("records/selected-publication.json"))
        manifest = json.loads(archive.read("manifest.json"))
        paths = [row["path"] for row in manifest["files"] if row["origin"].get("item_id") == args.check_id and row["path"].startswith("records/reference-")]
        if len(paths) != 1:
            raise ValueError("expected one exact referenced Check in the saved bundle")
        check = json.loads(archive.read(paths[0]))
    if publication["question_id"] != args.study_id or publication["publication_hash"] != args.publication_hash or check["id"] != args.check_id or check["payload"]["result"]["result_hash"] != args.result_hash:
        raise ValueError("pilot requires reviewed exact Study/publication/result")
    if not any(row.get("item_id") == args.check_id and row.get("kind") == "research_check" and row.get("result_hash") == args.result_hash for row in publication["references"]):
        raise ValueError("selected Check is not exact evidence for this interpretation")
    from market_data.frozen import semantic_hash
    from portal.backend.service.research.reports import normalize_html
    if semantic_hash({k: v for k, v in publication.items() if k != "publication_hash"}) != args.publication_hash:
        raise ValueError("publication hash mismatch")
    result = check["payload"]["result"]
    if semantic_hash({k: v for k, v in result.items() if k != "result_hash"}) != args.result_hash:
        raise ValueError("result hash mismatch")
    summary = result["result"]["first_passage_summary"]
    revision = source_revision()
    with tempfile.TemporaryDirectory(prefix="qt-quarto-pilot-") as temp:
        directory = Path(temp)
        plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 12, "axes.spines.top": False, "axes.spines.right": False, "figure.facecolor": "#faf9f6", "axes.facecolor": "#faf9f6"})
        fig, ax = plt.subplots(figsize=(9, 4.6), layout="constrained")
        values = [100 * summary["chain_weighted_shares"][k] for k in CLASSES]
        ax.barh([k.title() for k in CLASSES], values, color=COLORS)
        ax.invert_yaxis(); ax.set_xlim(0, 85); ax.set_xlabel(f"Share of all {summary['chain_count']} retained chains (%)")
        for i, (k, value) in enumerate(zip(CLASSES, values)):
            count = summary['class_counts'][k]
            ax.text(value + 1, i, f"{value:.1f}% · {count} {'chain' if count == 1 else 'chains'}", va="center")
        fig.savefig(directory / "outcomes.png", dpi=150); plt.close(fig)
        fig, ax = plt.subplots(figsize=(9, 4.8), layout="constrained")
        for offset, field, color, label in [(-.18, "chain_weighted_shares", "#147d79", "Each chain equally"), (.18, "equal_component_weighted_shares", "#b08b40", "Each overlap component equally")]:
            ax.barh([i + offset for i in range(4)], [100 * summary[field][k] for k in CLASSES], height=.34, label=label, color=color)
        ax.set_yticks(range(4), [k.title() for k in CLASSES]); ax.invert_yaxis()
        ax.set_xlim(0, 100); ax.set_xlabel("Share including all four outcome classes (%)"); ax.legend(loc="lower right", frameon=False)
        fig.savefig(directory / "weighting.png", dpi=150); plt.close(fig)
        fig, ax = plt.subplots(figsize=(9, 4), layout="constrained")
        counts = [c["chain_count"] for c in summary["components"]]
        positions = range(1, len(counts) + 1)
        ax.bar(positions, counts, color="#889aab"); ax.set_xticks(positions, [f"Group {i}" for i in positions])
        ax.set_ylabel("Retained chains"); ax.set_ylim(0, max(counts) * 1.17)
        for i, count in enumerate(counts, 1):
            ax.text(i, count + 7, str(count), ha="center")
        fig.savefig(directory / "components.png", dpi=150); plt.close(fig)
        source = args.source_qmd.read_text()
        if len(source.encode()) > 128 * 1024:
            raise ValueError("private QMD source exceeds bound")
        (directory / "report.qmd").write_text(source)
        quarto = args.quarto.resolve()
        version = render([str(quarto), "--version"], directory).stdout.strip()
        render([str(quarto), "render", "report.qmd", "--no-execute"], directory)
        fragment = Fragment(directory)
        fragment.feed((directory / "report.html").read_text())
        html = normalize_html("".join(fragment.parts))
        payload = {"title": "Study report pilot", "publication_hash": args.publication_hash,
                   "html": html, "source_qmd": source, "build": {"quarto_version": version, "source_revision": revision,
                   "evidence_bundle_sha256": hashlib.sha256(bundle).hexdigest()}}
        with args.output.open("x") as output:
            json.dump(payload, output)
    print(json.dumps({"study_id": args.study_id, "publication_hash": args.publication_hash, "output": str(args.output), "quarto_version": version}))


if __name__ == "__main__":
    main()
