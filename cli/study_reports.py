"""Offline packaging of reviewed QMD and prepared PNGs; never runs research.

Production only admits the resulting inert presentation through the Study owner.
Run from a clean source checkout, retaining private inputs outside Git.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
from html import escape
from html.parser import HTMLParser
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile
from zipfile import ZipFile

def limits():
    import resource

    resource.setrlimit(resource.RLIMIT_CPU, (60, 60))
    resource.setrlimit(resource.RLIMIT_FSIZE, (32 * 1024**2, 32 * 1024**2))


def render(command, directory):
    quarto_root = Path(command[0]).parent.parent
    isolated = ["bwrap", "--unshare-all", "--die-with-parent", "--ro-bind", "/usr", "/usr", "--symlink", "usr/bin", "/bin",
        "--ro-bind", "/lib", "/lib", "--ro-bind", "/lib64", "/lib64", "--dir", "/etc", "--ro-bind", "/etc/alternatives", "/etc/alternatives",
        "--ro-bind", str(quarto_root), "/opt/quarto", "--bind", str(directory), "/work",
        "--tmpfs", "/tmp", "--proc", "/proc", "--dev", "/dev", "--chdir", "/work", "--clearenv",
        "--setenv", "PATH", "/usr/bin:/bin", "--setenv", "HOME", "/work", "--setenv", "TMPDIR", "/tmp",
        "--setenv", "LANG", "C.UTF-8", "--setenv", "QUARTO_DENO_V8_OPTIONS", "--max-old-space-size=256,--max-heap-size=256",
        "/opt/quarto/bin/quarto", *command[1:]]
    result = subprocess.run(isolated, cwd=directory,
        env={"PATH": "/usr/bin:/bin"},
        capture_output=True, text=True, timeout=90, preexec_fn=limits)
    if result.returncode:
        raise RuntimeError(f"isolated Quarto build failed ({result.returncode}): {result.stderr[-4000:]}")
    return result


class Fragment(HTMLParser):
    def __init__(self, directory, assets=None):
        super().__init__(convert_charrefs=True)
        self.parts, self.body, self.skip = [], False, 0
        self.assets = assets if assets is not None else {name: (directory / name).read_bytes() for name in ("outcomes.png", "weighting.png", "components.png")}

    def handle_starttag(self, tag, attrs):
        from portal.backend.service.research.reports import TAGS
        if tag == "body":
            self.body = True
        if not self.body:
            return
        if tag in {"script", "style"}:
            self.skip += 1
        if self.skip or tag not in TAGS:
            return
        attrs = dict(attrs)
        kept = {k: v for k, v in attrs.items() if k in {"id", "alt", "colspan", "rowspan"}}
        if tag == "img":
            name = attrs.get("src", "")
            if name not in self.assets:
                raise ValueError("unexpected image path")
            kept["src"] = "data:image/png;base64," + base64.b64encode(self.assets[name]).decode()
        if tag == "a":
            if not attrs.get("href", "").startswith("#"):
                raise ValueError("unexpected report link")
            kept["href"] = attrs["href"]
        self.parts.append("<" + tag + "".join(f' {k}="{escape(v, quote=True)}"' for k, v in kept.items()) + ">")

    def handle_endtag(self, tag):
        from portal.backend.service.research.reports import TAGS, VOID
        if tag in {"script", "style"} and self.skip:
            self.skip -= 1
            return
        if self.body and not self.skip and tag in TAGS - VOID:
            self.parts.append(f"</{tag}>")
        if tag == "body":
            self.body = False

    def handle_data(self, data):
        if self.body and not self.skip:
            self.parts.append(escape(data))


def source_revision():
    """Attribute the builder only to a clean, committed source tree."""
    root = Path(__file__).resolve().parents[1]
    command = ["git", "-C", str(root), "-c", "core.filemode=false"]
    if subprocess.check_output([*command, "status", "--porcelain=v1", "--untracked-files=normal"], text=True).strip():
        raise ValueError("report build requires a clean committed checkout; keep private inputs outside Git")
    return subprocess.check_output([*command, "rev-parse", "HEAD"], text=True).strip()


def read_bounded(path, limit):
    if path.is_symlink() or not path.is_file():
        raise ValueError("report input must be an explicit regular file, not a symlink")
    with path.open("rb") as stream:
        value = stream.read(limit + 1)
    if not value or len(value) > limit:
        raise ValueError("report input is empty or exceeds its size limit")
    return value


def verify_bundle(path, study_id, publication_hash):
    """Verify saved bytes and selected interpretation, without replay or new science."""
    from io import BytesIO
    from market_data.frozen import semantic_hash

    content = read_bounded(path, 32 * 1024**2)
    with ZipFile(BytesIO(content)) as archive:
        names = archive.namelist()
        if len(names) != len(set(names)) or len(names) > 160 or sum(row.file_size for row in archive.infolist()) > 32 * 1024**2:
            raise ValueError("duplicate files or oversized evidence bundle")
        for name in names:
            if "\\" in name or PurePosixPath(name).is_absolute() or any(part in {"", ".", ".."} for part in name.split("/")):
                raise ValueError("unsafe evidence bundle member")
        checksums = {}
        for row in archive.read("checksums.sha256").decode("utf-8").splitlines():
            digest, name = row.split("  ", 1)
            if name in checksums or not re.fullmatch(r"[0-9a-f]{64}", digest):
                raise ValueError("invalid evidence checksum inventory")
            checksums[name] = digest
        if set(checksums) != set(names) - {"checksums.sha256"}:
            raise ValueError("incomplete evidence checksum inventory")
        for name, digest in checksums.items():
            if hashlib.sha256(archive.read(name)).hexdigest() != digest:
                raise ValueError("evidence bundle checksum mismatch")
        publication = json.loads(archive.read("records/selected-publication.json"))
        manifest = json.loads(archive.read("manifest.json"))
        if manifest.get("schema_version") != "research_saved_evidence_bundle.v1" or manifest.get("item_id") != study_id or manifest.get("publication_hash") != publication_hash:
            raise ValueError("evidence bundle selected Study mismatch")
        if publication.get("question_id") != study_id or publication.get("publication_hash") != publication_hash or semantic_hash({k: v for k, v in publication.items() if k != "publication_hash"}) != publication_hash:
            raise ValueError("selected interpretation identity mismatch")
    return hashlib.sha256(content).hexdigest()


def prepare_assets(paths):
    from portal.backend.service.research.images import decode_image

    if len(paths) > 8:
        raise ValueError("at most eight prepared PNG assets")
    assets, pixels = {}, 0
    for path in paths:
        name = path.name
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*\.png", name) or name in assets:
            raise ValueError("PNG assets require distinct simple basenames (letters, numbers, hyphen, underscore)")
        content = read_bounded(path, 4 * 1024**2)
        _, metadata = decode_image(base64.b64encode(content).decode("ascii"))
        if metadata["media_type"] != "image/png":
            raise ValueError("prepared assets must be PNG")
        pixels += metadata["width"] * metadata["height"]
        if pixels > 24_000_000:
            raise ValueError("prepared assets exceed 24 megapixels total")
        assets[name] = content
    return assets


def build_report(*, bundle, source_qmd, assets, quarto, study_id, publication_hash, title):
    from portal.backend.service.research.reports import normalize_html

    if not isinstance(title, str) or not title.strip() or len(title) > 255:
        raise ValueError("report title must contain 1..255 characters")
    revision = source_revision()
    bundle_sha = verify_bundle(bundle, study_id, publication_hash)
    source = read_bounded(source_qmd, 128 * 1024).decode("utf-8")
    prepared = prepare_assets(assets)
    with tempfile.TemporaryDirectory(prefix="qt-quarto-report-") as temp:
        directory = Path(temp)
        (directory / "report.qmd").write_text(source, encoding="utf-8")
        for name, content in prepared.items():
            (directory / name).write_bytes(content)
        version = render([str(quarto.resolve()), "--version"], directory).stdout.strip()
        render([str(quarto.resolve()), "render", "report.qmd", "--to", "html", "--no-execute", "--output", "report.html"], directory)
        fragment = Fragment(directory, prepared)
        fragment.feed(read_bounded(directory / "report.html", 32 * 1024**2).decode("utf-8"))
        fragment.close()
        html = normalize_html("".join(fragment.parts))
        if not html.strip():
            raise ValueError("Quarto produced no readable report content")
    if source_revision() != revision:
        raise ValueError("builder checkout changed during report rendering")
    return {"title": title.strip(), "publication_hash": publication_hash, "html": html,
            "source_qmd": source, "build": {"quarto_version": version, "source_revision": revision,
                                           "evidence_bundle_sha256": bundle_sha}}


def write_report(args):
    if args.output.exists():
        raise ValueError("output already exists; identical publication retries reuse the original payload")
    payload = build_report(bundle=args.bundle, source_qmd=args.source_qmd, assets=args.asset,
                           quarto=args.quarto, study_id=args.study_id, publication_hash=args.publication_hash, title=args.title)
    # Publish a complete local payload atomically, without replacing an existing
    # retry request. The private temporary file stays on the same filesystem.
    with tempfile.TemporaryDirectory(prefix=".qt-report-upload-", dir=args.output.parent) as temp:
        completed = Path(temp) / "payload.json"
        with completed.open("x", encoding="utf-8") as stream:
            os.chmod(completed, 0o600)
            json.dump(payload, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(completed, args.output)
    print(json.dumps({"study_id": args.study_id, "publication_hash": args.publication_hash,
                      "output": str(args.output), "build": payload["build"]}))


def add_build_arguments(parser):
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--source-qmd", type=Path, required=True)
    parser.add_argument("--asset", type=Path, action="append", default=[], help="Explicit prepared PNG; repeat for each chart.")
    parser.add_argument("--quarto", type=Path, required=True)
    parser.add_argument("--study-id", required=True)
    parser.add_argument("--publication-hash", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--output", type=Path, required=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_build_arguments(parser)
    args = parser.parse_args()
    write_report(args)


if __name__ == "__main__":
    main()
