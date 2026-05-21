#!/usr/bin/env python3
"""Render Mermaid .mmd files to sibling SVG files."""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ROOT = ROOT / "docs" / "architecture"


class RenderError(RuntimeError):
    def __init__(self, source: Path, command: list[str], result: subprocess.CompletedProcess[str]) -> None:
        self.source = source
        self.command = command
        self.result = result
        super().__init__(f"failed to render {source}")


def _display_path(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _resolve_path(raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    return path.resolve()


def _parse_command(raw_command: str) -> list[str]:
    command = shlex.split(raw_command)
    if not command:
        raise ValueError("Mermaid CLI command cannot be empty")

    executable = command[0]
    if Path(executable).parent == Path("."):
        found = shutil.which(executable)
    else:
        candidate = Path(executable)
        found = shutil.which(executable) or (executable if candidate.exists() and os.access(candidate, os.X_OK) else None)
    if not found:
        raise FileNotFoundError(executable)

    command[0] = found
    return command


def _find_sources(roots: list[str], pattern: str) -> list[Path]:
    sources: list[Path] = []
    for raw_root in roots:
        root = _resolve_path(raw_root)
        if root.is_dir():
            sources.extend(sorted(root.rglob(pattern)))
            continue
        if root.is_file() and root.suffix == ".mmd":
            sources.append(root)
            continue
        raise FileNotFoundError(f"{raw_root} is not a directory or .mmd file")
    return sorted(set(sources))


def _render_one(source: Path, command: list[str], extra_args: list[str], dry_run: bool) -> Path:
    output = source.with_suffix(".svg")
    if dry_run:
        print(f"would render {_display_path(source)} -> {_display_path(output)}")
        return output

    fd, tmp_name = tempfile.mkstemp(prefix=f".{source.stem}.", suffix=".svg", dir=source.parent)
    os.close(fd)
    tmp_output = Path(tmp_name)
    tmp_output.unlink(missing_ok=True)

    render_command = [*command, *extra_args, "-i", str(source), "-o", str(tmp_output)]
    result = subprocess.run(render_command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        tmp_output.unlink(missing_ok=True)
        raise RenderError(source, render_command, result)
    if not tmp_output.exists() or tmp_output.stat().st_size == 0:
        tmp_output.unlink(missing_ok=True)
        raise RuntimeError(f"renderer produced no SVG for {_display_path(source)}")

    os.replace(tmp_output, output)
    return output


def _print_render_error(error: RenderError) -> None:
    print(f"error: failed to render {_display_path(error.source)}", file=sys.stderr)
    print(f"command: {shlex.join(error.command)}", file=sys.stderr)
    if error.result.stdout.strip():
        print("stdout:", file=sys.stderr)
        print(error.result.stdout.rstrip(), file=sys.stderr)
    if error.result.stderr.strip():
        print("stderr:", file=sys.stderr)
        print(error.result.stderr.rstrip(), file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render Mermaid .mmd files to sibling .svg files, replacing existing SVGs atomically."
    )
    parser.add_argument(
        "--root",
        action="append",
        dest="roots",
        help="Directory to scan recursively, or a single .mmd file. Repeatable. Defaults to docs/architecture.",
    )
    parser.add_argument(
        "--pattern",
        default="*.mmd",
        help="Filename pattern used when scanning directories. Default: *.mmd.",
    )
    parser.add_argument(
        "--mmdc",
        default=os.environ.get("MERMAID_CLI", "mmdc"),
        help="Mermaid CLI command. Default: mmdc.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print planned renders without writing SVG files.")
    parser.add_argument(
        "mmdc_args",
        nargs=argparse.REMAINDER,
        help="Extra arguments passed to Mermaid CLI after a -- separator.",
    )
    args = parser.parse_args()

    roots = args.roots or [str(DEFAULT_ROOT.relative_to(ROOT))]
    extra_args = list(args.mmdc_args)
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    try:
        command = [] if args.dry_run else _parse_command(args.mmdc)
        sources = _find_sources(roots, args.pattern)
    except FileNotFoundError as exc:
        missing = str(exc)
        mmdc_tokens = shlex.split(args.mmdc or "")
        if mmdc_tokens and missing == mmdc_tokens[0]:
            print(f"error: Mermaid CLI command not found: {missing}", file=sys.stderr)
            print("Install @mermaid-js/mermaid-cli or set MERMAID_CLI to an available command.", file=sys.stderr)
            print("Example: make architecture-svgs MERMAID_CLI='npx -y @mermaid-js/mermaid-cli'", file=sys.stderr)
        else:
            print(f"error: {missing}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if not sources:
        print(f"no Mermaid files found under: {', '.join(roots)}")
        return 0

    failures = 0
    for source in sources:
        try:
            output = _render_one(source, command, extra_args, args.dry_run)
        except RenderError as exc:
            failures += 1
            _print_render_error(exc)
            continue
        except RuntimeError as exc:
            failures += 1
            print(f"error: {exc}", file=sys.stderr)
            continue
        if not args.dry_run:
            print(f"rendered {_display_path(source)} -> {_display_path(output)}")

    if failures:
        print(f"error: {failures} Mermaid render(s) failed", file=sys.stderr)
        return 1

    action = "checked" if args.dry_run else "rendered"
    print(f"{action} {len(sources)} Mermaid file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
