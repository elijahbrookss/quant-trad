"""Backend application package for the Quant-Trad portal."""

from pathlib import Path
import sys


def _ensure_src_on_path() -> None:
    """Guarantee that the shared `src` package directory is importable."""

    src_dir = Path(__file__).resolve().parents[2] / "src"
    if src_dir.is_dir():
        src_path = str(src_dir)
        if src_path not in sys.path:
            sys.path.insert(0, src_path)


_ensure_src_on_path()


