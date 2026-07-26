"""ATM (Automatic Trade Management) template processing and validation."""

from .schema import DEFAULT_ATM_TEMPLATE
from .template import normalise_template

__all__ = [
    "DEFAULT_ATM_TEMPLATE",
    "normalise_template",
]
