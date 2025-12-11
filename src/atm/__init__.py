"""ATM (Automatic Trade Management) template processing and validation."""

from .schema import DEFAULT_ATM_TEMPLATE
from .template import merge_templates, normalise_template, template_metrics

__all__ = [
    "DEFAULT_ATM_TEMPLATE",
    "merge_templates",
    "normalise_template",
    "template_metrics",
]
