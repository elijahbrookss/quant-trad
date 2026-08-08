"""Compatibility import for the research-owned pass-gate evaluator.

Experiment orchestration calls the backend application operation.  This import
keeps existing Python callers readable during the CLI compatibility window.
"""

from portal.backend.service.research.pass_gates import evaluate_pass_gates

__all__ = ["evaluate_pass_gates"]
