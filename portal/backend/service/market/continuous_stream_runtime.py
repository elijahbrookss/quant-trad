"""Public provider-neutral continuous collector runtime surface.

The legacy ``continuous_stream_collector`` module retains implementation and
compatibility imports. New collectors should depend on this module and provide
both ``ContinuousTransportAdapter`` and ``ContinuousProjectionAdapter``
implementations. Projection adapters own their analyzer and domain state. The
runtime deliberately has no default provider, channel, analyzer, or projection.
"""

from .continuous_stream_collector import (
    ContinuousCaptureAnalyzer,
    ContinuousProjectionAdapter,
    ContinuousStreamRuntime,
    ContinuousTransportAdapter,
    continuous_stream_runtime,
)


__all__ = [
    "ContinuousCaptureAnalyzer",
    "ContinuousProjectionAdapter",
    "ContinuousStreamRuntime",
    "ContinuousTransportAdapter",
    "continuous_stream_runtime",
]
