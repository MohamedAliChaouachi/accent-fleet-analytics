"""Ingestion sources — same interface, batch or stream.

Stream implementation is deferred. See ``_deferred/stream_source.py``.
"""

from accent_fleet.ingestion.batch_source import BatchStagingSource
from accent_fleet.ingestion.source import Source

__all__ = ["BatchStagingSource", "Source"]
