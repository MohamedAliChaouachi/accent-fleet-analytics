"""Ingestion sources — same interface, batch or stream."""

from accent_fleet.ingestion.batch_source import BatchStagingSource
from accent_fleet.ingestion.source import Source
from accent_fleet.ingestion.stream_source import StreamKafkaSource

__all__ = ["BatchStagingSource", "Source", "StreamKafkaSource"]
