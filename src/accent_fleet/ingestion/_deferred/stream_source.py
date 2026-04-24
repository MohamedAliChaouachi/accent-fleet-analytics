"""
Streaming source (Kafka / Redpanda).

This is a working stub that activates the moment Accent stands up a
Kafka-compatible broker. The interface matches BatchStagingSource, so
the rest of the pipeline (rule engine, transforms, loaders) is unchanged.

Design:
  - One consumer instance per source topic (one per staging table).
  - Messages are JSON, validated by the pydantic contracts.
  - Micro-batches: we poll up to `batch_size` messages or `max_poll_ms`,
    whichever comes first, then yield a Polars DataFrame. This gives
    downstream loaders bulk throughput while keeping latency bounded.
  - Offsets are committed AFTER the warehouse load commits, so a crash
    mid-batch is recovered by re-reading from the last committed offset.
    Combined with the UPSERT idempotency, this gives exactly-once effect.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Iterator

import polars as pl

from accent_fleet.config import settings


class StreamKafkaSource:
    """
    Kafka consumer wrapped as a Source.

    Activation checklist:
      1. Bring up Kafka/Redpanda (see docker-compose.yml).
      2. Producer side: publish staging-schema JSON to the configured topics.
      3. Set KAFKA_BOOTSTRAP_SERVERS in .env.
      4. Swap BatchStagingSource for StreamKafkaSource in pipeline/flow_stream.
    """

    def __init__(self, table_name: str, time_column: str) -> None:
        self.table_name = table_name
        self.time_column = time_column
        self._topic = self._topic_for(table_name)
        self._consumer = None  # lazy — avoid importing confluent_kafka in tests

    # ------------------------------------------------------------------
    @staticmethod
    def _topic_for(table_name: str) -> str:
        s = settings()
        return {
            "path": s.kafka_topic_path,
            "stop": s.kafka_topic_stop,
            "rep_overspeed": s.kafka_topic_overspeed,
        }.get(table_name, f"fleet.{table_name}.v1")

    # ------------------------------------------------------------------
    def _ensure_consumer(self):
        if self._consumer is not None:
            return
        # Lazy import so the package is importable even when confluent_kafka
        # isn't installed (e.g., in CI for unit tests).
        from confluent_kafka import Consumer

        s = settings()
        self._consumer = Consumer(
            {
                "bootstrap.servers": s.kafka_bootstrap_servers,
                "group.id": s.kafka_consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,     # commit AFTER DB commit
            }
        )
        self._consumer.subscribe([self._topic])

    # ------------------------------------------------------------------
    def iter_batches(
        self,
        *,
        window_start: datetime,  # noqa: ARG002 — unused in stream mode
        window_end: datetime,    # noqa: ARG002 — kept for interface parity
        batch_size: int,
    ) -> Iterator[pl.DataFrame]:
        """
        Yield micro-batches of at most `batch_size` messages each.
        The window arguments are ignored in stream mode (kept for Source
        protocol compatibility).
        """
        self._ensure_consumer()
        buffer: list[dict] = []
        max_poll_ms = 2000    # flush at least every 2 s even if below batch_size

        while True:
            msg = self._consumer.poll(timeout=max_poll_ms / 1000.0)
            if msg is None:
                if buffer:
                    yield pl.from_dicts(buffer, infer_schema_length=None)
                    buffer = []
                continue
            if msg.error():
                # TODO: structured-log the error and continue.
                continue
            try:
                record = json.loads(msg.value().decode("utf-8"))
                buffer.append(record)
            except json.JSONDecodeError:
                continue

            if len(buffer) >= batch_size:
                yield pl.from_dicts(buffer, infer_schema_length=None)
                buffer = []

    # ------------------------------------------------------------------
    def commit(self) -> None:
        """Commit offsets after a successful downstream load."""
        if self._consumer is not None:
            self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        if self._consumer is not None:
            self._consumer.close()
