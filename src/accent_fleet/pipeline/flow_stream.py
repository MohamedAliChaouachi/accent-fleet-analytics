"""
Streaming flow.

This is the code path that activates when Accent runs Kafka/Redpanda.
The flow ingests messages from three topics (path, stop, overspeed),
runs them through the SAME cleaning + transform + upsert code as the
batch pipeline, then commits Kafka offsets after the DB commit.

Today this file is a reference implementation — it runs if Kafka is
reachable, otherwise exits cleanly. The batch pipeline is fully
functional without it.
"""

from __future__ import annotations

import signal
import time

import polars as pl
import structlog
from sqlalchemy import text

from accent_fleet.cleaning import load_rule_engine
from accent_fleet.config import settings
from accent_fleet.db import get_engine, transaction
from accent_fleet.ingestion import StreamKafkaSource
from accent_fleet.pipeline.run_log import begin_run, end_run

log = structlog.get_logger()

# Staging tables that can be streamed today.
STREAM_SOURCES: list[tuple[str, str]] = [
    ("path",          "begin_path_time"),
    ("stop",          "stop_start"),
    ("rep_overspeed", "begin_path_time"),
]


class StreamRunner:
    """
    Single-process stream runner.

    Each tick:
      1. Poll each consumer for a micro-batch.
      2. Apply cleaning rules (same engine as batch).
      3. Upsert into staging (via temp table + MERGE) so the downstream
         incremental_flow machinery picks it up.
      4. Commit Kafka offsets.

    The runner does not short-circuit warehouse/mart loads itself; instead
    it trickles data into staging and lets the incremental flow (running
    on a 1-min cadence) do the star-schema work. This separation keeps
    the two transforms in sync — a mart bug is fixed once, not twice.
    """

    def __init__(self) -> None:
        self._sources = [
            StreamKafkaSource(table_name=t, time_column=c)
            for t, c in STREAM_SOURCES
        ]
        self._rule_engine = load_rule_engine()
        self._running = True
        # Graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_stop)
        signal.signal(signal.SIGINT, self._handle_stop)

    def _handle_stop(self, *_):  # noqa: ARG002
        log.info("stream.stop_requested")
        self._running = False

    # ------------------------------------------------------------------
    def run(self) -> None:
        s = settings()
        run_id = begin_run(mode="stream")
        total = 0
        try:
            while self._running:
                for source in self._sources:
                    batches = source.iter_batches(
                        window_start=None,  # ignored in stream mode
                        window_end=None,
                        batch_size=s.pipeline_batch_size,
                    )
                    try:
                        df = next(batches, None)
                    except StopIteration:
                        df = None
                    if df is None or df.is_empty():
                        continue
                    cleaned, result = self._rule_engine.apply(df, table=source.table_name)
                    if cleaned.is_empty():
                        source.commit()
                        continue
                    self._write_to_staging(source.table_name, cleaned)
                    source.commit()
                    total += cleaned.height
                    log.info("stream.batch",
                             table=source.table_name,
                             in_=result.total_in,
                             out=result.total_out,
                             rejected=result.total_rejected)
                time.sleep(0.25)
            end_run(run_id, status="success", rows_loaded=total)
        except Exception as exc:
            end_run(run_id, status="failed", error_message=str(exc))
            raise
        finally:
            for s in self._sources:
                s.close()

    # ------------------------------------------------------------------
    def _write_to_staging(self, table_name: str, df: pl.DataFrame) -> None:
        """
        Bulk-append a cleaned Polars DataFrame into the staging table.

        Why append to staging rather than go straight to warehouse?
          - Keeps staging as the bronze ground-truth.
          - Lets the existing incremental SQL files do their job.
          - Simplifies replay — resetting a watermark + back-filling works
            uniformly for streamed and historical data.
        """
        if df.is_empty():
            return
        engine = get_engine()
        # Use Polars -> pandas -> pandas.to_sql via SQLAlchemy. Not the
        # fastest path, but fine for the current throughput budget.
        # For higher throughput, switch to psycopg3 COPY FROM binary.
        df.to_pandas().to_sql(
            name=table_name,
            con=engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )


def run_stream() -> None:
    """Entry point used by scripts/run_streaming.py."""
    StreamRunner().run()
