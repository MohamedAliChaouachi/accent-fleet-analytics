"""
Batch source: reads a staging table in window-bounded chunks.

Used by the batch and backfill pipelines. In streaming mode it is replaced
by StreamKafkaSource with identical downstream contracts.

Performance notes:
  - We SELECT only the columns the downstream transform needs (projection
    pushdown), keeping memory light even for wide staging tables.
  - The query uses event-time range predicates that match the indexes
    on staging.path.begin_path_time, staging.stop.stop_start, etc.
  - Paging uses a simple OFFSET + LIMIT on event-time ordering. For very
    large windows (backfill) we recommend shrinking the window instead
    of relying on OFFSET, which is O(n).
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterator

import polars as pl
from sqlalchemy import text

from accent_fleet.db import get_engine


# Projection maps: table -> columns to pull.
# Keeping this explicit makes schema drift in staging non-fatal.
PROJECTIONS: dict[str, list[str]] = {
    "path": [
        "tenant_id", "device_id", "begin_path_time", "end_path_time",
        "begin_path_latitude", "begin_path_longitude",
        "end_path_latitude", "end_path_longitude",
        "max_speed", "path_duration", "distance_driven",
        "fuel_used", "start_odo", "end_odo",
    ],
    "stop": [
        "tenant_id", "device_id", "stop_start", "stop_end",
        "stop_duration", "in_path", "stop_lat", "stop_long", "stop_adress",
    ],
    "rep_overspeed": [
        "tenant_id", "device_id", "begin_path_time", "end_path_time",
        "max_speed", "path_duration", "distance_driven",
    ],
    "notification": [
        "id AS notif_id", "tenant_id", "device_id", "created_at",
        "description", "alert_value", "lat", "lng",
    ],
    "rep_activity_daily": [
        "tenant_id", "device_id", "activity_start_time", "activity_end_time",
        "activity_start_odo", "activity_end_odo", "working_time",
    ],
}


class BatchStagingSource:
    """Pull rows from a staging table inside an event-time window."""

    def __init__(self, table_name: str, time_column: str) -> None:
        if table_name not in PROJECTIONS:
            raise ValueError(f"No projection defined for staging table {table_name!r}")
        self.table_name = table_name
        self.time_column = time_column
        self._columns = PROJECTIONS[table_name]

    # ------------------------------------------------------------------
    def iter_batches(
        self,
        *,
        window_start: datetime,
        window_end: datetime,
        batch_size: int,
    ) -> Iterator[pl.DataFrame]:
        """
        Yield Polars DataFrames covering [window_start, window_end).

        We use server-side cursors (SQLAlchemy's `yield_per`) so that a
        100-day backfill window doesn't load everything into memory.
        """
        select_list = ", ".join(self._columns)
        # The optional SPEED% filter for the notification table is added here.
        extra_where = ""
        if self.table_name == "notification":
            extra_where = " AND description LIKE 'SPEED%'"

        sql = f"""
            SELECT {select_list}
            FROM staging.{self.table_name}
            WHERE {self.time_column} >= :window_start
              AND {self.time_column} <  :window_end
              {extra_where}
            ORDER BY {self.time_column}
        """

        engine = get_engine()
        with engine.connect().execution_options(yield_per=batch_size) as conn:
            result = conn.execute(
                text(sql),
                {"window_start": window_start, "window_end": window_end},
            )
            keys = list(result.keys())
            while True:
                chunk = result.fetchmany(batch_size)
                if not chunk:
                    break
                # Materialise to Polars. `infer_schema_length=None` disables
                # the per-column type inference limit, giving us stable dtypes.
                records = [dict(zip(keys, row, strict=False)) for row in chunk]
                yield pl.from_dicts(records, infer_schema_length=None)

    # ------------------------------------------------------------------
    def count(self, *, window_start: datetime, window_end: datetime) -> int:
        """Row count inside the window — used for monitoring."""
        extra_where = ""
        if self.table_name == "notification":
            extra_where = " AND description LIKE 'SPEED%'"
        sql = f"""
            SELECT COUNT(*) AS n FROM staging.{self.table_name}
            WHERE {self.time_column} >= :window_start
              AND {self.time_column} <  :window_end
              {extra_where}
        """
        with get_engine().connect() as conn:
            row = conn.execute(
                text(sql),
                {"window_start": window_start, "window_end": window_end},
            ).first()
            return int(row.n) if row else 0
