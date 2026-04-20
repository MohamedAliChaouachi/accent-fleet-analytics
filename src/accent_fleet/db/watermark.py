"""
Watermark store.

This is the heart of the incremental refactor. Every fact load reads the
watermark, processes rows strictly newer than (watermark - overlap), then
advances the watermark on commit.

Concurrency: we rely on Postgres row locks via `SELECT ... FOR UPDATE`.
A second runner trying to process the same table blocks until the first
commits. This is deliberate — two runners processing the same window at
once would be safe (idempotent inserts) but wasteful.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Connection


@dataclass
class WatermarkWindow:
    """A half-open [start, end) window in event-time."""

    start: datetime
    end: datetime
    table_name: str

    @property
    def is_empty(self) -> bool:
        return self.start >= self.end


class WatermarkStore:
    """
    Reader/writer for warehouse.etl_watermark.

    A typical incremental run looks like:

        with transaction() as conn:
            ws = WatermarkStore(conn)
            window = ws.get_window("fact_trip", overlap_minutes=10)
            # ... execute fact load with window.start / window.end ...
            ws.advance("fact_trip", new_event_time=window.end, run_id=run_id)
    """

    def __init__(self, conn: Connection, layer: str = "warehouse") -> None:
        self.conn = conn
        self.layer = layer

    # ------------------------------------------------------------------
    def get_last_event_time(self, table_name: str) -> datetime | None:
        """
        Return the last event-time successfully loaded, or None if this
        table has never been loaded. Acquires a row lock so concurrent
        runners don't race on the same table.
        """
        row = self.conn.execute(
            text(
                """
                SELECT last_event_time
                FROM warehouse.etl_watermark
                WHERE layer = :layer AND table_name = :table_name
                FOR UPDATE
                """
            ),
            {"layer": self.layer, "table_name": table_name},
        ).first()
        if row is None:
            raise KeyError(
                f"Watermark row missing for ({self.layer}, {table_name}). "
                f"Did you run bootstrap?"
            )
        return row.last_event_time

    # ------------------------------------------------------------------
    def get_window(
        self,
        table_name: str,
        *,
        overlap_minutes: int,
        now: datetime | None = None,
        max_age_cap: datetime | None = None,
    ) -> WatermarkWindow:
        """
        Compute the incremental window for this table.

        Start = (last_event_time - overlap) or max_age_cap if never loaded.
        End   = now (processing time, used as the upper event-time bound).

        Overlap handles late-arriving data: a row with event_time slightly
        earlier than the last watermark still gets picked up on the next run.
        The natural-key UPSERT makes this safe — re-seeing a row is a no-op.
        """
        last = self.get_last_event_time(table_name)
        end = now or datetime.utcnow()

        if last is None:
            # Never loaded — start from max_age_cap (typically 2019-10-01)
            if max_age_cap is None:
                raise ValueError(
                    f"Table {table_name} has no watermark and no max_age_cap given."
                )
            start = max_age_cap
        else:
            start = last - timedelta(minutes=overlap_minutes)

        return WatermarkWindow(start=start, end=end, table_name=table_name)

    # ------------------------------------------------------------------
    def advance(
        self,
        table_name: str,
        *,
        new_event_time: datetime,
        run_id: int,
        rows_loaded: int = 0,
    ) -> None:
        """
        Move the watermark forward. Only accepts monotonic advances —
        never moves backward. This protects against a buggy loader that
        processes an old window and tries to roll the watermark back.
        """
        self.conn.execute(
            text(
                """
                UPDATE warehouse.etl_watermark
                SET last_event_time = GREATEST(
                        COALESCE(last_event_time, '1970-01-01'::timestamp),
                        :new_event_time
                    ),
                    last_run_at       = NOW(),
                    last_etl_run_id   = :run_id,
                    rows_loaded_total = rows_loaded_total + :rows_loaded
                WHERE layer = :layer AND table_name = :table_name
                """
            ),
            {
                "new_event_time": new_event_time,
                "run_id": run_id,
                "rows_loaded": rows_loaded,
                "layer": self.layer,
                "table_name": table_name,
            },
        )

    # ------------------------------------------------------------------
    def reset(self, table_name: str) -> None:
        """
        Danger: reset a watermark to NULL, causing the next run to back-fill.
        Exposed for operational recovery (e.g., after dropping a fact table).
        """
        self.conn.execute(
            text(
                """
                UPDATE warehouse.etl_watermark
                SET last_event_time = NULL,
                    rows_loaded_total = 0
                WHERE layer = :layer AND table_name = :table_name
                """
            ),
            {"layer": self.layer, "table_name": table_name},
        )
