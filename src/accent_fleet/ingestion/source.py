"""
Source protocol: the interface every ingestion source must satisfy.

The rest of the pipeline (cleaning, transforms, loaders) consumes this
protocol — it does not care whether data comes from a SQL query or a
Kafka topic. That's the whole point of the refactor.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterator, Protocol

import polars as pl


class Source(Protocol):
    """A data source that yields batches of events inside an event-time window."""

    table_name: str                    # e.g. "path", used to pick cleaning rules
    time_column: str                   # event-time column in the yielded DataFrames

    def iter_batches(
        self,
        *,
        window_start: datetime,
        window_end: datetime,
        batch_size: int,
    ) -> Iterator[pl.DataFrame]:
        """Yield DataFrames of at most `batch_size` rows covering the window."""
        ...
