"""
SQLAlchemy engine factory.

We use a single process-wide engine with a small pool. The pipeline is
either single-process (Prefect task) or, in streaming mode, one consumer
per topic partition — neither needs a large pool.
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import lru_cache
from typing import Iterator

from sqlalchemy import Engine, create_engine
from sqlalchemy.engine import Connection

from accent_fleet.config import settings


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Create (once) and return the process-wide engine."""
    s = settings()
    return create_engine(
        s.sqlalchemy_url,
        pool_size=5,
        max_overflow=5,
        pool_pre_ping=True,
        future=True,
        echo=False,
    )


@contextmanager
def transaction() -> Iterator[Connection]:
    """
    Yield a Connection inside an explicit transaction.

    Why explicit? Because a single pipeline task may run multiple SQL
    statements (insert into fact + advance watermark) and they must
    commit together. SQLAlchemy's `engine.begin()` is the right pattern.
    """
    engine = get_engine()
    with engine.begin() as conn:
        yield conn
