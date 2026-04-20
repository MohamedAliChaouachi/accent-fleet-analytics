#!/usr/bin/env python
"""
Batch pipeline entry point.

Usage:
    python scripts/run_batch.py --mode bootstrap
    python scripts/run_batch.py --mode backfill [--chunk-days 30]
    python scripts/run_batch.py --mode incremental
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Make the src/ package importable when run as a plain script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import structlog

from accent_fleet.config import settings
from accent_fleet.pipeline import (
    backfill_flow,
    bootstrap_flow,
    incremental_flow,
)


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Accent Fleet Phase 3 batch pipeline")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["bootstrap", "backfill", "incremental"],
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=None,
        help="Override backfill chunk size (only used in --mode backfill).",
    )
    args = parser.parse_args()

    _configure_logging(settings().pipeline_log_level)
    log = structlog.get_logger()
    log.info("batch.start", mode=args.mode)

    try:
        if args.mode == "bootstrap":
            bootstrap_flow()
        elif args.mode == "backfill":
            backfill_flow(chunk_days=args.chunk_days)
        elif args.mode == "incremental":
            incremental_flow()
    except Exception as exc:  # noqa: BLE001
        log.error("batch.failed", error=str(exc), exc_info=True)
        return 1

    log.info("batch.done", mode=args.mode)
    return 0


if __name__ == "__main__":
    sys.exit(main())
