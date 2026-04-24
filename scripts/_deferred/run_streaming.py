#!/usr/bin/env python
"""
Streaming pipeline entry point.

Starts a Kafka consumer loop that trickles events into staging. Requires
a reachable Kafka broker configured in .env. Exits cleanly on SIGTERM.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import structlog

from accent_fleet.config import settings
from accent_fleet.pipeline.flow_stream import run_stream


def main() -> int:
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, settings().pipeline_log_level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.dev.ConsoleRenderer(),
        ]
    )

    log = structlog.get_logger()
    log.info("stream.start", broker=settings().kafka_bootstrap_servers)

    try:
        run_stream()
    except KeyboardInterrupt:
        log.info("stream.interrupted")
        return 0
    except Exception as exc:  # noqa: BLE001
        log.error("stream.failed", error=str(exc), exc_info=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
