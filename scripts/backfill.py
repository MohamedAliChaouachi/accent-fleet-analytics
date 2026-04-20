#!/usr/bin/env python
"""
Backfill wrapper — equivalent to `run_batch.py --mode backfill`.

Kept separate so ops can schedule it distinctly (e.g., run once via
kubectl job, while incremental runs under a cronjob).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accent_fleet.pipeline import backfill_flow


if __name__ == "__main__":
    backfill_flow()
