#!/usr/bin/env python
"""
Refresh the reference fuel price used by the dashboards.

Pulls the live diesel price from the configured provider (FUEL_PRICE_API_URL)
and appends it to warehouse.ref_fuel_price. The executive (sql/33) and
fleet-efficiency (sql/36) views read the latest row to value fuel cost in
DT/L. When the provider is unset or unreachable nothing is written and the
dashboards keep the last good value (or the seeded STIR reference).

The ETL flow already calls this on a monthly-gated cadence; this script is for
the monthly scheduler (docker/retrain.crontab) and ad-hoc operator runs.

Usage:
    python scripts/refresh_fuel_price.py            # respects the refresh gate
    python scripts/refresh_fuel_price.py --force    # fetch now, ignore the gate
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Make src/ importable when run as a plain script (mirrors run_batch.py).
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from accent_fleet.ingestion.fuel_price import refresh_fuel_price


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force",
        action="store_true",
        help="Fetch immediately, ignoring the FUEL_PRICE_REFRESH_DAYS gate.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    result = refresh_fuel_price(force=args.force)
    if result.status == "inserted":
        print(
            f"updated: {result.price_per_litre} DT/L from {result.source}"
        )
        return 0
    if result.status in ("skipped_fresh", "disabled"):
        print(f"no change ({result.status}): {result.detail or ''}".rstrip())
        return 0
    # failed
    print(f"failed: {result.detail or 'see logs'}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
