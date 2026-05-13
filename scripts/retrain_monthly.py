r"""
Monthly retraining entry point with silhouette-gated promotion.

Runs the gated retrain flow, prints a JSON summary, and exits with:
  0 — gate passed and promotion succeeded (OR cold-start, no current model)
  0 — gate held (this is the correct outcome of a regression check; not
       a CLI error — the operator decides whether to investigate)
  1 — retraining or MLflow plumbing failed before the gate could decide

Examples:
    python scripts/retrain_monthly.py
    python scripts/retrain_monthly.py --month-from 2024-06
    python scripts/retrain_monthly.py --tolerance 0.01

Scheduled use (host cron, first Monday of the month at 04:00):
    0 4 * * MON [ "$(date +\%d)" -le 7 ] && \
        cd /opt/accent-fleet-analytics && \
        .venv/bin/python scripts/retrain_monthly.py \
        >> /var/log/accent-retrain.log 2>&1
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from accent_fleet.ml.promotion import DEFAULT_SILHOUETTE_TOLERANCE
from accent_fleet.pipeline.flow_batch import retrain_flow


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month-from",
        default="2025-01",
        help="Lower bound (inclusive) for the training window, e.g. '2024-06'.",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_SILHOUETTE_TOLERANCE,
        help=(
            "Allowed silhouette drop vs. current Production. "
            f"Default: {DEFAULT_SILHOUETTE_TOLERANCE} "
            "(matches the literature noise floor)."
        ),
    )
    args = parser.parse_args(argv)

    # The flow logs through structlog (when setup_logging() runs in the
    # Prefect process); guard against the case where this CLI is invoked
    # outside that path with a plain stdlib basicConfig fallback.
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    result = retrain_flow(month_from=args.month_from)

    # Pretty-print for the cron log. `default=str` handles None / NaN
    # without exploding when the gate skipped MLflow lookups.
    json.dump(result, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")

    # Distinguish "couldn't decide" (exit 1) from "decided to hold" (exit 0).
    reason = (result or {}).get("reason", "")
    if reason.startswith(("import_error", "retrain_exception")):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
