r"""
Monthly retraining entry point for the per-tenant Isolation Forest risk
model, with stability-gated promotion.

Runs ``retrain_risk_flow``, prints a JSON summary, and exits with:
  0 -- gate passed and promotion succeeded (OR cold-start, no current model)
  0 -- gate held (this is the correct outcome of a regression check; not
       a CLI error -- the operator decides whether to investigate)
  1 -- retraining or MLflow plumbing failed before the gate could decide

The risk gate is stability-based, NOT silhouette-based -- IF is unsupervised
and silhouette doesn't apply to its raw-anomaly-score distribution. The
gate checks instead:

  promote IFF |delta %critical| <= max_critical_shift_pp
          AND |delta %high|     <= max_high_shift_pp
          AND PSI(score)        <  max_score_psi

with tolerances loaded from config/feature_definitions.yaml under
``risk_score_model.promotion_gate``. To re-tune the gate, edit YAML and
re-deploy -- there are intentionally no CLI flags for the tolerances so
operators can't silently shift the bar from a one-off command.

Examples:
    python scripts/retrain_risk_monthly.py
    python scripts/retrain_risk_monthly.py --month-from 2024-06

Scheduled use (preferred -- compose-managed supercronic service):
    docker compose --profile scheduler up -d retrain-risk-scheduler

    The scheduler fires every Monday 04:30 UTC (30 min after the
    clustering retrainer to avoid stampeding the DB) and the wrapper
    gates on "first Monday of the month" in shell. See
    docker/retrain-risk.crontab and docker/scripts/run_monthly_retrain_risk.sh.

Scheduled use (alternative -- host cron, no docker stack):
    30 4 * * MON [ "$(date +\%d)" -le 7 ] && \
        cd /opt/accent-fleet-analytics && \
        .venv/bin/python scripts/retrain_risk_monthly.py \
        >> /var/log/accent-retrain-risk.log 2>&1
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from accent_fleet.pipeline.flow_batch import retrain_risk_flow


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month-from",
        default="2025-01",
        help="Lower bound (inclusive) for the training window, e.g. '2024-06'.",
    )
    args = parser.parse_args(argv)

    # Same fallback as retrain_monthly.py: the flow logs through structlog
    # in the Prefect process, but a bare cron invocation might not have
    # setup_logging() running -- fall back to plain stdlib so we never go
    # silent in production.
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
    )

    result = retrain_risk_flow(month_from=args.month_from)

    # Pretty-print for the cron log. `default=str` handles None / NaN
    # without exploding when the gate skipped MLflow lookups (cold start)
    # or when the score-PSI provider returned None.
    json.dump(result, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")

    # Distinguish "couldn't decide" (exit 1) from "decided to hold" (exit 0).
    # The risk task's reason strings start with the same import_error /
    # retrain_exception prefixes as task_retrain_with_gate, so the same
    # parse logic works.
    reason = (result or {}).get("reason", "")
    if reason.startswith(("import_error", "retrain_exception")):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
