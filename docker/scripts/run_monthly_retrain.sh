#!/bin/sh
# Wrapper invoked by supercronic every Monday at 04:00 UTC. Exits early
# unless today is in the first 7 days of the month — i.e. unless today
# is the FIRST Monday of the month.
#
# Cron has no native "first Monday of the month" syntax. The standard
# workaround is to fire weekly and short-circuit in the shell. Without
# this gate the retrain would run 4-5 times per month, which:
#
#   - Wastes MLflow registry slots (each candidate becomes a version)
#   - Risks the gate flapping back and forth on noisy run-to-run
#     silhouette (KMeans re-fits don't produce identical scores)
#   - Breaks the contract documented in scripts/retrain_monthly.py:
#     "monthly retraining entry point"
#
# Why a shell wrapper rather than putting the gate inside the Python
# CLI? Two reasons:
#   1. The CLI is also used for ad-hoc / backfill retrains (passing
#      --month-from manually). Gating it on first-Monday-only would
#      break that use case.
#   2. Keeping the schedule logic in shell means the schedule lives
#      next to the cron line that triggers it (docker/retrain.crontab)
#      rather than buried in a Python module.
set -eu

DAY=$(date +%d)
NOW=$(date -u +%FT%TZ)

if [ "$DAY" -gt 7 ]; then
    echo "retrain-scheduler $NOW skip — not first Monday (day-of-month=$DAY)"
    exit 0
fi

echo "retrain-scheduler $NOW firing monthly retrain (day-of-month=$DAY)"
exec python /app/scripts/retrain_monthly.py
