"""
Thin entrypoint that trains the device-risk-score Isolation Forest models
(one per tenant, bundled into a single MLflow registry version) and writes
artifacts to MLflow + models/risk_score/.

Examples:
    python scripts/train_risk_score.py
    python scripts/train_risk_score.py --month-from 2024-06
    python scripts/train_risk_score.py --no-promote
"""

from __future__ import annotations

import argparse
import json
import sys

from accent_fleet.ml.train_risk import run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--month-from",
        default="2025-01",
        help="Lower bound (inclusive) for year_month, e.g. '2024-06'.",
    )
    parser.add_argument(
        "--no-promote",
        action="store_true",
        help="Do not transition the new MLflow version to Production.",
    )
    args = parser.parse_args(argv)

    result = run(month_from=args.month_from, promote=not args.no_promote)
    summary = {
        "tenants": result.tenants,
        "skipped_tenants": result.skipped_tenants,
        "n_rows": result.n_rows,
        "rows_by_tenant": result.rows_by_tenant,
        "overall_share": {k: round(v, 4) for k, v in result.overall_share.items()},
        "tenant_stats": {
            str(tid): {
                "n_rows": st.n_rows,
                "share_critical": round(st.share_critical, 4),
                "share_high": round(st.share_high, 4),
                "threshold_critical": round(st.threshold_critical, 2),
                "threshold_high": round(st.threshold_high, 2),
            }
            for tid, st in result.tenant_stats.items()
        },
        "training_window": result.training_window,
    }
    json.dump(summary, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
