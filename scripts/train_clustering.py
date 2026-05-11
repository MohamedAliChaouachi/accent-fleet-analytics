"""
Thin entrypoint that trains the device-clustering model and writes artifacts
to MLflow + models/clustering/.

Examples:
    python scripts/train_clustering.py
    python scripts/train_clustering.py --month-from 2024-06
    python scripts/train_clustering.py --no-promote
"""

from __future__ import annotations

import argparse
import json
import sys

from accent_fleet.ml.train_clustering import run


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
        "k": result.k,
        "silhouette": round(result.silhouette, 4),
        "n_rows": result.n_rows,
        "cluster_sizes": result.cluster_sizes,
        "training_window": result.training_window,
    }
    json.dump(summary, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
