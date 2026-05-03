"""
extract_to_csv.py
==================
Run a manifest of SQL queries against the analytical PostgreSQL database
of the Accent Fleet Analytics project and export each result as a
timestamped CSV under the ``exports/`` directory.

Usage
-----
    python extract_to_csv.py --manifest config/queries.yaml
    python extract_to_csv.py --manifest config/queries.yaml --dry-run

The connection parameters are read from environment variables:
    PG_HOST, PG_PORT, PG_USER, PG_PWD, PG_DB

A typical workflow on a developer workstation opens an SSH tunnel to the
Azure VM first:

    ssh -i ~/.ssh/azure_id_ed25519 -L 5432:localhost:5432 -N \
        azureuser@<vm-public-ip>
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml


# SQLAlchemy and the psycopg2 driver are only required for a live run.
# We defer the import so that ``--help`` and ``--dry-run`` keep working on
# a workstation that does not have them installed.
def build_engine():
    try:
        from sqlalchemy import create_engine
    except ImportError as exc:
        sys.stderr.write(
            "error: SQLAlchemy is required for live extraction.\n"
            "       Install it with:\n"
            "           pip install sqlalchemy psycopg2-binary pandas pyyaml\n"
            "       Or run with --dry-run to validate the manifest only.\n"
        )
        raise SystemExit(2) from exc

    missing = [v for v in ("PG_USER", "PG_PWD", "PG_DB") if v not in os.environ]
    if missing:
        sys.stderr.write(
            f"error: missing required environment variables: {', '.join(missing)}\n"
            "       Open the SSH tunnel to the Azure VM and export PG_USER, "
            "PG_PWD and PG_DB before running.\n"
        )
        raise SystemExit(2)

    dsn = (
        f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PWD']}"
        f"@{os.environ.get('PG_HOST', 'localhost')}:{os.environ.get('PG_PORT', '5432')}"
        f"/{os.environ['PG_DB']}"
    )
    return create_engine(dsn, pool_pre_ping=True, future=True)


def run_query(engine, sql: str) -> pd.DataFrame:
    from sqlalchemy import text  # imported lazily, see build_engine()
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export SQL query results to CSV.")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--out-dir", default=Path("exports"), type=Path)
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse the manifest but do not run the queries.")
    args = parser.parse_args()

    manifest = yaml.safe_load(args.manifest.read_text(encoding="utf-8"))
    args.out_dir.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        for q in manifest["queries"]:
            print(f"[DRY] {q['name']:40s} -> {args.out_dir / (q['name'] + '.csv')}")
        return

    engine = build_engine()
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    for q in manifest["queries"]:
        name = q["name"]
        sql = q["sql"]
        out = args.out_dir / f"{name}__{stamp}.csv"
        print(f"[RUN] {name} -> {out}")
        df = run_query(engine, sql)
        df.to_csv(out, index=False)


if __name__ == "__main__":
    main()
