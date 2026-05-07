#!/usr/bin/env python
"""Reconstruct last-month trips from archive telemetry for a tenant.

Default use:
    python scripts/reconstruct_telemetry_trips.py --tenant-id 7486

The SQL backfill inserts rows into warehouse.fact_trip with
trip_source='telemetry_reconstructed', then recomputes the dependent marts
for the touched month/days so ML and dashboard views see the tenant.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from sqlalchemy import text

from accent_fleet.config import load_pipeline_config
from accent_fleet.db import get_engine, run_sql_file
from accent_fleet.pipeline.run_log import begin_run, end_run


def _month_bounds(month: str | None) -> tuple[datetime | None, datetime | None]:
    if month is None:
        return None, None
    start = datetime.strptime(month, "%Y-%m")
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def _add_month(start: datetime) -> datetime:
    if start.month == 12:
        return start.replace(year=start.year + 1, month=1)
    return start.replace(month=start.month + 1)


def _parse_month(month: str) -> datetime:
    return datetime.strptime(month, "%Y-%m")


def _iter_months(start: datetime, end_inclusive: datetime):
    cursor = start
    while cursor <= end_inclusive:
        yield cursor, _add_month(cursor)
        cursor = _add_month(cursor)


def _resolve_latest_month(tenant_id: int) -> tuple[datetime, datetime]:
    with get_engine().connect() as conn:
        month_start = conn.execute(
            text(
                """
                SELECT date_trunc('month', MAX(date))::timestamp
                FROM staging.archive
                WHERE tenant_id = :tenant_id
                """
            ),
            {"tenant_id": tenant_id},
        ).scalar_one_or_none()
    if month_start is None:
        raise RuntimeError(f"No staging.archive telemetry found for tenant {tenant_id}")
    if month_start.month == 12:
        month_end = month_start.replace(year=month_start.year + 1, month=1)
    else:
        month_end = month_start.replace(month=month_start.month + 1)
    return month_start, month_end


def _archive_ping_count(tenant_id: int, month_start: datetime, month_end: datetime) -> int:
    with get_engine().connect() as conn:
        return int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM staging.archive
                    WHERE tenant_id = :tenant_id
                      AND date >= :month_start
                      AND date <  :month_end
                    """
                ),
                {
                    "tenant_id": tenant_id,
                    "month_start": month_start,
                    "month_end": month_end,
                },
            ).scalar_one()
        )


def _reconstruct_one_month(args, month_start: datetime, month_end: datetime, ping_seconds: int) -> int:
    run_id = begin_run(
        mode="telemetry_trip_reconstruction",
        window_start=month_start,
        window_end=month_end,
    )

    try:
        params = {
            "tenant_id": args.tenant_id,
            "month_start": month_start,
            "month_end": month_end,
            "etl_run_id": run_id,
            "max_gap_minutes": args.max_gap_minutes,
            "min_duration_seconds": args.min_duration_seconds,
            "min_distance_km": args.min_distance_km,
            "max_segment_speed_kmh": args.max_segment_speed_kmh,
            "ping_seconds": ping_seconds,
        }

        print(
            f"reconstructing tenant {args.tenant_id} from "
            f"{month_start:%Y-%m-%d} to {month_end:%Y-%m-%d}",
            flush=True,
        )

        with get_engine().begin() as conn:
            result = run_sql_file(
                conn,
                "27_fact_trip_reconstruct_telemetry_last_month.sql",
                params=params,
            )
            summary = result.mappings().one()

        touched_months = [summary["source_month"]]
        with get_engine().connect() as conn:
            touched_dates = [
                r[0].isoformat()
                for r in conn.execute(
                    text(
                        """
                        SELECT DISTINCT trip_date
                        FROM warehouse.fact_trip
                        WHERE tenant_id = :tenant_id
                          AND trip_source = 'telemetry_reconstructed'
                          AND reconstruction_method = 'ignition_gap_haversine_v1'
                          AND begin_path_time >= :month_start
                          AND begin_path_time <  :month_end
                        ORDER BY trip_date
                        """
                    ),
                    {
                        "tenant_id": args.tenant_id,
                        "month_start": month_start,
                        "month_end": month_end,
                    },
                )
            ]

        print(
            f"reconstruction loaded {summary['reconstructed_trips']} trips; "
            f"recomputing {summary['source_month']} marts",
            flush=True,
        )

        with get_engine().begin() as conn:
            print("recomputing mart_device_monthly_behavior", flush=True)
            run_sql_file(
                conn,
                "20_mart_device_monthly_behavior.sql",
                params={"touched_months": touched_months, "etl_run_id": run_id},
            )
            print(f"recomputing mart_fleet_daily for {len(touched_dates)} dates", flush=True)
            run_sql_file(
                conn,
                "30_mart_fleet_daily.sql",
                params={"touched_dates": touched_dates, "etl_run_id": run_id},
            )
            print("recomputing mart_vehicle_monthly", flush=True)
            run_sql_file(
                conn,
                "31_mart_vehicle_monthly.sql",
                params={"touched_months": touched_months, "etl_run_id": run_id},
            )
            print("recomputing mart_tenant_monthly_summary", flush=True)
            run_sql_file(
                conn,
                "32_mart_tenant_monthly_summary.sql",
                params={"touched_months": touched_months, "etl_run_id": run_id},
            )

        rows_loaded = int(summary["reconstructed_trips"] or 0)
        end_run(run_id, status="success", rows_loaded=rows_loaded)

        print(
            "reconstructed "
            f"{summary['reconstructed_trips']} trips for tenant {args.tenant_id} "
            f"({summary['reconstructed_devices']} devices, "
            f"{summary['total_distance_km']} km, "
            f"{summary['total_duration_hours']} h) "
            f"in {summary['source_month']}"
        )
        print("recomputed marts: device_monthly_behavior, fleet_daily, vehicle_monthly, tenant_monthly_summary")
        return rows_loaded
    except Exception as exc:  # noqa: BLE001
        end_run(run_id, status="failed", error_message=str(exc))
        raise


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconstruct tenant trips from staging.archive telemetry."
    )
    parser.add_argument("--tenant-id", type=int, default=7486)
    parser.add_argument(
        "--month",
        help="Single month to reconstruct as YYYY-MM. Defaults to the latest archive month.",
    )
    parser.add_argument(
        "--from-month",
        help="First modeling month to reconstruct as YYYY-MM, inclusive.",
    )
    parser.add_argument(
        "--to-month",
        help="Last month to reconstruct as YYYY-MM, inclusive. Defaults to latest archive month.",
    )
    parser.add_argument("--max-gap-minutes", type=int, default=10)
    parser.add_argument("--min-duration-seconds", type=int, default=60)
    parser.add_argument("--min-distance-km", type=float, default=0.1)
    parser.add_argument("--max-segment-speed-kmh", type=float, default=200.0)
    args = parser.parse_args()

    if args.month and (args.from_month or args.to_month):
        raise SystemExit("Use either --month or --from-month/--to-month, not both.")

    cfg = load_pipeline_config()
    ping_seconds = int(cfg.get("archive_telemetry", {}).get("ping_seconds", 30))
    latest_start, latest_end = _resolve_latest_month(args.tenant_id)

    if args.from_month:
        start = _parse_month(args.from_month)
        end = _parse_month(args.to_month) if args.to_month else latest_start
        total = 0
        skipped = 0
        for month_start, month_end in _iter_months(start, end):
            n = _archive_ping_count(args.tenant_id, month_start, month_end)
            if n == 0:
                print(f"skipping {month_start:%Y-%m}: no archive telemetry", flush=True)
                skipped += 1
                continue
            print(f"{month_start:%Y-%m}: {n:,} archive pings", flush=True)
            total += _reconstruct_one_month(args, month_start, month_end, ping_seconds)
        print(
            f"range reconstruction complete: {total} trips loaded, {skipped} empty months skipped",
            flush=True,
        )
        return 0

    month_start, month_end = _month_bounds(args.month)
    if month_start is None or month_end is None:
        month_start, month_end = latest_start, latest_end
    _reconstruct_one_month(args, month_start, month_end, ping_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
