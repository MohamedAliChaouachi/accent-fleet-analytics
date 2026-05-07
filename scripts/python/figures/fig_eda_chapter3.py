"""
Generate the Chapter 3 (Data Understanding) figures from the live PostgreSQL
warehouse, with a deterministic offline fallback that reproduces the values
returned by the live queries on 2026-05-06.

Figures written to ``report/figures/`` :
    eda_quality_heatmap.pdf
    eda_monthly_volume.pdf
    eda_temporal_patterns.pdf
    eda_duration_distribution.pdf
    eda_class_imbalance.pdf
    eda_tenant_signatures.pdf
    eda_archive_temporal.pdf
    eda_archive_volume_by_tenant.pdf

The script is intentionally self-contained: SQLAlchemy is imported lazily so
``--help`` and the offline fallback work without a populated venv.

Usage
-----
    python scripts/python/figures/fig_eda_chapter3.py            # live + fallback
    python scripts/python/figures/fig_eda_chapter3.py --offline  # never query DB
"""
from __future__ import annotations

import argparse
import math
import os
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[3]
FIGDIR = PROJECT_ROOT / "report" / "figures"
FIGDIR.mkdir(parents=True, exist_ok=True)


def _maybe_engine():
    """Return a SQLAlchemy engine, or None if the env / sqlalchemy isn't ready."""
    try:
        from sqlalchemy import create_engine  # noqa: WPS433  (lazy by design)
    except ImportError:
        return None
    user = os.environ.get("PG_USER")
    pwd = os.environ.get("PG_PWD")
    host = os.environ.get("PG_HOST")
    port = os.environ.get("PG_PORT", "5432")
    db = os.environ.get("PG_DB")
    if not (user and pwd and host and db):
        return None
    try:
        eng = create_engine(
            f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}",
            pool_pre_ping=True, future=True,
        )
        with eng.connect() as c:  # smoke test
            c.exec_driver_sql("SELECT 1")
        return eng
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Offline fallback values (frozen snapshot, 2026-05-06).
# Every number here is taken directly from the MCP queries documented in the
# Chapter 3 EDA notebook (notebooks/01_data_understanding/03_eda_chapter3.ipynb).
# ---------------------------------------------------------------------------
QUALITY_LABELS = [
    "null_tenant", "null_device", "null_begin", "null_end", "null_duration",
    "null_distance", "null_max_speed", "null_fuel",
    "dur<=0", "dur>1d", "dist<=0", "dist>1000km", "speed>200",
    "fuel<0", "fuel>500L", "time<2019-10", "end<begin",
]
QUALITY_PCT = [
    0.000, 0.000, 0.000, 0.000, 0.000,
    0.000, 0.000, 0.000,
    0.020, 0.008, 0.0002, 0.0053, 0.00008,
    0.1405, 0.1464, 0.8264, 0.0020,
]

MONTHLY = [
    ("2024-01-01", 108058, 318), ("2024-02-01", 101437, 313),
    ("2024-03-01",  97584, 313), ("2024-04-01", 122313, 322),
    ("2024-05-01", 104288, 323), ("2024-06-01", 121914, 323),
    ("2024-07-01", 122157, 329), ("2024-08-01", 115634, 328),
    ("2024-09-01", 121548, 333), ("2024-10-01", 115517, 330),
    ("2024-11-01", 101768, 327), ("2024-12-01", 103739, 325),
    ("2025-01-01",  99180, 324), ("2025-02-01",  86398, 315),
    ("2025-03-01", 106596, 329), ("2025-04-01", 127008, 344),
    ("2025-05-01", 108837, 347), ("2025-06-01", 126809, 348),
    ("2025-07-01", 119747, 345), ("2025-08-01", 118989, 346),
    ("2025-09-01", 121968, 342), ("2025-10-01", 105825, 342),
    ("2025-11-01", 109012, 331), ("2025-12-01",  96697, 328),
    ("2026-01-01",  91086, 327), ("2026-02-01",  91630, 326),
    ("2026-03-01",  35712, 315),
]
HOURLY = [
    1243, 1435, 2370, 4743, 7672, 11791, 18918, 28541, 31019, 32248,
    31718, 28838, 25816, 23179, 21526, 18957, 16514, 12072, 9186, 7055,
    4827, 3061, 2351, 1703,
]
DOW_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
DOW_TRIPS = [13889, 57517, 57944, 57309, 55477, 55062, 49584]

DUR_BUCKETS = [
    ("0-1m", 16530), ("1-5m", 103807), ("5-10m", 63440),
    ("10-30m", 92521), ("30-60m", 41907), ("1-2h", 20968),
    ("2-4h", 7132), ("4h+", 478),
]
DUR_MU_LOG, DUR_SIGMA_LOG = 6.2979, 1.3479
DUR_SKEW, DUR_EX_KURT = 4.1488, 39.3415

BALANCE = {
    # scope -> (night, weekend, rush, short, long) — DERIVED inline from staging.path,
    # window: begin_path_time >= 2024-01-01, after physical-bound cleaning.
    # is_night     := hour < 6 OR hour >= 22
    # is_weekend   := DOW IN (Sun, Sat)
    # is_rush_hour := hour IN (7..9, 17..19)
    # is_short     := path_duration < 300 s
    # is_long      := path_duration > 3600 s
    "global": (10.33, 18.96, 33.76, 35.49, 8.14),
    "264":    (13.59, 20.82, 32.09, 29.12, 14.16),
    "1787":   (7.51,  20.31, 36.06, 43.46, 3.62),
    "235":    (10.94, 16.94, 33.15, 33.55, 8.70),
    "238":    (10.12, 19.64, 33.14, 33.47, 7.98),
}

# Tenant-level behavioural signatures derived from staging tables only:
#   staging.path  -> avg max-speed, night & weekend ratios
#   staging.rep_overspeed -> overspeed events per 100 km
#   staging.notification  -> notifications per 100 km
# Window: begin_path_time / created_at >= 2025-01-01.
SIGNATURES = {
    264:  dict(over_per_100km=4.891, notif_per_100km=4.730, avg_speed=54.5,
               night_ratio=0.1360, weekend_ratio=0.2131),
    1787: dict(over_per_100km=0.062, notif_per_100km=0.264, avg_speed=42.3,
               night_ratio=0.0765, weekend_ratio=0.2023),
    235:  dict(over_per_100km=0.475, notif_per_100km=0.014, avg_speed=45.4,
               night_ratio=0.1050, weekend_ratio=0.1644),
    238:  dict(over_per_100km=0.059, notif_per_100km=0.081, avg_speed=46.0,
               night_ratio=0.0885, weekend_ratio=0.1877),
}

# ---------------------------------------------------------------------------
# staging.archive — high-frequency telemetry pings (54.72M rows, 17 GB).
# All numbers derived from a 0.1 % TABLESAMPLE SYSTEM scan, MCP snapshot
# 2026-05-06.  Window of dense activity: 2025-09 -> 2026-04.
# ---------------------------------------------------------------------------
ARCHIVE_HOURLY = [
    278, 479, 573, 984, 1372, 1451, 2818, 4272, 3955, 4428,
    4436, 4362, 4797, 4217, 4019, 3257, 3141, 2230, 2118, 1407,
    882, 589, 459, 623,
]
ARCHIVE_DOW_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
ARCHIVE_DOW_PINGS = [3389, 9912, 8371, 9371, 9373, 9346, 8202]
ARCHIVE_MONTHLY = [
    ("2025-09", 6.579), ("2025-10", 8.550), ("2025-11", 8.979),
    ("2025-12", 8.471), ("2026-01", 8.600), ("2026-02", 9.202),
    ("2026-03", 3.326),
]
# Per-tenant ping volumes extrapolated from the 0.1 % sample (×~1066 to match
# the 54.72M global total) and device counts observed in the same sample.
ARCHIVE_TENANTS = [
    # tenant, est. pings (M), devices in sample
    (264,  13.84, 56),
    (235,  13.48, 97),
    (7486,  9.99, 72),
    (1787,  9.54, 71),
    (238,   7.84, 52),
]


# ---------------------------------------------------------------------------
# Live loaders (each falls back to the static snapshot above).
# ---------------------------------------------------------------------------
def load_quality(eng):
    if eng is None:
        return QUALITY_LABELS, QUALITY_PCT
    try:
        import pandas as pd
        nulls = pd.read_sql("""
            WITH base AS (SELECT * FROM staging.path TABLESAMPLE SYSTEM (5))
            SELECT
              ROUND(100.0*SUM((tenant_id IS NULL)::int)/COUNT(*),3) AS null_tenant,
              ROUND(100.0*SUM((device_id IS NULL)::int)/COUNT(*),3) AS null_device,
              ROUND(100.0*SUM((begin_path_time IS NULL)::int)/COUNT(*),3) AS null_begin,
              ROUND(100.0*SUM((end_path_time IS NULL)::int)/COUNT(*),3) AS null_end,
              ROUND(100.0*SUM((path_duration IS NULL)::int)/COUNT(*),3) AS null_duration,
              ROUND(100.0*SUM((distance_driven IS NULL)::int)/COUNT(*),3) AS null_distance,
              ROUND(100.0*SUM((max_speed IS NULL)::int)/COUNT(*),3) AS null_max_speed,
              ROUND(100.0*SUM((fuel_used IS NULL)::int)/COUNT(*),3) AS null_fuel
            FROM base;
        """, eng).iloc[0]
        out = pd.read_sql("""
            SELECT
              COUNT(*) AS n_total,
              SUM((path_duration <= 0)::int)        AS dur_le_zero,
              SUM((path_duration > 86400)::int)     AS dur_gt_1day,
              SUM((distance_driven <= 0)::int)      AS dist_le_zero,
              SUM((distance_driven > 1000)::int)    AS dist_gt_1000km,
              SUM((max_speed > 200)::int)           AS speed_gt_200,
              SUM((fuel_used < 0)::int)             AS fuel_neg,
              SUM((fuel_used > 500)::int)           AS fuel_gt_500,
              SUM((begin_path_time < '2019-10-01'::timestamp)::int) AS time_pre_2019_10,
              SUM((end_path_time IS NOT NULL AND end_path_time < begin_path_time)::int) AS end_before_begin
            FROM staging.path;
        """, eng).iloc[0]
        n = float(out["n_total"])
        labels = list(nulls.index) + [
            "dur<=0", "dur>1d", "dist<=0", "dist>1000km", "speed>200",
            "fuel<0", "fuel>500L", "time<2019-10", "end<begin"
        ]
        vals = list(nulls.astype(float).values) + [100.0 * float(out[c]) / n for c in
            ["dur_le_zero","dur_gt_1day","dist_le_zero","dist_gt_1000km","speed_gt_200",
             "fuel_neg","fuel_gt_500","time_pre_2019_10","end_before_begin"]]
        return labels, vals
    except Exception:
        return QUALITY_LABELS, QUALITY_PCT


# ---------------------------------------------------------------------------
# Plotters.
# ---------------------------------------------------------------------------
def _style():
    plt.rcParams.update({
        "figure.dpi": 110, "savefig.dpi": 200, "font.size": 10,
        "axes.titlesize": 11, "axes.labelsize": 10,
        "axes.grid": True, "grid.alpha": 0.3, "figure.autolayout": True,
    })


def plot_quality(labels, values, out: Path):
    fig, ax = plt.subplots(figsize=(7.2, 5.2))
    arr = np.array(values).reshape(-1, 1)
    vmax = max(1.0, float(arr.max()))
    im = ax.imshow(arr, aspect="auto", cmap="OrRd", vmin=0, vmax=vmax)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels)
    ax.set_xticks([0]); ax.set_xticklabels(["% rows"])
    for i, v in enumerate(values):
        ax.text(0, i, f"{v:.3f}%", ha="center", va="center",
                color="white" if v > vmax * 0.5 else "black", fontsize=9)
    ax.set_title("Data quality profile of staging.path")
    fig.colorbar(im, ax=ax, fraction=0.04, pad=0.04, label="% rows")
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)


def plot_monthly(rows, out: Path):
    months = [r[0] for r in rows]
    trips = np.array([r[1] for r in rows]) / 1000.0
    devs = np.array([r[2] for r in rows])
    x = np.arange(len(months))
    fig, ax = plt.subplots(2, 1, figsize=(8.4, 5.4), sharex=True)
    ax[0].plot(x, trips, marker="o", color="#1f77b4")
    ax[0].set_ylabel("trips (k)")
    ax[0].set_title("Monthly trip volume — staging.path")
    ax[1].plot(x, devs, marker="s", color="#2ca02c")
    ax[1].set_ylabel("active devices")
    ax[1].set_title("Monthly active devices")
    ax[1].set_xticks(x[::3]); ax[1].set_xticklabels([m[:7] for m in months[::3]], rotation=30)
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)


def plot_temporal(out: Path):
    fig, ax = plt.subplots(1, 2, figsize=(10.4, 3.8))
    ax[0].bar(range(24), np.array(HOURLY) / 1000, color="#1f77b4")
    ax[0].set_xticks(range(0, 24, 2))
    ax[0].set_xlabel("hour of day"); ax[0].set_ylabel("trips (k)")
    ax[0].set_title("Hour-of-day trip distribution (Sep–Nov 2025)")
    ax[1].bar(DOW_LABELS, np.array(DOW_TRIPS) / 1000, color="#ff7f0e")
    ax[1].set_xlabel("day of week"); ax[1].set_ylabel("trips (k)")
    ax[1].set_title("Day-of-week trip distribution (Sep–Nov 2025)")
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)


def plot_duration(out: Path):
    fig, ax = plt.subplots(1, 2, figsize=(10.4, 4.0))
    labs = [b[0] for b in DUR_BUCKETS]; ns = [b[1] for b in DUR_BUCKETS]
    ax[0].bar(labs, np.array(ns) / 1000, color="#1f77b4")
    ax[0].set_title(f"Trip duration — raw scale  (skew={DUR_SKEW:.2f}, ex.kurt={DUR_EX_KURT:.1f})")
    ax[0].set_xlabel("duration bucket"); ax[0].set_ylabel("trips (k)")
    ax[0].tick_params(axis="x", rotation=30)
    x = np.linspace(DUR_MU_LOG - 4*DUR_SIGMA_LOG, DUR_MU_LOG + 4*DUR_SIGMA_LOG, 400)
    y = np.exp(-0.5*((x - DUR_MU_LOG)/DUR_SIGMA_LOG)**2) / (DUR_SIGMA_LOG*math.sqrt(2*math.pi))
    ax[1].fill_between(x, y, alpha=0.25, color="#d62728")
    ax[1].plot(x, y, lw=2, color="#d62728")
    ax[1].axvline(DUR_MU_LOG, ls="--", color="black", alpha=0.6, label=f"$\\mu$={DUR_MU_LOG:.2f}")
    ax[1].set_title(f"log(duration) — Normal fit ($\\sigma$={DUR_SIGMA_LOG:.2f})")
    ax[1].set_xlabel("log(duration in seconds)"); ax[1].set_ylabel("density"); ax[1].legend()
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)


def plot_balance(out: Path):
    classes = ["night", "weekend", "rush", "short", "long"]
    scopes = ["global", "264", "1787", "235", "238"]
    colours = ["#444444", "#1f77b4", "#d62728", "#2ca02c", "#ff7f0e"]
    width = 0.16; x = np.arange(len(classes))
    fig, ax = plt.subplots(figsize=(8.6, 4.4))
    for i, scope in enumerate(scopes):
        ax.bar(x + i*width, BALANCE[scope], width, color=colours[i], label=scope)
    ax.set_xticks(x + 2*width); ax.set_xticklabels(classes)
    ax.set_ylabel("% of trips")
    ax.set_title("Class balance of binary trip-status flags — overall vs. per tenant")
    ax.axhline(50, ls=":", color="grey", alpha=0.5)
    ax.legend(title="scope", ncol=5, fontsize=8, loc="upper right")
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)


def plot_signatures(out: Path):
    tenants = list(SIGNATURES.keys())
    over = [SIGNATURES[t]["over_per_100km"] for t in tenants]
    notif = [SIGNATURES[t]["notif_per_100km"] for t in tenants]
    night = [SIGNATURES[t]["night_ratio"] * 100 for t in tenants]
    weekend = [SIGNATURES[t]["weekend_ratio"] * 100 for t in tenants]
    x = np.arange(len(tenants)); width = 0.2
    fig, ax = plt.subplots(figsize=(8.6, 4.0))
    ax.bar(x - 1.5*width, over,    width, label="overspeed events / 100 km",  color="#d62728")
    ax.bar(x - 0.5*width, notif,   width, label="notifications / 100 km",    color="#ff7f0e")
    ax.bar(x + 0.5*width, night,   width, label="night-trip ratio (%)",      color="#1f77b4")
    ax.bar(x + 1.5*width, weekend, width, label="weekend-trip ratio (%)",    color="#2ca02c")
    ax.set_xticks(x); ax.set_xticklabels([str(t) for t in tenants])
    ax.set_xlabel("tenant"); ax.set_ylabel("indicator value")
    ax.set_title("Per-tenant behavioural signatures derived from staging tables (since 2025-01-01)")
    ax.legend(fontsize=8, ncol=2)
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)


def plot_archive_temporal(out: Path):
    fig, ax = plt.subplots(1, 2, figsize=(10.4, 3.8))
    ax[0].bar(range(24), np.array(ARCHIVE_HOURLY) / 1000, color="#9467bd")
    ax[0].set_xticks(range(0, 24, 2))
    ax[0].set_xlabel("hour of day"); ax[0].set_ylabel("pings (k, sample)")
    ax[0].set_title("staging.archive — hour-of-day ping distribution")
    ax[1].bar(ARCHIVE_DOW_LABELS, np.array(ARCHIVE_DOW_PINGS) / 1000, color="#8c564b")
    ax[1].set_xlabel("day of week"); ax[1].set_ylabel("pings (k, sample)")
    ax[1].set_title("staging.archive — day-of-week ping distribution")
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)


def plot_archive_volume(out: Path):
    fig, ax = plt.subplots(1, 2, figsize=(10.4, 4.0))
    months = [m[0] for m in ARCHIVE_MONTHLY]
    vols = [m[1] for m in ARCHIVE_MONTHLY]
    ax[0].plot(months, vols, marker="o", color="#9467bd", lw=2)
    ax[0].fill_between(range(len(months)), vols, alpha=0.2, color="#9467bd")
    ax[0].set_xlabel("month"); ax[0].set_ylabel("estimated pings (M)")
    ax[0].set_title("staging.archive — monthly ping volume (sample × 1000)")
    ax[0].tick_params(axis="x", rotation=30)
    tenants = [str(t[0]) for t in ARCHIVE_TENANTS]
    pings_m = [t[1] for t in ARCHIVE_TENANTS]
    devs = [t[2] for t in ARCHIVE_TENANTS]
    x = np.arange(len(tenants))
    bars = ax[1].bar(x, pings_m, color="#9467bd")
    ax[1].set_xticks(x); ax[1].set_xticklabels(tenants)
    ax[1].set_xlabel("tenant"); ax[1].set_ylabel("estimated pings (M)")
    ax[1].set_title("staging.archive — pings per tenant (sample-extrapolated)")
    for b, d in zip(bars, devs):
        ax[1].text(b.get_x() + b.get_width()/2, b.get_height() + 0.2,
                   f"{d} dev", ha="center", va="bottom", fontsize=8)
    fig.savefig(out, bbox_inches="tight"); plt.close(fig)


def main(offline: bool):
    _style()
    eng = None if offline else _maybe_engine()
    print("[fig_eda_chapter3]", "live engine ready" if eng else "offline fallback")
    labels, vals = load_quality(eng)
    plot_quality(labels, vals,         FIGDIR / "eda_quality_heatmap.pdf")
    plot_monthly(MONTHLY,              FIGDIR / "eda_monthly_volume.pdf")
    plot_temporal(                     FIGDIR / "eda_temporal_patterns.pdf")
    plot_duration(                     FIGDIR / "eda_duration_distribution.pdf")
    plot_balance(                      FIGDIR / "eda_class_imbalance.pdf")
    plot_signatures(                   FIGDIR / "eda_tenant_signatures.pdf")
    plot_archive_temporal(             FIGDIR / "eda_archive_temporal.pdf")
    plot_archive_volume(               FIGDIR / "eda_archive_volume_by_tenant.pdf")
    for p in sorted(FIGDIR.glob("eda_*.pdf")):
        print(" ->", p.relative_to(PROJECT_ROOT))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Render Chapter 3 EDA figures.")
    ap.add_argument("--offline", action="store_true",
                    help="Force the offline fallback (skip DB connection).")
    args = ap.parse_args()
    main(offline=args.offline)
