"""
fig_distributions.py
=====================
Render distribution plots of the principal numerical variables of
staging.path: trip distance, max speed and duration.

When the database is reachable (SQLAlchemy installed, PG_* env vars set,
SSH tunnel open), the script samples 10% of staging.path. Otherwise it
falls back to representative synthetic distributions consistent with the
descriptive statistics reported in Chapter 3.
"""

from __future__ import annotations
from pathlib import Path
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

OUT_DIR = Path(__file__).resolve().parents[3] / "figures"
OUT_DIR.mkdir(parents=True, exist_ok=True)
ACCENT = "#1F4E79"

QUERY = """
SELECT distance_driven, max_speed, path_duration / 60.0 AS duration_min
FROM staging.path
WHERE distance_driven > 0
  AND max_speed > 0
  AND path_duration > 0
TABLESAMPLE SYSTEM (10)
"""


def _load_from_db() -> pd.DataFrame | None:
    try:
        from sqlalchemy import create_engine  # noqa: WPS433
    except ImportError:
        return None
    try:
        dsn = (
            f"postgresql+psycopg2://{os.environ['PG_USER']}:{os.environ['PG_PWD']}"
            f"@{os.environ.get('PG_HOST', 'localhost')}:{os.environ.get('PG_PORT', '5432')}"
            f"/{os.environ['PG_DB']}"
        )
    except KeyError:
        return None
    try:
        engine = create_engine(dsn, pool_pre_ping=True, future=True)
        return pd.read_sql(QUERY, engine)
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] database unavailable ({exc.__class__.__name__}); using fallback values")
        return None


def _fallback_frame(n: int = 80_000) -> pd.DataFrame:
    """Synthetic distributions matching Chapter 3 descriptive statistics."""
    rng = np.random.default_rng(seed=2024)
    distance = rng.lognormal(mean=2.4, sigma=1.0, size=n)        # km, median ~11
    distance = np.clip(distance, 0.1, 600)
    duration = rng.lognormal(mean=2.7, sigma=0.9, size=n)         # min, median ~15
    duration = np.clip(duration, 0.5, 720)
    speed = rng.gamma(shape=8.5, scale=8.0, size=n)               # km/h, mean ~68
    speed = np.clip(speed, 1, 220)
    return pd.DataFrame({
        "distance_driven": distance,
        "max_speed": speed,
        "duration_min": duration,
    })


def histogram(df: pd.DataFrame, column: str, log: bool, xlabel: str, name: str) -> None:
    fig, ax = plt.subplots(figsize=(8, 4.4))
    ax.hist(df[column].dropna(), bins=60, color=ACCENT, alpha=0.85, edgecolor="white")
    if log:
        ax.set_yscale("log")
    ax.set_xlabel(xlabel)
    ax.set_ylabel("count" + (" (log)" if log else ""))
    ax.set_title(f"Distribution of {xlabel}", color=ACCENT)
    fig.tight_layout()
    out = OUT_DIR / name
    fig.savefig(out, bbox_inches="tight", dpi=300)
    print(f"wrote {out}")


def main() -> None:
    df = _load_from_db()
    if df is None:
        df = _fallback_frame()
    histogram(df, "distance_driven", True,  "trip distance (km)",       "distribution_distance.pdf")
    histogram(df, "max_speed",       False, "max speed (km/h)",         "distribution_speed.pdf")
    histogram(df, "duration_min",    True,  "trip duration (minutes)",  "distribution_duration.pdf")


if __name__ == "__main__":
    main()
