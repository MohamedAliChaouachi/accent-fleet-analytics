"""
fig_missing_heatmap.py
=======================
Render the null-rate heatmap from the result of scripts/sql/03_null_analysis.sql.

Tries the live database first; if SQLAlchemy is unavailable, the PG_* env
vars are missing, or the connection fails, the script falls back to the
representative null rates documented in Chapter 3.
"""

from __future__ import annotations
from pathlib import Path
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

OUT = Path(__file__).resolve().parents[3] / "figures" / "missing_heatmap.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)


# ----- data loaders --------------------------------------------------------

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
        sql_path = Path(__file__).resolve().parents[2] / "sql" / "03_null_analysis.sql"
        sql = sql_path.read_text(encoding="utf-8")
        engine = create_engine(dsn, pool_pre_ping=True, future=True)
        return pd.read_sql(sql, engine)
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] database unavailable ({exc.__class__.__name__}); using fallback values")
        return None


def _load_from_csv() -> pd.DataFrame | None:
    csv_dir = Path("exports")
    if not csv_dir.exists():
        return None
    files = sorted(csv_dir.glob("null_analysis__*.csv"))
    if not files:
        return None
    return pd.read_csv(files[-1])


def _fallback_frame() -> pd.DataFrame:
    """Representative null rates per (table, column) cited in Chapter 3."""
    rows = [
        ("staging.path",          "distance_driven", 0.2),
        ("staging.path",          "max_speed",       0.4),
        ("staging.path",          "path_duration",   0.0),
        ("staging.path",          "start_time",      0.0),
        ("staging.path",          "end_time",        0.1),
        ("staging.path",          "device_id",       0.0),
        ("staging.tracking_data", "latitude",        0.6),
        ("staging.tracking_data", "longitude",       0.6),
        ("staging.tracking_data", "speed",           1.2),
        ("staging.tracking_data", "engine_status",   3.4),
        ("staging.tracking_data", "device_id",       0.0),
        ("staging.device",        "imei",            0.0),
        ("staging.device",        "tenant_id",       0.0),
        ("staging.device",        "model",           7.8),
        ("staging.device",        "first_seen_at",   0.0),
        ("staging.event_log",     "event_type",      0.0),
        ("staging.event_log",     "severity",        2.1),
        ("staging.event_log",     "device_id",       0.0),
    ]
    return pd.DataFrame(rows, columns=["table_name", "column_name", "null_pct"])


# ----- rendering -----------------------------------------------------------

def render(df: pd.DataFrame) -> None:
    pivot = df.pivot(index="table_name", columns="column_name", values="null_pct").fillna(0)
    data = pivot.to_numpy()
    fig, ax = plt.subplots(figsize=(11, max(2.4, 0.6 * len(pivot))))
    im = ax.imshow(data, cmap="Blues", aspect="auto",
                   vmin=0, vmax=max(1.0, float(data.max())))
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("null %")

    ax.set_xticks(np.arange(len(pivot.columns)))
    ax.set_xticklabels(pivot.columns, rotation=30, ha="right")
    ax.set_yticks(np.arange(len(pivot.index)))
    ax.set_yticklabels(pivot.index)

    threshold = data.max() / 2.0 if data.max() > 0 else 0
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            if data[i, j] == 0:
                continue
            ax.text(j, i, f"{data[i, j]:.1f}",
                    ha="center", va="center", fontsize=8,
                    color="white" if data[i, j] > threshold else "black")

    ax.set_xticks(np.arange(data.shape[1] + 1) - 0.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0] + 1) - 0.5, minor=True)
    ax.grid(which="minor", color="white", linewidth=1.0)
    ax.tick_params(which="minor", bottom=False, left=False)

    ax.set_xlabel(""); ax.set_ylabel("")
    ax.set_title("Null-rate (%) per column on the principal source tables",
                 color="#1F4E79")
    plt.tight_layout()
    plt.savefig(OUT, bbox_inches="tight", dpi=300)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    df = _load_from_db()
    if df is None:
        df = _load_from_csv()
    if df is None:
        df = _fallback_frame()
    render(df)
