"""
fig_confusion_matrix.py
========================
Compute and render the confusion matrix between the rule baseline and
the Isolation Forest risk band, on the marts.v_device_risk_profile view.

If the database is reachable (SQLAlchemy installed and PG_* env vars set)
the matrix is computed live; otherwise the script falls back to the
representative counts cited in Chapter 5 so that the figure can still be
regenerated offline for the report.
"""

from __future__ import annotations
from pathlib import Path
import os

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

OUT = Path(__file__).resolve().parents[3] / "figures" / "confusion_matrix.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)
ACCENT = "#1F4E79"

SQL = """
WITH rule AS (
    SELECT tenant_id, device_id, year_month,
           CASE WHEN
               (overspeed_per_100km    > PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY overspeed_per_100km)
                                          OVER (PARTITION BY tenant_id))::int
             + (harsh_brake_per_100km > PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY harsh_brake_per_100km)
                                          OVER (PARTITION BY tenant_id))::int
             + (harsh_accel_per_100km > PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY harsh_accel_per_100km)
                                          OVER (PARTITION BY tenant_id))::int
             + (p95_max_speed         > PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY p95_max_speed)
                                          OVER (PARTITION BY tenant_id))::int
                >= 2
           THEN 'high' ELSE 'low' END AS rule_flag
    FROM marts.mart_device_monthly_behavior
)
SELECT r.rule_flag, p.risk_band
FROM rule r
JOIN marts.v_device_risk_profile p USING (tenant_id, device_id, year_month);
"""

LABELS = ["low", "medium", "high"]
RULE_LABELS = ["low", "high"]


def _load_from_db() -> pd.DataFrame | None:
    """Return rule_flag/risk_band frame from the warehouse, or None on failure."""
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
        return pd.read_sql(SQL, engine)
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] database unavailable ({exc.__class__.__name__}); using fallback values")
        return None


def _fallback_matrix() -> pd.DataFrame:
    """Representative counts cited in Chapter 5 (1,723 device-months)."""
    return pd.DataFrame(
        [[1380, 142, 31],
         [  14,  47, 109]],
        index=RULE_LABELS, columns=LABELS,
    )


def main() -> None:
    df = _load_from_db()
    if df is not None:
        matrix = pd.crosstab(df["rule_flag"], df["risk_band"]).reindex(
            index=RULE_LABELS, columns=LABELS, fill_value=0)
    else:
        matrix = _fallback_matrix()

    fig, ax = plt.subplots(figsize=(6.5, 4.4))
    data = matrix.to_numpy()
    im = ax.imshow(data, cmap="Blues", aspect="auto")
    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("count")

    ax.set_xticks(np.arange(len(matrix.columns)))
    ax.set_xticklabels(matrix.columns)
    ax.set_yticks(np.arange(len(matrix.index)))
    ax.set_yticklabels(matrix.index)

    threshold = data.max() / 2.0 if data.max() > 0 else 0
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            ax.text(j, i, f"{int(data[i, j])}",
                    ha="center", va="center",
                    color="white" if data[i, j] > threshold else "black",
                    fontsize=11)

    ax.set_xticks(np.arange(data.shape[1] + 1) - 0.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0] + 1) - 0.5, minor=True)
    ax.grid(which="minor", color="white", linewidth=1.2)
    ax.tick_params(which="minor", bottom=False, left=False)

    ax.set_xlabel("Isolation Forest band")
    ax.set_ylabel("Rule baseline flag")
    ax.set_title("Confusion matrix: rule vs Isolation Forest", color=ACCENT)
    fig.tight_layout()
    fig.savefig(OUT, bbox_inches="tight", dpi=300)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
