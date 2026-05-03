"""
fig_architecture.py
====================
Render the four-tier logical architecture of the platform: source tier,
processing tier, storage tier (bronze/silver/gold) and serving tier.
"""

from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT = Path(__file__).resolve().parents[3] / "figures" / "architecture_dataflow.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

ACCENT = "#1F4E79"
LIGHT = "#D9E1F2"
GREY = "#595959"

fig, ax = plt.subplots(figsize=(13, 6.2))
ax.set_xlim(0, 13); ax.set_ylim(0, 6.2); ax.axis("off")

def block(x, y, w, h, label, sub=""):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h,
        boxstyle="round,pad=0.05,rounding_size=0.12",
        facecolor=LIGHT, edgecolor=ACCENT, linewidth=1.4))
    ax.text(x + w / 2, y + h - 0.32, label,
            ha="center", va="top", fontsize=10, color=ACCENT, weight="bold")
    if sub:
        ax.text(x + w / 2, y + h - 0.62, sub,
                ha="center", va="top", fontsize=8, color=GREY)

def arrow(x1, y1, x2, y2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2),
        arrowstyle="->", mutation_scale=14, color=ACCENT, linewidth=1.3))

# Tier titles (left margin)
for y, t in [(5.4, "Source"), (4.0, "Processing"), (2.4, "Storage"), (0.6, "Serving")]:
    ax.text(0.2, y + 0.4, t, fontsize=10, color=GREY, weight="bold", rotation=90)

# Source
block(1.2, 5.0, 4.0, 0.9, "Operational PostgreSQL (Accent platform)", "path, archive, notification, ...")
block(7.5, 5.0, 4.0, 0.9, "GPS telematics devices (633)", "telemetry pings, alerts")

# Processing
block(2.5, 3.5, 8.0, 0.9, "Prefect flow (Python + SQL on Azure VM)", "extraction -> cleaning -> facts -> marts")

# Storage (3 layers)
block(1.2, 1.9, 3.4, 1.1, "staging (bronze)", "faithful source replica")
block(4.9, 1.9, 3.4, 1.1, "warehouse (silver)", "5 dims + 11 facts + bridge")
block(8.6, 1.9, 3.4, 1.1, "marts (gold)", "ML + BI marts and views")

# Serving
block(1.2, 0.3, 3.4, 0.9, "REST API (FastAPI)")
block(4.9, 0.3, 3.4, 0.9, "BI dashboard (Streamlit)")
block(8.6, 0.3, 3.4, 0.9, "Model artefact store")

# Arrows
arrow(3.2, 5.0, 6.5, 4.4)
arrow(9.5, 5.0, 6.5, 4.4)
arrow(6.5, 3.5, 2.9, 3.0)
arrow(6.5, 3.5, 6.6, 3.0)
arrow(6.5, 3.5, 10.3, 3.0)
arrow(2.9, 1.9, 2.9, 1.2)
arrow(6.6, 1.9, 6.6, 1.2)
arrow(10.3, 1.9, 10.3, 1.2)

plt.tight_layout()
plt.savefig(OUT, bbox_inches="tight", dpi=300)
print(f"wrote {OUT}")
