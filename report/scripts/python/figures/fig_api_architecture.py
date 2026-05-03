"""
fig_api_architecture.py
========================
Render the architecture of the API and dashboard layer behind the Nginx
reverse proxy, with TLS termination and a connection to the marts.
"""

from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT = Path(__file__).resolve().parents[3] / "figures" / "dashboard_architecture.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)
ACCENT = "#1F4E79"; LIGHT = "#D9E1F2"; GREY = "#595959"

fig, ax = plt.subplots(figsize=(13, 5.6))
ax.set_xlim(0, 13); ax.set_ylim(0, 5.6); ax.axis("off")


def block(x, y, w, h, label, sub=""):
    ax.add_patch(FancyBboxPatch((x, y), w, h,
        boxstyle="round,pad=0.05,rounding_size=0.12",
        facecolor=LIGHT, edgecolor=ACCENT, linewidth=1.4))
    ax.text(x + w / 2, y + h - 0.32, label,
            ha="center", va="top", fontsize=10, color=ACCENT, weight="bold")
    if sub:
        ax.text(x + w / 2, y + h - 0.62, sub,
                ha="center", va="top", fontsize=8, color=GREY)


def arrow(x1, y1, x2, y2, lbl=""):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2),
        arrowstyle="->", mutation_scale=14, color=ACCENT, linewidth=1.3))
    if lbl:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.18, lbl,
                fontsize=9, color=GREY, ha="center")


# Clients
block(0.4, 4.1, 3.0, 1.0, "Web browser", "fleet manager")
block(0.4, 2.4, 3.0, 1.0, "Consumer apps", "external integrations")

# Edge: Nginx with TLS
block(4.4, 3.2, 3.4, 1.2, "Nginx reverse proxy", "TLS, Let's Encrypt cert.")

# Backend
block(8.8, 4.1, 3.8, 1.0, "Streamlit dashboard", "systemd unit")
block(8.8, 2.4, 3.8, 1.0, "FastAPI / Uvicorn", "systemd unit")

# Storage
block(4.4, 0.6, 8.2, 1.0, "PostgreSQL marts schema (read-only role)",
      "v_executive_dashboard, v_device_risk_profile, ...")

arrow(3.4, 4.6, 4.4, 3.9, "443/TCP")
arrow(3.4, 2.9, 4.4, 3.7, "443/TCP")
arrow(7.8, 4.0, 8.8, 4.6, "127.0.0.1")
arrow(7.8, 3.5, 8.8, 2.9, "127.0.0.1")
arrow(10.7, 4.1, 8.5, 1.6, "SQL")
arrow(10.7, 2.4, 8.5, 1.6, "SQL")

plt.tight_layout()
plt.savefig(OUT, bbox_inches="tight", dpi=300)
print(f"wrote {OUT}")
