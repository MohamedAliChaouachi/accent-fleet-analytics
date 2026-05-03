"""
fig_azure_topology.py
======================
Render the Microsoft Azure topology of the platform: developer workstation
on the left, encrypted SSH tunnel crossing the Network Security Group with
an IP allowlist, and the Azure Virtual Machine on the right hosting
PostgreSQL, the Prefect flow, the FastAPI service, the Streamlit dashboard
and the Nginx reverse proxy.
"""

from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle

OUT = Path(__file__).resolve().parents[3] / "figures" / "azure_topology.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)
ACCENT = "#1F4E79"
LIGHT = "#D9E1F2"
GREY = "#595959"
NSG = "#C00000"
TUNNEL = "#2E75B6"
AZURE = "#E8F1FB"

fig, ax = plt.subplots(figsize=(14, 7))
ax.set_xlim(0, 14)
ax.set_ylim(0, 7)
ax.axis("off")


def block(x, y, w, h, label, sub="", face=LIGHT, edge=ACCENT):
    ax.add_patch(FancyBboxPatch((x, y), w, h,
        boxstyle="round,pad=0.05,rounding_size=0.10",
        facecolor=face, edgecolor=edge, linewidth=1.4))
    ax.text(x + w / 2, y + h - 0.30, label,
            ha="center", va="top", fontsize=10, color=ACCENT, weight="bold")
    if sub:
        ax.text(x + w / 2, y + h - 0.62, sub,
                ha="center", va="top", fontsize=8, color=GREY)


def arrow(x1, y1, x2, y2, lbl="", color=ACCENT, style="->"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2),
        arrowstyle=style, mutation_scale=14, color=color, linewidth=1.4))
    if lbl:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.20, lbl,
                fontsize=9, color=GREY, ha="center")


# Azure cloud boundary
ax.add_patch(Rectangle((6.0, 0.4), 7.7, 6.3,
    facecolor=AZURE, edgecolor=ACCENT, linewidth=1.2, linestyle="--"))
ax.text(6.15, 6.5, "Microsoft Azure (region: France Central)",
        fontsize=10, color=ACCENT, weight="bold")

# Network Security Group boundary
ax.add_patch(Rectangle((6.5, 0.7), 7.0, 5.4,
    facecolor="white", edgecolor=NSG, linewidth=1.4, linestyle=":"))
ax.text(6.65, 5.95, "Network Security Group (IP allowlist, ports 22 / 443)",
        fontsize=9, color=NSG, weight="bold")

# Developer workstation
block(0.3, 4.4, 2.6, 1.4, "Developer\nworkstation",
      "psql, VS Code, ssh client", face="#FFF8E1", edge=ACCENT)
block(0.3, 2.4, 2.6, 1.2, "Local port 5432", "forwarded loopback")

# SSH tunnel arrows crossing the NSG
arrow(2.9, 5.1, 6.5, 4.7, lbl="SSH 22/TCP", color=TUNNEL)
arrow(2.9, 3.0, 6.5, 3.5, lbl="tunnel -> 5432", color=TUNNEL)

# Public IP / load balancer
block(6.7, 4.5, 1.7, 1.0, "Public IP", "static, allowlisted")

# Azure VM
block(8.7, 0.9, 4.6, 5.0, "Azure VM (Standard D4s v5)",
      "Ubuntu 22.04 LTS, 4 vCPU, 16 GiB", face="white", edge=ACCENT)

# Services on the VM
block(8.95, 4.4, 4.1, 0.9, "Nginx reverse proxy",
      "TLS 443, Let's Encrypt cert.")
block(8.95, 3.3, 1.95, 0.9, "FastAPI / Uvicorn", "systemd unit")
block(11.05, 3.3, 1.95, 0.9, "Streamlit dashboard", "systemd unit")
block(8.95, 2.2, 4.1, 0.9, "Prefect flow + cron",
      "/etc/cron.d/accent-fleet, 5 min")
block(8.95, 1.1, 4.1, 0.9, "PostgreSQL 16",
      "localhost:5432, marts read-only role")

# Internal arrows on the VM
arrow(10.0, 3.3, 10.0, 2.1, color=ACCENT)
arrow(12.0, 3.3, 12.0, 2.1, color=ACCENT)
arrow(11.0, 2.2, 11.0, 2.0, color=ACCENT)
arrow(11.0, 1.1, 11.0, 0.95, color=ACCENT, style="<-")

# Tunnel endpoint -> PostgreSQL
arrow(8.4, 4.95, 8.95, 4.85, color=TUNNEL)
arrow(8.4, 3.5, 8.95, 1.55, lbl="loopback 5432", color=TUNNEL)

# Legend
ax.text(0.3, 0.85,
        "Solid arrows: HTTPS / internal traffic   |   "
        "Blue arrows: SSH tunnel forwarding 5432 -> 5432   |   "
        "Red dotted: NSG boundary",
        fontsize=8, color=GREY)

plt.tight_layout()
plt.savefig(OUT, bbox_inches="tight", dpi=300)
print(f"wrote {OUT}")
