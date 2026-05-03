"""
fig_schema_map.py
==================
Render the simplified schema map of the source tables of interest with
their natural keys and main relationships.
"""

from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT = Path(__file__).resolve().parents[3] / "figures" / "schema_map.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

ACCENT = "#1F4E79"; LIGHT = "#D9E1F2"; GREY = "#595959"

fig, ax = plt.subplots(figsize=(13, 7))
ax.set_xlim(0, 13); ax.set_ylim(0, 7); ax.axis("off")

def entity(x, y, name, cols):
    h = 0.5 + 0.36 * len(cols)
    ax.add_patch(FancyBboxPatch((x, y - h), 3.2, h,
        boxstyle="round,pad=0.05,rounding_size=0.1",
        facecolor=LIGHT, edgecolor=ACCENT, linewidth=1.3))
    ax.text(x + 1.6, y - 0.25, name, ha="center", va="top",
            fontsize=10, color=ACCENT, weight="bold")
    for i, c in enumerate(cols):
        ax.text(x + 0.15, y - 0.55 - i * 0.32, c,
                ha="left", va="top", fontsize=8, color=GREY, family="monospace")

entity(0.4, 6.8, "staging.path", [
    "tenant_id  PK*", "device_id  PK*", "begin_path_time PK*",
    "end_path_time", "distance_driven", "max_speed",
    "path_duration", "fuel_used"])
entity(4.6, 6.8, "staging.rep_overspeed", [
    "tenant_id PK*", "device_id PK*", "begin_path_time PK*",
    "severity"])
entity(8.8, 6.8, "staging.stop", [
    "tenant_id PK*", "device_id PK*", "stop_start PK*",
    "stop_duration"])

entity(0.4, 3.5, "staging.notification", [
    "tenant_id PK*", "notif_id PK*",
    "category", "created_at"])
entity(4.6, 3.5, "staging.rep_activity_daily", [
    "tenant_id PK*", "device_id PK*", "day PK*",
    "trips", "working_hours"])
entity(8.8, 3.5, "staging.archive", [
    "tenant_id PK*", "device_id PK*", "ts PK*",
    "speed", "rpm", "ignition", "accel_x/y/z"])

entity(2.5, 1.4, "staging.vehicule", [
    "vehicule_id PK", "tenant_id", "make", "class"])
entity(7.5, 1.4, "staging.driver", [
    "driver_id PK", "tenant_id", "full_name"])

def link(x1, y1, x2, y2, lbl=""):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2),
        arrowstyle="->", mutation_scale=12, color=ACCENT, linewidth=1.0))
    if lbl:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2, lbl,
                fontsize=8, color=GREY, ha="center")

link(2.0, 4.4, 2.0, 5.5, "device_id")
link(6.2, 4.4, 6.2, 5.5)
link(10.4, 4.4, 10.4, 5.5)
link(4.1, 1.9, 2.0, 4.6, "vehicule_id")
link(9.1, 1.9, 6.2, 4.6, "driver_id")

plt.tight_layout()
plt.savefig(OUT, bbox_inches="tight", dpi=300)
print(f"wrote {OUT}")
