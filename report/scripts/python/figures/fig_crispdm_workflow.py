"""
fig_crispdm_workflow.py
========================
Render the six-phase CRISP-DM workflow as a clean horizontal flow diagram
with feedback arrows. Outputs a publication-quality PDF.
"""

from __future__ import annotations
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT = Path(__file__).resolve().parents[3] / "figures" / "crispdm_workflow.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

PHASES = [
    "Business\nUnderstanding",
    "Data\nUnderstanding",
    "Data\nPreparation",
    "Modeling",
    "Evaluation",
    "Deployment",
]
ACCENT = "#1F4E79"
LIGHT = "#D9E1F2"

fig, ax = plt.subplots(figsize=(13, 3.4))
ax.set_xlim(0, 13)
ax.set_ylim(0, 3.4)
ax.axis("off")

box_w, box_h, gap = 1.8, 1.2, 0.3
y0 = 1.4
for i, label in enumerate(PHASES):
    x = 0.4 + i * (box_w + gap)
    ax.add_patch(FancyBboxPatch(
        (x, y0), box_w, box_h,
        boxstyle="round,pad=0.05,rounding_size=0.12",
        facecolor=LIGHT, edgecolor=ACCENT, linewidth=1.6,
    ))
    ax.text(x + box_w / 2, y0 + box_h / 2, label,
            ha="center", va="center", fontsize=10, color=ACCENT, weight="bold")
    if i < len(PHASES) - 1:
        ax.add_patch(FancyArrowPatch(
            (x + box_w, y0 + box_h / 2), (x + box_w + gap, y0 + box_h / 2),
            arrowstyle="->", mutation_scale=18, color=ACCENT, linewidth=1.4,
        ))

# Feedback loops (Evaluation -> Business Understanding) and (Modeling -> Data Preparation)
ax.add_patch(FancyArrowPatch(
    (0.4 + 4 * (box_w + gap) + box_w / 2, y0),
    (0.4 + 0 * (box_w + gap) + box_w / 2, y0),
    connectionstyle="arc3,rad=-0.35", arrowstyle="->", mutation_scale=15,
    color=ACCENT, linewidth=1.0, linestyle="dashed",
))
ax.text(6.5, 0.15, "iterative refinement", ha="center", fontsize=9, color=ACCENT, style="italic")

plt.tight_layout()
plt.savefig(OUT, bbox_inches="tight", dpi=300)
print(f"wrote {OUT}")
