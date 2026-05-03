"""
fig_deployment_pipeline.py
===========================
Render the end-to-end deployment pipeline of the platform: code repo,
CI step, deployment to the Azure VM, cron-driven incremental run,
post-run validation and alerting.
"""

from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT = Path(__file__).resolve().parents[3] / "figures" / "deployment_pipeline.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)
ACCENT = "#1F4E79"; LIGHT = "#D9E1F2"; GREY = "#595959"

STEPS = [
    ("Git\nrepository",      "GitHub main branch"),
    ("CI checks",             "ruff, pytest"),
    ("Deploy to\nAzure VM",  "git pull, .venv install"),
    ("Cron\n5-min trigger",  "/etc/cron.d/accent-fleet"),
    ("Prefect flow",          "extract -> clean -> facts -> marts"),
    ("Validation\nsuite",    "V1..V8"),
    ("Monitoring\n& alerts", "etl_run_log, drift, Slack"),
]

fig, ax = plt.subplots(figsize=(14, 3.2))
ax.set_xlim(0, 14); ax.set_ylim(0, 3.2); ax.axis("off")
w, h, gap = 1.7, 1.5, 0.18; y0 = 0.8

for i, (label, sub) in enumerate(STEPS):
    x = 0.2 + i * (w + gap)
    ax.add_patch(FancyBboxPatch((x, y0), w, h,
        boxstyle="round,pad=0.05,rounding_size=0.12",
        facecolor=LIGHT, edgecolor=ACCENT, linewidth=1.4))
    ax.text(x + w / 2, y0 + h - 0.30, label,
            ha="center", va="top", fontsize=10, color=ACCENT, weight="bold")
    ax.text(x + w / 2, y0 + h - 0.95, sub,
            ha="center", va="top", fontsize=8, color=GREY)
    if i < len(STEPS) - 1:
        ax.add_patch(FancyArrowPatch(
            (x + w, y0 + h / 2), (x + w + gap, y0 + h / 2),
            arrowstyle="->", mutation_scale=15, color=ACCENT, linewidth=1.2))

plt.tight_layout()
plt.savefig(OUT, bbox_inches="tight", dpi=300)
print(f"wrote {OUT}")
