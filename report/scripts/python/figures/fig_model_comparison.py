"""
fig_model_comparison.py
========================
Render a comparative dashboard of the candidate models, mirroring the
qualitative comparison table of Chapter 5.
"""

from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt

OUT = Path(__file__).resolve().parents[3] / "figures" / "model_comparison.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)
ACCENT = "#1F4E79"

MODELS = [
    "Rule baseline", "PCA component", "K-Means", "Hierarchical",
    "DBSCAN", "Isolation Forest", "One-Class SVM", "LOF", "Autoencoder", "MLP (proxy)",
]
SCALABILITY      = [5, 5, 5, 4, 3, 5, 2, 3, 3, 4]
ROBUSTNESS       = [3, 4, 4, 5, 5, 5, 3, 3, 3, 3]
INTERPRETABILITY = [5, 4, 5, 5, 2, 4, 2, 2, 2, 3]
STABILITY        = [5, 5, 5, 4, 2, 5, 3, 3, 3, 3]

x = np.arange(len(MODELS)); w = 0.20
fig, ax = plt.subplots(figsize=(13, 5))
ax.bar(x - 1.5 * w, SCALABILITY,      width=w, color="#1F4E79", label="Scalability")
ax.bar(x - 0.5 * w, ROBUSTNESS,       width=w, color="#2E75B6", label="Robustness")
ax.bar(x + 0.5 * w, INTERPRETABILITY, width=w, color="#9DC3E6", label="Interpretability")
ax.bar(x + 1.5 * w, STABILITY,        width=w, color="#C00000", label="Stability")
ax.set_xticks(x); ax.set_xticklabels(MODELS, rotation=25, ha="right")
ax.set_ylim(0, 5.5); ax.set_yticks(range(0, 6))
ax.set_ylabel("qualitative score (0--5)")
ax.set_title("Comparative qualitative scoring of the candidate models", color=ACCENT)
ax.legend(loc="upper right", ncol=4)
fig.tight_layout()
fig.savefig(OUT, bbox_inches="tight", dpi=300)
print(f"wrote {OUT}")
