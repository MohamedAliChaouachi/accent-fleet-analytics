# How We Evaluate the Two Models (Plain English)

This file explains **how we check that our two models are any good** — in
simple language, with the real numbers from this project.

> The hard part: both models are **unsupervised**. We have **no labels**.
> Nobody handed us a list saying "device X crashed" or "device Y is risky."
> So we **cannot** just compute accuracy like a normal exam score.
> Evaluation here means: *"Do the outputs make sense, stay stable, and
> separate good behaviour from bad behaviour?"* — proven with numbers,
> not opinions.

The two models:

| Model | What it outputs | The evaluation question |
| --- | --- | --- |
| **K-Means** (clustering) | a `cluster_id` per device-month | Are the groups actually *separated*, or just random blobs? |
| **Isolation Forest** (risk) | a `risk_score 0–100` + band | Do high-risk device-months really behave worse than low-risk ones? |

---

## Part 1 — Evaluating K-Means (the clusters)

### The intuition

A good clustering means: **points in the same group are close together,
and far from the other groups.** A bad clustering means the groups overlap
and you can't tell them apart.

We measure this with three numbers. The main one is the **silhouette score**.

### 1.1 Silhouette score — the headline metric

For **one** device-month point, silhouette asks two questions:

- **a** = how far am I, on average, from the *other* points **in my own cluster**? (want this **small** — my cluster is tight)
- **b** = how far am I from the points in the **nearest other cluster**? (want this **big** — other clusters are far away)

Then:

```
silhouette = (b - a) / max(a, b)
```

Read the result like this:

| Value | Meaning |
| --- | --- |
| close to **+1** | point sits snugly in its own cluster, far from others → great |
| around **0** | point is on the border between two clusters → meh |
| **negative** | point is closer to another cluster than its own → probably mis-assigned |

The **silhouette score** for the whole model is just the **average** over
all points. Higher = cleaner separation.

#### Tiny worked example (3 points)

Say after scaling we have points on a line: A=1.0, B=1.2 (cluster 0) and
C=5.0 (cluster 1). Take point A:

- a = distance to others in cluster 0 = |1.0 − 1.2| = **0.2** (tight)
- b = distance to nearest other cluster = |1.0 − 5.0| = **4.0** (far)
- silhouette(A) = (4.0 − 0.2) / max(0.2, 4.0) = 3.8 / 4.0 = **0.95** → excellent

If instead C were at 1.3 (almost on top of A and B), b would be ~0.3, and
silhouette(A) ≈ (0.3 − 0.2)/0.3 ≈ **0.33** → the clusters are mushy.

### 1.2 How silhouette is used to PICK K (the number of clusters)

This is the key role in our code. We don't guess K. In
[`train_clustering.py`](../src/accent_fleet/ml/train_clustering.py) we **try
every K in {3, 4, 5, 6}**, compute the silhouette for each, and **keep the K
with the highest silhouette.** That's the `fit_clustering()` loop.

So silhouette is doing **two jobs**: it selects K *and* it is our quality
score for the winner.

### 1.3 The two backup metrics

Silhouette can be fooled, so we report two sanity checks alongside it:

- **Davies-Bouldin** — average "how similar is each cluster to its most
  similar neighbour." **Lower is better** (0 = perfectly distinct).
- **Calinski-Harabasz** — ratio of between-cluster spread to within-cluster
  spread. **Higher is better.**

If all three agree, we trust the result.

### 1.4 The real numbers in this project

From [`notebooks/05_evaluation/01_cluster_quality.ipynb`](../notebooks/05_evaluation/README.md),
run live against the database:

| tenant | n points | chosen K | silhouette | Davies-Bouldin | Calinski-Harabasz |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 235  | 613 | 6 | 0.225 | 1.271 | 110.5 |
| 238  | 339 | 6 | 0.236 | 1.165 |  72.2 |
| 264  | 354 | 6 | 0.174 | 1.627 |  52.9 |
| 1787 | 417 | 5 | 0.269 | 1.313 |  82.5 |
| 7486 | 434 | 5 | 0.253 | 1.374 |  61.9 |

**How to read this:** silhouettes of 0.17–0.27 are **modest but real**
separation — normal for messy real-world fleet behaviour data (you don't get
0.9 like the toy example). Every tenant clears our **gate of ≥ 0.15**, so the
clusters are meaningful, not random.

### 1.5 One more check: cluster-size balance

A clustering can have a great silhouette but be useless if **one cluster
swallows 95% of the points**. So we also check the **largest cluster's
share** against a **70% dominance threshold**.

Real result: the biggest single cluster is tenant 7486's at **65.4%** — under
70%, so it passes. (When a small cluster holds a few % of devices, we label it
as an "exceptional persona," not a broad fleet segment.)

---

## Part 2 — Evaluating Isolation Forest (the risk score)

This is harder. There's no "true risk" column to compare against. We use
**three** angles.

### 2.1 Internal-consistency check (THE main one)

The idea is brilliant in its simplicity:

> If the score works, then the device-months it labels **high risk** should,
> when we go back and look, have **objectively worse driving features** than
> the ones it labels **low risk**.

So we split each tenant's device-months into the **high band** and the
**low band**, then compute, for each unsafe feature:

```
ratio = (average of that feature in HIGH band) / (average in LOW band)
```

A ratio **> 1** means high-risk months really do have more of the bad stuff.
The bigger the ratio, the sharper the separation.

**The gate:** at least **5 of 7** unsafe features must have ratio > 1, and at
least **2** must have ratio ≥ 2.

#### The real numbers

| tenant | features w/ ratio > 1 (of 7) | features w/ ratio ≥ 2 | approx. mean ratio |
| ---: | ---: | ---: | ---: |
| 235  | 6 | 5 |  18.79 |
| 238  | 6 | 4 | 143.69 |
| 264  | 5 | 4 |  19.93 |
| 1787 | 6 | 5 |   9.02 |
| 7486 | 7 | 6 |   6.84 |

**Every tenant passes.** Concrete example — tenant **7486** high-band
device-months versus low-band, same fleet:

- **4.5×** more overspeed events
- **5.4×** more harsh brakes
- **5.5×** more harsh accelerations
- **10.5×** more harsh corners
- **18.5×** more high-RPM minutes

That is the score doing its job: it found the genuinely aggressive
device-months **without ever being told which ones they were.**

### 2.2 External validation against maintenance (honest limitation)

The *ideal* test: do high-risk device-months actually lead to **maintenance
events / breakdowns** later? We check this against
`warehouse.fact_maintenance`.

**Problem:** maintenance ground-truth data only really exists for **one
tenant (7486, 136 events).** So we can only treat this as **directional
evidence for that one tenant**, not a fleet-wide proof.

We are upfront about this — it's a **data coverage limitation, not a model
failure.** The dashboard carries a mandatory footer saying outcome
backtesting is currently tenant-limited.

### 2.3 Stability — does the score flip around month to month?

A risk score nobody can trust if a device is "critical" one month, "low" the
next, "critical" again — with no real change in behaviour. So we measure
**band churn**: the fraction of device-pairs (same device, consecutive
months) where the risk band **changed**.

Real numbers (band churn, **lower = more stable**, gate is < 50%):

| tenant | band churn |
| ---: | ---: |
| 235  | 0.116 |
| 238  | 0.108 |
| 264  | 0.153 |
| 1787 | 0.107 |

~11–15% churn — well under the gate. And the **transition matrix** shows the
bands are "sticky":

|  from \ to | low | medium | high |
| --- | ---: | ---: | ---: |
| **low**    | 0.935 | 0.063 | 0.002 |
| **medium** | 0.292 | 0.639 | 0.069 |
| **high**   | 0.050 | 0.350 | 0.600 |

Read the diagonal: a **low** device stays low **94%** of the time; a **high**
device stays high **60%** of the time. Risk is a property of the device, not
random noise — exactly what we want.

---

## Part 3 — Drift monitoring (is the model going stale?)

Even a good model rots if the **world changes** (new vehicles, new routes,
seasonal driving). We watch for this with **PSI (Population Stability
Index)** in [`drift.py`](../src/accent_fleet/ml/drift.py).

### The intuition

PSI compares **two histograms of the same thing**: the feature distribution
**now** vs. a **recent baseline** (the prior 6 months). If the shape barely
moved, PSI ≈ 0. If it shifted a lot, PSI is large.

```
PSI = Σ over bins  (now% − before%) × ln(now% / before%)
```

The industry-standard thresholds we use:

| PSI | Meaning | Action |
| --- | --- | --- |
| **< 0.10** | stable | nothing |
| **0.10 – 0.25** | moderate shift | watch it |
| **≥ 0.25** | real drift | **alert / consider retrain** |

We run this on **each of the 13 features** *and* on the **risk-score
distribution itself**. Drift is **informational only** — it never crashes the
pipeline; it raises a flag so a human decides whether to retrain.

---

## Part 4 — Promotion gates (the automated go/no-go)

When we retrain, the new model **does not automatically replace** the old one.
It must **beat a gate**, in [`promotion.py`](../src/accent_fleet/ml/promotion.py):

- **Clustering gate** — the new model's silhouette must not drop by more than
  **0.02** vs. the current production model. (No silent quality regressions.)
- **Risk gate (stability)** — the new scoring must not shift the population
  too much:
  - change in **% critical** ≤ **5 percentage points**
  - change in **% high** ≤ **5 percentage points**
  - **risk-score PSI** < **0.25** (the new scores look like the old ones)

If the candidate fails, we **keep the old model** and log why. This is what
makes monthly retraining safe to run unattended.

---

## Part 5 — The verdict (go / no-go)

| Artifact | Verdict | Why |
| --- | --- | --- |
| **Cluster labels** (5 tenants) | **SHIP** | silhouette ≥ 0.15 + no >70% dominant cluster, all five tenants |
| **Risk score** | **SHIP provisionally** + dashboard footer | internal-consistency gate clears everywhere; external (maintenance) evidence is tenant-limited |
| **Risk band** | **SHIP** | sticky transitions (P[stay] ≥ 0.6 for low & high), churn < 16% |

### The honest summary in one paragraph

We can't prove these models with accuracy because we have no labels. But we
**can** show that (1) the clusters are genuinely separated (silhouette
0.17–0.27, all gates pass), (2) the high-risk device-months objectively drive
worse (up to **18×** more high-RPM minutes than low-risk ones), and (3) the
outputs are stable month to month (band churn ~11%, sticky transitions). The
one gap we state openly: outcome backtesting against real breakdowns is
limited to one tenant until a broader incident feed is ingested.

---

### Where each check lives in the code

| Check | File |
| --- | --- |
| silhouette K-selection | [`train_clustering.py`](../src/accent_fleet/ml/train_clustering.py) `fit_clustering()` |
| cluster quality / sizes | `notebooks/05_evaluation/01_cluster_quality.ipynb` |
| risk internal-consistency + maintenance | `notebooks/05_evaluation/02_risk_score_validation.ipynb` |
| stability / band churn | `notebooks/05_evaluation/03_stability_and_fairness.ipynb` |
| PSI drift (feature + score) | [`drift.py`](../src/accent_fleet/ml/drift.py) |
| promotion gates | [`promotion.py`](../src/accent_fleet/ml/promotion.py) |
