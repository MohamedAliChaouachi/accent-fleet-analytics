# Risk Score Model — A Full Worked Example (Start to Finish)

> This walks through the **Device Risk Score** model (Isolation Forest) with
> a tiny, concrete dataset, from loading data all the way to the final API
> output. Every number here is **real** — produced by running the actual
> scikit-learn code with the same settings the project uses
> (`n_estimators=200, contamination="auto", random_state=42`).
>
> Read it top to bottom. Each step is plain English first, then the numbers.

---

## The goal (in one sentence)

> For each fleet, learn what "normal driving" looks like, then give every
> device a **0–100 risk score** measuring how *unusual* it is compared to the
> rest of its own fleet — and label it **low / moderate / high / critical**.

We have **no "this driver crashed" labels**, so we can't train a normal
classifier. Instead we detect **anomalies**: devices that behave very
differently from their fleet-mates.

---

## The data we're working with

Each row is a **device-month**: one device, one month, summarized by 13
numbers ("features"). To keep this readable we'll show just **4 of the 13**
features and **10 device-months** for **one fleet (Fleet 235)**.

| device-month | overspeed_per_100km | harsh_brake_per_100km | monthly_idle_ratio | high_rpm_min_per_day |
|---|---|---|---|---|
| A | 2  | 1  | 0.10 | 5   |
| B | 3  | 0  | 0.12 | 8   |
| C | 1  | 2  | 0.08 | 6   |
| D | 4  | 1  | 0.15 | 10  |
| E | 2  | 1  | 0.11 | 7   |
| F | 3  | 2  | 0.09 | 9   |
| G | 5  | 3  | 0.18 | 12  |
| H | 2  | 0  | 0.10 | 6   |
| I | 4  | 2  | 0.14 | 11  |
| **J** | **38** | **22** | **0.61** | **140** |

Look at the numbers: **A–I are all small and similar** (calm, normal
driving). **J is enormous** — ~10–20× everyone else. J is our obvious "bad
driver". The model's job is to discover that *on its own*, without being
told.

---

## STEP 1 — Load data

**Plain English:** Pull all the active device-months for the fleet out of the
database. "Active" means the device actually drove enough to be worth scoring.

**In the code:** `load_training_frame()`
([train_risk.py:236](../src/accent_fleet/ml/train_risk.py:236)) runs SQL that
keeps only rows with `total_distance_km >= 100` and
`total_ignition_on_minutes > 0`. A device that barely moved is dropped — it
has no behaviour to judge.

**Result:** the 10-row table above (in reality, hundreds of rows per fleet).

---

## STEP 2 — Split by fleet

**Plain English:** Handle each fleet separately. A delivery-van fleet and a
school-bus fleet have totally different "normal", so each gets its own model.

**In the code:** `df.groupby("tenant_id")`
([train_risk.py:442](../src/accent_fleet/ml/train_risk.py:442)). Everything
below happens **once per fleet**. We're following Fleet 235.

---

## STEP 3 — Scale the features

**Plain English (the important bit):** The features are on wildly different
scales. `high_rpm_minutes` runs into the hundreds; `monthly_idle_ratio` is a
tiny fraction like 0.10. If we left them as-is, the big numbers would
completely drown out the small ones. **Scaling** rewrites every value as
*"how many standard deviations away from this fleet's average"* so all 13
features count equally.

The formula for each feature:

```
scaled_value = (value − fleet_mean) / fleet_standard_deviation
```

**Real numbers** — for Fleet 235 the scaler learned:

| feature | fleet mean | fleet std |
|---|---|---|
| overspeed_per_100km | 6.40 | 10.59 |
| harsh_brake_per_100km | 3.40 | 6.26 |
| monthly_idle_ratio | 0.168 | 0.150 |
| high_rpm_min_per_day | 21.40 | 39.59 |

So device **A** (overspeed = 2) becomes `(2 − 6.40) / 10.59 = −0.415`
→ "a bit below average". Device **J** (overspeed = 38) becomes
`(38 − 6.40) / 10.59 = +2.98` → "almost 3 standard deviations above average".

| device | scaled values (4 features) |
|---|---|
| A | `[−0.415, −0.383, −0.453, −0.414]` (all slightly below average) |
| J | `[+2.983, +2.969, +2.944, +2.995]` (far above average on everything) |

**In the code:** `StandardScaler().fit(X)` then `.transform(X)`
([train_risk.py:358-359](../src/accent_fleet/ml/train_risk.py:358)).

---

## STEP 4 — Fit one Isolation Forest

**Plain English:** An Isolation Forest plays a game of "20 questions" with
random cuts. It repeatedly picks a random feature and a random split point,
slicing the data into smaller and smaller groups until every device is alone.

- A **weird** device (like J) sits far away from the crowd, so just a few
  random cuts separate it → it gets "isolated" quickly.
- A **normal** device sits in the dense middle, so it takes many cuts to
  isolate.

It builds **200** such random trees and averages the result. **Fewer cuts to
isolate = more anomalous.**

**The output number — `decision_function`:** sklearn gives each device a
score where **higher = more normal, negative = anomalous** (the cutoff is 0).
We flip the sign (`raw = −decision_function`) so that **higher = more
risky**, which is more intuitive.

**Real numbers** (`raw` = our flipped anomaly score):

| device | raw score | normal or anomaly? |
|---|---|---|
| E | −0.1113 | most normal |
| A | −0.0945 | normal |
| H | −0.0837 | normal |
| F | −0.0799 | normal |
| B | −0.0713 | normal |
| I | −0.0698 | normal |
| D | −0.0626 | normal |
| C | −0.0243 | normal |
| G | +0.0404 | mild outlier |
| **J** | **+0.3070** | **strong outlier** |

The forest figured out that **J is the odd one** — its raw score (+0.3070) is
far above everyone else's. We never told it; it discovered it.

**In the code:** `IsolationForest(...).fit(Xs)` and
`raw = -iso.decision_function(Xs)`
([train_risk.py:361-368](../src/accent_fleet/ml/train_risk.py:361)).

---

## STEP 5 — Turn raw scores into 0–100 + bands

The raw scores (−0.11 to +0.31) are ugly and unbounded. Two clean-ups:

### 5a — Rescale to 0–100

**Plain English:** Stretch the scores so the *least* anomalous device = 0 and
the *most* anomalous = 100.

```
score = (raw − raw_min) / (raw_max − raw_min) × 100
      = (raw − (−0.1113)) / (0.3070 − (−0.1113)) × 100
```

| device | raw | → 0–100 score |
|---|---|---|
| E | −0.1113 | **0.0** |
| A | −0.0945 | 4.0 |
| H | −0.0837 | 6.6 |
| F | −0.0799 | 7.5 |
| B | −0.0713 | 9.6 |
| I | −0.0698 | 9.9 |
| D | −0.0626 | 11.6 |
| C | −0.0243 | 20.8 |
| G | +0.0404 | 36.3 |
| **J** | +0.3070 | **100.0** |

(We **save** `raw_min = −0.1113` and `raw_max = +0.3070` — we'll reuse them to
score future devices on the same scale.)

### 5b — Set the category bands

**Plain English:** We cut the fleet's scores at the **50th, 80th, and 95th
percentiles** of *this fleet's own scores*. So "critical" always means "top
5% within your fleet", never an absolute number.

**Real thresholds** for Fleet 235:

| band | rule | range |
|---|---|---|
| **low** | below p50 | score < 9.7 |
| **moderate** | p50–p80 | 9.7 ≤ score < 23.9 |
| **high** | p80–p95 | 23.9 ≤ score < 71.3 |
| **critical** | top 5% | score ≥ 71.3 |

Applying them:

| device | score | **category** |
|---|---|---|
| E, A, H, F | 0–7.5 | **low** |
| B, I, D | 9.6–11.6 | **moderate** |
| C | 20.8 | **moderate** |
| G | 36.3 | **high** |
| **J** | 100.0 | **critical** |

**In the code:** `rescale_raw_to_0_100`
([train_risk.py:266](../src/accent_fleet/ml/train_risk.py:266)),
`quantile_thresholds`
([train_risk.py:286](../src/accent_fleet/ml/train_risk.py:286)),
`categorize_scores`
([train_risk.py:301](../src/accent_fleet/ml/train_risk.py:301)).

---

## STEP 6 — Bundle and save

**Plain English:** Pack everything this fleet needs to score future devices
into one little package, and store every fleet's package together in **one
file** so they can never get mismatched.

Fleet 235's package looks like:

```python
{
  "scaler":  <the StandardScaler with the means/stds from Step 3>,
  "model":   <the fitted Isolation Forest from Step 4>,
  "raw_min": -0.1113,            # from Step 5a
  "raw_max":  0.3070,
  "thresholds": {"moderate": 9.7, "high": 23.9, "critical": 71.3},  # Step 5b
  "feature_means": [6.40, 3.40, 0.168, 21.40, ...],   # for explaining "why"
  "feature_stds":  [10.59, 6.26, 0.15, 39.59, ...],
  "score_share": {"low": 0.4, "moderate": 0.4, "high": 0.1, "critical": 0.1},
}
```

All five fleets (235, 238, 264, 1787, 7486) each get one of these, bundled
into a single file `models/risk_score/risk_v1.joblib`, and registered in
MLflow as the **Production** version.

**In the code:** the package is built in `fit_one_tenant`
([train_risk.py:380](../src/accent_fleet/ml/train_risk.py:380)), bundled in
`fit_risk_model`
([train_risk.py:456](../src/accent_fleet/ml/train_risk.py:456)), saved by
`save_local`
([train_risk.py:491](../src/accent_fleet/ml/train_risk.py:491)) and published
by `log_to_mlflow`
([train_risk.py:532](../src/accent_fleet/ml/train_risk.py:532)).

**Training is now done.** The model is trained and saved. Everything below is
*using* it.

---

## USING THE MODEL — scoring a brand-new device-month

A new month of data arrives for a device in Fleet 235:

```
overspeed_per_100km = 30,  harsh_brake_per_100km = 15,
monthly_idle_ratio  = 0.50, high_rpm_min_per_day  = 110
```

This driver is bad — but not *quite* as extreme as J. Let's see what the model
says.

**1. Load Fleet 235's package** from the bundle. (If the device's fleet had no
model, the API returns HTTP 503 — it never scores against another fleet's
model.)

**2. Scale** using 235's saved means/stds:
- overspeed: `(30 − 6.40) / 10.59 = +2.23` → 2.2 std above average
- harsh_brake: `(15 − 3.40) / 6.26 = +1.85`
- idle: `(0.50 − 0.168) / 0.15 = +2.21`
- high_rpm: `(110 − 21.40) / 39.59 = +2.24`

**3. Run the forest:** `raw = −decision_function = +0.2175`.

**4. Rescale with 235's saved bounds:**
`(0.2175 − (−0.1113)) / 0.4183 × 100 = 78.6`.

**5. Band it:** `78.6 ≥ 71.3 (critical threshold)` → **critical**.

**6. Explain it** — the "components" are just the scaled values from step 2
(z-scores), so the dashboard can say *why*:

---

## THE FINAL OUTPUT

What the API (`POST /v1/score/risk`) returns:

```json
{
  "risk_score": 78.6,
  "category": "critical",
  "components": {
    "overspeed_per_100km":     2.23,
    "harsh_brake_per_100km":   1.85,
    "monthly_idle_ratio":      2.21,
    "high_rpm_minutes_per_day":2.24
  },
  "model_version": "3"
}
```

**How a fleet manager reads this:**

> This device scored **78.6 / 100 → CRITICAL** for its fleet. The biggest
> reasons: it's **+2.24σ** on high-RPM minutes, **+2.23σ** on overspeeding,
> and **+2.21σ** on idling — i.e. it's an outlier on engine stress, speeding,
> and idle time all at once.

The `components` answer the manager's natural next question — *"why is it
critical?"* — because the Isolation Forest itself gives no per-feature
weights.

---

## The one big idea to remember

The risk score is **relative to the device's own fleet**, not an absolute
danger rating. The exact same driving (30 overspeeds, etc.) could come out as
"high" instead of "critical" in a more aggressive fleet — because the 0–100
scale (`raw_min`/`raw_max`) and the bands (50/80/95 percentiles) are
calibrated separately for each fleet. That's the whole reason we **split by
fleet in Step 2**.

---

## The 6 steps + code, at a glance

| Step | What happens | Function |
|---|---|---|
| 1. Load | pull active device-months | `load_training_frame` |
| 2. Split | one model per fleet | `fit_risk_model` (groupby) |
| 3. Scale | values → standard deviations | `StandardScaler` in `fit_one_tenant` |
| 4. Fit | Isolation Forest → raw anomaly score | `IsolationForest` + `-decision_function` |
| 5. Bands | rescale 0–100, cut at p50/p80/p95 | `rescale_raw_to_0_100`, `quantile_thresholds`, `categorize_scores` |
| 6. Bundle | pack all fleets into one file, publish | `save_local`, `log_to_mlflow` |

All in [`src/accent_fleet/ml/train_risk.py`](../src/accent_fleet/ml/train_risk.py).
