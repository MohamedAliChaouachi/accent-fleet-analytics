"""Generate the three evaluation notebooks (CRISP-DM Phase 5).

This generator exists so the notebooks are reproducible and the JSON is
guaranteed valid. Run once:

    python scripts/build_eval_notebooks.py
"""
from __future__ import annotations

import json
import pathlib
import textwrap

OUT_DIR = pathlib.Path(__file__).resolve().parents[1] / 'notebooks' / '05_evaluation'


def md(text: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)}


def code(text: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": text.splitlines(keepends=True),
    }


def nb(cells: list) -> dict:
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "pygments_lexer": "ipython3"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


# ---------------------------------------------------------------------------
# Shared bootstrap (sys.path + imports + DB pull). Re-fits the models so the
# evaluation is self-contained and does not depend on parquet artifacts.
# ---------------------------------------------------------------------------
BOOT = textwrap.dedent("""
    from __future__ import annotations
    import sys, pathlib
    PROJECT_ROOT = pathlib.Path().resolve().parents[1] if pathlib.Path().resolve().name != 'accent-fleet-analytics' else pathlib.Path().resolve()
    for c in (PROJECT_ROOT, PROJECT_ROOT.parent):
        src = c / 'src'
        if src.exists() and str(src) not in sys.path:
            sys.path.insert(0, str(src)); break

    import pandas as pd, numpy as np
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.ensemble import IsolationForest
    from sklearn.metrics import silhouette_score, davies_bouldin_score, calinski_harabasz_score
    import matplotlib.pyplot as plt
    from accent_fleet.db import get_engine
    from sqlalchemy import text

    FEATURES = [
        'overspeed_per_100km', 'avg_speed_over_limit', 'high_speed_trip_ratio',
        'speed_alert_per_100km',
        'harsh_brake_per_100km', 'harsh_accel_per_100km', 'harsh_corner_per_100km',
        'monthly_idle_ratio', 'high_rpm_minutes_per_day',
        'night_trip_ratio', 'rush_hour_trip_ratio',
        'stddev_trip_distance', 'short_trip_ratio',
    ]
    ID_COLS = ['tenant_id', 'device_id', 'year_month']

    with get_engine().connect() as conn:
        df = pd.read_sql(text('''
            SELECT * FROM marts.v_ml_features_full
            WHERE year_month >= '2025-01'
              AND total_distance_km >= 100
              AND total_ignition_on_minutes > 0
        '''), conn)
    print('feature rows:', len(df), '|', 'tenants:', sorted(df.tenant_id.unique().tolist()))
""").strip()


# ===========================================================================
# Notebook 01 — Cluster quality & persona profiles
# ===========================================================================
nb01 = nb([
    md("""# M10.1 · Cluster Quality & Persona Profiles (CRISP-DM Phase 5)

**What this notebook answers**

1. Are the clusters from `04_modeling/01_device_behavior_clustering.ipynb` *technically* sound — silhouette, Davies-Bouldin, Calinski-Harabasz?
2. Are they *interpretable*? Each cluster gets an auto-generated persona based on the per-feature z-score deviation from the tenant mean.
3. Are the cluster sizes balanced enough to be useful, or does one cluster swallow everything?

**Re-fits the model in-notebook** (deterministic, `random_state=42`) so this evaluation is reproducible without the parquet artifact.
"""),
    md("## 1. Bootstrap"),
    code(BOOT),
    md("""## 2. Re-fit per-tenant clustering (matches `04_modeling/01`)

Same logic as the modeling notebook: scale → KMeans(k=3..6) → silhouette-driven k. We additionally compute Davies-Bouldin (lower=better) and Calinski-Harabasz (higher=better) so we have three internal-validity views, not one."""),
    code(textwrap.dedent("""
        def fit_one_tenant(sub: pd.DataFrame, k_range=range(3, 7)):
            X = sub[FEATURES].fillna(0).to_numpy()
            if len(X) < 50:
                return None
            Xs = StandardScaler().fit_transform(X)
            best = None
            for k in k_range:
                if k >= len(X):
                    continue
                km = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Xs)
                sil = silhouette_score(Xs, km.labels_)
                if best is None or sil > best['sil']:
                    best = {'k': k, 'sil': sil, 'km': km, 'Xs': Xs, 'labels': km.labels_}
            db = davies_bouldin_score(best['Xs'], best['labels'])
            ch = calinski_harabasz_score(best['Xs'], best['labels'])
            out = sub[ID_COLS].copy(); out['cluster'] = best['labels']
            return {
                'best_k': best['k'], 'silhouette': best['sil'],
                'davies_bouldin': db, 'calinski_harabasz': ch,
                'labels': out, 'Xs': best['Xs'],
            }

        results = {}
        for tenant_id, sub in df.groupby('tenant_id'):
            r = fit_one_tenant(sub)
            if r is None:
                print(f'tenant {tenant_id}: only {len(sub)} rows -- skipped'); continue
            results[tenant_id] = r

        metrics_df = pd.DataFrame({
            t: {'n': len(r['labels']), 'k': r['best_k'],
                'silhouette': round(r['silhouette'], 3),
                'davies_bouldin': round(r['davies_bouldin'], 3),
                'calinski_harabasz': round(r['calinski_harabasz'], 1)}
            for t, r in results.items()
        }).T
        metrics_df.index.name = 'tenant_id'
        metrics_df
    """).strip()),
    md("""## 3. Cluster size balance

A cluster that holds <5% of the tenant cohort is usually noise; a cluster holding >70% means the model is finding one large 'normal' blob and a few outliers. We flag both extremes."""),
    code(textwrap.dedent("""
        balance_rows = []
        for tenant_id, r in results.items():
            sizes = r['labels'].cluster.value_counts(normalize=True).sort_index()
            for cl, frac in sizes.items():
                balance_rows.append({'tenant_id': tenant_id, 'cluster': int(cl),
                                     'share': round(float(frac), 3),
                                     'flag': ('tiny <5%' if frac < 0.05 else
                                              'dominant >70%' if frac > 0.70 else 'ok')})
        balance = pd.DataFrame(balance_rows)
        balance
    """).strip()),
    md("""## 4. Auto-named personas

For each cluster we compute the per-feature **z-score relative to the tenant mean** and pick the top-2 features by absolute z-score to label the cluster. This is a heuristic, not a substitute for human review, but it makes the table directly readable for non-technical reviewers."""),
    code(textwrap.dedent("""
        FEATURE_LABELS = {
            'overspeed_per_100km': 'overspeeds',
            'avg_speed_over_limit': 'speed-over-limit',
            'high_speed_trip_ratio': 'highway-trips',
            'speed_alert_per_100km': 'speed-alerts',
            'harsh_brake_per_100km': 'harsh-brakes',
            'harsh_accel_per_100km': 'harsh-accels',
            'harsh_corner_per_100km': 'harsh-corners',
            'monthly_idle_ratio': 'idle-time',
            'high_rpm_minutes_per_day': 'high-RPM',
            'night_trip_ratio': 'night-driving',
            'rush_hour_trip_ratio': 'rush-hour',
            'stddev_trip_distance': 'trip-variance',
            'short_trip_ratio': 'short-trips',
        }

        def name_cluster(z: pd.Series) -> str:
            top = z.abs().sort_values(ascending=False).head(2)
            parts = []
            for feat in top.index:
                arrow = 'high' if z[feat] > 0 else 'low'
                parts.append(f'{arrow} {FEATURE_LABELS.get(feat, feat)}')
            return ' + '.join(parts)

        persona_rows = []
        for tenant_id, r in results.items():
            full = r['labels'].merge(df[ID_COLS + FEATURES + ['total_distance_km']],
                                     on=ID_COLS, how='left')
            tenant_mean = full[FEATURES].mean()
            tenant_std = full[FEATURES].std().replace(0, 1)
            for cl, sub in full.groupby('cluster'):
                z = (sub[FEATURES].mean() - tenant_mean) / tenant_std
                persona_rows.append({
                    'tenant_id': tenant_id, 'cluster': int(cl), 'n': len(sub),
                    'share': round(len(sub) / len(full), 3),
                    'avg_distance_km': round(sub.total_distance_km.mean(), 1),
                    'persona': name_cluster(z),
                })
        personas = pd.DataFrame(persona_rows).sort_values(['tenant_id', 'cluster'])
        personas
    """).strip()),
    md("""## 5. Per-tenant feature z-heatmap

Each row is a cluster, each column is a feature. Values are z-scores vs the tenant mean. Bright red = much higher than tenant average; deep blue = much lower. Use this to sanity-check the auto-generated personas."""),
    code(textwrap.dedent("""
        n = len(results)
        fig, axes = plt.subplots(n, 1, figsize=(11, 1.6 * sum(r['best_k'] for r in results.values())), squeeze=False)
        for ax, (tenant_id, r) in zip(axes[:, 0], results.items()):
            full = r['labels'].merge(df[ID_COLS + FEATURES], on=ID_COLS, how='left')
            mean = full[FEATURES].mean(); std = full[FEATURES].std().replace(0, 1)
            z = full.groupby('cluster')[FEATURES].mean().sub(mean).div(std)
            im = ax.imshow(z.values, cmap='RdBu_r', vmin=-2, vmax=2, aspect='auto')
            ax.set_xticks(range(len(FEATURES))); ax.set_xticklabels(FEATURES, rotation=45, ha='right', fontsize=8)
            ax.set_yticks(range(len(z))); ax.set_yticklabels([f'cl {c}' for c in z.index])
            ax.set_title(f'tenant {tenant_id}  k={r["best_k"]}  sil={r["silhouette"]:.2f}', fontsize=10, loc='left')
            plt.colorbar(im, ax=ax, fraction=0.02, pad=0.01)
        plt.tight_layout(); plt.show()
    """).strip()),
    md("""## 6. Verdict

The exit gate from `04_modeling/01` was *every tenant must reach `best_k >= 3` and `silhouette >= 0.15`*. The technical numbers tell us whether the clusters are real; the persona table tells us whether they are useful. Both must hold."""),
    code(textwrap.dedent("""
        passed_tech = all(r['silhouette'] >= 0.15 and r['best_k'] >= 3 for r in results.values())
        big_dom = balance.query('flag == "dominant >70%"')
        passed_balance = big_dom.empty
        unique_personas_per_tenant = personas.groupby('tenant_id').persona.nunique()
        passed_personas = (unique_personas_per_tenant >= 3).all()

        verdict = {
            'silhouette_gate (>=0.15 & k>=3)': passed_tech,
            'no_dominant_cluster (>70%)': passed_balance,
            'distinct_personas (>=3 per tenant)': bool(passed_personas),
        }
        verdict_df = pd.DataFrame.from_dict(verdict, orient='index', columns=['pass'])
        verdict_df
    """).strip()),
    md("""**Go / No-go.** If every row of the verdict table is `True`, clustering is releasable for the executive dashboard. If the silhouette gate fails for a tenant we ship the model only for the tenants that pass and document the gap. If the persona gate fails, we are producing labels we cannot explain — do not ship.

**Next action.** Pass labels to `02_risk_score_validation.ipynb` to check whether the *high-risk* clusters and the Isolation-Forest *high* band agree on which device-months are problematic."""),
])

# ===========================================================================
# Notebook 02 — Risk score validation against ground-truth signals
# ===========================================================================
nb02 = nb([
    md("""# M10.2 · Risk Score Validation vs Ground-Truth Signals (CRISP-DM Phase 5)

**What this notebook answers**

1. Do device-months in the **high** Isolation-Forest band experience more **maintenance events** in the same or following month than device-months in the **low** band?
2. What is the **lift** of the top-decile risk score over the cohort base rate?
3. Do clustering and anomaly scoring **agree** on which device-months are problematic?

**Caveats baked into the analysis (not hidden):**

- `staging.sinistre` is empty (0 rows). No accident ground truth.
- `staging.offense` has 4 rows globally. Statistically useless.
- `warehouse.fact_maintenance` overlaps the modeling frame only when tenant `7486`
  has been reconstructed into the trip-side mart. This is **tenant-limited
  directional evidence**, not a tight all-tenant statistical claim.

That is why we report **lift** and **per-band rates**, not p-values. We also avoid claiming precision @ k for k smaller than 30."""),
    md("## 1. Bootstrap (re-pull features, re-fit Isolation Forest)"),
    code(BOOT),
    code(textwrap.dedent("""
        def fit_iso_one_tenant(sub: pd.DataFrame):
            X = sub[FEATURES].fillna(0).to_numpy()
            if len(X) < 50:
                return None
            Xs = StandardScaler().fit_transform(X)
            iso = IsolationForest(n_estimators=200, contamination='auto',
                                  random_state=42, n_jobs=-1).fit(Xs)
            raw = -iso.decision_function(Xs)
            score = (raw - raw.min()) / (raw.max() - raw.min() + 1e-9)
            out = sub[ID_COLS].copy()
            out['risk_score'] = score
            out['risk_band'] = pd.cut(score, bins=[-0.01, 0.4, 0.7, 1.01],
                                       labels=['low', 'medium', 'high'])
            return out

        scores = pd.concat([r for r in (fit_iso_one_tenant(s) for _, s in df.groupby('tenant_id')) if r is not None],
                            ignore_index=True)
        scores.risk_band.value_counts()
    """).strip()),
    md("""## 2. Pull maintenance ground truth

Chain: `fact_maintenance.vehicle_id` → `dim_vehicle.vehicule_id` → `dim_vehicle.vehicle_sk` → `dim_device.device_id`.

We bucket maintenance by `maintenance_date`'s YYYY-MM, then join twice:
- **Concurrent month** — risk in month *T* matches maintenance in *T*. (Risk should rise the month maintenance happens.)
- **Predictive month** — risk in month *T* matches maintenance in *T+1*. (Does today's high-risk score precede tomorrow's maintenance?)"""),
    code(textwrap.dedent("""
        with get_engine().connect() as conn:
            maint = pd.read_sql(text('''
                SELECT dd.device_id, fm.tenant_id,
                       TO_CHAR(fm.maintenance_date, 'YYYY-MM') AS year_month,
                       COUNT(*) AS maint_events, SUM(fm.total_cost) AS maint_cost
                FROM warehouse.fact_maintenance fm
                JOIN warehouse.dim_vehicle dv
                  ON fm.vehicle_id = dv.vehicule_id AND fm.tenant_id = dv.tenant_id
                JOIN warehouse.dim_device dd ON dv.vehicle_sk = dd.vehicle_sk
                WHERE fm.maintenance_date >= '2025-01-01'
                GROUP BY 1, 2, 3
            '''), conn)
        print('maintenance device-month rows:', len(maint), '|', 'sum events:', int(maint.maint_events.sum()))
        maint.head()
    """).strip()),
    md("""### 2a. Tenant-overlap diagnostic — read this before interpreting any join

Maintenance ground truth is only meaningful if the **same tenants** appear in both `marts.v_ml_features_full` and `warehouse.fact_maintenance`. We check that explicitly. If the overlap is empty, the maintenance-based gates below will all read as zero — that is a *data coverage* result, not a model failure."""),
    code(textwrap.dedent("""
        feat_tenants = set(scores.tenant_id.unique())
        maint_tenants = set(maint.tenant_id.unique())
        overlap = feat_tenants & maint_tenants
        print('tenants with features:', sorted(feat_tenants))
        print('tenants with maintenance:', sorted(maint_tenants))
        print('overlap:', sorted(overlap))
        MAINT_USABLE = bool(overlap)
        if not MAINT_USABLE:
            print('\\n[WARN] maintenance data and feature data are in disjoint tenants.')
            print('       Concurrent / predictive / lift gates will all be 0.')
            print('       Verdict will fall back to internal-consistency checks.')
    """).strip()),
    md("## 3. Concurrent-month band rates"),
    code(textwrap.dedent("""
        joined = scores.merge(maint, on=ID_COLS, how='left')
        joined['has_maint'] = joined.maint_events.fillna(0) > 0

        per_band = (joined.groupby(['tenant_id', 'risk_band'], observed=True)
                          .agg(n=('device_id', 'size'),
                               n_with_maint=('has_maint', 'sum'),
                               maint_rate=('has_maint', 'mean'))
                          .round(4))
        per_band
    """).strip()),
    code(textwrap.dedent("""
        # Aggregate across all tenants for the headline number
        global_band = (joined.groupby('risk_band', observed=True)
                              .agg(n=('device_id', 'size'),
                                   maint_rate=('has_maint', 'mean'))
                              .assign(lift=lambda x: x['maint_rate'] / x['maint_rate'].loc['low']
                                                       if 'low' in x.index else np.nan)
                              .round(3))
        global_band
    """).strip()),
    md("""## 4. Predictive: risk in month T → maintenance in month T+1"""),
    code(textwrap.dedent("""
        scores_for_join = scores.copy()
        scores_for_join['next_year_month'] = (
            pd.to_datetime(scores_for_join.year_month + '-01') +
            pd.offsets.MonthBegin(1)).dt.strftime('%Y-%m')

        next_maint = maint.rename(columns={'year_month': 'next_year_month'})
        pred = scores_for_join.merge(
            next_maint, on=['tenant_id', 'device_id', 'next_year_month'], how='left')
        pred['has_next_maint'] = pred.maint_events.fillna(0) > 0

        pred_band = (pred.groupby('risk_band', observed=True)
                          .agg(n=('device_id', 'size'),
                               next_month_maint_rate=('has_next_maint', 'mean'))
                          .round(4))
        pred_band
    """).strip()),
    md("""## 5. Top-decile lift

A common executive question: *"how much more likely is a maintenance event in the riskiest 10% of device-months vs the average?"* This is the operational metric we'd put on a dashboard."""),
    code(textwrap.dedent("""
        joined['decile'] = (joined.groupby('tenant_id', group_keys=False)
                                   .risk_score.transform(lambda s: pd.qcut(s, 10, labels=False, duplicates='drop')))
        base_rate = joined.has_maint.mean()
        top_rate = joined.loc[joined.decile == 9, 'has_maint'].mean()
        lift = top_rate / base_rate if base_rate > 0 else float('nan')
        print(f'base maintenance rate: {base_rate:.3%}')
        print(f'top-decile maintenance rate: {top_rate:.3%}')
        print(f'lift @ top decile: {lift:.2f}x  (n top decile = {(joined.decile==9).sum()})')
    """).strip()),
    md("""## 6. Cluster ↔ Risk-band agreement

If clustering and anomaly scoring identify *roughly the same* device-months as problematic, the two methods reinforce each other. If they disagree wildly, one of them is wrong (or they are picking up different definitions of risk)."""),
    code(textwrap.dedent("""
        def fit_km_one_tenant(sub):
            X = sub[FEATURES].fillna(0).to_numpy()
            if len(X) < 50: return None
            Xs = StandardScaler().fit_transform(X)
            best = None
            for k in range(3, 7):
                km = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Xs)
                sil = silhouette_score(Xs, km.labels_)
                if best is None or sil > best['sil']:
                    best = {'k': k, 'sil': sil, 'labels': km.labels_}
            out = sub[ID_COLS].copy(); out['cluster'] = best['labels']
            return out

        clusters = pd.concat([r for r in (fit_km_one_tenant(s) for _, s in df.groupby('tenant_id')) if r is not None],
                             ignore_index=True)
        agree = clusters.merge(scores, on=ID_COLS, how='inner')

        # For each tenant, find the 'risk-leaning' cluster (highest mean risk_score)
        # and check what fraction of its members fall in the high band.
        agg_rows = []
        for t, sub in agree.groupby('tenant_id'):
            cl_mean = sub.groupby('cluster').risk_score.mean()
            riskiest = cl_mean.idxmax()
            mask = sub.cluster == riskiest
            agg_rows.append({
                'tenant_id': t, 'riskiest_cluster': int(riskiest),
                'n_in_cluster': int(mask.sum()),
                'mean_risk_in_cluster': round(float(cl_mean.loc[riskiest]), 3),
                'frac_high_band': round(float((sub.loc[mask, 'risk_band'] == 'high').mean()), 3),
                'frac_high_band_overall': round(float((sub.risk_band == 'high').mean()), 3),
            })
        pd.DataFrame(agg_rows)
    """).strip()),
    md("""## 7. Internal-consistency fallback (used when external ground truth is unavailable)

If the maintenance overlap is empty, the only legitimate evidence we have is **internal consistency**: do the high-band rows actually look riskier on the *input features themselves*? We compute, per tenant, the ratio of mean overspeed/harsh/idle in the **high** band vs the **low** band. A working anomaly score should produce ratios > 1 on the unsafe-direction features, even without a label."""),
    code(textwrap.dedent("""
        unsafe_features = ['overspeed_per_100km', 'speed_alert_per_100km',
                           'harsh_brake_per_100km', 'harsh_accel_per_100km',
                           'harsh_corner_per_100km', 'high_rpm_minutes_per_day',
                           'monthly_idle_ratio']
        scored_with_features = scores.merge(df[ID_COLS + unsafe_features], on=ID_COLS, how='left')
        ic_rows = []
        for t, sub in scored_with_features.groupby('tenant_id'):
            high = sub[sub.risk_band == 'high'][unsafe_features].mean()
            low = sub[sub.risk_band == 'low'][unsafe_features].mean()
            ratios = (high / low.replace(0, np.nan)).round(2)
            ic_rows.append({'tenant_id': t,
                            'features_with_ratio_gt_1': int((ratios > 1).sum()),
                            'features_with_ratio_gt_2': int((ratios > 2).sum()),
                            'mean_ratio': round(float(ratios.mean(skipna=True)), 2)})
        internal_consistency = pd.DataFrame(ic_rows)
        internal_consistency
    """).strip()),
    md("""## 8. Verdict

We treat any of the following as *passing evidence* for the risk score:

- **External (preferred):** the **high** band has maintenance rate >= **2x** the **low** band (concurrent or predictive); top decile lift >= **1.5x**.
- **Internal (fallback when ground truth missing):** in every tenant, **>= 5 of 7** unsafe features show high-band mean > low-band mean, and **>= 2** show >= 2x ratio.
- Cluster ↔ risk-band agreement: the riskiest cluster shows `frac_high_band` >= 2x `frac_high_band_overall` in at least one tenant."""),
    code(textwrap.dedent("""
        # External gates (will be NaN/0 when MAINT_USABLE is False)
        try:
            ratio_concurrent = (global_band.loc['high', 'maint_rate']
                                / max(global_band.loc['low', 'maint_rate'], 1e-9))
        except KeyError:
            ratio_concurrent = float('nan')
        try:
            ratio_predictive = (pred_band.loc['high', 'next_month_maint_rate']
                                / max(pred_band.loc['low', 'next_month_maint_rate'], 1e-9))
        except KeyError:
            ratio_predictive = float('nan')

        # Internal-consistency gate
        ic_pass = ((internal_consistency.features_with_ratio_gt_1 >= 5).all()
                   and (internal_consistency.features_with_ratio_gt_2 >= 2).all())

        # Cluster <-> band agreement
        agg_df = pd.DataFrame(agg_rows)
        cluster_agreement = ((agg_df.frac_high_band >= 2 * agg_df.frac_high_band_overall).any())

        verdict = pd.DataFrame.from_dict({
            'external | concurrent high/low maint ratio (>=2)': [round(ratio_concurrent, 2), ratio_concurrent >= 2],
            'external | predictive high/low maint ratio (>=2)': [round(ratio_predictive, 2), ratio_predictive >= 2],
            'external | top-decile lift (>=1.5)': [round(lift, 2) if not np.isnan(lift) else float('nan'), (lift >= 1.5) if not np.isnan(lift) else False],
            'internal | unsafe features gate (>=5 ratios>1, >=2 ratios>2 per tenant)': [bool(ic_pass), bool(ic_pass)],
            'cluster<->band agreement (>=2x)': [bool(cluster_agreement), bool(cluster_agreement)],
        }, orient='index', columns=['value', 'pass'])
        verdict
    """).strip()),
    md("""**Go / No-go.**

- If any of the **external** rows pass → ship as a directional risk indicator without caveats.
- If only the **internal-consistency** + **cluster-agreement** rows pass (and external evidence is absent or too tenant-limited) → ship as a **provisional** indicator with an explicit dashboard footer:  *"validated against feature-space anomalies, not against incident outcomes — outcome backtesting remains tenant-limited in `staging.maintenance` and is pending a broader incident or claims feed."*
- If neither external nor internal gates pass → do not put `risk_score` in front of a customer; surface only cluster personas.

**Next action.** Run `03_stability_and_fairness.ipynb` to confirm the score is stable month-over-month and balanced across tenants before any production deployment."""),
])

# ===========================================================================
# Notebook 03 — Stability over time + cross-tenant fairness
# ===========================================================================
nb03 = nb([
    md("""# M10.3 · Stability over Time + Cross-tenant Fairness (CRISP-DM Phase 5)

**What this notebook answers**

1. **Stability.** If a device's behavior doesn't change much, does its **cluster** assignment stay the same month over month? Does its **risk band** stay stable? High churn = the model is noise-sensitive and not deployable as-is.
2. **Fairness across tenants.** Does the model produce sensible distributions for *every* tenant, or do small/atypical tenants get pathological outputs (everyone-is-high or everyone-is-low)?
3. **Coverage.** What fraction of device-months are actually scored? (Inactive months are filtered out — that's expected, but we report it.)
"""),
    md("## 1. Bootstrap"),
    code(BOOT),
    md("""## 2. Re-fit clustering & Isolation Forest per tenant"""),
    code(textwrap.dedent("""
        def fit_one(sub):
            X = sub[FEATURES].fillna(0).to_numpy()
            if len(X) < 50: return None
            Xs = StandardScaler().fit_transform(X)

            best = None
            for k in range(3, 7):
                km = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Xs)
                sil = silhouette_score(Xs, km.labels_)
                if best is None or sil > best['sil']:
                    best = {'k': k, 'sil': sil, 'labels': km.labels_}

            iso = IsolationForest(n_estimators=200, contamination='auto',
                                  random_state=42, n_jobs=-1).fit(Xs)
            raw = -iso.decision_function(Xs)
            score = (raw - raw.min()) / (raw.max() - raw.min() + 1e-9)

            out = sub[ID_COLS].copy()
            out['cluster'] = best['labels']; out['silhouette'] = best['sil']
            out['risk_score'] = score
            out['risk_band'] = pd.cut(score, bins=[-0.01, 0.4, 0.7, 1.01],
                                       labels=['low', 'medium', 'high'])
            return out

        labels = pd.concat([r for r in (fit_one(s) for _, s in df.groupby('tenant_id')) if r is not None],
                           ignore_index=True)
        len(labels)
    """).strip()),
    md("""## 3. Cluster-assignment churn between consecutive months

For each device, look at its labels in months `T` and `T+1` (when both exist). The **churn rate** is the share of (device, month-pair) records where the cluster id differs. Note: cluster ids are not stable across re-fits — but here we fit *once globally* and slice by month, so the labels are comparable inside a single tenant.

A churn rate of 30-50% is normal for behavior data; >70% suggests the model is unstable."""),
    code(textwrap.dedent("""
        labels_sorted = labels.sort_values(ID_COLS).copy()
        labels_sorted['ym_dt'] = pd.to_datetime(labels_sorted.year_month + '-01')

        # Build pairs of (T, T+1) per device
        labels_sorted['next_ym'] = (labels_sorted.ym_dt + pd.offsets.MonthBegin(1)).dt.strftime('%Y-%m')
        nxt = labels_sorted[['tenant_id', 'device_id', 'year_month', 'cluster', 'risk_band']].rename(
            columns={'year_month': 'next_ym', 'cluster': 'cluster_next', 'risk_band': 'risk_band_next'})
        pairs = labels_sorted.merge(nxt, on=['tenant_id', 'device_id', 'next_ym'], how='inner')
        pairs['cluster_changed'] = pairs.cluster != pairs.cluster_next
        pairs['band_changed'] = pairs.risk_band.astype(str) != pairs.risk_band_next.astype(str)

        churn = (pairs.groupby('tenant_id')
                       .agg(n_pairs=('device_id', 'size'),
                            cluster_churn=('cluster_changed', 'mean'),
                            band_churn=('band_changed', 'mean'))
                       .round(3))
        churn
    """).strip()),
    md("""## 4. Risk-band transition matrix

Of devices that were `low` in month T, what share are `low/medium/high` in month T+1? A diagonal-heavy matrix means the score is sticky (good); a uniform matrix means the score is essentially random month-to-month (bad)."""),
    code(textwrap.dedent("""
        trans = (pairs
                 .groupby(['risk_band', 'risk_band_next'], observed=True)
                 .size().unstack(fill_value=0))
        trans_norm = trans.div(trans.sum(axis=1), axis=0).round(3)
        print('counts:'); display(trans)
        print('row-normalised (= P[next | current]):'); display(trans_norm)
    """).strip()),
    md("""## 5. Fairness: per-tenant risk-band distribution

Every tenant should get a recognisable low/medium/high split, *not* "all-low" or "all-high". If one tenant is 95% high band, the score is meaningless for that tenant."""),
    code(textwrap.dedent("""
        share = (labels.groupby(['tenant_id', 'risk_band'], observed=True).size()
                       .groupby(level=0).apply(lambda s: s / s.sum())
                       .unstack(fill_value=0).round(3))
        share
    """).strip()),
    code(textwrap.dedent("""
        fig, axes = plt.subplots(1, len(share.index), figsize=(4*len(share.index), 3.5), squeeze=False)
        for ax, t in zip(axes[0], share.index):
            row = share.loc[t]
            ax.bar(row.index.astype(str), row.values,
                   color=['#4caf50', '#ff9800', '#e53935'])
            ax.set_title(f'tenant {t}'); ax.set_ylim(0, 1)
            for i, v in enumerate(row.values):
                ax.text(i, v + 0.02, f'{v:.0%}', ha='center', fontsize=9)
        plt.tight_layout(); plt.show()
    """).strip()),
    md("""## 6. Coverage report

How many (tenant, year_month) cells did we score, vs how many active device-months exist? The gap is purely the modeling-window filter (`year_month >= '2025-01' AND distance >= 100km AND ignition > 0`)."""),
    code(textwrap.dedent("""
        with get_engine().connect() as conn:
            total = pd.read_sql(text('''
                SELECT tenant_id, year_month, COUNT(*) AS device_months
                FROM marts.v_ml_features_full
                GROUP BY 1, 2
            '''), conn)
        scored = labels.groupby(['tenant_id', 'year_month']).size().rename('scored_device_months').reset_index()
        cov = total.merge(scored, on=['tenant_id', 'year_month'], how='left')
        cov['scored_device_months'] = cov.scored_device_months.fillna(0).astype(int)
        cov['coverage'] = (cov.scored_device_months / cov.device_months).round(3)
        cov.tail(20)
    """).strip()),
    code(textwrap.dedent("""
        cov_summary = (cov.groupby('tenant_id')
                          .agg(total_device_months=('device_months', 'sum'),
                               scored_device_months=('scored_device_months', 'sum'))
                          .assign(coverage=lambda x: (x.scored_device_months / x.total_device_months).round(3)))
        cov_summary
    """).strip()),
    md("""## 7. Verdict

Stability gates we hold the model to:

- **Cluster churn <= 60%** per tenant (a third of devices may legitimately move; more than that is noise).
- **Band churn <= 50%** per tenant.
- **Risk-band transition matrix** is diagonal-heavy: P[stay in same band] >= 0.5 for `low` and `high` rows.
- **No tenant** has >90% of its rows in a single band. (Isolation Forest with `contamination='auto'` is *designed* to put ~80-85% of rows in the low band — that is correct behavior, not a defect. We only flag truly pathological collapses.)"""),
    code(textwrap.dedent("""
        cluster_churn_ok = (churn.cluster_churn <= 0.60).all()
        band_churn_ok = (churn.band_churn <= 0.50).all()

        diag_ok = True
        for b in ['low', 'high']:
            if b in trans_norm.index and b in trans_norm.columns:
                if trans_norm.loc[b, b] < 0.5:
                    diag_ok = False

        no_dominant_band = (share.max(axis=1) <= 0.90).all()

        verdict = pd.DataFrame.from_dict({
            'cluster churn (<=60%)': cluster_churn_ok,
            'band churn (<=50%)': band_churn_ok,
            'transition diagonal (P[stay] >=0.5 for low & high)': diag_ok,
            'no pathological band collapse (>90%) in any tenant': bool(no_dominant_band),
        }, orient='index', columns=['pass'])
        verdict
    """).strip()),
    md("""**Go / No-go.** All four rows must pass for production deployment. Any failure → the model still has analytical value but should not be wired into automated alerts; surface it as a monthly report instead.

**Next action.** If verdict is green across all three evaluation notebooks, hand off to `06_deployment/` for the dashboard wiring + scheduling. If not, document the failing gate(s) in `05_evaluation/README.md` so the deployment team knows what *not* to publish."""),
])


def write(name: str, nb_dict: dict) -> None:
    path = OUT_DIR / name
    path.write_text(json.dumps(nb_dict, indent=1, ensure_ascii=False), encoding='utf-8')
    # Round-trip validate
    json.loads(path.read_text(encoding='utf-8'))
    print('wrote', path)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    write('01_cluster_quality.ipynb', nb01)
    write('02_risk_score_validation.ipynb', nb02)
    write('03_stability_and_fairness.ipynb', nb03)


if __name__ == '__main__':
    main()
