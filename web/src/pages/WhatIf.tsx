import { useState } from "react";
import { useForm, type SubmitHandler } from "react-hook-form";
import { useMutation } from "@tanstack/react-query";
import { scoreCluster, scoreRisk } from "@/api/score";
import { ApiError } from "@/api/client";
import type {
  ClusterScoreResponse,
  FeatureVector,
  RiskScoreResponse,
} from "@/api/types";
import { PageHeader } from "@/components/PageHeader";
import { Panel } from "@/components/Panel";
import { StateMessage } from "@/components/StateMessage";
import { BarChart } from "@/components/charts/BarChart";
import { RISK_COLORS } from "@/lib/colors";
import { fmtDec, fmtInt } from "@/lib/format";

interface FormValues {
  overspeed_per_100km: number;
  overspeed_count: number;
  overspeed_severity_high: number;
  overspeed_severity_extreme: number;
  high_speed_trip_ratio: number;
  speed_alert_per_100km: number;
  night_trip_ratio: number;
  avg_max_speed_kmh: number;
}

// Defaults mirror the Streamlit page so the two dashboards score identically
// for a "Score with no changes" baseline.
const DEFAULTS: FormValues = {
  overspeed_per_100km: 3.0,
  overspeed_count: 50,
  overspeed_severity_high: 10,
  overspeed_severity_extreme: 2,
  high_speed_trip_ratio: 0.1,
  speed_alert_per_100km: 5.0,
  night_trip_ratio: 0.15,
  avg_max_speed_kmh: 110,
};

interface SliderSpec {
  name: keyof FormValues;
  min: number;
  max: number;
  step: number;
}

const SLIDERS: ReadonlyArray<SliderSpec> = [
  { name: "overspeed_per_100km", min: 0, max: 20, step: 0.1 },
  { name: "overspeed_count", min: 0, max: 500, step: 1 },
  { name: "overspeed_severity_high", min: 0, max: 200, step: 1 },
  { name: "overspeed_severity_extreme", min: 0, max: 100, step: 1 },
  { name: "high_speed_trip_ratio", min: 0, max: 1, step: 0.01 },
  { name: "speed_alert_per_100km", min: 0, max: 40, step: 0.1 },
  { name: "night_trip_ratio", min: 0, max: 1, step: 0.01 },
  { name: "avg_max_speed_kmh", min: 0, max: 220, step: 1 },
];

interface ScoreResult {
  risk: RiskScoreResponse | null;
  cluster: ClusterScoreResponse | null;
  clusterError: string | null;
}

export function WhatIf() {
  const [result, setResult] = useState<ScoreResult | null>(null);

  const { register, handleSubmit, watch } = useForm<FormValues>({
    defaultValues: DEFAULTS,
  });
  const values = watch();

  const scoreMutation = useMutation({
    mutationFn: async (features: FeatureVector): Promise<ScoreResult> => {
      // Risk is required; cluster is best-effort because the API returns
      // 503 if no clustering model is registered yet.
      const risk = await scoreRisk(features);
      let cluster: ClusterScoreResponse | null = null;
      let clusterError: string | null = null;
      try {
        cluster = await scoreCluster(features);
      } catch (err) {
        if (err instanceof ApiError && err.status === 503) {
          clusterError =
            "Cluster model not yet available — the API returned 503. Run `python scripts/train_clustering.py` to register one.";
        } else {
          clusterError = `Cluster API call failed: ${(err as Error).message}`;
        }
      }
      return { risk, cluster, clusterError };
    },
    onSuccess: setResult,
  });

  const onSubmit: SubmitHandler<FormValues> = (form) => {
    setResult(null);
    scoreMutation.mutate(form as FeatureVector);
  };

  return (
    <section>
      <PageHeader
        title="What-if scoring"
        caption={
          <>
            Hits <code className="rounded bg-slate-200 px-1 py-0.5">POST /v1/score/risk</code>
            {" "}and <code className="rounded bg-slate-200 px-1 py-0.5">/v1/score/cluster</code>.
            Tweak the sliders and re-score.
          </>
        }
      />

      <form onSubmit={handleSubmit(onSubmit)} className="space-y-6">
        <Panel title="Feature inputs">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            {SLIDERS.map((s) => (
              <label key={s.name} className="block">
                <span className="mb-1 flex items-center justify-between text-xs font-medium text-slate-600">
                  <code>{s.name}</code>
                  <span className="tabular-nums text-slate-500">
                    {formatSliderValue(values[s.name], s.step)}
                  </span>
                </span>
                <input
                  type="range"
                  min={s.min}
                  max={s.max}
                  step={s.step}
                  {...register(s.name, { valueAsNumber: true })}
                  className="w-full accent-brand-accent"
                />
              </label>
            ))}
          </div>
          <div className="mt-4 flex items-center gap-3">
            <button
              type="submit"
              disabled={scoreMutation.isPending}
              className="rounded-md bg-brand px-4 py-2 text-sm font-medium text-white hover:bg-brand-accent disabled:cursor-not-allowed disabled:opacity-50"
            >
              {scoreMutation.isPending ? "Scoring…" : "Score"}
            </button>
            {scoreMutation.isError ? (
              <span className="text-sm text-red-700">
                Risk API call failed: {(scoreMutation.error as Error).message}
              </span>
            ) : null}
          </div>
        </Panel>
      </form>

      {result?.risk ? <ScoreOutput result={result} /> : null}

      {!result && !scoreMutation.isPending ? (
        <p className="mt-6 text-sm text-slate-500">Set values and click <strong>Score</strong> to call the API.</p>
      ) : null}
    </section>
  );
}

function ScoreOutput({ result }: { result: ScoreResult }) {
  const { risk, cluster, clusterError } = result;
  if (!risk) return null;

  const components = Object.entries(risk.components ?? {}).map(([factor, contribution]) => ({
    factor,
    contribution,
  }));

  return (
    <div className="mt-6 space-y-6">
      <Panel title="Risk score">
        <div className="flex items-baseline gap-4">
          <span className="text-4xl font-semibold tabular-nums text-slate-900">
            {fmtDec(risk.risk_score)}
          </span>
          <span
            className="rounded-md px-3 py-1 text-sm font-semibold uppercase text-white"
            style={{ backgroundColor: RISK_COLORS[risk.category] ?? "#94a3b8" }}
          >
            {risk.category}
          </span>
          <span className="text-xs text-slate-500">
            Scoring model version: <code>{risk.version}</code>
          </span>
        </div>
      </Panel>

      {components.length ? (
        <Panel title="Per-factor contribution">
          <BarChart
            data={components as unknown as Array<Record<string, unknown>>}
            xKey="factor"
            series={[{ dataKey: "contribution", label: "Contribution" }]}
            yFormatter={(v) => fmtDec(v)}
            height={280}
          />
        </Panel>
      ) : null}

      <Panel title="Cluster prediction">
        {clusterError ? (
          <StateMessage tone="warning">{clusterError}</StateMessage>
        ) : cluster ? (
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
            <div className="rounded-md border border-slate-200 bg-white px-4 py-3 shadow-sm">
              <p className="text-xs uppercase tracking-wider text-slate-500">Cluster</p>
              <p className="mt-1 text-2xl font-semibold text-slate-900">#{fmtInt(cluster.cluster_id)}</p>
            </div>
            <div className="rounded-md border border-slate-200 bg-white px-4 py-3 shadow-sm">
              <p className="text-xs uppercase tracking-wider text-slate-500">Distance to centroid</p>
              <p className="mt-1 text-2xl font-semibold text-slate-900">{fmtDec(cluster.distance)}</p>
            </div>
            <div className="rounded-md border border-slate-200 bg-white px-4 py-3 shadow-sm">
              <p className="text-xs uppercase tracking-wider text-slate-500">Model version</p>
              <p className="mt-1 truncate font-mono text-sm text-slate-900">
                {cluster.model_version || cluster.model_name}
              </p>
            </div>
          </div>
        ) : null}
      </Panel>

      <details className="rounded-lg border border-slate-200 bg-white p-4 text-sm shadow-sm">
        <summary className="cursor-pointer font-medium text-slate-700">Raw responses</summary>
        <pre className="mt-2 overflow-x-auto rounded bg-slate-900 p-3 text-xs text-slate-100">
{JSON.stringify({ risk, cluster, clusterError }, null, 2)}
        </pre>
      </details>
    </div>
  );
}

function formatSliderValue(v: number, step: number): string {
  if (Number.isNaN(v)) return "—";
  if (step >= 1) return fmtInt(v);
  return fmtDec(v);
}
