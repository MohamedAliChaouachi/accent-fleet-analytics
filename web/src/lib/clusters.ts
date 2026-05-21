// Cluster personas — human-readable descriptions for the numeric cluster_id
// the K-Means model emits. The model returns integers 0..k-1; this catalog
// translates them for the dashboard so fleet managers see a behaviour label
// instead of "cluster 2".
//
// IDs are assigned by k-means after training and are *not* semantically stable
// across re-trainings. When you re-train and the centroid order changes, edit
// this file to re-map ids → personas using the z-score heatmap from
// `notebooks/01_cluster_quality.ipynb`.

export interface ClusterPersona {
  id: number;
  name: string;
  short: string;
  description: string;
  recommendation: string;
  icon: string; // single emoji used as a glyph in chips/cards
  color: string; // hex for badges/charts (avoid the risk palette)
}

const PERSONA_CATALOG: Record<number, ClusterPersona> = {
  0: {
    id: 0,
    name: "Calm City Driver",
    short: "Calm city",
    description:
      "Low harsh-event rate, short urban trips, modest top speeds. Healthy baseline behaviour.",
    recommendation: "Reward / retain. Use as a benchmark for new drivers.",
    icon: "\u{1F33F}",
    color: "#16a085",
  },
  1: {
    id: 1,
    name: "Highway Cruiser",
    short: "Highway",
    description:
      "Long trips, high average speed, low harsh events. Vehicle wear concentrated on engine / tyres.",
    recommendation: "Tighten maintenance cadence (kms-based, not time-based).",
    icon: "\u{1F6E3}",
    color: "#2a9df4",
  },
  2: {
    id: 2,
    name: "Aggressive Urban Driver",
    short: "Aggressive urban",
    description:
      "High harsh-brake & harsh-accel rate, frequent overspeed alerts, short city trips.",
    recommendation: "Schedule driver coaching. Review last 30 days of alerts.",
    icon: "\u{26A1}",
    color: "#e67e22",
  },
  3: {
    id: 3,
    name: "Night Owl",
    short: "Night driver",
    description:
      "High night-trip ratio, elevated speeding alerts after-hours, irregular schedule.",
    recommendation: "Verify route authorisations and after-hours policy.",
    icon: "\u{1F319}",
    color: "#8e44ad",
  },
  4: {
    id: 4,
    name: "Idle-Heavy Operator",
    short: "Idle heavy",
    description:
      "Long idle periods, high engine RPM at rest, low km/day. Fuel-cost hotspot.",
    recommendation: "Audit job dispatch; consider auto-stop policy.",
    icon: "\u{1F6E2}",
    color: "#f39c12",
  },
  5: {
    id: 5,
    name: "Short-Hop Courier",
    short: "Short hops",
    description:
      "Many short trips, frequent stops, high stop-density per 100km. Typical last-mile pattern.",
    recommendation: "Optimise route planning; monitor cold-start wear.",
    icon: "\u{1F4E6}",
    color: "#9b59b6",
  },
  6: {
    id: 6,
    name: "Heavy Hauler",
    short: "Heavy hauler",
    description:
      "Long trips, high distance variance, moderate harsh events under load.",
    recommendation: "Match maintenance to load profile; check brakes early.",
    icon: "\u{1F69B}",
    color: "#34495e",
  },
};

const FALLBACK_COLORS = [
  "#2a9df4",
  "#16a085",
  "#9b59b6",
  "#f39c12",
  "#8e44ad",
  "#34495e",
  "#e67e22",
  "#7f8c8d",
];

export function clusterPersona(id: number | null | undefined): ClusterPersona {
  if (id === null || id === undefined) {
    return {
      id: -1,
      name: "Unclustered",
      short: "Unclustered",
      description: "No cluster assignment yet for this device-month.",
      recommendation:
        "Run the clustering job (scripts/train_clustering.py) or wait for the next incremental flow.",
      icon: "\u{2753}",
      color: "#94a3b8",
    };
  }
  return (
    PERSONA_CATALOG[id] ?? {
      id,
      name: `Cluster ${id}`,
      short: `Cluster ${id}`,
      description:
        "Behaviour group discovered by the model. Edit web/src/lib/clusters.ts to label it.",
      recommendation: "Review centroid features in the notebook to assign a label.",
      icon: "\u{1F4CD}",
      color: FALLBACK_COLORS[id % FALLBACK_COLORS.length] ?? "#64748b",
    }
  );
}
