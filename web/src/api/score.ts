import { request } from "./client";
import type { ClusterScoreResponse, FeatureVector, RiskScoreResponse } from "./types";

export function scoreRisk(features: FeatureVector): Promise<RiskScoreResponse> {
  return request<RiskScoreResponse>("/score/risk", {
    method: "POST",
    body: features,
  });
}

export function scoreCluster(features: FeatureVector): Promise<ClusterScoreResponse> {
  return request<ClusterScoreResponse>("/score/cluster", {
    method: "POST",
    body: features,
  });
}
