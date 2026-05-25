import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { AuthProvider } from "@/auth/AuthContext";
import { LoginPage } from "@/auth/LoginPage";
import { RequireAuth } from "@/auth/RequireAuth";
import { FiltersProvider } from "@/filters/FiltersContext";
import { Layout } from "@/components/Layout";
import { ExecutiveOverview } from "@/pages/ExecutiveOverview";
import { Operations } from "@/pages/Operations";
import { Maintenance } from "@/pages/Maintenance";
import { RiskAndBehavior } from "@/pages/RiskAndBehavior";
import { FleetEfficiency } from "@/pages/FleetEfficiency";
import { SafetyScorecard } from "@/pages/SafetyScorecard";
import { PredictiveAlerts } from "@/pages/PredictiveAlerts";
import { TenantBilling } from "@/pages/TenantBilling";
import { WhatIf } from "@/pages/WhatIf";
import { AIChat } from "@/pages/AIChat";

// staleTime mirrors the Streamlit @st.cache_data ttl (5 min): the marts
// views are refreshed by the Prefect incremental flow on the same cadence
// so anything fresher wouldn't pay rent.
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000,
      refetchOnWindowFocus: false,
    },
  },
});

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AuthProvider>
          <FiltersProvider>
            <Routes>
              <Route path="/login" element={<LoginPage />} />
              <Route element={<RequireAuth />}>
                <Route element={<Layout />}>
                  <Route index element={<Navigate to="/executive" replace />} />
                  <Route path="/executive" element={<ExecutiveOverview />} />
                  <Route path="/operations" element={<Operations />} />
                  <Route path="/maintenance" element={<Maintenance />} />
                  <Route path="/risk" element={<RiskAndBehavior />} />
                  <Route path="/fleet-efficiency" element={<FleetEfficiency />} />
                  <Route path="/safety" element={<SafetyScorecard />} />
                  <Route path="/alerts" element={<PredictiveAlerts />} />
                  <Route path="/billing" element={<TenantBilling />} />
                  <Route path="/what-if" element={<WhatIf />} />
                  <Route path="/ai" element={<AIChat />} />
                </Route>
              </Route>
              <Route path="*" element={<Navigate to="/" replace />} />
            </Routes>
          </FiltersProvider>
        </AuthProvider>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
