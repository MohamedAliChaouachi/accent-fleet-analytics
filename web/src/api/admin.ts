// Thin wrappers around /v1/admin/* — superadmin-only endpoints.
//
// Today this exposes only the tenants list used by the AI Chat tenant
// picker. As we add user-management UIs (create user / reset password
// / disable tenant), the corresponding client functions land here so
// the AuthContext and other places never call /v1/admin/* directly.

import { request } from "./client";
import type { TenantSummary } from "./types";

export function listTenants(): Promise<TenantSummary[]> {
  return request<TenantSummary[]>("/admin/tenants");
}
