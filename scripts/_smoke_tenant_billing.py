"""Verify the tenant-billing endpoint now picks the latest *complete* month.

Issue this script reproduces:
  Seeded warehouse contains trip data only through 2026-04-10. The billing
  dashboard used to surface 2026-04 as the "current month", which made the
  MoM-growth chart show -50% to -90% drops across every tenant simply
  because April was a partial month vs full March.

Expected after the fix:
  latest_month / KPI / MoM bars use 2026-03 (the latest *complete* month).
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


def call(method: str, path: str, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = json.dumps(body or {}).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:8000{path}", data=data, headers=headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return {"status": resp.status, "body": json.loads(resp.read().decode() or "{}")}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "body": e.read().decode()}


def main() -> None:
    pw = os.environ.get("DASHBOARD_API_PASSWORD", "daldal")
    out = call("POST", "/v1/auth/login", {"email": "superadmin@accent.local", "password": pw})
    if out["status"] != 200:
        # Fallback to a tenant admin if superadmin isn't seeded locally.
        out = call("POST", "/v1/auth/login", {"email": "admin@tenant_235.local", "password": pw})
    print("login:", out["status"])
    token = out["body"]["access_token"]

    out = call(
        "GET",
        "/v1/dashboards/tenant-billing?start=2026-02-27&end=2026-05-28",
        token=token,
    )
    print("status:", out["status"])
    b = out["body"]
    if not isinstance(b, dict):
        print("raw:", b)
        return
    print("latest_month:", b.get("latest_month"))
    kpi = b.get("kpi") or {}
    print("kpi.year_month:", kpi.get("year_month"))
    print("kpi.total_revenue:", kpi.get("total_revenue"))
    print("kpi.total_devices:", kpi.get("total_devices"))
    monthly = b.get("monthly") or []
    print(f"monthly months returned ({len(monthly)}): {[m.get('year_month') for m in monthly]}")

    # Show the MoM growth bars the chart will render.
    print("\nMoM bars (top by revenue) for latest_month:")
    rows = b.get("rows") or []
    latest_rows = [r for r in rows if r.get("year_month") == b.get("latest_month")]
    latest_rows.sort(key=lambda r: r.get("estimated_revenue") or 0, reverse=True)
    print(f"{'tenant':>30} {'dev%':>7} {'trips%':>8} {'alerts%':>9}")
    for r in latest_rows[:5]:
        name = r.get("tenant_name") or f"#{r.get('tenant_id')}"
        print(
            f"{name:>30} "
            f"{(r.get('devices_mom_growth_pct') or 0):>7.1f} "
            f"{(r.get('trips_mom_growth_pct') or 0):>8.1f} "
            f"{(r.get('alerts_mom_growth_pct') or 0):>9.1f}"
        )


if __name__ == "__main__":
    main()
