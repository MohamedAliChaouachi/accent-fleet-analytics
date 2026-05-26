"""Reproduce the exact failing question from the user's screenshot."""

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
    out = call("POST", "/v1/auth/login", {"email": "admin@tenant_235.local", "password": pw})
    print("login:", out["status"])
    token = out["body"]["access_token"]

    for q in [
        "What's the distribution of risk categories across the fleet?",
        "How many devices are in each behaviour cluster?",
    ]:
        print(f"\n=== {q!r}")
        out = call("POST", "/v1/ai/query", {"question": q}, token=token)
        print("status:", out["status"])
        b = out["body"]
        if not isinstance(b, dict):
            print("raw:", b)
            continue
        print("chart_type:", b.get("chart_type"))
        print("columns:", b.get("columns"))
        print("row_count:", b.get("row_count"))
        print("rows:", json.dumps(b.get("rows"), indent=2))
        print("summary:", b.get("summary"))


if __name__ == "__main__":
    main()
