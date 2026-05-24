#!/usr/bin/env python
"""
Benchmark the FastAPI scoring service.

Boots a fresh `uvicorn app.main:app` subprocess on a free port, waits for
/health to return 200, then hits each target endpoint N times with C
concurrent httpx clients and reports p50 / p95 / p99 / max latencies in
milliseconds.

Default targets:
  - POST /score/risk           — pure-Python, no DB hit (always runs)
  - GET  /devices/{id}/profile — DB-backed (auto-skipped when no DB or
                                  no row available in v_device_risk_profile)

Usage:
    python scripts/bench_api.py                  # both endpoints, 500 req, 16 concurrent
    python scripts/bench_api.py --requests 2000  # heavier sample
    python scripts/bench_api.py --no-db          # skip /devices/{id}/profile
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path

import httpx

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Synthetic payload that exercises the per-tenant RiskPredictor.
# Mid-range numbers so the StandardScaler doesn't saturate at the tails —
# closer to what real traffic looks like than an all-zero feature vector.
# tenant_id is REQUIRED on /score/risk — defaults to 235, the first tenant
# in the modeling cohort (see config/pipeline.yaml::modeling).
RISK_PAYLOAD = {
    "tenant_id": 235,
    "device_id": 1,
    "month": "2026-04",
    "overspeed_per_100km": 5.2,
    "avg_speed_over_limit": 8.4,
    "high_speed_trip_ratio": 0.18,
    "speed_alert_per_100km": 8.7,
    "harsh_brake_per_100km": 2.1,
    "harsh_accel_per_100km": 1.4,
    "harsh_corner_per_100km": 0.9,
    "monthly_idle_ratio": 0.22,
    "high_rpm_minutes_per_day": 7.3,
    "night_trip_ratio": 0.22,
    "rush_hour_trip_ratio": 0.35,
    "stddev_trip_distance": 12.4,
    "short_trip_ratio": 0.41,
}


def _free_port() -> int:
    """Ask the OS for an unused TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _wait_for_health(client: httpx.AsyncClient, url: str, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            r = await client.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        await asyncio.sleep(0.25)
    raise RuntimeError(f"server never became healthy at {url}: {last_exc}")


async def _bench_one(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    json: dict | None = None,
) -> float:
    """Return wall-clock latency in milliseconds for a single request."""
    t0 = time.perf_counter()
    if method == "GET":
        r = await client.get(url, timeout=10.0)
    else:
        r = await client.post(url, json=json, timeout=10.0)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {url} returned {r.status_code}: {r.text[:200]}")
    return elapsed_ms


async def _bench_endpoint(
    client: httpx.AsyncClient,
    label: str,
    method: str,
    url: str,
    *,
    json: dict | None,
    requests: int,
    concurrency: int,
) -> dict[str, float]:
    """Drive `requests` calls through a semaphore-capped concurrency pool."""
    sem = asyncio.Semaphore(concurrency)
    latencies: list[float] = []

    async def _one() -> None:
        async with sem:
            ms = await _bench_one(client, method, url, json=json)
            latencies.append(ms)

    # Warm-up: 1% of the run (min 5) — drops cold-start jitter from the sample.
    warm = max(5, requests // 100)
    await asyncio.gather(*[_one() for _ in range(warm)])
    latencies.clear()

    t0 = time.perf_counter()
    await asyncio.gather(*[_one() for _ in range(requests)])
    wall_s = time.perf_counter() - t0

    latencies.sort()
    n = len(latencies)
    return {
        "label": label,
        "n": n,
        "wall_s": wall_s,
        "rps": n / wall_s if wall_s > 0 else float("inf"),
        "mean_ms": statistics.fmean(latencies),
        "p50_ms": latencies[int(n * 0.50)],
        "p95_ms": latencies[int(n * 0.95)],
        "p99_ms": latencies[min(n - 1, int(n * 0.99))],
        "max_ms": latencies[-1],
    }


def _print_results(results: list[dict]) -> None:
    header = f"{'endpoint':<32} {'n':>6} {'rps':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'max':>8}"
    print()
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['label']:<32} "
            f"{r['n']:>6d} "
            f"{r['rps']:>8.1f} "
            f"{r['p50_ms']:>7.1f}ms "
            f"{r['p95_ms']:>7.1f}ms "
            f"{r['p99_ms']:>7.1f}ms "
            f"{r['max_ms']:>7.1f}ms"
        )
    print()


async def _resolve_device_id(base_url: str) -> int | None:
    """Pick a real device_id from the warehouse, or None if DB/data unavailable."""
    try:
        from sqlalchemy import text

        from accent_fleet.db.engine import get_engine

        with get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT device_id FROM marts.v_device_risk_profile LIMIT 1")
            ).first()
            return int(row[0]) if row else None
    except Exception as exc:  # noqa: BLE001
        print(f"  skipping /devices/{{id}}/profile: {type(exc).__name__}: {exc}", flush=True)
        return None


async def _run(args: argparse.Namespace) -> int:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"

    # Spawn uvicorn. Use the same Python so we get the local venv.
    proc = subprocess.Popen(  # noqa: S603
        [
            sys.executable, "-m", "uvicorn",
            "app.main:app",
            "--host", "127.0.0.1",
            "--port", str(port),
            "--log-level", "warning",
        ],
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    print(f"started uvicorn pid={proc.pid} on {base_url}", flush=True)

    results: list[dict] = []
    try:
        async with httpx.AsyncClient(base_url=base_url) as client:
            await _wait_for_health(client, "/health")
            print("server healthy — starting bench", flush=True)

            results.append(
                await _bench_endpoint(
                    client,
                    label="POST /score/risk",
                    method="POST",
                    url="/score/risk",
                    json=RISK_PAYLOAD,
                    requests=args.requests,
                    concurrency=args.concurrency,
                )
            )

            if not args.no_db:
                device_id = await _resolve_device_id(base_url)
                if device_id is not None:
                    results.append(
                        await _bench_endpoint(
                            client,
                            label=f"GET /devices/{device_id}/profile",
                            method="GET",
                            url=f"/devices/{device_id}/profile?months=6",
                            json=None,
                            requests=args.requests,
                            concurrency=args.concurrency,
                        )
                    )
    finally:
        proc.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            proc.wait(timeout=5.0)
        if proc.poll() is None:
            proc.kill()

    _print_results(results)

    # Targets from NEXT_STEPS.md §1.2.
    targets = {
        "POST /score/risk": 100.0,
        "GET /devices/{id}/profile": 300.0,
    }
    ok = True
    for r in results:
        key = "GET /devices/{id}/profile" if r["label"].startswith("GET /devices/") else r["label"]
        target = targets.get(key)
        if target is None:
            continue
        status = "OK" if r["p95_ms"] <= target else "OVER"
        print(f"  {key}: p95={r['p95_ms']:.1f}ms target<={target:.0f}ms [{status}]")
        if status == "OVER":
            ok = False

    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=500, help="requests per endpoint")
    parser.add_argument("--concurrency", type=int, default=16, help="parallel clients")
    parser.add_argument("--no-db", action="store_true", help="skip DB-backed endpoints")
    args = parser.parse_args()
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
